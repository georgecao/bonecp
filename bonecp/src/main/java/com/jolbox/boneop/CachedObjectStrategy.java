/**
 * Copyright 2010 Wallace Wadge
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.jolbox.boneop;

import java.lang.ref.Reference;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.FinalizableWeakReference;

/**
 * A connection strategy that is optimized to store/retrieve the connection inside a thread local variable. This makes
 * getting a connection in a managed thread environment such as Tomcat very fast but has the limitation that it only
 * works if # of threads <= # of connections. Should it detect that this isn't the case anymore, this class will flip
 * back permanently to the configured fallback strategy (i.e. default strategy) and makes sure that currently assigned
 * and unused connections are taken back.
 *
 * @author wallacew
 * @param <T> object type.
 *
 */
public class CachedObjectStrategy<T> extends AbstractObjectStrategy<T> {

    /**
     * Logger class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(CachedObjectStrategy.class);

    /**
     * Just to give out a warning once.
     */
    private volatile AtomicBoolean warnApp = new AtomicBoolean();
    /**
     * Keep track of connections tied to thread.
     */
    final Map<ObjectHandle<T>, Reference<Thread>> finalizableRefs = new ConcurrentHashMap<>();
    /**
     * Keep track of connections tied to thread.
     */
    private FinalizableReferenceQueue finalizableRefQueue = new FinalizableReferenceQueue();
    /**
     * Obtain connections using this fallback strategy at first (or if this strategy cannot succeed.
     */
    private ObjectStrategy<T> fallbackStrategy;

    /**
     * Singleton.
     */
    private CachedObjectStrategy() { /* singleton */    }

    /**
     * Singleton pattern.
     */
    private static class SingletonHolder {

        /**
         * Singleton pattern.
         */
        @SuppressWarnings("synthetic-access")
        public static final CachedObjectStrategy INSTANCE = new CachedObjectStrategy();
    }

    /**
     * Singleton pattern.
     *
     * @param <T> object type.
     * @param pool object pool
     * @param fallbackStrategy real object creator
     * @return CachedConnectionStrategy singleton instance
     */
    public static <T> ObjectStrategy<T> getInstance(BoneOP<T> pool, ObjectStrategy<T> fallbackStrategy) {
        CachedObjectStrategy<T> cs = (CachedObjectStrategy<T>) SingletonHolder.INSTANCE;
        cs.pool = pool;
        cs.fallbackStrategy = fallbackStrategy;
        return cs;
    }

    /**
     * Connections are stored here. No need for static here, this class is a singleton.
     */
    private final ThreadLocal<ObjectHandle<T>> objects = new ThreadLocal<ObjectHandle<T>>() {

        @Override
        protected ObjectHandle<T> initialValue() {
            ObjectHandle<T> result;
            try {
                // grab a connection from any other configured fallback strategy
                result = pollFallbackObject();
            } catch (PoolException e) {
                LOG.warn("Initial thread local object handle", e);
                result = null;
            }
            return result;
        }

        @Override
        public ObjectHandle<T> get() {
            ObjectHandle<T> result = super.get();
            // have we got one that's cached and unused? Mark it as in use. 
            if (result != null && !result.inUseInThreadLocalContext.compareAndSet(false, true)) {
                try {
                    // ... otherwise grab a new connection 
                    result = pollFallbackObject();
                } catch (PoolException e) {
                    LOG.warn("Get thread local object handle", e);
                    result = null;
                }
            }
            return result;
        }
    };

    /**
     * Try to obtain a connection from the fallback strategy.
     *
     * @return handle
     * @throws SQLException
     */
    ObjectHandle<T> pollFallbackObject() throws PoolException {
        ObjectHandle<T> result = this.fallbackStrategy.pollObject();
        // if we were successfull remember this connection to be able to shutdown cleanly.
        if (result != null) {
            threadWatch(result);
        }
        return result;
    }

    /**
     * Tries to close off all the unused assigned connections back to the pool. Assumes that the strategy mode has
     * already been flipped prior to calling this routine. Called whenever our no of connection requests > no of
     * threads.
     */
    private synchronized void stealExistingAllocations() {

        for (ObjectHandle<T> handle : this.finalizableRefs.keySet()) {
            // if they're not in use, pretend they are in use now and close them off.
            // this method assumes that the strategy has been flipped back to non-caching mode
            // prior to this method invocation.
            if (handle.inUseInThreadLocalContext.compareAndSet(false, true)) {
                try {
                    this.pool.releaseObject(handle);
                } catch (PoolException e) {
                    LOG.error("Cannot release object handle {}", handle, e);
                }
            }
        }
        if (this.warnApp.compareAndSet(false, true)) { // only issue warning once.
            LOG.warn("Cached strategy chosen, but more threads are requesting a connection than are configured. Switching permanently to default strategy.");
        }
        this.finalizableRefs.clear();
    }

    /**
     * Keep track of this handle tied to which thread so that if the thread is terminated we can reclaim our connection
     * handle.
     *
     * @param handle connection handle to track.
     */
    private void threadWatch(final ObjectHandle<T> handle) {
        this.finalizableRefs.put(handle, new FinalizableWeakReference<Thread>(Thread.currentThread(), this.finalizableRefQueue) {
            @Override
            public void finalizeReferent() {
                if (!CachedObjectStrategy.this.pool.poolShuttingDown) {
                    LOG.debug("Monitored thread is dead, closing off allocated connection.");
                }
                try {
                    handle.close();
                } catch (PoolException ex) {
                    LOG.error("Failed to close handle {}", handle, ex);
                }
                CachedObjectStrategy.this.finalizableRefs.remove(handle);
            }
        });
    }

    @Override
    protected ObjectHandle<T> getObjectInternal() throws PoolException {
        // try to get the connection from thread local storage.
        ObjectHandle<T> result = this.objects.get();
        // we should always be successfull. If not, it means we have more threads asking
        // us for a connection than we've got available. This is not supported so we flip
        // back our strategy.
        if (result == null) {
            this.pool.cachedPoolStrategy = false;
            this.pool.connectionStrategy = this.fallbackStrategy;
            stealExistingAllocations();
            // get a connection as if under our fallback strategy now.
            result = this.pool.connectionStrategy.getObject();
        }
        return result;
    }

    @Override
    public void destroyAllObjects() {
        for (ObjectHandle<T> handle : this.finalizableRefs.keySet()) {
            this.pool.destroyObject(handle);
        }
        this.finalizableRefs.clear();
        this.fallbackStrategy.destroyAllObjects();
    }

}
