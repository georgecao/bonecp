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

import com.google.common.base.FinalizableWeakReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.ref.Reference;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Object Partition structure.
 *
 * @param <T>
 * @author wwadge
 */
public class ObjectPartition<T> implements Serializable {

    /**
     * Logger class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ObjectPartition.class);
    /**
     * Serialization UID
     */
    private static final long serialVersionUID = -7864443421028454573L;
    /**
     * Objects available to be taken
     */
    private TransferQueue<ObjectHandle<T>> freeObjects;
    /**
     * When objects start running out, add these number of new objects.
     */
    private final int acquireIncrement;
    /**
     * Minimum number of objects to start off with.
     */
    private final int minObjects;
    /**
     * Maximum number of objects that will ever be created.
     */
    private final int maxObjects;
    /**
     * Number of objects that have been created.
     */
    private AtomicInteger createdObjects = new AtomicInteger(0);
    /**
     * If set to true, don't bother calling method to attempt to create more objects because we've hit our limit.
     */
    private volatile boolean unableToCreateMoreObjects = false;
    /**
     * Scratch queue of objects awaiting to be placed back in queue.
     */
    private final TransferQueue<ObjectHandle<T>> objectsPendingRelease;
    /**
     * Reference of objects that are to be watched.
     * Each partition has it's own object reference map.
     * TODO finish this feature later.
     */
    private final Map<T, Reference<ObjectHandle<T>>> finalizableRefs = new ConcurrentHashMap<>();
    /**
     * Config setting.
     */
    private final boolean disableTracking;
    /**
     * Signal trigger to pool watch thread. Making it a queue means our signal is persistent.
     */
    private final BlockingQueue<Object> poolWatchThreadSignalQueue = new ArrayBlockingQueue<>(1);
    /**
     * Store the unit translation here to avoid recalculating it in statement handles.
     */
    private final long objectOccupyTimeLimitInNanos;
    /**
     * Cached copy of the config-specified pool name.
     */
    private final String poolName;
    /**
     * Handle to the pool.
     */
    protected BoneOP<T> pool;

    /**
     * Returns a handle to the poolWatchThreadSignalQueue
     *
     * @return the poolWatchThreadSignal
     */
    protected BlockingQueue<Object> getPoolWatchThreadSignalQueue() {
        return this.poolWatchThreadSignalQueue;
    }

    /**
     * Updates leased connections statistics.
     *
     * @param increment value to add/subtract
     * @return value after this incrementation.
     */
    protected int updateCreatedObjects(int increment) {
        return this.createdObjects.addAndGet(increment);
    }

    /**
     * Adds a free object.
     *
     * @param handle object handle.
     * @throws PoolException on error
     */
    protected void addFreeObject(ObjectHandle<T> handle) throws PoolException {
        handle.setOriginatingPartition(this);
        // assume success to avoid racing where we insert an item in a queue and having that item immediately
        // taken and closed off thus decrementing the created connection count.
        updateCreatedObjects(1);
        if (!this.disableTracking) {
            trackObjectFinalizer(handle);
        }

        // the instant the following line is executed, consumers can start making use of this
        // connection.
        if (!this.freeObjects.offer(handle)) {
            // we failed. rollback.
            updateCreatedObjects(-1); // compensate our createdObject count.

            if (!this.disableTracking) {
                this.pool.getFinalizableRefs().remove(handle.getInternalObject());
            }
            // terminate the internal handle.
            handle.internalClose();
        }
    }

    /**
     * This method is a replacement for finalize() but avoids all its pitfalls (see Joshua Bloch et. all).
     * Keeps a handle on the connection. If the application called closed, then it means that the handle gets pushed
     * back to the connection pool and thus we get a strong reference again. If the application forgot to call close()
     * and subsequently lost the strong reference to it, the handle becomes eligible to garbage connection and thus the
     * the finalizeReferent method kicks in to safely close off the database handle. Note that we do not return the
     * connectionHandle back to the pool since that is not possible (for otherwise the GC would not have kicked in), but
     * we merely safely release the database internal handle and update our counters instead.
     *
     * @param handle handle to watch
     */
    protected void trackObjectFinalizer(ObjectHandle<T> handle) {
        if (!this.disableTracking) {
            //	assert !connectionHandle.getPool().getFinalizableRefs().containsKey(connectionHandle) : "Already tracking this handle";
            final T obj = handle.getInternalObject();
            final BoneOP<T> associatedPool = handle.getPool();
            handle.getPool().getFinalizableRefs().put(obj, new FinalizableWeakReference<ObjectHandle<T>>(handle, handle.getPool().getFinalizableRefQueue()) {
                @Override
                public void finalizeReferent() {
                    try {
                        associatedPool.getFinalizableRefs().remove(obj);
                        if (obj != null && associatedPool.factory.validateObject(obj)) { // safety!

                            LOG.warn("BoneCP detected an unclosed connection " + ObjectPartition.this.poolName + "and will now attempt to close it for you. "
                                    + "You should be closing this connection in your application - enable connectionWatch for additional debugging assistance.");
                            associatedPool.factory.destroyObject(obj);
                            updateCreatedObjects(-1);
                        }
                    } catch (Exception t) {
                        LOG.error("Error while closing off internal db connection", t);
                    }
                }
            });
        }
    }

    /**
     * @return the freeConnections
     */
    protected TransferQueue<ObjectHandle<T>> getFreeObjects() {
        return this.freeObjects;
    }

    /**
     * @param freeObjects the freeObjects to set
     */
    protected void setFreeObjects(TransferQueue<ObjectHandle<T>> freeObjects) {
        this.freeObjects = freeObjects;
    }

    /**
     * Partition constructor
     *
     * @param pool handle to object pool
     */
    public ObjectPartition(BoneOP<T> pool) {
        BoneOPConfig config = pool.getConfig();
        this.minObjects = config.getMinObjectsPerPartition();
        this.maxObjects = config.getMaxObjectsPerPartition();
        this.acquireIncrement = config.getAcquireIncrement();
        this.poolName = config.getPoolName() != null ? "(in pool '" + config.getPoolName() + "') " : "";
        this.pool = pool;

        this.objectsPendingRelease = new LinkedTransferQueue<>();
        this.disableTracking = config.isDisableObjectTracking();
        this.objectOccupyTimeLimitInNanos = TimeUnit.MILLISECONDS.toNanos(config.getObjectOccupyTimeLimitInMillis());
        /**
         * Create a number of helper threads for connection release.
         */
        int helperThreads = config.getReleaseHelperThreads();
        for (int i = 0; i < helperThreads; i++) {
            // go through pool.getReleaseHelper() rather than releaseHelper directly to aid unit testing (i.e. mocking)
            pool.getReleaseHelper().execute(new ObjectReleaseHelperThread<>(this.objectsPendingRelease, pool));
        }
    }

    /**
     * @return the acquireIncrement
     */
    protected int getAcquireIncrement() {
        return this.acquireIncrement;
    }

    /**
     * @return the minConnections
     */
    protected int getMinObjects() {
        return this.minObjects;
    }

    /**
     * @return the maxConnections
     */
    protected int getMaxObjects() {
        return this.maxObjects;
    }

    /**
     * @return the leasedConnections
     */
    protected int getCreatedObjects() {
        return this.createdObjects.get();
    }

    /**
     * Returns true if we have created all the connections we can
     *
     * @return true if we have created all the connections we can
     */
    protected boolean isUnableToCreateMoreObjects() {
        return this.unableToCreateMoreObjects;
    }

    /**
     * Sets connection creation possible status
     *
     * @param unableToCreateMoreObjects t/f
     */
    protected void setUnableToCreateMoreObjects(boolean unableToCreateMoreObjects) {
        this.unableToCreateMoreObjects = unableToCreateMoreObjects;
    }

    /**
     * Disable create more objects.
     */
    protected void disableCreateMoreObjects() {
        setUnableToCreateMoreObjects(true);
    }

    /**
     * Allow to create more objects.
     */
    protected void enableCreateMoreObjects() {
        setUnableToCreateMoreObjects(false);
    }

    /**
     * Gets handle to a release connection handle queue.
     *
     * @return release object handle queue
     */
    protected TransferQueue<ObjectHandle<T>> getObjectsPendingRelease() {
        return this.objectsPendingRelease;
    }

    /**
     * Returns the number of avail connections
     *
     * @return avail objects.
     */
    protected int getAvailableObjects() {
        return this.freeObjects.size();
    }

    /**
     * Returns no of free slots.
     *
     * @return remaining capacity.
     */
    public int getRemainingCapacity() {
        return this.freeObjects.remainingCapacity();
    }

    /**
     * Store the unit translation here to avoid recalculating it in the constructor of StatementHandle.
     *
     * @return value
     */
    protected long getObjectOccupyTimeLimitInNanos() {
        return this.objectOccupyTimeLimitInNanos;
    }

    protected boolean hasWaitingConsumer() {
        return freeObjects.hasWaitingConsumer();
    }
}
