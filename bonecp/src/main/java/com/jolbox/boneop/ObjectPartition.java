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

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.FinalizableWeakReference;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

/**
 * Connection Partition structure
 *
 * @author wwadge
 * @param <T>
 *
 */
public class ObjectPartition<T> implements Serializable {

    /**
     * Serialization UID
     */
    private static final long serialVersionUID = -7864443421028454573L;
    /**
     * Logger class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ObjectPartition.class);
    /**
     * Connections available to be taken
     */
    private TransferQueue<ObjectHandle<T>> freeObjects;
    /**
     * When connections start running out, add these number of new connections.
     */
    private final int acquireIncrement;
    /**
     * Minimum number of connections to start off with.
     */
    private final int minObjects;
    /**
     * Maximum number of connections that will ever be created.
     */
    private final int maxObjects;
    /**
     * Statistics lock.
     */
    protected ReentrantReadWriteLock statsLock = new ReentrantReadWriteLock();
    /**
     * Number of connections that have been created.
     */
    private int createdObjects = 0;
    /**
     * If set to true, don't bother calling method to attempt to create more connections because we've hit our limit.
     */
    private volatile boolean unableToCreateMoreTransactions = false;
    /**
     * Scratch queue of connections awaiting to be placed back in queue.
     */
    private final LinkedTransferQueue<ObjectHandle<T>> objectsPendingRelease;
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
    private final long queryExecuteTimeLimitInNanoSeconds;
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
     * Updates leased connections statistics
     *
     * @param increment value to add/subtract
     */
    protected void updateCreatedObjects(int increment) {
        try {
            this.statsLock.writeLock().lock();
            this.createdObjects += increment;
        } finally {
            this.statsLock.writeLock().unlock();
        }
    }

    /**
     * Adds a free connection.
     *
     * @param handle
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
            updateCreatedObjects(-1); // compensate our createdConnection count.

            if (!this.disableTracking) {
                this.pool.getFinalizableRefs().remove(handle.getInternalObject());
            }
            // terminate the internal handle.
            handle.internalClose();
        }
    }

    /**
     * This method is a replacement for finalize() but avoids all its pitfalls (see Joshua Bloch et. all).
     *
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
            T con = handle.getInternalObject();
            final T obj = con;
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
     * @param freeObjects the freeConnections to set
     */
    protected void setFreeObjects(TransferQueue<ObjectHandle<T>> freeObjects) {
        this.freeObjects = freeObjects;
    }

    /**
     * Partition constructor
     *
     * @param pool handle to connection pool
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
        this.queryExecuteTimeLimitInNanoSeconds = TimeUnit.NANOSECONDS.convert(config.getQueryExecuteTimeLimitInMs(), TimeUnit.MILLISECONDS);
        /**
         * Create a number of helper threads for connection release.
         */
        int helperThreads = config.getReleaseHelperThreads();
        for (int i = 0; i < helperThreads; i++) {
            // go through pool.getReleaseHelper() rather than releaseHelper directly to aid unit testing (i.e. mocking)
            pool.getReleaseHelper().execute(new ObjectReleaseHelperThread(this.objectsPendingRelease, pool));
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
        try {
            this.statsLock.readLock().lock();
            return this.createdObjects;
        } finally {
            this.statsLock.readLock().unlock();
        }
    }

    /**
     * Returns true if we have created all the connections we can
     *
     * @return true if we have created all the connections we can
     */
    protected boolean isUnableToCreateMoreTransactions() {
        return this.unableToCreateMoreTransactions;
    }

    /**
     * Sets connection creation possible status
     *
     * @param unableToCreateMoreTransactions t/f
     */
    protected void setUnableToCreateMoreTransactions(boolean unableToCreateMoreTransactions) {
        this.unableToCreateMoreTransactions = unableToCreateMoreTransactions;
    }

    /**
     * Gets handle to a release connection handle queue.
     *
     * @return release connection handle queue
     */
    protected LinkedTransferQueue<ObjectHandle<T>> getObjectsPendingRelease() {
        return this.objectsPendingRelease;
    }

    /**
     * Returns the number of avail connections
     *
     * @return avail connections.
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
    protected long getQueryExecuteTimeLimitinNanoSeconds() {
        return this.queryExecuteTimeLimitInNanoSeconds;
    }
}
