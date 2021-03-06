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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches a partition to create new connections when required.
 *
 * @param <T>
 * @author wwadge
 */
public class PoolWatchThread<T> implements Runnable {

    /**
     * Partition being monitored.
     */
    private final ObjectPartition<T> partition;
    /**
     * Pool handle.
     */
    private final BoneOP<T> pool;
    /**
     * Mostly used to break out easily in unit testing.
     */
    private boolean signalled = false;
    /**
     * How long to wait before retrying to add a connection upon failure.
     */
    private long acquireRetryDelayInMs = 1000L;
    /**
     * Start off lazily.
     */
    private final boolean lazyInit;
    /**
     * Occupancy% threshold.
     */
    private final int poolAvailabilityThreshold;
    /**
     * Logger handle.
     */
    private static final Logger LOG = LoggerFactory.getLogger(PoolWatchThread.class);

    /**
     * Thread constructor
     *
     * @param partition partition to monitor
     * @param pool      Pool handle.
     */
    public PoolWatchThread(ObjectPartition<T> partition, BoneOP<T> pool) {
        this.partition = partition;
        this.pool = pool;
        this.lazyInit = this.pool.getConfig().isLazyInit();
        this.acquireRetryDelayInMs = this.pool.getConfig().getAcquireRetryDelayInMillis();
        this.poolAvailabilityThreshold = this.pool.getConfig().getPoolAvailabilityThreshold();
    }

    @Override
    public void run() {
        while (!this.signalled) {
            try {
                if (this.lazyInit) { // block the first time if this is on.
                    this.partition.getPoolWatchThreadSignalQueue().take();
                }
                int maxNewConnections = this.partition.getMaxObjects() - this.partition.getCreatedObjects();
                // loop for spurious interrupt
                while (maxNewConnections == 0 || (this.partition.getAvailableObjects() * 100 / this.partition.getMaxObjects() > this.poolAvailabilityThreshold)) {
                    if (maxNewConnections == 0) {
                        this.partition.setUnableToCreateMoreObjects(true);
                    }
                    this.partition.getPoolWatchThreadSignalQueue().take();
                    maxNewConnections = this.partition.getMaxObjects() - this.partition.getCreatedObjects();
                }
                if (maxNewConnections > 0 && !this.pool.poolShuttingDown) {
                    fillObjects(Math.min(maxNewConnections, this.partition.getAcquireIncrement()));
                }
                if (this.pool.poolShuttingDown) {
                    return;
                }

            } catch (InterruptedException e) {
                return; // we've been asked to terminate.
            }
        }
    }

    /**
     * Adds new connections to the partition.
     *
     * @param objectsToCreate number of connections to create
     * @throws InterruptedException
     */
    private void fillObjects(int objectsToCreate) throws InterruptedException {
        try {
            for (int i = 0; i < objectsToCreate; i++) {
                boolean isDown = this.pool.getDown().get();
                if (this.pool.poolShuttingDown) {
                    break;
                }
                ObjectHandle<T> handle = ObjectHandle.createObjectHandle(this.pool);
                if (isDown && !this.pool.getDown().get()) { // we've just recovered
                    ObjectHandle<T> maybePoison = this.partition.getFreeObjects().poll();
                    if (maybePoison != null && !maybePoison.isPoison()) {
                        // wasn't poison, push it back
                        this.partition.getFreeObjects().offer(maybePoison);
                        // otherwise just consume it.
                    }
                }
                this.partition.addFreeObject(handle);
            }
        } catch (PoolException e) {
            LOG.error("Error in trying to obtain a connection. Retrying in {} ms", this.acquireRetryDelayInMs, e);
            Thread.sleep(this.acquireRetryDelayInMs);
        }

    }

}
