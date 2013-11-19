/**
 * Copyright 2010 Wallace Wadge
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jolbox.boneop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches a partition to create new connections when required.
 *
 * @author wwadge
 *
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
    private boolean signalled;
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
    private static final Logger logger = LoggerFactory.getLogger(PoolWatchThread.class);

    /**
     * Thread constructor
     *
     * @param connectionPartition partition to monitor
     * @param pool Pool handle.
     */
    public PoolWatchThread(ObjectPartition<T> connectionPartition, BoneOP<T> pool) {
        this.partition = connectionPartition;
        this.pool = pool;
        this.lazyInit = this.pool.getConfig().isLazyInit();
        this.acquireRetryDelayInMs = this.pool.getConfig().getAcquireRetryDelayInMs();
        this.poolAvailabilityThreshold = this.pool.getConfig().getPoolAvailabilityThreshold();
    }

    @Override
    public void run() {
        int maxNewConnections;
        while (!this.signalled) {
            maxNewConnections = 0;

            try {
                if (this.lazyInit) { // block the first time if this is on.
                    this.partition.getPoolWatchThreadSignalQueue().take();
                }
                maxNewConnections = this.partition.getMaxConnections() - this.partition.getCreatedConnections();
                // loop for spurious interrupt
                while (maxNewConnections == 0 || (this.partition.getAvailableConnections() * 100 / this.partition.getMaxConnections() > this.poolAvailabilityThreshold)) {
                    if (maxNewConnections == 0) {
                        this.partition.setUnableToCreateMoreTransactions(true);
                    }
                    this.partition.getPoolWatchThreadSignalQueue().take();
                    maxNewConnections = this.partition.getMaxConnections() - this.partition.getCreatedConnections();

                }
                if (maxNewConnections > 0 && !this.pool.poolShuttingDown) {
                    fillConnections(Math.min(maxNewConnections, this.partition.getAcquireIncrement()));
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
     * @param connectionsToCreate number of connections to create
     * @throws InterruptedException
     */
    private void fillConnections(int connectionsToCreate) throws InterruptedException {
        try {
            for (int i = 0; i < connectionsToCreate; i++) {
                boolean dbDown = this.pool.getDbIsDown().get();
                if (this.pool.poolShuttingDown) {
                    break;
                }
                ObjectHandle<T> handle = ObjectHandle.<T>createConnectionHandle(this.pool);

                if (dbDown && !this.pool.getDbIsDown().get()) { // we've just recovered
                    ObjectHandle<T> maybePoison = this.partition.getFreeObjects().poll();
                    if (maybePoison != null && !maybePoison.isPoison()) {
                        // wasn't poison, push it back
                        this.partition.getFreeObjects().offer(maybePoison);
                        // otherwise just consume it.
                    }
                }
                this.partition.addFreeConnection(handle);

            }
        } catch (PoolException e) {
            logger.error("Error in trying to obtain a connection. Retrying in " + this.acquireRetryDelayInMs + "ms", e);
            Thread.sleep(this.acquireRetryDelayInMs);
        }

    }

}
