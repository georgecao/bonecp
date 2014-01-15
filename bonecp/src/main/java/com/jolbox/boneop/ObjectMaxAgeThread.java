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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically checks for connections to see if the connection has expired.
 *
 * @author wwadge
 */
public class ObjectMaxAgeThread<T> implements Runnable {

    /**
     * Max no of ms to wait before a connection that isn't used is killed off.
     */
    private long maxAgeInMs;
    /**
     * Partition being handled.
     */
    private ObjectPartition<T> partition;
    /**
     * Scheduler handle. *
     */
    private ScheduledExecutorService scheduler;
    /**
     * Handle to connection pool.
     */
    private BoneOP<T> pool;
    /**
     * If true, we're operating in a LIFO fashion.
     */
    private boolean lifoMode;
    /**
     * Logger handle.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ObjectTesterThread.class);

    /**
     * Constructor
     *
     * @param objectPartition partition to work on
     * @param scheduler       Scheduler handler.
     * @param pool            pool handle
     * @param maxAgeInMs      Threads older than this are killed off
     * @param lifoMode        if true, we're running under a lifo fashion.
     */
    protected ObjectMaxAgeThread(ObjectPartition<T> objectPartition, ScheduledExecutorService scheduler,
                                 BoneOP<T> pool, long maxAgeInMs, boolean lifoMode) {
        this.partition = objectPartition;
        this.scheduler = scheduler;
        this.maxAgeInMs = maxAgeInMs;
        this.pool = pool;
        this.lifoMode = lifoMode;
    }

    /**
     * Invoked periodically.
     */
    public void run() {
        long tmp;
        long nextCheckInMs = this.maxAgeInMs;

        int partitionSize = this.partition.getAvailableObjects();
        long currentTime = System.currentTimeMillis();
        for (int i = 0; i < partitionSize; i++) {
            try {
                ObjectHandle<T> objectHandle = this.partition.getFreeObjects().poll();
                if (objectHandle != null) {
                    objectHandle.setOriginatingPartition(this.partition);
                    tmp = this.maxAgeInMs - (currentTime - objectHandle.getObjectCreationTimeInMs());
                    if (tmp < nextCheckInMs) {
                        nextCheckInMs = tmp;
                    }
                    if (objectHandle.isExpired(currentTime)) {
                        // kill off this connection
                        closeObject(objectHandle);
                        continue;
                    }
                    if (this.lifoMode) {
                        // we can't put it back normally or it will end up in front again.
                        if (!((LIFOQueue<ObjectHandle<T>>) objectHandle.getOriginatingPartition().getFreeObjects()).offerLast(objectHandle)) {
                            objectHandle.internalClose();
                        }
                    } else {
                        this.pool.putObjectBackInPartition(objectHandle);
                    }

                    Thread.sleep(20L); // test slowly, this is not an operation that we're in a hurry to deal with (avoid CPU spikes)...
                }

            } catch (Exception e) {
                if (this.scheduler.isShutdown()) {
                    LOG.debug("Shutting down connection max age thread.");
                    break;
                }
                LOG.error("Connection max age thread exception.", e);
            }

        } // throw it back on the queue

        if (!this.scheduler.isShutdown()) {
            this.scheduler.schedule(this, nextCheckInMs, TimeUnit.MILLISECONDS);
        }

    }

    /**
     * Closes off this connection
     *
     * @param objectHandle to close
     */
    protected void closeObject(ObjectHandle<T> objectHandle) {
        if (objectHandle != null) {
            try {
                objectHandle.internalClose();
            } catch (Throwable t) {
                LOG.error("Destroy connection exception", t);
            } finally {
                this.pool.postDestroyObject(objectHandle);
            }
        }
    }
}
