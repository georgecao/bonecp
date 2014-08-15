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
 * Periodically sends a keep-alive statement to idle threads and kills off any
 * connections that have been unused for a long time (or broken).
 *
 * @author wwadge
 */
public class ObjectTesterThread<T> implements Runnable {

    /**
     * Connections used less than this time ago are not keep-alive tested.
     */
    private final long idleObjectTestPeriodInMillis;
    /**
     * Max no of ms to wait before a connection that isn't used is killed off.
     */
    private final long idleMaxAgeInMillis;
    /**
     * Partition being handled.
     */
    private final ObjectPartition<T> partition;
    /**
     * Scheduler handle. *
     */
    private final ScheduledExecutorService scheduler;
    /**
     * Handle to connection pool.
     */
    private final BoneOP<T> pool;
    /**
     * If true, we're operating in a LIFO fashion.
     */
    private final boolean lifoMode;
    /**
     * Logger handle.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ObjectTesterThread.class);

    /**
     * Constructor
     *
     * @param connectionPartition          partition to work on
     * @param scheduler                    Scheduler handler.
     * @param pool                         pool handle
     * @param idleMaxAgeInMillis           Threads older than this are killed off
     * @param idleObjectTestPeriodInMillis Threads that are idle for more than
     *                                     this time are sent a keep-alive.
     * @param lifoMode                     if true, we're running under a lifo fashion.
     */
    protected ObjectTesterThread(ObjectPartition<T> connectionPartition, ScheduledExecutorService scheduler,
                                 BoneOP<T> pool, long idleMaxAgeInMillis, long idleObjectTestPeriodInMillis, boolean lifoMode) {
        this.partition = connectionPartition;
        this.scheduler = scheduler;
        this.idleMaxAgeInMillis = idleMaxAgeInMillis;
        this.idleObjectTestPeriodInMillis = idleObjectTestPeriodInMillis;
        this.pool = pool;
        this.lifoMode = lifoMode;
    }

    /**
     * Invoked periodically.
     */
    public void run() {
        ObjectHandle<T> objectHandle;
        long tmp;
        try {
            long nextCheckInMs = this.idleObjectTestPeriodInMillis;
            if (this.idleMaxAgeInMillis > 0) {
                if (this.idleObjectTestPeriodInMillis == 0) {
                    nextCheckInMs = this.idleMaxAgeInMillis;
                } else {
                    nextCheckInMs = Math.min(nextCheckInMs, this.idleMaxAgeInMillis);
                }
            }

            int partitionSize = this.partition.getAvailableObjects();
            long currentTimeInMs = System.currentTimeMillis();
            // go thru all partitions
            for (int i = 0; i < partitionSize; i++) {
                // grab connections one by one.
                objectHandle = this.partition.getFreeObjects().poll();
                if (objectHandle != null) {
                    objectHandle.setOriginatingPartition(this.partition);

                    // check if connection has been idle for too long (or is marked as broken)
                    if (!objectHandle.isPoison() && objectHandle.isPossiblyBroken()
                            || ((this.idleMaxAgeInMillis > 0) && (this.partition.getAvailableObjects() >= this.partition.getMinObjects() && System.currentTimeMillis() - objectHandle.getObjectLastUsedInMillis() > this.idleMaxAgeInMillis))) {
                        // kill off this connection - it's broken or it has been idle for too long
                        closeObject(objectHandle);
                        continue;
                    }

                    // check if it's time to send a new keep-alive test statement.
                    if (!objectHandle.isPoison() && this.idleObjectTestPeriodInMillis > 0 && (currentTimeInMs - objectHandle.getObjectLastUsedInMillis() > this.idleObjectTestPeriodInMillis)
                            && (currentTimeInMs - objectHandle.getObjectLastResetInMillis() >= this.idleObjectTestPeriodInMillis)) {
                        // send a keep-alive, close off connection if we fail.
                        if (!this.pool.isObjectHandleAlive(objectHandle)) {
                            closeObject(objectHandle);
                            continue;
                        }
                        // calculate the next time to wake up
                        tmp = this.idleObjectTestPeriodInMillis;
                        if (this.idleMaxAgeInMillis > 0) { // wake up earlier for the idleMaxAge test?
                            tmp = Math.min(tmp, this.idleMaxAgeInMillis);
                        }
                    } else {
                        // determine the next time to wake up (connection test time or idle Max age?) 
                        tmp = Math.abs(this.idleObjectTestPeriodInMillis - (currentTimeInMs - objectHandle.getObjectLastResetInMillis()));
                        long tmp2 = Math.abs(this.idleMaxAgeInMillis - (currentTimeInMs - objectHandle.getObjectLastUsedInMillis()));
                        if (this.idleMaxAgeInMillis > 0) {
                            tmp = Math.min(tmp, tmp2);
                        }
                    }
                    if (tmp < nextCheckInMs) {
                        nextCheckInMs = tmp;
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

            } // throw it back on the queue
//				System.out.println("Scheduling for " + nextCheckInMs);
            // offset by a bit to avoid firing a lot for slightly offset connections
            this.scheduler.schedule(this, nextCheckInMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (this.scheduler.isShutdown()) {
                LOG.debug("Shutting down connection tester thread.");
            } else {
                LOG.error("Connection tester thread interrupted", e);
            }
        }
    }

    /**
     * Closes off this connection
     *
     * @param objectHandle to close
     */
    private void closeObject(ObjectHandle<T> objectHandle) {
        if (objectHandle != null) {
            try {
                try {
                    objectHandle.internalClose();
                } catch (PoolException ex) {
                    LOG.error("", ex);
                }
            } finally {
                this.pool.postDestroyObject(objectHandle);
            }
        }
    }

}
