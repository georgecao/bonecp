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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Parent class for the different pool strategies.
 *
 * @author wallacew
 *
 */
public abstract class AbstractObjectStrategy<T> implements ObjectStrategy<T> {

    /**
     * Pool handle
     */
    protected BoneOP<T> pool;

    /**
     * Prevent repeated termination of all connections when the DB goes down.
     */
    protected Lock terminationLock = new ReentrantLock();

    /**
     * Prep for a new connection
     *
     * @return if stats are enabled, return the nanoTime when this connection
     * was requested.
     * @throws com.jolbox.bonecp.PoolException
     */
    protected long preConnection() throws PoolException {
        long statsObtainTime = 0;

        if (this.pool.poolShuttingDown) {
            throw new PoolException(this.pool.shutdownStackTrace);
        }

        if (this.pool.statisticsEnabled) {
            statsObtainTime = System.nanoTime();
            this.pool.statistics.incrementConnectionsRequested();
        }

        return statsObtainTime;
    }

    public ObjectPartition<T> currentPartition() {
        return getPartition(selectPartition());
    }

    public ObjectPartition<T> getPartition(int index) {
        return this.pool.partitions.get(index);
    }

    public int selectPartition() {
        return (int) (Thread.currentThread().getId() % this.pool.partitionCount);
    }

    /**
     * After obtaining a connection, perform additional tasks.
     *
     * @param handle
     * @param statsObtainTime
     */
    protected void postConnection(ObjectHandle<T> handle, long statsObtainTime) {

        handle.renewObject(); // mark it as being logically "open"

        // Give an application a chance to do something with it.
        if (handle.getObjectListener() != null) {
            handle.getObjectListener().onCheckOut(handle);
        }

        if (this.pool.closeConnectionWatch) { // a debugging tool
            this.pool.watchConnection(handle);
        }

        if (this.pool.statisticsEnabled) {
            this.pool.statistics.addCumulativeConnectionWaitTime(System.nanoTime() - statsObtainTime);
        }
    }

    @Override
    public ObjectHandle<T> getConnection() throws PoolException {
        long statsObtainTime = preConnection();

        ObjectHandle<T> result = getConnectionInternal();
        if (result != null) {
            postConnection(result, statsObtainTime);
        }

        return result;
    }

    /**
     * Actual call that returns a connection
     *
     * @return Connection
     * @throws com.jolbox.bonecp.PoolException
     */
    protected abstract ObjectHandle<T> getConnectionInternal() throws PoolException;

    @Override
    public ObjectHandle<T> pollConnection() {
        // usually overridden
        return null;
    }
}
