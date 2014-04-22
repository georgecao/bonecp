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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Parent class for the different pool strategies.
 *
 * @param <T>
 * @author wallacew
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
     * @return if stats are enabled, return the nanoTime when this connection was requested.
     * @throws com.jolbox.boneop.PoolException
     */
    protected long preObject() throws PoolException {
        long statsObtainTime = 0;

        if (this.pool.poolShuttingDown) {
            throw new PoolException(this.pool.shutdownStackTrace);
        }

        if (this.pool.statisticsEnabled) {
            statsObtainTime = System.nanoTime();
            this.pool.statistics.incrementObjectsRequested();
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
        return (int) (Thread.currentThread().getId() & this.pool.mask);
    }

    /**
     * After obtaining a connection, perform additional tasks.
     *
     * @param handle          object handle
     * @param statsObtainTime create time
     */
    protected void postObject(ObjectHandle<T> handle, long statsObtainTime) {

        handle.renewObject(); // mark it as being logically "open"

        // Give an application a chance to do something with it.
        if (handle.getObjectListener() != null) {
            handle.getObjectListener().onCheckOut(handle);
        }

        if (this.pool.closeObjectWatch) { // a debugging tool
            this.pool.watchObject(handle);
        }

        if (this.pool.statisticsEnabled) {
            this.pool.statistics.addCumulativeObjectWaitTime(System.nanoTime() - statsObtainTime);
        }
    }

    @Override
    public ObjectHandle<T> getObject() throws PoolException {
        long statsObtainTime = preObject();
        ObjectHandle<T> result = getObjectInternal();
        if (result != null) {
            postObject(result, statsObtainTime);
        }
        return result;
    }

    /**
     * Actual call that returns a connection
     *
     * @return Connection
     * @throws com.jolbox.boneop.PoolException
     */
    protected abstract ObjectHandle<T> getObjectInternal() throws PoolException;

    @Override
    public ObjectHandle<T> pollObject() {
        throw new UnsupportedOperationException();
    }
}
