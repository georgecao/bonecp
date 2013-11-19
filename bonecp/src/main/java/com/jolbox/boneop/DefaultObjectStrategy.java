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

import java.util.concurrent.TimeUnit;

/**
 * The normal getConnection() strategy class in use. Attempts to get a
 * connection from one or more configured partitions.
 *
 * @author wallacew
 *
 */
public class DefaultObjectStrategy<T> extends AbstractObjectStrategy<T> {

    public DefaultObjectStrategy(BoneOP<T> pool) {
        this.pool = pool;
    }

    @Override
    public ObjectHandle<T> pollConnection() {
        ObjectHandle<T> result = null;
        int partition = selectPartition();
        ObjectPartition<T> connectionPartition = getPartition(partition);
        result = connectionPartition.getFreeObjects().poll();
        if (result == null) {
            // we ran out of space on this partition, pick another free one
            for (int i = 0; i < this.pool.partitionCount; i++) {
                if (i == partition) {
                    continue; // we already determined it's not here
                }
                result = getPartition(i).getFreeObjects().poll(); // try our luck with this partition
                connectionPartition = getPartition(i);
                if (result != null) {
                    break;  // we found a connection
                }
            }
        }

        if (!connectionPartition.isUnableToCreateMoreTransactions()) { // unless we can't create any more connections...
            this.pool.maybeSignalForMoreConnections(connectionPartition);  // see if we need to create more
        }

        return result;

    }

    @Override
    protected ObjectHandle<T> getConnectionInternal() throws PoolException {
        ObjectHandle<T> result = pollConnection();
        ObjectPartition<T> connectionPartition = currentPartition();
        // we still didn't find an empty one, wait forever (or as per config) until our partition is free
        if (result == null) {
            try {
                result = connectionPartition.getFreeObjects().poll(this.pool.connectionTimeoutInMs, TimeUnit.MILLISECONDS);
                if (result == null) {
                    if (this.pool.nullOnConnectionTimeout) {
                        return null;
                    }
                    // 08001 = The application requester is unable to establish the connection.
                    throw new PoolException("Timed out waiting for a free available connection.", "08001");
                }
            } catch (InterruptedException e) {
                if (this.pool.nullOnConnectionTimeout) {
                    return null;
                }
                throw new PoolException(e);
            }
        }

        if (result.isPoison()) {
            if (this.pool.getDbIsDown().get() && connectionPartition.getFreeObjects().hasWaitingConsumer()) {
                // poison other waiting threads.
                connectionPartition.getFreeObjects().offer(result);
            }
            throw new PoolException("Pool connections have been terminated. Aborting getConnection() request.", "08001");
        }
        return result;
    }

    /**
     * Closes off all connections in all partitions.
     */
    @Override
    public void terminateAllConnections() {
        this.terminationLock.lock();
        try {
            ObjectHandle<T> conn;
            // close off all connections.
            for (ObjectPartition<T> partition : this.pool.partitions) {
                partition.setUnableToCreateMoreTransactions(false); // we can create new ones now, this is an optimization
                while ((conn = partition.getFreeObjects().poll()) != null) {
                    this.pool.destroyConnection(conn);
                }

            }
        } finally {
            this.terminationLock.unlock();
        }
    }

}
