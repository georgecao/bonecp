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

import java.util.concurrent.TimeUnit;

/**
 * The normal getConnection() strategy class in use. Attempts to get a connection from one or more configured
 * partitions.
 *
 * @param <T> object type.
 * @author wallacew
 */
public class DefaultObjectStrategy<T> extends AbstractObjectStrategy<T> {

    public DefaultObjectStrategy(BoneOP<T> pool) {
        this.pool = pool;
    }

    @Override
    public ObjectHandle<T> poll() {
        int pid = selectPartition();
        ObjectPartition<T> partition = getPartition(pid);
        ObjectHandle<T> result = partition.getFreeObjects().poll();
        if (result == null) {
            // we ran out of space on this partition, pick another free one
            int count = 1;
            // We already try one partition but with no luck.
            // So now we start with the next partition and traverse all partition util we find an available object.
            while (count++ < this.pool.partitionCount) {
                pid = nextPartition(pid);
                partition = getPartition(pid);
                result = partition.getFreeObjects().poll(); // try our luck with this partition
                if (result != null) {
                    break;  // we found a connection
                }
            }
        }
        if (!partition.isUnableToCreateMoreObjects()) { // unless we can't create any more connections...
            this.pool.maybeSignalForMoreObjects(partition);  // see if we need to create more
        }
        return result;
    }

    @Override
    protected ObjectHandle<T> getObjectInternal() throws PoolException {
        ObjectHandle<T> result = poll();
        ObjectPartition<T> partition = currentPartition();
        // we still didn't find an empty one, wait forever (or as per config) until our partition is free
        if (result == null) {
            try {
                result = partition.getFreeObjects().poll(this.pool.waitTimeInMillis, TimeUnit.MILLISECONDS);
                if (result == null) {
                    if (this.pool.nullOnObjectTimeout) {
                        return null;
                    }
                    // 08001 = The application requester is unable to establish the connection.
                    throw new PoolException("Timed out waiting for a free available object.", String.valueOf(this.pool.waitTimeInMillis));
                }
            } catch (InterruptedException e) {
                if (this.pool.nullOnObjectTimeout) {
                    return null;
                }
                throw new PoolException(e);
            }
        }

        if (result.isPoison()) {
            if (this.pool.getDown().get() && partition.hasWaitingConsumer()) {
                // poison other waiting threads.
                partition.getFreeObjects().offer(result);
            }
            throw new PoolException("Pool objects have been terminated. Aborting take() request.", "08001");
        }
        return result;
    }

    /**
     * Closes off all connections in all partitions.
     */
    @Override
    public void destroy() {
        this.terminationLock.lock();
        try {
            ObjectHandle<T> objectHandle;
            // close off all objects.
            for (ObjectPartition<T> partition : this.pool.partitions) {
                partition.setUnableToCreateMoreObjects(false); // we can create new ones now, this is an optimization
                while ((objectHandle = partition.getFreeObjects().poll()) != null) {
                    this.pool.destroyObject(objectHandle);
                }
            }
        } finally {
            this.terminationLock.unlock();
        }
    }
}
