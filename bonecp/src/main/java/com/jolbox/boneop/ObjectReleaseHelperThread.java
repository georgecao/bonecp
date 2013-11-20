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

import java.util.concurrent.BlockingQueue;

/**
 * A thread that monitors a queue containing connections to be released and
 * moves those connections to the partition queue.
 *
 * @author wallacew
 * @param <T>
 */
public class ObjectReleaseHelperThread<T> implements Runnable {

    /**
     * Queue of connections awaiting to be released back to each partition.
     */
    private final BlockingQueue<ObjectHandle<T>> queue;
    /**
     * Handle to the connection pool.
     */
    private final BoneOP<T> pool;

    /**
     * Helper Thread constructor.
     *
     * @param queue Handle to the release queue.
     * @param pool handle to the connection pool.
     */
    public ObjectReleaseHelperThread(BlockingQueue<ObjectHandle<T>> queue, BoneOP<T> pool) {
        this.queue = queue;
        this.pool = pool;
    }

    /**
     * {@inheritDoc}
     *
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        boolean interrupted = false;
        while (!interrupted) {
            try {
                ObjectHandle<T> connection = this.queue.take();
                this.pool.internalReleaseObject(connection);
            } catch (PoolException e) {
                interrupted = true;
            } catch (InterruptedException e) {
                if (this.pool.poolShuttingDown) {
                    // cleanup any remaining stuff. This is a gentle shutdown
                    ObjectHandle<T> connection;
                    while ((connection = this.queue.poll()) != null) {
                        try {
                            this.pool.internalReleaseObject(connection);
                        } catch (PoolException ignored) {
                            // yeah we're shutting down, shut up for a bit...
                        }
                    }

                }
                interrupted = true;
            }
        }
    }

}
