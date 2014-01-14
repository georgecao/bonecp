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

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded version of {@link LinkedTransferQueue}.
 *
 * @param <E>
 * @author wallacew
 */
public class BoundedLinkedTransferQueue<E> extends LinkedTransferQueue<E> {

    /**
     * UUID
     */
    private static final long serialVersionUID = -1875525368357897907L;
    /**
     * No of elements in queue.
     */
    private final AtomicInteger size = new AtomicInteger(0);
    /**
     * bound of queue.
     */
    private final int maxQueueSize;
    /**
     * Main lock guarding all access
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Constructor.
     *
     * @param maxQueueSize
     */
    public BoundedLinkedTransferQueue(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    @Override
    public int size() {
        return this.size.get();
    }

    /**
     * Returns the number of free slots in this queue.
     *
     * @return number of free slots.
     */
    @Override
    public int remainingCapacity() {
        return this.maxQueueSize - size();
    }

    @Override
    public E poll() {
        E result = super.poll();
        if (result != null) {
            this.size.decrementAndGet();
        }
        return result;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E result = super.poll(timeout, unit);
        if (result != null) {
            this.size.decrementAndGet();
        }
        return result;
    }

    @Override
    public boolean tryTransfer(E e) {
        boolean result = super.tryTransfer(e);
        if (result) {
            this.size.incrementAndGet();
        }
        return result;
    }

    public boolean isFull() {
        return remainingCapacity() <= 0;
    }

    /**
     * Inserts the specified element at the tail of this queue. Will return
     * false if the queue limit has been reached.
     *
     * @param e element to add
     * @return true
     */
    @Override
    public boolean offer(E e) {
        boolean result = false;
        if (!isFull()) {
            this.lock.lock();
            try {
                if (!isFull()) {
                    result = super.offer(e);
                    if (result) {
                        this.size.incrementAndGet();
                    }
                }
            } finally {
                this.lock.unlock();
            }
        }
        return result;
    }

    @Override
    public void put(E e) {
        throw new UnsupportedOperationException("We don't offer blocking yet");
    }
}
