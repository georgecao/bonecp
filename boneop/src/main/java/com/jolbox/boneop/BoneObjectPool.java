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

/**
 * Bone Object Pool implementation.
 *
 * @author George Cao
 * @since 2014-08-11 15
 */
public class BoneObjectPool<T> implements ObjectPool<T> {

    private final BoneOP<T> underlying;

    public BoneObjectPool(BoneOP<T> underlying) {
        this.underlying = underlying;
    }

    public BoneOP<T> getUnderlying() {
        return underlying;
    }

    @Override
    public T borrowObject() throws PoolException {
        return underlying.getObject();
    }

    @Override
    public void returnObject(T obj) throws PoolException {
        underlying.releaseObject(obj);
    }

    @Override
    public void invalidateObject(T obj) throws PoolException {

    }

    @Override
    public void addObject() throws PoolException {

    }

    @Override
    public int getNumIdle() throws PoolException {
        return underlying.getTotalFree();
    }

    @Override
    public int getNumActive() throws PoolException {
        return underlying.getTotalLeased();
    }

    @Override
    public void clear() throws PoolException {

    }

    @Override
    public void close() throws PoolException {
        underlying.shutdown();
    }
}
