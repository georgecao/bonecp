package com.jolbox.boneop;

/**
 * Bone Object Pool implementation.
 *
 * @author George Cao(caozhangzhi@qiyi.com)
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
