package com.jolbox.benchmark;

import com.jolbox.boneop.PoolableObjectFactory;

/**
 * Say something?
 *
 * @author George Cao(caozhangzhi@qiyi.com)
 * @since 2014-08-15 19
 */
public class BenchmarkObjectFactory implements PoolableObjectFactory<BenchmarkObject> {
    @Override
    public BenchmarkObject makeObject() throws Exception {
        return null;
    }

    @Override
    public void destroyObject(BenchmarkObject obj) throws Exception {

    }

    @Override
    public boolean validateObject(BenchmarkObject obj) {
        return false;
    }

    @Override
    public void activateObject(BenchmarkObject obj) throws Exception {

    }

    @Override
    public void passivateObject(BenchmarkObject obj) throws Exception {

    }
}
