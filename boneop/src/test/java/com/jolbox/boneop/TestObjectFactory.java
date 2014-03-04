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

import org.apache.commons.pool.PoolableObjectFactory;

/**
 * @author george
 */
public class TestObjectFactory implements PoolableObjectFactory<TestObject> {

    @Override
    public TestObject makeObject() throws Exception {
        return new TestObject();
    }

    @Override
    public void destroyObject(TestObject obj) throws Exception {
    }

    @Override
    public boolean validateObject(TestObject obj) {
        return true;
    }

    @Override
    public void activateObject(TestObject obj) throws Exception {
    }

    @Override
    public void passivateObject(TestObject obj) throws Exception {
    }

}
