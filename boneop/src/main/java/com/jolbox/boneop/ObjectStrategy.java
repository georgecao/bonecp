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
 * Marker interface.
 *
 * @author wallacew
 * @param <T> Object type.
 *
 */
public interface ObjectStrategy<T> {

    /**
     * Obtains a connection using the configured strategy. Main entry point.
     *
     * @return pooled object.
     * @throws com.jolbox.boneop.PoolException
     */
    ObjectHandle<T> getObject() throws PoolException;

    /**
     * Obtains a object using the configured strategy without blocking.
     *
     * @return object instance
     */
    ObjectHandle<T> pollObject();

    /**
     * Destroys all objects using this strategy.
     */
    void destroyAllObjects();
}
