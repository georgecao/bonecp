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
 * A pooling interface.
 * <p>
 * <code>ObjectPool</code> defines a trivially simple pooling interface. The
 * only required methods are
 * {@link #borrowObject borrowObject}, {@link #returnObject returnObject} and
 * {@link #invalidateObject invalidateObject}.
 * </p>
 * <p>
 * Example of use:
 * <pre style="border:solid thin; padding: 1ex;" > Object obj = <code
 * style="color:#00C">null</code>;
 * <p/>
 * <code style="color:#00C">try</code> { obj = pool.borrowObject();
 * <code style="color:#00C">try</code> {
 * <code style="color:#0C0">//...use the object...</code> } <code
 * style="color:#00C">catch</code>(Exception e) {
 * <code style="color:#0C0">// invalidate the object</code>
 * pool.invalidateObject(obj);
 * <code style="color:#0C0">// do not return the object to the pool twice</code>
 * obj = <code style="color:#00C">null</code>; } <code
 * style="color:#00C">finally</code> {
 * <code style="color:#0C0">// make sure the object is returned to the
 * pool</code>
 * <code style="color:#00C">if</code>(<code style="color:#00C">null</code> !=
 * obj) { pool.returnObject(obj); } } } <code
 * style="color:#00C">catch</code>(Exception e) {
 * <code style="color:#0C0">// failed to borrow an object</code> }</pre>
 * </p>
 * <p/>
 *
 * @since Pool 1.0
 */
public interface ObjectPool<T> {

    T borrowObject() throws PoolException;

    /**
     * Return an instance to the pool. By contract, <code>obj</code>
     * <strong>must</strong> have been obtained using
     * {@link #borrowObject() borrowObject} or a related method as defined in an
     * implementation or sub-interface.
     *
     * @param obj a {@link #borrowObject borrowed} instance to be returned.
     * @throws Exception
     */
    void returnObject(T obj) throws PoolException;

    /**
     * <p>
     * Invalidates an object from the pool.</p>
     * <p/>
     * <p>
     * By contract, <code>obj</code> <strong>must</strong> have been obtained
     * using {@link #borrowObject borrowObject} or a related method as defined
     * in an implementation or sub-interface.</p>
     * <p/>
     * <p>
     * This method should be used when an object that has been borrowed is
     * determined (due to an exception or other problem) to be invalid.</p>
     *
     * @param obj a {@link #borrowObject borrowed} instance to be disposed.
     * @throws Exception
     */
    void invalidateObject(T obj) throws PoolException;

    void addObject() throws PoolException;

    /**
     * Return the number of instances currently idle in this pool (optional
     * operation). This may be considered an approximation of the number of
     * objects that can be {@link #borrowObject borrowed} without creating any
     * new instances. Returns a negative value if this information is not
     * available.
     *
     * @return the number of instances currently idle in this pool or a negative
     * value if unsupported
     * @throws UnsupportedOperationException <strong>deprecated</strong>: if
     * this implementation does not support the operation
     */
    int getNumIdle() throws PoolException;

    /**
     * Return the number of instances currently borrowed from this pool
     * (optional operation). Returns a negative value if this information is not
     * available.
     *
     * @return the number of instances currently borrowed from this pool or a
     * negative value if unsupported
     * @throws UnsupportedOperationException <strong>deprecated</strong>: if
     * this implementation does not support the operation
     */
    int getNumActive() throws PoolException;

    void clear() throws PoolException;

    /**
     * Close this pool, and free any resources associated with it.
     * <p>
     * Calling {@link #addObject} or {@link #borrowObject} after invoking this
     * method on a pool will cause them to throw an
     * {@link IllegalStateException}.
     * </p>
     *
     * @throws Exception <strong>deprecated</strong>: implementations should
     * silently fail if not all resources can be freed.
     */
    void close() throws PoolException;
}
