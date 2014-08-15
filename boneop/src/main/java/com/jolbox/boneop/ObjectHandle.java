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

import com.jolbox.boneop.listener.AcquireFailConfig;
import com.jolbox.boneop.listener.ObjectListener;
import com.jolbox.boneop.listener.ObjectState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Reference;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Object handle wrapper around a JDBC connection.
 *
 * @author wwadge
 */
public class ObjectHandle<T> {
    /**
     * Logger handle.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ObjectHandle.class);
    /**
     * Exception message.
     */
    private static final String LOG_ERROR_MESSAGE = "Object closed twice exception detected.\n%s\n%s\n";
    /**
     * Exception message.
     */
    private static final String CLOSED_TWICE_EXCEPTION_MESSAGE = "Connection closed from thread [%s] was closed again.\nStack trace of location where connection was first closed follows:\n";
    /**
     * If true, this object might have failed communicating with the database. We assume that exceptions should be
     * rare here i.e. the normal case is assumed to succeed.
     */
    protected boolean possiblyBroken;
    /**
     * If true, we've called close() on this connection.
     */
    protected boolean logicallyClosed = false;

    /**
     * Keep track of the thread.
     */
    protected Thread threadUsingObject;
    /**
     * If true, this connection is in use in a thread-local context.
     */
    protected AtomicBoolean inUseInThreadLocalContext = new AtomicBoolean();
    /**
     * Object handle.
     */
    private T object = null;
    /**
     * Last time this connection was used by an application.
     */
    private long objectLastUsedInMillis = System.currentTimeMillis();
    /**
     * Last time we sent a reset to this connection.
     */
    private long objectLastResetInMillis = System.currentTimeMillis();
    /**
     * Time when this connection was created.
     */
    private long objectCreationTimeInMillis = System.currentTimeMillis();
    /**
     * Pool handle.
     */
    private BoneOP<T> pool;
    /**
     * Config setting.
     */
    private boolean resetObjectOnClose;
    /**
     * Original partition.
     */
    private ObjectPartition<T> originatingPartition = null;
    /**
     * An opaque handle for an application to use in any way it deems fit.
     */
    private Object debugHandle;
    /**
     * Handle to the object hook as defined in the config.
     */
    private ObjectListener objectListener;
    /**
     * If true, give warnings if application tried to issue a close twice (for debugging only).
     */
    private boolean doubleCloseCheck;
    /**
     * exception trace if doubleCloseCheck is enabled.
     */
    private volatile String doubleCloseException = null;
    /**
     * Configured max connection age.
     */
    private long maxObjectAgeInMillis;
    /**
     * if true, we care about statistics.
     */
    private boolean statisticsEnabled;
    /**
     * Statistics handle.
     */
    private Statistics statistics;
    /**
     * Pointer to a thread that is monitoring this connection (for the case where closeObjectWatch) is enabled.
     */
    private volatile Thread threadWatch;
    /**
     * Handle to pool finalizationRefs.
     */
    private Map<T, Reference<ObjectHandle<T>>> finalizableRefs;
    /**
     * If true, connection tracking is disabled in the config.
     */
    private boolean objectTrackingDisabled;
    /**
     * If true, this is a poison dummy connection used to unblock threads that are currently waiting on {@link BoneOP#getObject()}
     * for nothing while the pool is trying to revive itself.
     */
    private boolean poison;


    /**
     * Object wrapper constructor
     *
     * @param pool pool handle.
     * @throws PoolException on error
     */
    private ObjectHandle(BoneOP<T> pool) throws PoolException {
        this.pool = pool;
        this.object = obtainInternalObject(pool);
        fillObjectFields(pool);
    }

    /**
     * Create a dummy handle.
     */
    private ObjectHandle() {
    }

    /**
     * Create a connection Handle.
     *
     * @param pool pool handle.
     * @return connection handle.
     * @throws PoolException on error
     */
    public static <T> ObjectHandle<T> createObjectHandle(BoneOP<T> pool) throws PoolException {
        return new ObjectHandle<>(pool);
    }

    /**
     * Create a dummy handle that is marked as poison (i.e. causes receiving thread to terminate).
     *
     * @return connection handle.
     */
    public static <T> ObjectHandle<T> createPoisonObjectHandle() {
        ObjectHandle<T> handle = new ObjectHandle<>();
        handle.setPoison(true);
        return handle;
    }

    /**
     * Private -- used solely for unit testing.
     *
     * @param <T>    Object type.
     * @param object object associated with created handle.
     * @param pool   the pool this handle belongs to.
     * @return Object Handle
     */
    protected static <T> ObjectHandle<T> createTestObjectHandle(T object, BoneOP<T> pool) {
        ObjectHandle<T> handle = new ObjectHandle<>();
        handle.object = object;
        handle.pool = pool;
        return handle;
    }

    /**
     * Creates the object handle again. We use this method to create a brand new object handle. That way if the
     * application (wrongly) tries to do something else with the object that has already been "closed", it will
     * fail.
     * <p/>
     * Copy the handle's other properties except the object.
     *
     * @return ObjectHandle
     * @throws PoolException
     */
    public ObjectHandle<T> recreateObjectHandle() throws PoolException {
        ObjectHandle<T> handle = new ObjectHandle<>();
        handle.pool = this.pool;
        handle.object = this.object;
        handle.originatingPartition = this.originatingPartition;
        handle.objectCreationTimeInMillis = this.objectCreationTimeInMillis;
        handle.objectLastResetInMillis = this.objectLastResetInMillis;
        handle.objectLastUsedInMillis = this.objectLastUsedInMillis;
        handle.possiblyBroken = this.possiblyBroken;

        handle.debugHandle = this.debugHandle;
        handle.objectListener = this.objectListener;
        handle.doubleCloseCheck = this.doubleCloseCheck;
        handle.doubleCloseException = this.doubleCloseException;
        handle.threadUsingObject = this.threadUsingObject;
        handle.maxObjectAgeInMillis = this.maxObjectAgeInMillis;
        handle.statisticsEnabled = this.statisticsEnabled;
        handle.statistics = this.statistics;
        handle.threadWatch = this.threadWatch;
        handle.finalizableRefs = this.finalizableRefs;
        handle.objectTrackingDisabled = this.objectTrackingDisabled;
        handle.inUseInThreadLocalContext = this.inUseInThreadLocalContext;
        handle.poison = this.poison;
        this.object = null;
        return handle;
    }

    /**
     * Fills in any default fields in this handle.
     *
     * @param pool
     * @throws PoolException
     */
    private void fillObjectFields(BoneOP<T> pool) throws PoolException {
        this.pool = pool;
        this.finalizableRefs = pool.getFinalizableRefs();
        this.resetObjectOnClose = pool.getConfig().isResetObjectOnClose();
        this.objectTrackingDisabled = pool.getConfig().isDisableObjectTracking();
        this.statisticsEnabled = pool.getConfig().isStatisticsEnabled();
        this.statistics = pool.getStatistics();
        this.threadUsingObject = null;
        this.objectListener = this.pool.getConfig().getObjectListener();

        this.maxObjectAgeInMillis = pool.getConfig().getMaxObjectAge(TimeUnit.MILLISECONDS);
        this.doubleCloseCheck = pool.getConfig().isCloseObjectWatch();
    }

    /**
     * Obtains a object, retrying if necessary.
     *
     * @param pool pool handle
     * @return A DB connection.
     * @throws PoolException
     */
    protected final T obtainInternalObject(BoneOP<T> pool) throws PoolException {
        boolean tryAgain = false;
        T result = null;
        int acquireRetryAttempts = pool.getConfig().getAcquireRetryAttempts();
        long acquireRetryDelayInMillis = pool.getConfig().getAcquireRetryDelayInMillis();
        AcquireFailConfig acquireConfig = new AcquireFailConfig();
        acquireConfig.setAcquireRetryAttempts(new AtomicInteger(acquireRetryAttempts));
        acquireConfig.setAcquireRetryDelayInMillis(acquireRetryDelayInMillis);
        acquireConfig.setLogMessage("Failed to acquire connection to ");
        this.objectListener = pool.getConfig().getObjectListener();
        do {
            try {
                // keep track of this hook.
                this.object = pool.obtainRawInternalObject();
                tryAgain = false;

                if (acquireRetryAttempts != pool.getConfig().getAcquireRetryAttempts()) {
                    LOG.info("Successfully re-established object");
                }

                pool.getDown().set(false);

                // call the hook, if available.
                if (this.objectListener != null) {
                    this.objectListener.onAcquire(this);
                }
                result = this.object;
            } catch (PoolException e) {
                // call the hook, if available.
                if (this.objectListener != null) {
                    tryAgain = this.objectListener.onAcquireFail(e, acquireConfig);
                } else {
                    LOG.error("Failed to acquire object. Sleeping for {} ms. Attempts left: {}", acquireRetryDelayInMillis, acquireRetryAttempts, e);

                    try {
                        Thread.sleep(acquireRetryDelayInMillis);
                        if (acquireRetryAttempts > -1) {
                            tryAgain = (acquireRetryAttempts--) != 0;
                        }
                    } catch (InterruptedException e1) {
                        tryAgain = false;
                    }
                }
                if (!tryAgain) {
                    throw markPossiblyBroken(e);
                }
            }
        } while (tryAgain);

        return result;

    }

    /**
     * Given an exception, flag the object (or database) as being potentially broken. If the exception is a
     * data-specific exception, do nothing except throw it back to the application.
     *
     * @param e PoolException e
     * @return PoolException for further processing
     */
    protected PoolException markPossiblyBroken(PoolException e) {
        String state = "";
        ObjectState objectState = this.getObjectListener() != null ? this.getObjectListener().onMarkPossiblyBroken(this, state, e) : ObjectState.NOP;
        if (state == null) { // safety;
            state = "08999";
        }

        if (((objectState.equals(ObjectState.TERMINATE_ALL_OBJECTS)) && this.pool != null) && this.pool.getDown().compareAndSet(false, true)) {
            LOG.error("Database access problem. Killing off all remaining connections in the connection pool. SQL State = " + state);
            this.pool.objectStrategy.destroy();
            this.pool.poisonAndRepopulatePartitions();
        }

        // SQL-92 says:
        //		 Class values that begin with one of the <digit>s '5', '6', '7',
        //         '8', or '9' or one of the <simple Latin upper case letter>s 'I',
        //         'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
        //         'W', 'X', 'Y', or 'Z' are reserved for implementation-specified
        //         conditions.
        // FIXME: We should look into this.connection.getMetaData().getSQLStateType();
        // to determine if we have SQL:92 or X/OPEN sqlstatus codes.
        //		char firstChar = state.charAt(0);
        // if it's a communication exception, a mysql deadlock or an implementation-specific error code, flag this connection as being potentially broken.
        // state == 40001 is mysql specific triggered when a deadlock is detected
        char firstChar = '0';
        if (objectState.equals(ObjectState.OBJECT_POSSIBLY_BROKEN) || state.equals("40001")
                || state.startsWith("08") || (firstChar >= '5' && firstChar <= '9') /*|| (firstChar >='I' && firstChar <= 'Z')*/) {
            this.possiblyBroken = true;
        }

        // Notify anyone who's interested
        if (this.possiblyBroken && (this.getObjectListener() != null)) {
            this.possiblyBroken = this.getObjectListener().onConnectionException(this, state, e);
        }

        return e;
    }

    /**
     * Checks if the object is (logically) closed and throws an exception if it is.
     *
     * @throws PoolException on error
     */
    private void checkClosed() throws PoolException {
        if (this.logicallyClosed) {
            throw new PoolException("Connection is closed!");
        }
    }

    /**
     * Release the connection if called.
     */
    public void close() throws PoolException {
        try {
            if (!this.logicallyClosed) {
                if (this.threadWatch != null) {
                    this.threadWatch.interrupt(); // if we returned the connection to the pool, terminate thread watch thread if it's
                    // running even if thread is still alive (eg thread has been recycled for use in some
                    // container).
                    this.threadWatch = null;
                }

                ObjectHandle<T> handle = this.recreateObjectHandle();
                this.logicallyClosed = true;
                this.pool.releaseObject(handle);

                if (this.doubleCloseCheck) {
                    this.doubleCloseException = this.pool.captureStackTrace(CLOSED_TWICE_EXCEPTION_MESSAGE);
                }
            } else {
                if (this.doubleCloseCheck && this.doubleCloseException != null) {
                    String currentLocation = this.pool.captureStackTrace("Last closed trace from thread [" + Thread.currentThread().getName() + "]:\n");
                    LOG.error(String.format(LOG_ERROR_MESSAGE, this.doubleCloseException, currentLocation));
                }
            }
        } catch (PoolException e) {
            throw markPossiblyBroken(e);
        }
    }

    /**
     * Close off the object.
     *
     * @throws PoolException
     */
    protected void internalClose() throws PoolException {
        try {
            if (this.object != null) { // safety!
                pool.factory.destroyObject(object);
                if (!this.objectTrackingDisabled && this.finalizableRefs != null) {
                    this.finalizableRefs.remove(this.object);
                }
            }
            this.logicallyClosed = true;
        } catch (Exception e) {
            throw markPossiblyBroken(new PoolException(e));
        }
    }

    /**
     * Returns true if this connection has been (logically) closed.
     *
     * @return the logicallyClosed setting.
     */
    //	@Override
    public boolean isClosed() {
        return this.logicallyClosed;
    }

    /**
     * @return the connectionLastUsed
     */
    public long getObjectLastUsedInMillis() {
        return this.objectLastUsedInMillis;
    }

    /**
     * @param connectionLastUsed the connectionLastUsed to set
     */
    protected void setObjectLastUsedInMillis(long connectionLastUsed) {
        this.objectLastUsedInMillis = connectionLastUsed;
    }

    /**
     * @return the connectionLastReset
     */
    public long getObjectLastResetInMillis() {
        return this.objectLastResetInMillis;
    }

    /**
     * @param connectionLastReset the connectionLastReset to set
     */
    protected void setObjectLastResetInMillis(long connectionLastReset) {
        this.objectLastResetInMillis = connectionLastReset;
    }

    /**
     * Gets true if connection has triggered an exception at some point.
     *
     * @return true if the connection has triggered an error
     */
    public boolean isPossiblyBroken() {
        return this.possiblyBroken;
    }

    /**
     * Gets the partition this came from.
     *
     * @return the partition this came from
     */
    public ObjectPartition<T> getOriginatingPartition() {
        return this.originatingPartition;
    }

    /**
     * Sets Originating partition
     *
     * @param originatingPartition to set
     */
    protected void setOriginatingPartition(ObjectPartition<T> originatingPartition) {
        this.originatingPartition = originatingPartition;
    }

    /**
     * Renews this connection, i.e. Sets this connection to be logically open (although it was never really physically
     * closed)
     */
    protected void renewObject() {
        this.logicallyClosed = false;
        this.threadUsingObject = Thread.currentThread();
        if (this.doubleCloseCheck) {
            this.doubleCloseException = null;
        }
    }

    /**
     * Returns a debug handle as previously set by an application
     *
     * @return DebugHandle
     */
    public Object getDebugHandle() {
        return this.debugHandle;
    }

    /**
     * Sets a debugHandle, an object that is not used by the connection pool at all but may be set by an application to
     * track this particular connection handle for any purpose it deems fit.
     *
     * @param debugHandle any object.
     */
    public void setDebugHandle(Object debugHandle) {
        this.debugHandle = debugHandle;
    }

    /**
     * Returns the internal connection as obtained via the JDBC driver.
     *
     * @return the raw connection
     */
    public T getInternalObject() {
        return this.object;
    }

    /**
     * Sets the internal connection to use. Be careful how to use this method, normally you should never need it! This
     * is here for odd use cases only!
     *
     * @param rawConnection to set
     */
    public void setInternalObject(T rawConnection) {
        this.object = rawConnection;
    }

    /**
     * Returns the configured connection hook object.
     *
     * @return the connectionHook that was set in the config
     */
    public ObjectListener getObjectListener() {
        return this.objectListener;
    }

    /**
     * Sends a test query to the underlying connection and return true if connection is alive.
     *
     * @return True if connection is valid, false otherwise.
     */
    public boolean isObjectAlive() {
        return this.pool.isObjectHandleAlive(this);
    }

    /**
     * Returns a handle to the global pool from where this connection was obtained.
     *
     * @return BoneCP handle
     */
    public BoneOP<T> getPool() {
        return this.pool;
    }

    /**
     * This method will be intercepted by the proxy if it is enabled to return the internal target.
     *
     * @return the target.
     */
    public Object getProxyTarget() {
        try {
            return Proxy.getInvocationHandler(this.object).invoke(null, this.getClass().getMethod("getProxyTarget"), null);
        } catch (Throwable t) {
            throw new RuntimeException("BoneCP: Internal error - transaction replay log is not turned on?", t);
        }
    }

    /**
     * Returns the thread that is currently utilizing this connection.
     *
     * @return the threadUsingObject
     */
    public Thread getThreadUsingObject() {
        return this.threadUsingObject;
    }

    /**
     * Returns the connectionCreationTime field.
     *
     * @return connectionCreationTime
     */
    public long getObjectCreationTimeInMillis() {
        return this.objectCreationTimeInMillis;
    }

    /**
     * Returns true if the given connection has exceeded the maxConnectionAge.
     *
     * @return true if the connection has expired.
     */
    public boolean isExpired() {
        return this.maxObjectAgeInMillis > 0 && isExpired(System.currentTimeMillis());
    }

    /**
     * Returns true if the given connection has exceeded the maxConnectionAge.
     *
     * @param currentTime current time to use.
     * @return true if the connection has expired.
     */
    protected boolean isExpired(long currentTime) {
        return this.maxObjectAgeInMillis > 0 && (currentTime - this.objectCreationTimeInMillis) > this.maxObjectAgeInMillis;
    }

    /**
     * Returns the thread watching over this connection.
     *
     * @return threadWatch
     */
    public Thread getThreadWatch() {
        return this.threadWatch;
    }

    /**
     * Sets the thread watching over this connection.
     *
     * @param threadWatch the threadWatch to set
     */
    protected void setThreadWatch(Thread threadWatch) {
        this.threadWatch = threadWatch;
    }

    /**
     * Returns the poison field.
     *
     * @return poison
     */
    protected boolean isPoison() {
        return this.poison;
    }

    /**
     * Sets the poison.
     *
     * @param poison the poison to set
     */
    protected void setPoison(boolean poison) {
        this.poison = poison;
    }

    /**
     * Destroys the internal connection handle and creates a new one.
     *
     * @throws PoolException
     */
    public void refreshObject() throws PoolException {
        try {
            this.pool.factory.destroyObject(object);
            this.object = this.pool.obtainRawInternalObject();
        } catch (Exception ex) {
            LOG.error("Try to refresh object {}", object, ex);
        }
    }
}
