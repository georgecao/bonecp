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

import java.io.Closeable;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.ref.Reference;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.jolbox.boneop.listener.AcquireFailConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import org.apache.commons.pool.BaseObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;

/**
 * Connection pool (main class).
 *
 * @author wwadge
 * @param <T>
 *
 */
public final class BoneOP<T> extends BaseObjectPool<T> implements Serializable, Closeable {

    /**
     * The object factory to create, validate and destroy object.
     */
    protected PoolableObjectFactory<T> factory;
    /**
     * Warning message.
     */
    private static final String THREAD_CLOSE_CONNECTION_WARNING = "Thread close connection monitoring has been enabled. This will negatively impact on your performance. Only enable this option for debugging purposes!";
    /**
     * Serialization UID
     */
    private static final long serialVersionUID = -8386816681977604817L;
    /**
     * Exception message.
     */
    private static final String ERROR_TEST_CONNECTION = "Unable to open a test connection to the given database. JDBC url = %s, username = %s. Terminating connection pool. Original Exception: %s";
    /**
     * Exception message.
     */
    private static final String SHUTDOWN_LOCATION_TRACE = "Attempting to obtain a connection from a pool that has already been shutdown. \nStack trace of location where pool was shutdown follows:\n";
    /**
     * Exception message.
     */
    private static final String UNCLOSED_EXCEPTION_MESSAGE = "Connection obtained from thread [%s] was never closed. \nStack trace of location where connection was obtained follows:\n";
    /**
     * JMX constant.
     */
    public static final String MBEAN_CONFIG = "com.jolbox.bonecp:type=BoneCPConfig";
    /**
     * JMX constant.
     */
    public static final String MBEAN_BONECP = "com.jolbox.bonecp:type=BoneCP";
    /**
     * Create more connections when we hit x% of our possible number of
     * connections.
     */
    protected final int poolAvailabilityThreshold;
    /**
     * Number of partitions passed in constructor. *
     */
    protected int partitionCount;
    /**
     * Partitions handle.
     */
    protected List<ObjectPartition<T>> partitions;
    /**
     * Handle to factory that creates 1 thread per partition that periodically
     * wakes up and performs some activity on the connection.
     */
    private ScheduledExecutorService keepAliveScheduler;
    /**
     * Handle to factory that creates 1 thread per partition that periodically
     * wakes up and performs some activity on the connection.
     */
    private ScheduledExecutorService maxAliveScheduler;
    /**
     * Executor for threads watching each partition to dynamically create new
     * threads/kill off excess ones.
     */
    private ExecutorService connectionsScheduler;
    /**
     * Configuration object used in constructor.
     */
    private BoneOPConfig config;
    /**
     * If set to true, config has specified the use of helper threads.
     */
    private boolean releaseHelperThreadsConfigured;
    /**
     * pointer to the thread containing the release helper threads.
     */
    private ExecutorService releaseHelper;
    /**
     * pointer to the service containing the statement close helper threads.
     */
    private ExecutorService statementCloseHelperExecutor;
    /**
     * Executor service for obtaining a connection in an asynchronous fashion.
     */
    private ListeningExecutorService asyncExecutor;
    /**
     * Logger class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(BoneOP.class);
    /**
     * JMX support.
     */
    private MBeanServer mbs;

    /**
     * If set to true, create a new thread that monitors a connection and
     * displays warnings if application failed to close the connection.
     */
    protected boolean closeConnectionWatch = false;
    /**
     * Threads monitoring for bad connection requests.
     */
    private ExecutorService closeConnectionExecutor;
    /**
     * set to true if the connection pool has been flagged as shutting down.
     */
    protected volatile boolean poolShuttingDown;
    /**
     * Placeholder to give more useful info in case of a double shutdown.
     */
    protected String shutdownStackTrace;
    /**
     * Reference of objects that are to be watched.
     */
    private final Map<T, Reference<ObjectHandle<T>>> finalizableRefs = new ConcurrentHashMap<>();
    /**
     * Watch for connections that should have been safely closed but the
     * application forgot.
     */
    private FinalizableReferenceQueue finalizableRefQueue;
    /**
     * Time to wait before timing out the connection. Default in config is
     * Long.MAX_VALUE milliseconds.
     */
    protected long waitTimeInMs;
    /**
     * No of ms to wait for thread.join() in connection watch thread.
     */
    private long closeConnectionWatchTimeoutInMs;
    /**
     * if true, we care about statistics.
     */
    protected boolean statisticsEnabled;
    /**
     * statistics handle.
     */
    protected Statistics statistics = new Statistics(this);
    /**
     * Config setting.
     */
    @VisibleForTesting
    protected boolean externalAuth;
    /**
     * Config setting.
     */
    @VisibleForTesting
    protected boolean nullOnConnectionTimeout;
    /**
     * Config setting.
     */
    @VisibleForTesting
    protected boolean resetConnectionOnClose;
    /**
     * Config setting.
     */
    protected volatile boolean cachedPoolStrategy;
    /**
     * Currently active get connection strategy class to use.
     */
    protected volatile ObjectStrategy connectionStrategy;
    /**
     * If true, there are no connections to be taken.
     */
    private AtomicBoolean dbIsDown = new AtomicBoolean();
    /**
     * Config setting.
     */
    @VisibleForTesting
    protected Properties clientInfo;

    /**
     * Closes off this connection pool.
     */
    public synchronized void shutdown() {

        if (!this.poolShuttingDown) {
            LOG.info("Shutting down connection pool...");
            this.poolShuttingDown = true;
            this.shutdownStackTrace = captureStackTrace(SHUTDOWN_LOCATION_TRACE);
            this.keepAliveScheduler.shutdownNow(); // stop threads from firing.
            this.maxAliveScheduler.shutdownNow(); // stop threads from firing.
            this.connectionsScheduler.shutdownNow(); // stop threads from firing.

            try {
                this.connectionsScheduler.awaitTermination(5, TimeUnit.SECONDS);

                this.maxAliveScheduler.awaitTermination(5, TimeUnit.SECONDS);
                this.keepAliveScheduler.awaitTermination(5, TimeUnit.SECONDS);

                if (this.releaseHelperThreadsConfigured) {
                    this.releaseHelper.shutdownNow();
                    this.releaseHelper.awaitTermination(5, TimeUnit.SECONDS);
                }
                if (this.asyncExecutor != null) {
                    this.asyncExecutor.shutdownNow();
                    this.asyncExecutor.awaitTermination(5, TimeUnit.SECONDS);
                }
                if (this.closeConnectionExecutor != null) {
                    this.closeConnectionExecutor.shutdownNow();
                    this.closeConnectionExecutor.awaitTermination(5, TimeUnit.SECONDS);
                }
                if (!this.config.isDisableJMX()) {
                    unregisterJMX();
                }

            } catch (InterruptedException e) {
                // do nothing
            }
            this.connectionStrategy.destroyAllObjects();
            registerUnregisterJMX(false);
            LOG.info("Connection pool has been shutdown.");
        }
    }

    /**
     * Just a synonym to shutdown.
     */
    @Override
    public void close() {
        shutdown();
    }

    /**
     * Add a poison connection handle so that waiting threads are terminated.
     */
    protected void poisonAndRepopulatePartitions() {
        for (ObjectPartition<T> partition : this.partitions) {
            partition.getFreeObjects().offer(ObjectHandle.<T>createPoisonConnectionHandle());
            // send a signal to try re-populating again.
            partition.getPoolWatchThreadSignalQueue().offer(new Object()); // item being pushed is not important.
        }
    }

    /**
     * @param conn
     */
    protected void destroyObject(ObjectHandle<T> conn) {
        postDestroyConnection(conn);
        try {
            if (!conn.isPoison()) {
                conn.internalClose();
            }
        } catch (PoolException e) {
            LOG.error("Error in attempting to close connection", e);
        }
    }

    /**
     * Update counters and call hooks.
     *
     * @param handle connection handle.
     */
    protected void postDestroyConnection(ObjectHandle<T> handle) {
        ObjectPartition<T> partition = handle.getOriginatingPartition();
        if (this.finalizableRefQueue != null) { //safety
            this.finalizableRefs.remove(handle.getInternalObject());
        }
        partition.updateCreatedObjects(-1);
        partition.setUnableToCreateMoreTransactions(false); // we can create new ones now, this is an optimization

        // "Destroying" for us means: don't put it back in the pool.
        if (handle.getObjectListener() != null) {
            handle.getObjectListener().onDestroy(handle);
        }
    }

    /**
     * Returns a database connection by using Driver.getConnection() or
     * DataSource.getConnection()
     *
     * @return Connection handle
     * @throws PoolException on error
     */
    protected T obtainRawInternalObject() throws PoolException {
        try {
            return factory.makeObject();
        } catch (Exception ex) {
            throw new PoolException(ex);
        }
    }

    /**
     * Constructor.
     *
     * @param config Configuration for pool
     * @param factory to create, validate and destroy the underlying object.
     * @throws PoolException on error
     */
    public BoneOP(BoneOPConfig config, PoolableObjectFactory<T> factory) throws PoolException {
        this.factory = factory;
        this.config = config;
        config.sanitize();

        this.statisticsEnabled = config.isStatisticsEnabled();
        this.closeConnectionWatchTimeoutInMs = config.getCloseObjectWatchTimeoutInMs();
        this.poolAvailabilityThreshold = config.getPoolAvailabilityThreshold();
        this.waitTimeInMs = config.getWaitTimeInMs();
        this.externalAuth = config.isExternalAuth();

        if (this.waitTimeInMs == 0) {
            this.waitTimeInMs = Long.MAX_VALUE;
        }
        this.nullOnConnectionTimeout = config.isNullOnObjectTimeout();
        this.resetConnectionOnClose = config.isResetObjectOnClose();
        AcquireFailConfig acquireConfig = new AcquireFailConfig();
        acquireConfig.setAcquireRetryAttempts(new AtomicInteger(0));
        acquireConfig.setAcquireRetryDelayInMs(0);
        acquireConfig.setLogMessage("Failed to obtain initial connection");

        if (!config.isLazyInit()) {
            try {
                T sanityConnection = obtainRawInternalObject();
            } catch (Exception e) {
                if (config.getObjectListener() != null) {
                    config.getObjectListener().onAcquireFail(e, acquireConfig);
                }
                // TODO throw new PoolException(String.format(ERROR_TEST_CONNECTION, config.getJdbcUrl(), config.getUsername(), PoolUtil.stringifyException(e)), e);
            }
        }
        if (!config.isDisableObjectTracking()) {
            this.finalizableRefQueue = new FinalizableReferenceQueue();
        }

        this.asyncExecutor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        int helperThreads = config.getReleaseHelperThreads();
        this.releaseHelperThreadsConfigured = helperThreads > 0;

        this.config = config;
        this.partitions = new ArrayList<>(config.getPartitionCount());
        String suffix = "";

        if (config.getPoolName() != null) {
            suffix = "-" + config.getPoolName();
        }

        if (this.releaseHelperThreadsConfigured) {
            this.releaseHelper = Executors.newFixedThreadPool(helperThreads * config.getPartitionCount(), new CustomThreadFactory("BoneCP-release-thread-helper-thread" + suffix, true));
        }
        this.keepAliveScheduler = Executors.newScheduledThreadPool(config.getPartitionCount(), new CustomThreadFactory("BoneCP-keep-alive-scheduler" + suffix, true));
        this.maxAliveScheduler = Executors.newScheduledThreadPool(config.getPartitionCount(), new CustomThreadFactory("BoneCP-max-alive-scheduler" + suffix, true));
        this.connectionsScheduler = Executors.newFixedThreadPool(config.getPartitionCount(), new CustomThreadFactory("BoneCP-pool-watch-thread" + suffix, true));

        this.partitionCount = config.getPartitionCount();
        this.closeConnectionWatch = config.isCloseConnectionWatch();
        this.cachedPoolStrategy = config.getPoolStrategy() != null && config.getPoolStrategy().equalsIgnoreCase("CACHED");
        if (this.cachedPoolStrategy) {
            this.connectionStrategy = CachedObjectStrategy.getInstance(this, new DefaultObjectStrategy(this));
        } else {
            this.connectionStrategy = new DefaultObjectStrategy(this);
        }
        boolean queueLIFO = config.getServiceOrder() != null && config.getServiceOrder().equalsIgnoreCase("LIFO");
        if (this.closeConnectionWatch) {
            LOG.warn(THREAD_CLOSE_CONNECTION_WARNING);
            this.closeConnectionExecutor = Executors.newCachedThreadPool(new CustomThreadFactory("BoneCP-connection-watch-thread" + suffix, true));

        }
        for (int p = 0; p < config.getPartitionCount(); p++) {

            ObjectPartition connectionPartition = new ObjectPartition(this);
            this.partitions.add(p, connectionPartition);
            TransferQueue<ObjectHandle<T>> connectionHandles;
            if (config.getMaxObjectsPerPartition() == config.getMinObjectsPerPartition()) {
                // if we have a pool that we don't want resized, make it even faster by ignoring
                // the size constraints.
                connectionHandles = queueLIFO ? new LIFOQueue<ObjectHandle<T>>() : new LinkedTransferQueue<ObjectHandle<T>>();
            } else {
                connectionHandles = queueLIFO ? new LIFOQueue<ObjectHandle<T>>(this.config.getMaxObjectsPerPartition()) : new BoundedLinkedTransferQueue<ObjectHandle<T>>(this.config.getMaxObjectsPerPartition());
            }

            this.partitions.get(p).setFreeObjects(connectionHandles);

            if (!config.isLazyInit()) {
                for (int i = 0; i < config.getMinObjectsPerPartition(); i++) {
                    final ObjectHandle<T> handle = ObjectHandle.createConnectionHandle(this);
                    this.partitions.get(p).addFreeObject(handle);
                }
            }
            if (config.getIdleConnectionTestPeriod(TimeUnit.SECONDS) > 0 || config.getIdleMaxAge(TimeUnit.SECONDS) > 0) {

                final Runnable connectionTester = new ObjectTesterThread(connectionPartition, this.keepAliveScheduler, this, config.getIdleMaxAge(TimeUnit.MILLISECONDS), config.getIdleConnectionTestPeriod(TimeUnit.MILLISECONDS), queueLIFO);
                long delayInSeconds = config.getIdleConnectionTestPeriod(TimeUnit.SECONDS);
                if (delayInSeconds == 0L) {
                    delayInSeconds = config.getIdleMaxAge(TimeUnit.SECONDS);
                }
                if (config.getIdleMaxAge(TimeUnit.SECONDS) != 0 && config.getIdleConnectionTestPeriod(TimeUnit.SECONDS) != 0 && config.getIdleMaxAge(TimeUnit.SECONDS) < delayInSeconds) {
                    delayInSeconds = config.getIdleMaxAge(TimeUnit.SECONDS);
                }
                this.keepAliveScheduler.schedule(connectionTester, delayInSeconds, TimeUnit.SECONDS);
            }

            if (config.getMaxObjectAgeInSeconds() > 0) {
                final Runnable connectionMaxAgeTester = new ObjectMaxAgeThread(connectionPartition, this.maxAliveScheduler, this, config.getMaxConnectionAge(TimeUnit.MILLISECONDS), queueLIFO);
                this.maxAliveScheduler.schedule(connectionMaxAgeTester, config.getMaxObjectAgeInSeconds(), TimeUnit.SECONDS);
            }
            // watch this partition for low no of threads
            this.connectionsScheduler.execute(new PoolWatchThread(connectionPartition, this));
        }
        if (!this.config.isDisableJMX()) {
            registerUnregisterJMX(true);
        }

    }

    /**
     * Initializes JMX stuff.
     *
     * @param doRegister if true, perform registration, if false unregister
     */
    protected void registerUnregisterJMX(boolean doRegister) {
        if (this.mbs == null) { // this way makes it easier for mocking.
            this.mbs = ManagementFactory.getPlatformMBeanServer();
        }
        try {
            String suffix = "";

            if (this.config.getPoolName() != null) {
                suffix = "-" + this.config.getPoolName();
            }

            ObjectName name = new ObjectName(MBEAN_BONECP + suffix);
            ObjectName configname = new ObjectName(MBEAN_CONFIG + suffix);

            if (doRegister) {
                if (!this.mbs.isRegistered(name)) {
                    this.mbs.registerMBean(this.statistics, name);
                }
                if (!this.mbs.isRegistered(configname)) {
                    this.mbs.registerMBean(this.config, configname);
                }
            } else {
                if (this.mbs.isRegistered(name)) {
                    this.mbs.unregisterMBean(name);
                }
                if (this.mbs.isRegistered(configname)) {
                    this.mbs.unregisterMBean(configname);
                }
            }
        } catch (Exception e) {
            LOG.error("Unable to start/stop JMX", e);
        }
    }

    /**
     * Returns a free connection.
     *
     * @return Connection handle.
     * @throws PoolException
     */
    public T getConnection() throws PoolException {
        ObjectHandle<T> handle = this.connectionStrategy.getObject();
        return handle.getInternalObject();
    }

    /**
     * Starts off a new thread to monitor this connection attempt.
     *
     * @param connectionHandle to monitor
     */
    protected void watchObject(ObjectHandle<T> connectionHandle) {
        String message = captureStackTrace(UNCLOSED_EXCEPTION_MESSAGE);
        this.closeConnectionExecutor.submit(new CloseThreadMonitor(Thread.currentThread(), connectionHandle, message, this.closeConnectionWatchTimeoutInMs));
    }

    /**
     * Throw an exception to capture it so as to be able to print it out later
     * on
     *
     * @param message message to display
     * @return Stack trace message
     *
     */
    protected String captureStackTrace(String message) {
        StringBuilder stringBuilder = new StringBuilder(String.format(message, Thread.currentThread().getName()));
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
            stringBuilder.append(" " + trace[i] + "\r\n");
        }

        stringBuilder.append("");

        return stringBuilder.toString();
    }

    /**
     * Obtain a connection asynchronously by queueing a request to obtain a
     * connection in a separate thread.
     *
     * Use as follows:
     * <p>
     * Future&lt;Connection&gt; result = pool.getAsyncConnection();
     * <p>
     * ... do something else in your application here
     * ...<p>
     * Connection connection = result.get(); // get the connection<p>
     *
     * @return A Future task returning a connection.
     */
    public ListenableFuture<T> getAsyncConnection() {

        return this.asyncExecutor.submit(new Callable<T>() {

            @Override
            public T call() throws Exception {
                return getConnection();
            }
        });
    }

    /**
     * Tests if this partition has hit a threshold and signal to the pool watch
     * thread to create new connections
     *
     * @param connectionPartition to test for.
     */
    protected void maybeSignalForMoreConnections(ObjectPartition connectionPartition) {

        if (!connectionPartition.isUnableToCreateMoreTransactions() && !this.poolShuttingDown
                && connectionPartition.getAvailableConnections() * 100 / connectionPartition.getMaxObjects() <= this.poolAvailabilityThreshold) {
            connectionPartition.getPoolWatchThreadSignalQueue().offer(new Object()); // item being pushed is not important.
        }
    }

    protected void releaseConnection(T connection) throws PoolException {
        // TODO connect to handle
        releaseConnection(connection);
    }

    /**
     * Releases the given connection back to the pool. This method is not
     * intended to be called by applications (hence set to protected). Call
     * connection.close() instead which will return the connection back to the
     * pool.
     *
     * @param handle connection to release
     * @throws PoolException
     */
    protected void releaseConnection(ObjectHandle<T> handle) throws PoolException {

        // hook calls
        if (handle.getObjectListener() != null) {
            handle.getObjectListener().onCheckIn(handle);
        }

        // release immediately or place it in a queue so that another thread will eventually close it. If we're shutting down,
        // close off the connection right away because the helper threads have gone away.
        if (!this.poolShuttingDown && this.releaseHelperThreadsConfigured && !this.cachedPoolStrategy) {
            if (!handle.getOriginatingPartition().getObjectsPendingRelease().tryTransfer(handle)) {
                handle.getOriginatingPartition().getObjectsPendingRelease().put(handle);
            }
        } else {
            internalReleaseObject(handle);
        }
    }

    /**
     * Release a connection by placing the connection back in the pool.
     *
     * @param objectHandle Object being released.
     * @throws PoolException
     *
     */
    protected void internalReleaseObject(ObjectHandle<T> objectHandle) throws PoolException {

        if (objectHandle.isExpired() || (!this.poolShuttingDown && objectHandle.isPossiblyBroken()
                && !isObjectHandleAlive(objectHandle))) {

            if (objectHandle.isExpired()) {
                objectHandle.internalClose();
            }

            ObjectPartition connectionPartition = objectHandle.getOriginatingPartition();
            postDestroyConnection(objectHandle);
            maybeSignalForMoreConnections(connectionPartition);
            return; // don't place back in queue - connection is broken or expired.
        }

        objectHandle.setObjectLastUsedInMs(System.currentTimeMillis());
        if (!this.poolShuttingDown) {

            putConnectionBackInPartition(objectHandle);
        } else {
            objectHandle.internalClose();
        }
    }

    /**
     * Places a connection back in the originating partition.
     *
     * @param objectHandle to place back
     * @throws PoolException on error
     */
    protected void putConnectionBackInPartition(ObjectHandle<T> objectHandle) throws PoolException {

        if (this.cachedPoolStrategy && objectHandle.inUseInThreadLocalContext.get()) {
            // this might fail if we have a thread that takes up more than one thread
            // (we only track one)
            objectHandle.inUseInThreadLocalContext.set(false);
        } else {
            TransferQueue<ObjectHandle<T>> queue = objectHandle.getOriginatingPartition().getFreeObjects();
            if (!queue.tryTransfer(objectHandle)) {
                if (!queue.offer(objectHandle)) {
                    objectHandle.internalClose();
                }
            }
        }

    }

    /**
     * Sends a dummy statement to the server to keep the connection alive
     *
     * @param handle Connection handle to perform activity on
     * @return true if test query worked, false otherwise
     */
    public boolean isObjectHandleAlive(ObjectHandle<T> handle) {
        boolean result = false;
        boolean logicallyClosed = handle.logicallyClosed;
        try {
            if (logicallyClosed) {
                handle.logicallyClosed = false; // avoid checks later on if it's marked as closed.
            }
            result = factory.validateObject(handle.getInternalObject());
        } catch (Exception ignored) {
            // connection must be broken!
            result = false;
        } finally {
            handle.logicallyClosed = logicallyClosed;
            handle.setObjectLastResetInMs(System.currentTimeMillis());
        }
        return result;
    }

    /**
     * Return total number of connections currently in use by an application
     *
     * @return no of leased connections
     */
    public int getTotalLeased() {
        int total = 0;
        for (ObjectPartition<T> partition : partitions) {
            total += partition.getCreatedObjects() - partition.getAvailableConnections();
        }
        return total;
    }

    /**
     * Return the number of free connections available to an application right
     * away (excluding connections that can be created dynamically)
     *
     * @return number of free connections
     */
    public int getTotalFree() {
        int total = 0;
        for (ObjectPartition<T> partition : partitions) {
            total += partition.getAvailableConnections();
        }
        return total;
    }

    /**
     * Return total number of connections created in all partitions.
     *
     * @return number of created connections
     */
    public int getTotalCreatedObjects() {
        int total = 0;
        for (ObjectPartition<T> partition : partitions) {
            total += partition.getCreatedObjects();
        }
        return total;
    }

    /**
     * Gets config object.
     *
     * @return config object
     */
    public BoneOPConfig getConfig() {
        return this.config;
    }

    /**
     * @return the releaseHelper
     */
    protected ExecutorService getReleaseHelper() {
        return this.releaseHelper;
    }

    /**
     * @param releaseHelper the releaseHelper to set
     */
    protected void setReleaseHelper(ExecutorService releaseHelper) {
        this.releaseHelper = releaseHelper;
    }

    /**
     * Return the finalizable refs handle.
     *
     * @return the finalizableRefs value.
     */
    protected Map<T, Reference<ObjectHandle<T>>> getFinalizableRefs() {
        return this.finalizableRefs;
    }

    /**
     * Watch for connections that should have been safely closed but the
     * application forgot.
     *
     * @return the finalizableRefQueue
     */
    protected FinalizableReferenceQueue getFinalizableRefQueue() {
        return this.finalizableRefQueue;
    }

    /**
     * Returns the statementCloseHelper field.
     *
     * @return statementCloseHelper
     */
    protected ExecutorService getStatementCloseHelperExecutor() {
        return this.statementCloseHelperExecutor;
    }

    /**
     * Sets the statementCloseHelper field.
     *
     * @param statementCloseHelper the statementCloseHelper to set
     */
    protected void setStatementCloseHelperExecutor(ExecutorService statementCloseHelper) {
        this.statementCloseHelperExecutor = statementCloseHelper;
    }

    /**
     * Returns the releaseHelperThreadsConfigured field.
     *
     * @return releaseHelperThreadsConfigured
     */
    protected boolean isReleaseHelperThreadsConfigured() {
        return this.releaseHelperThreadsConfigured;
    }

    /**
     * Returns a reference to the statistics class.
     *
     * @return statistics
     */
    public Statistics getStatistics() {
        return this.statistics;
    }

    /**
     * Returns the dbIsDown field.
     *
     * @return dbIsDown
     */
    public AtomicBoolean getDbIsDown() {
        return this.dbIsDown;
    }

    /**
     * Unregisters JMX stuff.
     */
    protected void unregisterJMX() {
        if (this.mbs == null) {
            return;
        }
        try {
            String suffix = "";

            if (this.config.getPoolName() != null) {
                suffix = "-" + this.config.getPoolName();
            }

            ObjectName name = new ObjectName(MBEAN_BONECP + suffix);
            ObjectName configname = new ObjectName(MBEAN_CONFIG + suffix);

            if (this.mbs.isRegistered(name)) {
                this.mbs.unregisterMBean(name);
            }
            if (this.mbs.isRegistered(configname)) {
                this.mbs.unregisterMBean(configname);
            }
        } catch (Exception e) {
            LOG.error("Unable to unregister JMX", e);
        }
    }

    @Override
    public T borrowObject() throws Exception {
        return getConnection();
    }

    @Override
    public void returnObject(T obj) throws Exception {
        releaseConnection(obj);
    }

    @Override
    public void invalidateObject(T obj) throws Exception {
        releaseConnection(obj);
        postDestroyConnection(null);
        releaseConnection(obj);
    }
}
