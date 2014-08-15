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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.jolbox.boneop.listener.AcquireFailConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.Closeable;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connection pool (main class).
 *
 * @param <T>
 * @author wwadge
 */
public class BoneOP<T> implements Serializable, Closeable {

    /**
     * JMX constant.
     */
    public static final String MBEAN_CONFIG = "com.jolbox.boneop:type=BoneOPConfig";
    /**
     * JMX constant.
     */
    public static final String MBEAN_BONECP = "com.jolbox.boneop:type=BoneOP";
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
     * Logger class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(BoneOP.class);
    /**
     * Create more connections when we hit x% of our possible number of connections.
     */
    protected final int poolAvailabilityThreshold;
    /**
     * Handle to factory that creates 1 thread per partition that periodically wakes up and performs some activity on
     * the connection.
     */
    private final ScheduledExecutorService keepAliveScheduler;
    /**
     * Handle to factory that creates 1 thread per partition that periodically wakes up and performs some activity on
     * the connection.
     */
    private final ScheduledExecutorService maxAliveScheduler;
    /**
     * Executor for threads watching each partition to dynamically create new threads/kill off excess ones.
     */
    private final ExecutorService objectScheduler;
    /**
     * If set to true, config has specified the use of helper threads.
     */
    private final boolean releaseHelperThreadsConfigured;
    /**
     * Executor service for obtaining a connection in an asynchronous fashion.
     */
    private final ListeningExecutorService asyncExecutor;
    /**
     * Reference of objects that are to be watched.
     */
    private final Map<T, Reference<ObjectHandle<T>>> finalizableRefs = new ConcurrentHashMap<>();
    /**
     * No of ms to wait for thread.join() in connection watch thread.
     */
    private final long closeObjectWatchTimeoutInMs;
    /**
     * If true, there are no connections to be taken.
     */
    private final AtomicBoolean down = new AtomicBoolean(false);
    /**
     * The object factory to create, validate and destroy object.
     */
    protected PoolableObjectFactory<T> factory;
    /**
     * Number of partitions passed in constructor. *
     */
    protected final int partitionCount;
    /**
     * Partitions handle.
     */
    protected List<ObjectPartition<T>> partitions;
    /**
     * If set to true, create a new thread that monitors a connection and displays warnings if application failed to
     * close the connection.
     */
    protected boolean closeObjectWatch = false;
    /**
     * set to true if the connection pool has been flagged as shutting down.
     */
    protected volatile boolean poolShuttingDown;
    /**
     * Placeholder to give more useful info in case of a double shutdown.
     */
    protected String shutdownStackTrace;
    /**
     * Time to wait before timing out the object. Default in config is Long.MAX_VALUE milliseconds.
     */
    protected final long waitTimeInMillis;
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
    protected boolean nullOnObjectTimeout;
    /**
     * Config setting.
     */
    @VisibleForTesting
    protected boolean resetObjectOnClose;
    /**
     * Config setting.
     */
    protected volatile boolean cachedPoolStrategy;
    /**
     * Currently active get object strategy class to use.
     */
    protected volatile ObjectStrategy<T> objectStrategy;
    /**
     * Configuration object used in constructor.
     */
    private BoneOPConfig config;
    /**
     * pointer to the thread containing the release helper threads.
     */
    private ExecutorService releaseHelper;
    /**
     * pointer to the service containing the statement close helper threads.
     */
    private ExecutorService statementCloseHelperExecutor;
    /**
     * JMX support.
     */
    private MBeanServer mbs;
    /**
     * Threads monitoring for bad connection requests.
     */
    private ExecutorService closeConnectionExecutor;
    /**
     * Watch for connections that should have been safely closed but the application forgot.
     */
    private FinalizableReferenceQueue finalizableRefQueue;

    /**
     * Constructor.
     *
     * @param config  Configuration for pool
     * @param factory to create, validate and destroy the underlying object.
     * @throws PoolException on error
     */
    public BoneOP(BoneOPConfig config, PoolableObjectFactory<T> factory) throws PoolException {
        this.factory = factory;
        this.config = config;
        config.sanitize();

        this.statisticsEnabled = config.isStatisticsEnabled();
        this.closeObjectWatchTimeoutInMs = config.getCloseObjectWatchTimeoutInMillis();
        this.poolAvailabilityThreshold = config.getPoolAvailabilityThreshold();
        this.waitTimeInMillis = config.getWaitTimeInMillis();
        this.nullOnObjectTimeout = config.isNullOnObjectTimeout();
        this.resetObjectOnClose = config.isResetObjectOnClose();
        AcquireFailConfig acquireConfig = new AcquireFailConfig();
        acquireConfig.setAcquireRetryAttempts(new AtomicInteger(0));
        acquireConfig.setAcquireRetryDelayInMillis(0);
        acquireConfig.setLogMessage("Failed to obtain initial connection");

        if (!config.isLazyInit()) {
            try {
                T sanityObject = obtainRawInternalObject();
                LOG.info("Factory work normally. {}", sanityObject);
            } catch (PoolException e) {
                if (config.getObjectListener() != null) {
                    config.getObjectListener().onAcquireFail(e, acquireConfig);
                }
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
            this.releaseHelper = Executors.newFixedThreadPool(helperThreads * config.getPartitionCount(), new CustomThreadFactory("BoneOP-release-thread-helper-thread" + suffix, true));
        }
        this.keepAliveScheduler = Executors.newScheduledThreadPool(config.getPartitionCount(), new CustomThreadFactory("BoneOP-keep-alive-scheduler" + suffix, true));
        this.maxAliveScheduler = Executors.newScheduledThreadPool(config.getPartitionCount(), new CustomThreadFactory("BoneOP-max-alive-scheduler" + suffix, true));
        this.objectScheduler = Executors.newFixedThreadPool(config.getPartitionCount(), new CustomThreadFactory("BoneOP-pool-watch-thread" + suffix, true));

        this.partitionCount = config.getPartitionCount();
        this.closeObjectWatch = config.isCloseObjectWatch();
        this.cachedPoolStrategy = config.getPoolStrategy() != null && config.getPoolStrategy().equalsIgnoreCase("CACHED");
        if (this.cachedPoolStrategy) {
            this.objectStrategy = CachedObjectStrategy.getInstance(this, new DefaultObjectStrategy<>(this));
        } else {
            this.objectStrategy = new DefaultObjectStrategy<>(this);
        }
        boolean queueLIFO = config.getServiceOrder() != null && config.getServiceOrder().equalsIgnoreCase("LIFO");
        if (this.closeObjectWatch) {
            LOG.warn(THREAD_CLOSE_CONNECTION_WARNING);
            this.closeConnectionExecutor = Executors.newCachedThreadPool(new CustomThreadFactory("BoneOP-connection-watch-thread" + suffix, true));
        }
        for (int p = 0; p < config.getPartitionCount(); p++) {

            ObjectPartition<T> partition = new ObjectPartition<>(this);
            this.partitions.add(p, partition);
            TransferQueue<ObjectHandle<T>> objectHandles;
            if (config.getMaxObjectsPerPartition() == config.getMinObjectsPerPartition()) {
                // if we have a pool that we don't want re-sized, make it even faster by ignoring
                // the size constraints.
                objectHandles = queueLIFO ? new LIFOQueue<ObjectHandle<T>>() : new LinkedTransferQueue<ObjectHandle<T>>();
            } else {
                objectHandles = queueLIFO ? new LIFOQueue<ObjectHandle<T>>(this.config.getMaxObjectsPerPartition()) : new BoundedLinkedTransferQueue<ObjectHandle<T>>(this.config.getMaxObjectsPerPartition());
            }

            partition.setFreeObjects(objectHandles);

            if (!config.isLazyInit()) {
                for (int i = 0; i < config.getMinObjectsPerPartition(); i++) {
                    final ObjectHandle<T> handle = ObjectHandle.createObjectHandle(this);
                    partition.addFreeObject(handle);
                }
            }
            if (config.getIdleObjectTestPeriod(TimeUnit.SECONDS) > 0 || config.getIdleMaxAge(TimeUnit.SECONDS) > 0) {

                final Runnable connectionTester = new ObjectTesterThread<>(partition, this.keepAliveScheduler, this, config.getIdleMaxAge(TimeUnit.MILLISECONDS), config.getIdleObjectTestPeriod(TimeUnit.MILLISECONDS), queueLIFO);
                long delayInSeconds = config.getIdleObjectTestPeriod(TimeUnit.SECONDS);
                if (delayInSeconds == 0L) {
                    delayInSeconds = config.getIdleMaxAge(TimeUnit.SECONDS);
                }
                if (config.getIdleMaxAge(TimeUnit.SECONDS) != 0 && config.getIdleObjectTestPeriod(TimeUnit.SECONDS) != 0 && config.getIdleMaxAge(TimeUnit.SECONDS) < delayInSeconds) {
                    delayInSeconds = config.getIdleMaxAge(TimeUnit.SECONDS);
                }
                this.keepAliveScheduler.schedule(connectionTester, delayInSeconds, TimeUnit.SECONDS);
            }

            if (config.getMaxObjectAgeInSeconds() > 0) {
                final ObjectMaxAgeThread<T> connectionMaxAgeTester
                        = new ObjectMaxAgeThread<>(partition, this.maxAliveScheduler,
                        this, config.getMaxObjectAge(TimeUnit.MILLISECONDS), queueLIFO);
                this.maxAliveScheduler.schedule(connectionMaxAgeTester, config.getMaxObjectAgeInSeconds(), TimeUnit.SECONDS);
            }
            // watch this partition for low NO. of threads
            this.objectScheduler.execute(new PoolWatchThread<>(partition, this));
        }
        if (!this.config.isDisableJMX()) {
            registerUnregisterJMX(true);
        }

    }

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
            this.objectScheduler.shutdownNow(); // stop threads from firing.

            try {
                this.objectScheduler.awaitTermination(5, TimeUnit.SECONDS);

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
            this.objectStrategy.destroy();
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
            partition.getFreeObjects().offer(ObjectHandle.<T>createPoisonObjectHandle());
            // send a signal to try re-populating again.
            partition.getPoolWatchThreadSignalQueue().offer(new Object()); // item being pushed is not important.
        }
    }

    /**
     * Destroying the object by don't put it back in the pool.
     *
     * @param handle object handle
     */
    protected void destroyObject(ObjectHandle<T> handle) {
        postDestroyObject(handle);
        try {
            if (!handle.isPoison()) {
                handle.internalClose();
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
    protected void postDestroyObject(ObjectHandle<T> handle) {
        ObjectPartition<T> partition = handle.getOriginatingPartition();
        if (this.finalizableRefQueue != null) { //safety
            this.finalizableRefs.remove(handle.getInternalObject());
        }
        partition.updateCreatedObjects(-1);
        partition.setUnableToCreateMoreObjects(false); // we can create new ones now, this is an optimization

        // "Destroying" for us means: don't put it back in the pool.
        if (handle.getObjectListener() != null) {
            handle.getObjectListener().onDestroy(handle);
        }
    }

    /**
     * Create an object by using {@link PoolableObjectFactory#makeObject()}.
     *
     * @return brand-new object just created. Must not return null ever.
     * @throws PoolException on error.
     */
    protected T obtainRawInternalObject() throws PoolException {
        try {
            return factory.makeObject();
        } catch (Exception ex) {
            throw new PoolException(ex);
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
    public T getObject() throws PoolException {
        ObjectHandle<T> handle = this.objectStrategy.take();
        return handle.getInternalObject();
    }

    /**
     * Starts off a new thread to monitor this connection attempt.
     *
     * @param objectHandle to monitor
     */
    protected void watchObject(ObjectHandle<T> objectHandle) {
        String message = captureStackTrace(UNCLOSED_EXCEPTION_MESSAGE);
        this.closeConnectionExecutor.submit(new CloseThreadMonitor<>(Thread.currentThread(), objectHandle, message, this.closeObjectWatchTimeoutInMs));
    }

    /**
     * Throw an exception to capture it so as to be able to print it out later on
     *
     * @param message message to display
     * @return Stack trace message
     */
    protected String captureStackTrace(String message) {
        StringBuilder stringBuilder = new StringBuilder(String.format(message, Thread.currentThread().getName()));
        StackTraceElement[] traces = Thread.currentThread().getStackTrace();
        for (StackTraceElement trace : traces) {
            stringBuilder.append(" ").append(trace).append("\r\n");
        }

        stringBuilder.append("");

        return stringBuilder.toString();
    }

    /**
     * Obtain a connection asynchronously by queueing a request to obtain a connection in a separate thread.
     * <p/>
     * Use as follows:
     * <p/>
     * Future&lt;Connection&gt; result = pool.getAsyncConnection();
     * <p/>
     * ... do something else in your application here
     * ...<p>
     * Connection connection = result.get(); // get the connection<p>
     *
     * @return A Future task returning a connection.
     */
    public ListenableFuture<T> getAsyncObject() {

        return this.asyncExecutor.submit(new Callable<T>() {

            @Override
            public T call() throws Exception {
                return getObject();
            }
        });
    }

    /**
     * Tests if this partition has hit a threshold and signal to the pool watch thread to create new connections
     *
     * @param partition to test for.
     */
    protected void maybeSignalForMoreObjects(ObjectPartition<T> partition) {
        if (!partition.isUnableToCreateMoreObjects() && !this.poolShuttingDown
                && partition.getAvailableObjects() * 100 / partition.getMaxObjects() <= this.poolAvailabilityThreshold) {
            partition.getPoolWatchThreadSignalQueue().offer(new Object()); // item being pushed is not important.
        }
    }

    protected void releaseObject(T object) throws PoolException {
        releaseObject(getObjectHandle(object));
    }

    /**
     * Releases the given connection back to the pool. This method is not intended to be called by applications (hence
     * set to protected). Call connection.close() instead which will return the connection back to the pool.
     *
     * @param handle connection to release
     * @throws PoolException
     */
    protected void releaseObject(ObjectHandle<T> handle) throws PoolException {

        // hook calls
        if (handle.getObjectListener() != null) {
            handle.getObjectListener().onCheckIn(handle);
        }

        // release immediately or place it in a queue so that another thread will eventually close it. If we're shutting down,
        // close off the connection right away because the helper threads have gone away.
        if (!this.poolShuttingDown && this.releaseHelperThreadsConfigured && !this.cachedPoolStrategy) {
            if (!handle.getOriginatingPartition().getObjectsPendingRelease().tryTransfer(handle) && !handle.getOriginatingPartition().getObjectsPendingRelease().offer(handle)) {
                try {
                    handle.getOriginatingPartition().getObjectsPendingRelease().put(handle);
                } catch (InterruptedException e) {
                    LOG.error("Cannot release object {}", handle, e);
                }
            }
        } else {
            internalReleaseObject(handle);
        }
    }

    /**
     * Release a object by placing the connection back in the pool.
     *
     * @param objectHandle Object being released.
     * @throws PoolException
     */
    protected void internalReleaseObject(ObjectHandle<T> objectHandle) throws PoolException {

        if (objectHandle.isExpired() || (!this.poolShuttingDown && objectHandle.isPossiblyBroken()
                && !isObjectHandleAlive(objectHandle))) {

            if (objectHandle.isExpired()) {
                objectHandle.internalClose();
            }

            ObjectPartition<T> connectionPartition = objectHandle.getOriginatingPartition();
            postDestroyObject(objectHandle);
            maybeSignalForMoreObjects(connectionPartition);
            return; // don't place back in queue - connection is broken or expired.
        }
        objectHandle.setObjectLastUsedInMillis(System.currentTimeMillis());
        if (!this.poolShuttingDown) {
            putObjectBackInPartition(objectHandle);
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
    protected void putObjectBackInPartition(ObjectHandle<T> objectHandle) throws PoolException {

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
            handle.setObjectLastResetInMillis(System.currentTimeMillis());
        }
        return result;
    }

    /**
     * Return total number of objects currently in use by an application
     *
     * @return NO. of leased objects
     */
    public int getTotalLeased() {
        int total = 0;
        for (ObjectPartition<T> partition : partitions) {
            total += partition.getCreatedObjects() - partition.getAvailableObjects();
        }
        return total;
    }

    /**
     * Return the number of free connections available to an application right away (excluding connections that can be
     * created dynamically)
     *
     * @return number of free connections
     */
    public int getTotalFree() {
        int total = 0;
        for (ObjectPartition<T> partition : partitions) {
            total += partition.getAvailableObjects();
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
     * Watch for connections that should have been safely closed but the application forgot.
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
     * Returns the down field.
     *
     * @return down
     */
    public AtomicBoolean getDown() {
        return this.down;
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

    private ObjectHandle<T> getObjectHandle(T object) {
        Reference<ObjectHandle<T>> reference = finalizableRefs.get(object);
        if (null != reference) {
            return reference.get();
        }
        return null;
    }
}
