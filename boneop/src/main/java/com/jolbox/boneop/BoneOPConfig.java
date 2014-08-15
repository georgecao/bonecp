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

import com.jolbox.boneop.listener.ObjectListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Configuration class.
 *
 * @author wallacew
 */
public class BoneOPConfig implements BoneOPConfigMBean, Cloneable, Serializable {

    /**
     * Serialization UID.
     */
    private static final long serialVersionUID = 6090570773474131622L;
    /**
     * Logger class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(BoneOPConfig.class);
    /**
     * Min number of connections per partition.
     */
    private int minObjectsPerPartition;
    /**
     * Max number of connections per partition.
     */
    private int maxObjectsPerPartition;
    /**
     * Number of new connections to create in 1 batch.
     */
    private int acquireIncrement = 2;
    /**
     * Number of partitions.
     */
    private int partitionCount = 1;
    /**
     * Connections older than this are sent a keep-alive statement.
     */
    private long idleObjectTestPeriodInSeconds = 240 * 60;
    /**
     * Maximum age of an unused connection before it is closed off.
     */
    private long idleMaxAgeInSeconds = 60 * 60;

    /**
     * Number of release-connection helper threads to create per partition.
     */
    private int releaseHelperThreads = 3;
    /**
     * Hook class (external).
     */
    private ObjectListener objectListener;
    /**
     * If set to true, create a new thread that monitors a connection and displays warnings if application failed to
     * close the connection. FOR DEBUG PURPOSES ONLY!
     */
    private boolean closeObjectWatch;
    /**
     * After attempting to acquire a connection and failing, wait for this value before attempting to acquire a new
     * connection again.
     */
    private long acquireRetryDelayInMillis = 7000;
    /**
     * After attempting to acquire a connection and failing, try to connect these many times before giving up.
     */
    private int acquireRetryAttempts = 5;
    /**
     * If set to true, the connection pool will remain empty until the first connection is obtained.
     */
    private boolean lazyInit;
    /**
     * Connection hook class name.
     */
    private String objectListenerClassName;
    /**
     * Class loader to use when loading the JDBC driver.
     */
    private ClassLoader classLoader = this.getClassLoader();
    /**
     * Name of the pool for JMX and thread names.
     */
    private String poolName;
    /**
     * Set to true to disable JMX.
     */
    private boolean disableJMX;
    /**
     * Queries taking longer than this limit to execute are logged.
     */
    private long objectOccupyTimeLimitInMillis = 0;
    /**
     * Create more connections when we hit x% of our possible number of connections.
     */
    private int poolAvailabilityThreshold = 20;
    /**
     * Disable connection tracking.
     */
    private boolean disableObjectTracking = false;
    /**
     * Time to wait before a call to getConnection() times out and returns an error.
     */
    private long waitTimeInMillis = 0;
    /**
     * Time in ms to wait for close connection watch thread.
     */
    private long closeObjectWatchTimeoutInMillis = 0;
    /**
     * A connection older than maxConnectionAge will be destroyed and purged from the pool.
     */
    private long maxObjectAgeInSeconds = 0;
    /**
     * Config property.
     */
    private String configFile;
    /**
     * Queue mode. Values currently understood are FIFO and LIFO.
     */
    private String serviceOrder;
    /**
     * If true, keep track of some statistics.
     */
    private boolean statisticsEnabled;

    /**
     * The default catalog state of created objects.
     */
    private String defaultCatalog;

    /**
     * If true, return null on connection timeout rather than throw an exception.
     */
    private boolean nullOnObjectTimeout;
    /**
     * If true, issue a reset (rollback) on connection close in case client forgot it.
     */
    private boolean resetObjectOnClose;

    /**
     * Determines pool operation Recognised strategies are: DEFAULT, CACHED.
     */
    private String poolStrategy = "DEFAULT";

    /**
     * Default constructor. Attempts to fill settings in this order: 1. boneop-default-config.xml file, usually found in
     * the pool jar 2. bonecp-config.xml file, usually found in your application's classpath 3. Other hardcoded defaults
     * in BoneCPConfig class.
     */
    public BoneOPConfig() {
        // try to load the default config file, if available from somewhere in the classpath
        loadProperties("/boneop-default-config.xml");
        // try to override with app specific config, if available
        loadProperties("/boneop-config.xml");
    }

    /**
     * Creates a new config using the given properties.
     *
     * @param props properties to set.
     * @throws Exception on error
     */
    public BoneOPConfig(Properties props) throws Exception {
        this();
        this.setProperties(props);
    }

    /**
     * Initialize the configuration by loading bonecp-config.xml containing the settings.
     *
     * @param sectionName section to load
     * @throws Exception on parse errors
     */
    public BoneOPConfig(String sectionName) throws Exception {
        this(BoneOPConfig.class.getResourceAsStream("/boneop-config.xml"), sectionName);
    }

    /**
     * Initialise the configuration by loading an XML file containing the settings.
     *
     * @param xmlConfigFile file to load
     * @param sectionName   section to load
     * @throws Exception
     */
    public BoneOPConfig(InputStream xmlConfigFile, String sectionName) throws Exception {
        this();
        setXMLProperties(xmlConfigFile, sectionName);
    }

    /**
     * Returns the name of the pool for JMX and thread names.
     *
     * @return a pool name.
     */
    @Override
    public String getPoolName() {
        return this.poolName;
    }

    /**
     * Sets the name of the pool for JMX and thread names.
     *
     * @param poolName to set.
     */
    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    /**
     * {@inheritDoc}
     *
     * @see #getMinObjectsPerPartition()
     */
    @Override
    public int getMinObjectsPerPartition() {
        return this.minObjectsPerPartition;
    }

    /**
     * Sets the minimum number of connections that will be contained in every partition. Also refer to
     * {@link #setPoolAvailabilityThreshold(int)}.
     *
     * @param minObjectsPerPartition number of connections
     */
    public void setMinObjectsPerPartition(int minObjectsPerPartition) {
        this.minObjectsPerPartition = minObjectsPerPartition;
    }

    /**
     * {@inheritDoc}
     *
     * @see BoneOPConfigMBean#getMaxObjectsPerPartition()
     */
    @Override
    public int getMaxObjectsPerPartition() {
        return this.maxObjectsPerPartition;
    }

    /**
     * Sets the maximum number of connections that will be contained in every partition. Setting this to 5 with 3
     * partitions means you will have 15 unique connections to the database. Note that the connection pool will not
     * create all these connections in one go but rather start off with minObjectsPerPartition and gradually
     * increase connections as required.
     *
     * @param maxObjectsPerPartition number of connections.
     */
    public void setMaxObjectsPerPartition(int maxObjectsPerPartition) {
        this.maxObjectsPerPartition = maxObjectsPerPartition;
    }

    /**
     * {@inheritDoc}
     *
     * @see BoneOPConfigMBean#getAcquireIncrement()
     */
    @Override
    public int getAcquireIncrement() {
        return this.acquireIncrement;
    }

    /**
     * Sets the acquireIncrement property.
     * <p/>
     * When the available connections are about to run out, BoneCP will dynamically create new ones in batches. This
     * property controls how many new connections to create in one go (up to a maximum of maxConnectionsPerPartition).
     * <p/>
     * Note: This is a per partition setting.
     *
     * @param acquireIncrement value to set.
     */
    public void setAcquireIncrement(int acquireIncrement) {
        this.acquireIncrement = acquireIncrement;
    }

    /**
     * {@inheritDoc}
     *
     * @see BoneOPConfigMBean#getPartitionCount()
     */
    @Override
    public int getPartitionCount() {
        return this.partitionCount;
    }

    /**
     * Sets number of partitions to use.
     * <p/>
     * In order to reduce lock contention and thus improve performance, each incoming connection request picks off a
     * connection from a pool that has thread-affinity, i.e. pool[threadId % partition_count]. The higher this number,
     * the better your performance will be for the case when you have plenty of short-lived threads. Beyond a certain
     * threshold, maintenance of these pools will start to have a negative effect on performance (and only for the case
     * when connections on a partition start running out). Has no effect in a CACHED strategy.
     * <p/>
     * <p/>
     * Default: 1, minimum: 1, recommended: 2-4 (but very app specific)
     *
     * @param partitionCount to set
     */
    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.jolbox.boneop.BoneOPConfigMBean#getIdleObjectTestPeriodInMinutes()
     */
    @Override
    public long getIdleObjectTestPeriodInMinutes() {
        return TimeUnit.SECONDS.toMinutes(this.idleObjectTestPeriodInSeconds);
    }

    /**
     * Sets the idleConnectionTestPeriod.
     * <p/>
     * This sets the time (in minutes), for a connection to remain idle before sending a test query to the DB. This is
     * useful to prevent a DB from timing out connections on its end. Do not use aggressive values here!
     * <p/>
     * <p/>
     * <p/>
     * Default: 240 min, set to 0 to disable
     *
     * @param idleConnectionTestPeriod to set
     */
    public void setIdleObjectTestPeriodInMinutes(long idleConnectionTestPeriod) {
        setIdleObjectTestPeriod(idleConnectionTestPeriod, TimeUnit.MINUTES);
    }

    /**
     * Returns the idleConnectionTestPeriod with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return Idle Connection test period
     */
    public long getIdleObjectTestPeriod(TimeUnit timeUnit) {
        return timeUnit.convert(this.idleObjectTestPeriodInSeconds, TimeUnit.SECONDS);
    }

    /**
     * Sets the idleConnectionTestPeriod.
     * <p/>
     * This sets the time (in seconds), for a connection to remain idle before sending a test query to the DB. This is
     * useful to prevent a DB from timing out connections on its end. Do not use aggressive values here!
     * <p/>
     * <p/>
     * <p/>
     * Default: 240 min, set to 0 to disable
     *
     * @param idleObjectTestPeriod to set
     */
    public void setIdleObjectTestPeriodInSeconds(long idleObjectTestPeriod) {
        setIdleObjectTestPeriod(idleObjectTestPeriod, TimeUnit.SECONDS);
    }

    /**
     * Wrapper method for idleConnectionTestPeriod for easier programmatic access.
     *
     * @param idleObjectTestPeriod time for a connection to remain idle before sending a test query to the DB.
     * @param timeUnit             Time granularity of given parameter.
     */
    public void setIdleObjectTestPeriod(long idleObjectTestPeriod, TimeUnit timeUnit) {
        this.idleObjectTestPeriodInSeconds = timeUnit.toSeconds(idleObjectTestPeriod);
    }

    /**
     * Returns the idleMaxAge currently set.
     *
     * @return idleMaxAge in minutes
     */
    @Override
    public long getIdleMaxAgeInMinutes() {
        return TimeUnit.SECONDS.toMinutes(this.idleMaxAgeInSeconds);
    }

    /**
     * Sets Idle max age (in min).
     * <p/>
     * The time (in minutes), for a connection to remain unused before it is closed off. Do not use aggressive values
     * here!
     * <p/>
     * <p/>
     * Default: 60 minutes, set to 0 to disable.
     *
     * @param idleMaxAge to set
     */
    public void setIdleMaxAgeInMinutes(long idleMaxAge) {
        setIdleMaxAge(idleMaxAge, TimeUnit.MINUTES);
    }

    /**
     * Sets Idle max age (in seconds).
     * <p/>
     * The time (in seconds), for a connection to remain unused before it is closed off. Do not use aggressive values
     * here!
     * <p/>
     * <p/>
     * Default: 60 minutes, set to 0 to disable.
     *
     * @param idleMaxAge to set
     */
    public void setIdleMaxAgeInSeconds(long idleMaxAge) {
        setIdleMaxAge(idleMaxAge, TimeUnit.SECONDS);
    }

    /**
     * Sets Idle max age.
     * <p/>
     * The time, for a connection to remain unused before it is closed off. Do not use aggressive values here!
     *
     * @param idleMaxAge time after which a connection is closed off
     * @param timeUnit   idleMaxAge time granularity.
     */
    public void setIdleMaxAge(long idleMaxAge, TimeUnit timeUnit) {
        this.idleMaxAgeInSeconds = timeUnit.toSeconds(idleMaxAge);
    }

    public long getIdleMaxAge(TimeUnit timeUnit) {
        return timeUnit.convert(this.idleMaxAgeInSeconds, TimeUnit.SECONDS);
    }

    /**
     * {@inheritDoc}
     *
     * @see BoneOPConfigMBean#getReleaseHelperThreads()
     */
    @Override
    public int getReleaseHelperThreads() {
        return this.releaseHelperThreads;
    }

    /**
     * Sets number of helper threads to create that will handle releasing a connection.
     * <p/>
     * When this value is set to zero, the application thread is blocked until the pool is able to perform all the
     * necessary cleanup to recycle the connection and make it available for another thread.
     * <p/>
     * When a non-zero value is set, the pool will create threads that will take care of recycling a connection when it
     * is closed (the application dumps the connection into a temporary queue to be processed asychronously to the
     * application via the release helper threads).
     * <p/>
     * Useful when your application is doing lots of work on each connection (i.e. perform an SQL query, do lots of
     * non-DB stuff and perform another query), otherwise will probably slow things down.
     *
     * @param releaseHelperThreads no to release
     */
    public void setReleaseHelperThreads(int releaseHelperThreads) {
        this.releaseHelperThreads = releaseHelperThreads;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.jolbox.boneop.BoneOPConfigMBean#getObjectListener()
     */
    @Override
    public ObjectListener getObjectListener() {
        return this.objectListener;
    }

    /**
     * Sets the connection hook.
     * <p/>
     * Fully qualified class name that implements the ConnectionHook interface (or extends AbstractConnectionHook).
     * BoneCP will callback the specified class according to the connection state (onAcquire, onCheckIn, onCheckout,
     * onDestroy).
     *
     * @param objectListener the connectionHook to set
     */
    public void setObjectListener(ObjectListener objectListener) {
        this.objectListener = objectListener;
    }

    /**
     * Returns if BoneCP is configured to create a helper thread to watch over connection acquires that are never
     * released (or released twice). FOR DEBUG PURPOSES ONLY.
     *
     * @return the current closeObjectWatch setting.
     */
    public boolean isCloseObjectWatch() {
        return this.closeObjectWatch;
    }

    /**
     * Instruct the pool to create a helper thread to watch over connection acquires that are never released (or
     * released twice). This is for debugging purposes only and will create a new thread for each call to
     * getConnection(). Enabling this option will have a big negative impact on pool performance.
     *
     * @param closeObjectWatch set to true to enable thread monitoring.
     */
    public void setCloseObjectWatch(boolean closeObjectWatch) {
        this.closeObjectWatch = closeObjectWatch;
    }

    /**
     * Returns the number of ms to wait before attempting to obtain a connection again after a failure. Default: 7000.
     *
     * @return the acquireRetryDelay
     */
    @Override
    public long getAcquireRetryDelayInMillis() {
        return this.acquireRetryDelayInMillis;
    }

    /**
     * Sets the number of ms to wait before attempting to obtain a connection again after a failure.
     *
     * @param acquireRetryDelay the acquireRetryDelay to set
     */
    public void setAcquireRetryDelayInMillis(long acquireRetryDelay) {
        setAcquireRetryDelay(acquireRetryDelay, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the acquireRetryDelay setting with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return acquireRetryDelay
     */
    public long getAcquireRetryDelay(TimeUnit timeUnit) {
        return timeUnit.convert(this.acquireRetryDelayInMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the number of ms to wait before attempting to obtain a connection again after a failure.
     *
     * @param acquireRetryDelay the acquireRetryDelay to set
     * @param timeUnit          time granularity
     */
    public void setAcquireRetryDelay(long acquireRetryDelay, TimeUnit timeUnit) {
        this.acquireRetryDelayInMillis = TimeUnit.MILLISECONDS.convert(acquireRetryDelay, timeUnit);
    }

    /**
     * Returns true if connection pool is to be initialized lazily.
     *
     * @return lazyInit setting
     */
    @Override
    public boolean isLazyInit() {
        return this.lazyInit;
    }

    /**
     * Set to true to force the connection pool to obtain the initial connections lazily.
     *
     * @param lazyInit the lazyInit setting to set
     */
    public void setLazyInit(boolean lazyInit) {
        this.lazyInit = lazyInit;
    }

    /**
     * After attempting to acquire a connection and failing, try to connect these many times before giving up. Default
     * 5.
     *
     * @return the acquireRetryAttempts value
     */
    @Override
    public int getAcquireRetryAttempts() {
        return this.acquireRetryAttempts;
    }

    /**
     * After attempting to acquire a connection and failing, try to connect these many times before giving up. Default
     * 5.
     *
     * @param acquireRetryAttempts the acquireRetryAttempts to set
     */
    public void setAcquireRetryAttempts(int acquireRetryAttempts) {
        this.acquireRetryAttempts = acquireRetryAttempts;
    }

    /**
     * Returns the connection hook class name as passed via the setter
     *
     * @return the objectListenerClassName.
     */
    @Override
    public String getObjectListenerClassName() {
        return this.objectListenerClassName;
    }

    /**
     * Sets the connection hook class name. Consider using setConnectionHook() instead.
     *
     * @param objectListenerClassName the connectionHook class name to set
     */
    public void setObjectListenerClassName(String objectListenerClassName) {
        this.objectListenerClassName = objectListenerClassName;
        if (objectListenerClassName != null) {
            Object listenerClass;
            try {
                listenerClass = loadClass(objectListenerClassName).newInstance();
                this.objectListener = (ObjectListener) listenerClass;
            } catch (Exception e) {
                LOG.error("Unable to create an instance of the object listener class (" + objectListenerClassName + ")");
                this.objectListener = null;
            }
        }
    }

    /**
     * Return true if JMX is disabled.
     *
     * @return the disableJMX.
     */
    @Override
    public boolean isDisableJMX() {
        return this.disableJMX;
    }

    /**
     * Set to true to disable JMX.
     *
     * @param disableJMX the disableJMX to set
     */
    public void setDisableJMX(boolean disableJMX) {
        this.disableJMX = disableJMX;
    }

    /**
     * Return the query execute time limit.
     *
     * @return the queryTimeLimit
     */
    @Override
    public long getObjectOccupyTimeLimitInMillis() {
        return this.objectOccupyTimeLimitInMillis;
    }

    /**
     * Queries taking longer than this limit to execute are logged.
     *
     * @param objectOccupyTimeLimit the limit to set in milliseconds.
     */
    public void setObjectOccupyTimeLimitInMillis(long objectOccupyTimeLimit) {
        setObjectOccupyTimeLimit(objectOccupyTimeLimit, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the queryExecuteTimeLimit setting with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return queryExecuteTimeLimit period
     */
    public long getObjectOccupyTimeLimit(TimeUnit timeUnit) {
        return timeUnit.convert(this.objectOccupyTimeLimitInMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Queries taking longer than this limit to execute are logged.
     *
     * @param objectOccupyTimeLimit the limit to set in milliseconds.
     * @param timeUnit
     */
    public void setObjectOccupyTimeLimit(long objectOccupyTimeLimit, TimeUnit timeUnit) {
        this.objectOccupyTimeLimitInMillis = timeUnit.toMillis(objectOccupyTimeLimit);
    }

    /**
     * Returns the pool watch connection threshold value.
     *
     * @return the poolAvailabilityThreshold currently set.
     */
    @Override
    public int getPoolAvailabilityThreshold() {
        return this.poolAvailabilityThreshold;
    }

    /**
     * Sets the Pool Watch thread threshold.
     * <p/>
     * The pool watch thread attempts to maintain a number of connections always available (between minConnections and
     * maxConnections). This value sets the percentage value to maintain. For example, setting it to 20 means that if
     * the following condition holds: Free Connections / MaxConnections < poolAvailabilityThreshold
     * <p/>
     * new connections will be created. In other words, it tries to keep at least 20% of the pool full of connections.
     * Setting the value to zero will make the pool create new connections when it needs them but it also means your
     * application may have to wait for new connections to be obtained at times.
     * <p/>
     * Default: 20.
     *
     * @param poolAvailabilityThreshold the poolAvailabilityThreshold to set
     */
    public void setPoolAvailabilityThreshold(int poolAvailabilityThreshold) {
        this.poolAvailabilityThreshold = poolAvailabilityThreshold;
    }

    /**
     * Returns true if connection tracking has been disabled.
     *
     * @return the disableConnectionTracking
     */
    @Override
    public boolean isDisableObjectTracking() {
        return this.disableObjectTracking;
    }

    /**
     * If set to true, the pool will not monitor connections for proper closure. Enable this option if you only ever
     * obtain your connections via a mechanism that is guaranteed to release the connection back to the pool (eg
     * Spring's jdbcTemplate, some kind of transaction manager, etc).
     *
     * @param disableObjectTracking set to true to disable. Default: false.
     */
    public void setDisableObjectTracking(boolean disableObjectTracking) {
        this.disableObjectTracking = disableObjectTracking;
    }

    /**
     * Returns the maximum time (in milliseconds) to wait before a call to getObject is timed out.
     *
     * @return the connectionTimeout
     */
    public long getWaitTimeInMillis() {
        return this.waitTimeInMillis;
    }

    /**
     * Sets the maximum time (in milliseconds) to wait before a call to getConnection is timed out.
     * <p/>
     * Setting this to zero is similar to setting it to Long.MAX_VALUE
     * <p/>
     * Default: 0 ( = wait forever )
     *
     * @param waitTimeInMillis the connectionTimeout to set
     */
    public void setWaitTimeInMillis(long waitTimeInMillis) {
        setWaitTime(waitTimeInMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the connectionTimeout with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return connectionTimeout period
     */
    public long getWaitTime(TimeUnit timeUnit) {
        return timeUnit.convert(this.waitTimeInMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum time to wait before a call to getConnection is timed out.
     * <p/>
     * Setting this to zero is similar to setting it to Long.MAX_VALUE
     *
     * @param waitTimeout timeout
     * @param timeUnit    the unit of the connectionTimeout argument
     */
    public void setWaitTime(long waitTimeout, TimeUnit timeUnit) {
        this.waitTimeInMillis = TimeUnit.MILLISECONDS.convert(waitTimeout, timeUnit);
    }

    /**
     * Returns the no of ms to wait when close connection watch threads are enabled. 0 = wait forever.
     *
     * @return the watchTimeout currently set.
     */
    @Override
    public long getCloseObjectWatchTimeoutInMillis() {
        return this.closeObjectWatchTimeoutInMillis;
    }

    /**
     * Sets the no of ms to wait when close connection watch threads are enabled. 0 = wait forever.
     *
     * @param closeConnectionWatchTimeout the watchTimeout to set
     */
    public void setCloseObjectWatchTimeoutInMillis(long closeConnectionWatchTimeout) {
        setCloseConnectionWatchTimeout(closeConnectionWatchTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the closeConnectionWatchTimeout with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return closeConnectionWatchTimeout period
     */
    public long getCloseConnectionWatchTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(this.closeObjectWatchTimeoutInMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the time to wait when close connection watch threads are enabled. 0 = wait forever.
     *
     * @param closeConnectionWatchTimeout the watchTimeout to set
     * @param timeUnit                    Time granularity
     */
    public void setCloseConnectionWatchTimeout(long closeConnectionWatchTimeout, TimeUnit timeUnit) {
        this.closeObjectWatchTimeoutInMillis = TimeUnit.MILLISECONDS.convert(closeConnectionWatchTimeout, timeUnit);
    }

    /**
     * Returns the maxConnectionAge field in seconds
     *
     * @return maxConnectionAge
     */
    public long getMaxObjectAgeInSeconds() {
        return this.maxObjectAgeInSeconds;
    }

    /**
     * Sets the maxConnectionAge in seconds. Any connections older than this setting will be closed off whether it is
     * idle or not. Connections currently in use will not be affected until they are returned to the pool.
     *
     * @param maxObjectAgeInSeconds the maxConnectionAge to set
     */
    public void setMaxObjectAgeInSeconds(long maxObjectAgeInSeconds) {
        setMaxObjectAge(maxObjectAgeInSeconds, TimeUnit.SECONDS);
    }

    /**
     * Returns the maxConnectionAge with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return maxObjectAge period
     */
    public long getMaxObjectAge(TimeUnit timeUnit) {
        return timeUnit.convert(this.maxObjectAgeInSeconds, TimeUnit.SECONDS);
    }

    /**
     * Sets the maxConnectionAge. Any connections older than this setting will be closed off whether it is idle or not.
     * Connections currently in use will not be affected until they are returned to the pool.
     *
     * @param maxObjectAge the maxConnectionAge to set.
     * @param timeUnit     the unit of the maxConnectionAge argument.
     */
    public void setMaxObjectAge(long maxObjectAge, TimeUnit timeUnit) {
        this.maxObjectAgeInSeconds = TimeUnit.SECONDS.convert(maxObjectAge, timeUnit);
    }

    /**
     * Returns the configFile field.
     *
     * @return configFile
     */
    public String getConfigFile() {
        return this.configFile;
    }

    /**
     * Sets the configFile. If configured, this will cause the pool to initialise using the config file in the same way
     * as if calling new BoneCPConfig(filename).
     *
     * @param configFile the configFile to set
     */
    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    /**
     * Returns the serviceOrder field.
     *
     * @return serviceOrder
     */
    public String getServiceOrder() {
        return this.serviceOrder;
    }

    /**
     * Sets the queue serviceOrder. Values currently understood are FIFO and LIFO.
     *
     * @param serviceOrder the serviceOrder to set
     */
    public void setServiceOrder(String serviceOrder) {
        this.serviceOrder = serviceOrder;
    }

    /**
     * Returns the statisticsEnabled field.
     *
     * @return statisticsEnabled
     */
    public boolean isStatisticsEnabled() {
        return this.statisticsEnabled;
    }

    /**
     * If set to true, keep track of some more statistics for exposure via JMX. Will slow down the pool operation.
     *
     * @param statisticsEnabled set to true to enable
     */
    public void setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }

    /**
     * Returns the defaultCatalog field.
     *
     * @return defaultCatalog
     */
    public String getDefaultCatalog() {
        return this.defaultCatalog;
    }

    /**
     * Sets the defaultCatalog setting for newly created connections. If not set, use driver default.
     *
     * @param defaultCatalog the defaultCatalog to set
     */
    public void setDefaultCatalog(String defaultCatalog) {
        this.defaultCatalog = defaultCatalog;
    }


    /**
     * @param xmlConfigFile file to load
     * @param sectionName   section to load
     * @throws Exception
     */
    private void setXMLProperties(InputStream xmlConfigFile, String sectionName)
            throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db;
        // ugly XML parsing, but this is built-in the JDK.
        db = dbf.newDocumentBuilder();
        Document doc = db.parse(xmlConfigFile);
        doc.getDocumentElement().normalize();
        Properties settings = parseXML(doc, null);
        if (sectionName != null) {
            // override with custom settings
            settings.putAll(parseXML(doc, sectionName));
        }
        setProperties(settings);
    }

    /**
     * Lowercases the first character.
     *
     * @param name
     * @return the same string with the first letter in lowercase
     */
    private String lowerFirst(String name) {
        return name.substring(0, 1).toLowerCase() + name.substring(1);
    }

    /**
     * Sets the properties by reading off entries in the given parameter (where each key is equivalent to the field
     * name)
     *
     * @param props Parameter list to set
     * @throws Exception on error
     */
    public void setProperties(Properties props) throws Exception {
        // Use reflection to read in all possible properties of int, String or boolean.
        for (Method method : BoneOPConfig.class.getDeclaredMethods()) {
            String tmp = null;
            if (method.getName().startsWith("is")) {
                tmp = lowerFirst(method.getName().substring(2));
            } else if (method.getName().startsWith("set")) {
                tmp = lowerFirst(method.getName().substring(3));
            } else {
                continue;
            }

            if (method.getParameterTypes().length == 1 && method.getParameterTypes()[0].equals(int.class)) {
                String val = props.getProperty(tmp);
                if (val == null) {
                    val = props.getProperty("boneop." + tmp); // hibernate provider style
                }
                if (val != null) {
                    try {
                        method.invoke(this, Integer.parseInt(val));
                    } catch (NumberFormatException e) {
                        // do nothing, use the default value
                    }
                }
            } else if (method.getParameterTypes().length == 1 && method.getParameterTypes()[0].equals(long.class)) {
                String val = props.getProperty(tmp);
                if (val == null) {
                    val = props.getProperty("boneop." + tmp); // hibernate provider style
                }
                if (val != null) {
                    try {
                        method.invoke(this, Long.parseLong(val));
                    } catch (NumberFormatException e) {
                        // do nothing, use the default value
                    }
                }
            } else if (method.getParameterTypes().length == 1 && method.getParameterTypes()[0].equals(String.class)) {
                String val = props.getProperty(tmp);
                if (val == null) {
                    val = props.getProperty("boneop." + tmp); // hibernate provider style
                }
                if (val != null) {
                    method.invoke(this, val);
                }
            }
            if (method.getParameterTypes().length == 1 && method.getParameterTypes()[0].equals(boolean.class)) {
                String val = props.getProperty(tmp);
                if (val == null) {
                    val = props.getProperty("boneop." + tmp); // hibernate provider style
                }
                if (val != null) {
                    method.invoke(this, Boolean.parseBoolean(val));
                }
            }
        }
    }

    /**
     * Parses the given XML doc to extract the properties and return them into a java.util.Properties.
     *
     * @param doc         to parse
     * @param sectionName which section to extract
     * @return Properties map
     */
    private Properties parseXML(Document doc, String sectionName) {
        int found = -1;
        Properties results = new Properties();
        NodeList config = null;
        if (sectionName == null) {
            config = doc.getElementsByTagName("default-config");
            found = 0;
        } else {
            config = doc.getElementsByTagName("named-config");
            if (config != null && config.getLength() > 0) {
                for (int i = 0; i < config.getLength(); i++) {
                    Node node = config.item(i);
                    if (node.getNodeType() == Node.ELEMENT_NODE) {
                        NamedNodeMap attributes = node.getAttributes();
                        if (attributes != null && attributes.getLength() > 0) {
                            Node name = attributes.getNamedItem("name");
                            if (name.getNodeValue().equalsIgnoreCase(sectionName)) {
                                found = i;
                                break;
                            }
                        }
                    }
                }
            }

            if (found == -1) {
                config = null;
                LOG.warn("Did not find " + sectionName + " section in config file. Reverting to defaults.");
            }
        }

        if (config != null && config.getLength() > 0) {
            Node node = config.item(found);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element elementEntry = (Element) node;
                NodeList childNodeList = elementEntry.getChildNodes();
                for (int j = 0; j < childNodeList.getLength(); j++) {
                    Node node_j = childNodeList.item(j);
                    if (node_j.getNodeType() == Node.ELEMENT_NODE) {
                        Element piece = (Element) node_j;
                        NamedNodeMap attributes = piece.getAttributes();
                        if (attributes != null && attributes.getLength() > 0) {
                            results.put(attributes.item(0).getNodeValue(), piece.getTextContent());
                        }
                    }
                }
            }
        }

        return results;
    }

    /**
     * Performs validation on the config object.
     */
    public void sanitize() {
        if (this.configFile != null) {
            loadProperties(this.configFile);
        }

        if (this.poolStrategy == null || !(this.poolStrategy.equalsIgnoreCase("DEFAULT") || this.poolStrategy.equalsIgnoreCase("CACHED"))) {
            LOG.warn("Unrecognised pool strategy. Allowed values are DEFAULT and CACHED. Setting to DEFAULT.");
            this.poolStrategy = "DEFAULT";
        }

        this.poolStrategy = this.poolStrategy.toUpperCase();

        if ((this.poolAvailabilityThreshold < 0) || (this.poolAvailabilityThreshold > 100)) {
            this.poolAvailabilityThreshold = 20;
        }
        if (this.maxObjectsPerPartition < 1) {
            LOG.warn("Max Connections < 1. Setting to 20");
            this.maxObjectsPerPartition = 20;
        }
        if (this.minObjectsPerPartition < 0) {
            LOG.warn("Min Connections < 0. Setting to 1");
            this.minObjectsPerPartition = 1;
        }

        if (this.minObjectsPerPartition > this.maxObjectsPerPartition) {
            LOG.warn("Min Connections > max connections");
            this.minObjectsPerPartition = this.maxObjectsPerPartition;
        }
        if (this.acquireIncrement <= 0) {
            LOG.warn("acquireIncrement <= 0. Setting to 1.");
            this.acquireIncrement = 1;
        }
        if (this.partitionCount < 1) {
            LOG.warn("partitions < 1! Setting to 1");
            this.partitionCount = 1;
        } else {
            this.partitionCount = findNextPositivePowerOfTwo(this.partitionCount);
        }

        if (this.releaseHelperThreads < 0) {
            LOG.warn("releaseHelperThreads < 0! Setting to 3");
            this.releaseHelperThreads = 3;
        }
        if (this.waitTimeInMillis <= 0) {
            LOG.warn("waitTimeInMillis <= 0! Setting to {}", Long.MAX_VALUE);
            this.waitTimeInMillis = Long.MAX_VALUE;
        }
        if (this.acquireRetryDelayInMillis <= 0) {
            LOG.warn("waitTimeInMillis <= 0! Setting to {}", 1000);
            this.acquireRetryDelayInMillis = 1000;
        }
        this.serviceOrder = this.serviceOrder != null ? this.serviceOrder.toUpperCase() : "FIFO";

        if (!(this.serviceOrder.equals("FIFO") || this.serviceOrder.equals("LIFO"))) {
            LOG.warn("Queue service order is not set to FIFO or LIFO. Defaulting to FIFO.");
            this.serviceOrder = "FIFO";
        }

    }

    public static int findNextPositivePowerOfTwo(final int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    /**
     * Loads the given properties file using the class loader.
     *
     * @param filename Config filename to load
     */
    protected void loadProperties(String filename) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL url = classLoader.getResource(filename);
        if (url != null) {
            try {
                this.setXMLProperties(url.openStream(), null);
            } catch (Exception e) {
                // do nothing
            }
        }
    }

    /**
     * Loads the given class, respecting the given class loader.
     *
     * @param clazz class to load
     * @return Loaded class
     * @throws ClassNotFoundException
     */
    protected Class<?> loadClass(String clazz) throws ClassNotFoundException {
        if (this.classLoader == null) {
            return Class.forName(clazz);
        }
        return Class.forName(clazz, true, this.classLoader);
    }

    /**
     * Returns the currently active classloader.
     *
     * @return the classLoader
     */
    public ClassLoader getClassLoader() {
        return this.classLoader;
    }

    /**
     * Sets the class loader to use to load JDBC driver and hooks (set to null to use default).
     *
     * @param classLoader the classLoader to set
     */
    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public BoneOPConfig clone() throws CloneNotSupportedException {

        BoneOPConfig clone = (BoneOPConfig) super.clone();
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            try {
                field.set(clone, field.get(this));
            } catch (Exception e) {
                // should never happen
            }
        }
        return clone;
    }

    /**
     * Returns true if this instance has the same config as a given config.
     *
     * @param that
     * @return true if the instance has the same config, false otherwise.
     */
    public boolean hasSameConfiguration(BoneOPConfig that) {
        return that != null && Objects.equals(this.acquireIncrement, that.getAcquireIncrement())
                && Objects.equals(this.acquireRetryDelayInMillis, that.getAcquireRetryDelayInMillis())
                && Objects.equals(this.closeObjectWatch, that.isCloseObjectWatch())
                && Objects.equals(this.objectListener, that.getObjectListener())
                && Objects.equals(this.idleObjectTestPeriodInSeconds, that.getIdleObjectTestPeriod(TimeUnit.SECONDS))
                && Objects.equals(this.idleMaxAgeInSeconds, that.getIdleMaxAge(TimeUnit.SECONDS))
                && Objects.equals(this.maxObjectsPerPartition, that.getMaxObjectsPerPartition())
                && Objects.equals(this.minObjectsPerPartition, that.getMinObjectsPerPartition())
                && Objects.equals(this.partitionCount, that.getPartitionCount())
                && Objects.equals(this.releaseHelperThreads, that.getReleaseHelperThreads())
                && Objects.equals(this.lazyInit, that.isLazyInit())
                && Objects.equals(this.acquireRetryAttempts, that.getAcquireRetryAttempts())
                && Objects.equals(this.closeObjectWatchTimeoutInMillis, that.getCloseObjectWatchTimeoutInMillis())
                && Objects.equals(this.waitTimeInMillis, that.getWaitTimeInMillis())
                && Objects.equals(this.objectOccupyTimeLimitInMillis, that.getObjectOccupyTimeLimitInMillis())
                && Objects.equals(this.poolAvailabilityThreshold, that.getPoolAvailabilityThreshold())
                && Objects.equals(this.poolName, that.getPoolName())
                && Objects.equals(this.disableObjectTracking, that.isDisableObjectTracking());

    }

    /**
     * Returns the nullOnConnectionTimeout field.
     *
     * @return nullOnConnectionTimeout
     */
    public boolean isNullOnObjectTimeout() {
        return this.nullOnObjectTimeout;
    }

    /**
     * Sets the nullOnConnectionTimeout.
     * <p/>
     * If true, return null on connection timeout rather than throw an exception. This performs better but must be
     * handled differently in your application. This only makes sense when using the connectionTimeout config option.
     *
     * @param nullOnObjectTimeout the nullOnConnectionTimeout to set
     */
    public void setNullOnObjectTimeout(boolean nullOnObjectTimeout) {
        this.nullOnObjectTimeout = nullOnObjectTimeout;
    }

    /**
     * Returns the resetConnectionOnClose setting.
     *
     * @return resetConnectionOnClose
     */
    public boolean isResetObjectOnClose() {
        return this.resetObjectOnClose;
    }

    /**
     * If true, issue a reset (rollback) on connection close in case client forgot it.
     *
     * @param resetObjectOnClose the resetConnectionOnClose to set
     */
    public void setResetObjectOnClose(boolean resetObjectOnClose) {
        this.resetObjectOnClose = resetObjectOnClose;
    }

    /**
     * Returns the poolStrategy field.
     *
     * @return poolStrategy
     */
    public String getPoolStrategy() {
        return this.poolStrategy;
    }

    /**
     * Sets the poolStrategy. Currently supported strategies are DEFAULT and CACHED.
     * <p/>
     * DEFAULT strategy operates in a manner that has been used in the pool since the very first version: it tries to
     * obtain a connection from a queue.
     * <p/>
     * CACHED stores each connection in a thread-local variable so that next time the same thread asks for a connection,
     * it gets the same one assigned to it (if it asks for more than one, it will be allocated a new one). This is very
     * fast but you must ensure that the number of threads asking for a connection is less than or equal to the number
     * of connections you have made available. Should you exceed this limit, the pool will switch back (permanently) to
     * the DEFAULT strategy which will cause a one-time performance hit. Use this strategy if your threads are managed
     * eg in a Tomcat environment where you can limit the number of threads that it can handle. A typical use case would
     * be a web service that always requires some form of database access, therefore a service would have little point
     * in accepting a new incoming socket connection if it still has to wait in order to obtain a connection.
     * <p/>
     * Essentially this means that you are pushing back the lock down to the socket or thread layer. While the first few
     * thread hits will be slower than in the DEFAULT strategy, significant performance gains are to be expected as the
     * thread gets increasingly re-used (i.e. initially you should expect the first few rounds to be measurably slower
     * than the DEFAULT strategy but once the caches get more hits you should get >2x better performance).
     * <p/>
     * Threads that are killed off are detected during the next garbage collection and result in their allocated
     * connections from being taken back though since GC timing is not guaranteed you should ideally set your minimum
     * pool size to be equal to the maximum pool size.
     * <p/>
     * Therefore for best results, make sure that the configured minConnectionPerPartition = maxConnectionPerPartition =
     * min Threads = max Threads.
     *
     * @param poolStrategy the poolStrategy to set
     */
    public void setPoolStrategy(String poolStrategy) {
        this.poolStrategy = poolStrategy;
    }
}
