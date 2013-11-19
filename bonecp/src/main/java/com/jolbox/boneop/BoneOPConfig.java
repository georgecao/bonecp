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

import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.jolbox.bonecp.hooks.ObjectListener;
import java.util.Objects;

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
    private static final Logger logger = LoggerFactory.getLogger(BoneOPConfig.class);
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
     * SQL statement to use for keep-alive/test of connection.
     */
    private String objectTestStatement;
    /**
     * Number of release-connection helper threads to create per partition.
     */
    private int releaseHelperThreads = 3;
    /**
     * Hook class (external).
     */
    private ObjectListener objectListener;
    /**
     * Query to send once per connection to the database.
     */
    private String initSQL;
    /**
     * If set to true, create a new thread that monitors a connection and
     * displays warnings if application failed to close the connection. FOR
     * DEBUG PURPOSES ONLY!
     */
    private boolean closeConnectionWatch;
    /**
     * If set to true, log SQL statements being executed.
     */
    private boolean logStatementsEnabled;
    /**
     * After attempting to acquire a connection and failing, wait for this value
     * before attempting to acquire a new connection again.
     */
    private long acquireRetryDelayInMs = 7000;
    /**
     * After attempting to acquire a connection and failing, try to connect
     * these many times before giving up.
     */
    private int acquireRetryAttempts = 5;
    /**
     * If set to true, the connection pool will remain empty until the first
     * connection is obtained.
     */
    private boolean lazyInit;
    /**
     * If set to true, stores all activity on this connection to allow for
     * replaying it again.
     */
    private boolean transactionRecoveryEnabled;
    /**
     * Connection hook class name.
     */
    private String connectionHookClassName;
    /**
     * Classloader to use when loading the JDBC driver.
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
     * If set, use datasourceBean.getConnection() to obtain a new connection.
     */
    private DataSource datasourceBean;
    /**
     * Queries taking longer than this limit to execute are logged.
     */
    private long queryExecuteTimeLimitInMs = 0;
    /**
     * Create more connections when we hit x% of our possible number of
     * connections.
     */
    private int poolAvailabilityThreshold = 20;
    /**
     * Disable connection tracking.
     */
    private boolean disableObjectTracking;
    /**
     * Used when the alternate way of obtaining a connection is required
     */
    private Properties driverProperties;
    /**
     * Time to wait before a call to getConnection() times out and returns an
     * error.
     */
    private long waitTimeInMs = 0;
    /**
     * Time in ms to wait for close connection watch thread.
     */
    private long closeObjectWatchTimeoutInMs = 0;
    /**
     * A connection older than maxConnectionAge will be destroyed and purged
     * from the pool.
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
     * The default auto-commit state of created connections.
     */
    private boolean defaultAutoCommit;
    /**
     * The default read-only state of created connections.
     */
    private boolean defaultReadOnly;
    /**
     * The default transaction isolation state of created connections.
     */
    private String defaultTransactionIsolation;
    /**
     * The default catalog state of created connections.
     */
    private String defaultCatalog;
    /**
     * The parsed transaction isolation value. Default = driver value
     */
    private int defaultTransactionIsolationValue = -1;
    /**
     * If true, stop caring about username/password when obtaining raw
     * connections.
     */
    private boolean externalAuth;
    /**
     * If true, try to unregister the JDBC driver when pool is shutdown.
     */
    private boolean deregisterDriverOnClose;
    /**
     * If true, return null on connection timeout rather than throw an
     * exception.
     */
    private boolean nullOnObjectTimeout;
    /**
     * If true, issue a reset (rollback) on connection close in case client
     * forgot it.
     */
    private boolean resetObjectOnClose;
    /**
     * Detect uncommitted transactions. If true, and resetConnectionOnClose is
     * also true, the pool will print out a stack trace of the location where
     * you had a connection that specified setAutoCommit(false) but then forgot
     * to call commit/rollback before closing it off. This feature is intended
     * for debugging only.
     */
    private boolean detectUnresolvedTransactions;
    /**
     * Determines pool operation Recognised strategies are: DEFAULT, CACHED.
     */
    private String poolStrategy = "DEFAULT";
    /**
     * If true, track statements and close them if application forgot to do so.
     * See also: detectUnclosedStatements.
     */
    private boolean closeOpenStatements;
    /**
     * If true, print out a stack trace of where a statement was opened but not
     * closed before the connection was closed. See also: closeOpenStatements.
     */
    private boolean detectUnclosedStatements;

    /**
     * If set, pool will call this for every new connection that's created.
     */
    private Properties clientInfo;

    /**
     * Returns the name of the pool for JMX and thread names.
     *
     * @return a pool name.
     */
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
     * @see com.jolbox.bonecp.BoneCPConfigMBean#getMinConnectionsPerPartition()
     */
    public int getMinObjectsPerPartition() {
        return this.minObjectsPerPartition;
    }

    /**
     * Sets the minimum number of connections that will be contained in every
     * partition. Also refer to {@link #setPoolAvailabilityThreshold(int)}.
     *
     * @param minObjectsPerPartition number of connections
     */
    public void setMinObjectsPerPartition(int minObjectsPerPartition) {
        this.minObjectsPerPartition = minObjectsPerPartition;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.jolbox.bonecp.BoneCPConfigMBean#getMaxConnectionsPerPartition()
     */
    public int getMaxObjectsPerPartition() {
        return this.maxObjectsPerPartition;
    }

    /**
     * Sets the maximum number of connections that will be contained in every
     * partition. Setting this to 5 with 3 partitions means you will have 15
     * unique connections to the database. Note that the connection pool will
     * not create all these connections in one go but rather start off with
     * minConnectionsPerPartition and gradually increase connections as
     * required.
     *
     * @param maxObjectsPerPartition number of connections.
     */
    public void setMaxObjectsPerPartition(int maxObjectsPerPartition) {
        this.maxObjectsPerPartition = maxObjectsPerPartition;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.jolbox.bonecp.BoneCPConfigMBean#getAcquireIncrement()
     */
    public int getAcquireIncrement() {
        return this.acquireIncrement;
    }

    /**
     * Sets the acquireIncrement property.
     *
     * When the available connections are about to run out, BoneCP will
     * dynamically create new ones in batches. This property controls how many
     * new connections to create in one go (up to a maximum of
     * maxConnectionsPerPartition).
     * <p>
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
     * @see com.jolbox.bonecp.BoneCPConfigMBean#getPartitionCount()
     */
    public int getPartitionCount() {
        return this.partitionCount;
    }

    /**
     * Sets number of partitions to use.
     *
     * In order to reduce lock contention and thus improve performance, each
     * incoming connection request picks off a connection from a pool that has
     * thread-affinity, i.e. pool[threadId % partition_count]. The higher this
     * number, the better your performance will be for the case when you have
     * plenty of short-lived threads. Beyond a certain threshold, maintenance of
     * these pools will start to have a negative effect on performance (and only
     * for the case when connections on a partition start running out). Has no
     * effect in a CACHED strategy.
     *
     * <p>
     * Default: 1, minimum: 1, recommended: 2-4 (but very app specific)
     *
     * @param partitionCount to set
     */
    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    /**
     * Deprecated.
     *
     * @deprecated Please use {@link #getIdleConnectionTestPeriodInMinutes()}
     * instead.
     * @return idleConnectionTest
     */
    @Deprecated
    public long getIdleConnectionTestPeriod() {
        logger.warn("Please use getIdleConnectionTestPeriodInMinutes in place of getIdleConnectionTestPeriod. This method has been deprecated.");
        return getIdleConnectionTestPeriodInMinutes();
    }

    /**
     * Sets the idleConnectionTestPeriod in minutes
     *
     * @deprecated Please use
     * {@link #setIdleConnectionTestPeriodInMinutes(long)} or
     * {@link #setIdleConnectionTestPeriod(long, TimeUnit)} instead
     * @param idleConnectionTestPeriod to set in minutes
     */
    @Deprecated
    public void setIdleConnectionTestPeriod(long idleConnectionTestPeriod) {
        logger.warn("Please use setIdleConnectionTestPeriodInMinutes in place of setIdleConnectionTestPeriod. This method has been deprecated.");
        setIdleConnectionTestPeriod(idleConnectionTestPeriod * 60, TimeUnit.SECONDS);
    }

    /**
     * {@inheritDoc}
     *
     * @see
     * com.jolbox.bonecp.BoneCPConfigMBean#getIdleConnectionTestPeriodInMinutes()
     */
    public long getIdleConnectionTestPeriodInMinutes() {
        return this.idleObjectTestPeriodInSeconds / 60;
    }

    /**
     * Returns the idleConnectionTestPeriod with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return Idle Connection test period
     */
    public long getIdleConnectionTestPeriod(TimeUnit timeUnit) {
        return timeUnit.convert(this.idleObjectTestPeriodInSeconds, TimeUnit.SECONDS);
    }

    /**
     * Sets the idleConnectionTestPeriod.
     *
     * This sets the time (in minutes), for a connection to remain idle before
     * sending a test query to the DB. This is useful to prevent a DB from
     * timing out connections on its end. Do not use aggressive values here!
     *
     *
     * <p>
     * Default: 240 min, set to 0 to disable
     *
     * @param idleConnectionTestPeriod to set
     */
    public void setIdleConnectionTestPeriodInMinutes(long idleConnectionTestPeriod) {
        // we use TimeUnit.SECONDS instead of TimeUnit.MINUTES because it's not supported
        // by JDK5
        setIdleConnectionTestPeriod(idleConnectionTestPeriod * 60, TimeUnit.SECONDS);
    }

    /**
     * Sets the idleConnectionTestPeriod.
     *
     * This sets the time (in seconds), for a connection to remain idle before
     * sending a test query to the DB. This is useful to prevent a DB from
     * timing out connections on its end. Do not use aggressive values here!
     *
     *
     * <p>
     * Default: 240 min, set to 0 to disable
     *
     * @param idleConnectionTestPeriod to set
     */
    public void setIdleConnectionTestPeriodInSeconds(long idleConnectionTestPeriod) {
        setIdleConnectionTestPeriod(idleConnectionTestPeriod, TimeUnit.SECONDS);
    }

    /**
     * Wrapper method for idleConnectionTestPeriod for easier programmatic
     * access.
     *
     * @param idleConnectionTestPeriod time for a connection to remain idle
     * before sending a test query to the DB.
     * @param timeUnit Time granularity of given parameter.
     */
    public void setIdleConnectionTestPeriod(long idleConnectionTestPeriod, TimeUnit timeUnit) {
        this.idleObjectTestPeriodInSeconds = TimeUnit.SECONDS.convert(idleConnectionTestPeriod, timeUnit);
    }

    /**
     * Deprecated.
     *
     * @return idleMaxAge in minutes
     * @deprecated Use {@link #getIdleMaxAgeInMinutes()} instead
     */
    @Deprecated
    public long getIdleMaxAge() {
        logger.warn("Please use getIdleMaxAgeInMinutes in place of getIdleMaxAge. This method has been deprecated.");
        return getIdleMaxAgeInMinutes();
    }

    /**
     * Returns the idleMaxAge with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return idleMaxAge value
     */
    public long getIdleMaxAge(TimeUnit timeUnit) {
        return timeUnit.convert(this.idleMaxAgeInSeconds, TimeUnit.SECONDS);
    }

    /**
     * Returns the idleMaxAge currently set.
     *
     * @return idleMaxAge in minutes
     */
    public long getIdleMaxAgeInMinutes() {
        return this.idleMaxAgeInSeconds / 60;
    }

    /**
     * Deprecated.
     *
     * @param idleMaxAge to set
     * @deprecated Use {@link #setIdleMaxAgeInMinutes(long)} or
     * {@link #setIdleMaxAge(long, TimeUnit)} instead.
     */
    @Deprecated
    public void setIdleMaxAge(long idleMaxAge) {
        logger.warn("Please use setIdleMaxAgeInMinutes in place of setIdleMaxAge. This method has been deprecated.");
        setIdleMaxAgeInMinutes(idleMaxAge);
    }

    /**
     * Sets Idle max age (in min).
     *
     * The time (in minutes), for a connection to remain unused before it is
     * closed off. Do not use aggressive values here!
     *
     * <p>
     * Default: 60 minutes, set to 0 to disable.
     *
     * @param idleMaxAge to set
     */
    public void setIdleMaxAgeInMinutes(long idleMaxAge) {
        setIdleMaxAge(idleMaxAge * 60, TimeUnit.SECONDS);
    }

    /**
     * Sets Idle max age (in seconds).
     *
     * The time (in seconds), for a connection to remain unused before it is
     * closed off. Do not use aggressive values here!
     *
     * <p>
     * Default: 60 minutes, set to 0 to disable.
     *
     * @param idleMaxAge to set
     */
    public void setIdleMaxAgeInSeconds(long idleMaxAge) {
        setIdleMaxAge(idleMaxAge, TimeUnit.SECONDS);
    }

    /**
     * Sets Idle max age.
     *
     * The time, for a connection to remain unused before it is closed off. Do
     * not use aggressive values here!
     *
     * @param idleMaxAge time after which a connection is closed off
     * @param timeUnit idleMaxAge time granularity.
     */
    public void setIdleMaxAge(long idleMaxAge, TimeUnit timeUnit) {
        this.idleMaxAgeInSeconds = TimeUnit.SECONDS.convert(idleMaxAge, timeUnit);
    }

    /**
     * {@inheritDoc}
     *
     * @see com.jolbox.bonecp.BoneCPConfigMBean#getConnectionTestStatement()
     */
    public String getConnectionTestStatement() {
        return this.objectTestStatement;
    }

    /**
     * Sets the connection test statement.
     *
     * The query to send to the DB to maintain keep-alives and test for dead
     * connections. This is database specific and should be set to a query that
     * consumes the minimal amount of load on the server. Examples: MySQL: "/*
     * ping *\/ SELECT 1", PostgreSQL: "SELECT NOW()". If you do not set this,
     * then BoneCP will issue a metadata request instead that should work on all
     * databases but is probably slower.
     *
     * (Note: In MySQL, prefixing the statement by /* ping *\/ makes the driver
     * issue 1 fast packet instead. See
     * http://blogs.sun.com/SDNChannel/entry/mysql_tips_for_java_developers )
     * <p>
     * Default: Use metadata request
     *
     * @param connectionTestStatement to set.
     */
    public void setConnectionTestStatement(String connectionTestStatement) {
        this.objectTestStatement = connectionTestStatement;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.jolbox.bonecp.BoneCPConfigMBean#getReleaseHelperThreads()
     */
    public int getReleaseHelperThreads() {
        return this.releaseHelperThreads;
    }

    /**
     * Sets number of helper threads to create that will handle releasing a
     * connection.
     *
     * When this value is set to zero, the application thread is blocked until
     * the pool is able to perform all the necessary cleanup to recycle the
     * connection and make it available for another thread.
     *
     * When a non-zero value is set, the pool will create threads that will take
     * care of recycling a connection when it is closed (the application dumps
     * the connection into a temporary queue to be processed asychronously to
     * the application via the release helper threads).
     *
     * Useful when your application is doing lots of work on each connection
     * (i.e. perform an SQL query, do lots of non-DB stuff and perform another
     * query), otherwise will probably slow things down.
     *
     * @param releaseHelperThreads no to release
     */
    public void setReleaseHelperThreads(int releaseHelperThreads) {
        this.releaseHelperThreads = releaseHelperThreads;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.jolbox.bonecp.BoneCPConfigMBean#getConnectionHook()
     */
    public ObjectListener getObjectListener() {
        return this.objectListener;
    }

    /**
     * Sets the connection hook.
     *
     * Fully qualified class name that implements the ConnectionHook interface
     * (or extends AbstractConnectionHook). BoneCP will callback the specified
     * class according to the connection state (onAcquire, onCheckIn,
     * onCheckout, onDestroy).
     *
     * @param objectListener the connectionHook to set
     */
    public void setObjectListener(ObjectListener objectListener) {
        this.objectListener = objectListener;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.jolbox.bonecp.BoneCPConfigMBean#getInitSQL()
     */
    public String getInitSQL() {
        return this.initSQL;
    }

    /**
     * Specifies an initial SQL statement that is run only when a connection is
     * first created.
     *
     * @param initSQL the initSQL to set
     */
    public void setInitSQL(String initSQL) {
        this.initSQL = initSQL;
    }

    /**
     * Returns if BoneCP is configured to create a helper thread to watch over
     * connection acquires that are never released (or released twice). FOR
     * DEBUG PURPOSES ONLY.
     *
     * @return the current closeConnectionWatch setting.
     */
    public boolean isCloseConnectionWatch() {
        return this.closeConnectionWatch;
    }

    /**
     * Instruct the pool to create a helper thread to watch over connection
     * acquires that are never released (or released twice). This is for
     * debugging purposes only and will create a new thread for each call to
     * getConnection(). Enabling this option will have a big negative impact on
     * pool performance.
     *
     * @param closeConnectionWatch set to true to enable thread monitoring.
     */
    public void setCloseConnectionWatch(boolean closeConnectionWatch) {
        this.closeConnectionWatch = closeConnectionWatch;
    }

    /**
     * Returns true if SQL logging is currently enabled, false otherwise.
     *
     * @return the logStatementsEnabled status
     */
    public boolean isLogStatementsEnabled() {
        return this.logStatementsEnabled;
    }

    /**
     * If enabled, log SQL statements being executed.
     *
     * @param logStatementsEnabled the logStatementsEnabled to set
     */
    public void setLogStatementsEnabled(boolean logStatementsEnabled) {
        this.logStatementsEnabled = logStatementsEnabled;
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #getAcquireRetryDelayInMs()} instead.
     * @return the acquireRetryDelay
     */
    @Deprecated
    public long getAcquireRetryDelay() {
        logger.warn("Please use getAcquireRetryDelayInMs in place of getAcquireRetryDelay. This method has been deprecated.");
        return this.acquireRetryDelayInMs;
    }

    /**
     * Deprecated.
     *
     * @param acquireRetryDelayInMs the acquireRetryDelay to set
     * @deprecated Use {@link #setAcquireRetryDelayInMs(long)}.
     */
    @Deprecated
    public void setAcquireRetryDelay(int acquireRetryDelayInMs) {
        logger.warn("Please use setAcquireRetryDelayInMs in place of setAcquireRetryDelay. This method has been deprecated.");
        this.acquireRetryDelayInMs = acquireRetryDelayInMs;
    }

    /**
     * Returns the number of ms to wait before attempting to obtain a connection
     * again after a failure. Default: 7000.
     *
     * @return the acquireRetryDelay
     */
    public long getAcquireRetryDelayInMs() {
        return this.acquireRetryDelayInMs;
    }

    /**
     * Returns the acquireRetryDelay setting with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return acquireRetryDelay
     */
    public long getAcquireRetryDelay(TimeUnit timeUnit) {
        return timeUnit.convert(this.acquireRetryDelayInMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the number of ms to wait before attempting to obtain a connection
     * again after a failure.
     *
     * @param acquireRetryDelay the acquireRetryDelay to set
     */
    public void setAcquireRetryDelayInMs(long acquireRetryDelay) {
        setAcquireRetryDelay(acquireRetryDelay, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the number of ms to wait before attempting to obtain a connection
     * again after a failure.
     *
     * @param acquireRetryDelay the acquireRetryDelay to set
     * @param timeUnit time granularity
     */
    public void setAcquireRetryDelay(long acquireRetryDelay, TimeUnit timeUnit) {
        this.acquireRetryDelayInMs = TimeUnit.MILLISECONDS.convert(acquireRetryDelay, timeUnit);
    }

    /**
     * Returns true if connection pool is to be initialized lazily.
     *
     * @return lazyInit setting
     */
    public boolean isLazyInit() {
        return this.lazyInit;
    }

    /**
     * Set to true to force the connection pool to obtain the initial
     * connections lazily.
     *
     * @param lazyInit the lazyInit setting to set
     */
    public void setLazyInit(boolean lazyInit) {
        this.lazyInit = lazyInit;
    }

    /**
     * Returns true if the pool is configured to record all transaction activity
     * and replay the transaction automatically in case of connection failures.
     *
     * @return the transactionRecoveryEnabled status
     */
    public boolean isTransactionRecoveryEnabled() {
        return this.transactionRecoveryEnabled;
    }

    /**
     * Set to true to enable recording of all transaction activity and replay
     * the transaction automatically in case of a connection failure.
     *
     * @param transactionRecoveryEnabled the transactionRecoveryEnabled status
     * to set
     */
    public void setTransactionRecoveryEnabled(boolean transactionRecoveryEnabled) {
        this.transactionRecoveryEnabled = transactionRecoveryEnabled;
    }

    /**
     * After attempting to acquire a connection and failing, try to connect
     * these many times before giving up. Default 5.
     *
     * @return the acquireRetryAttempts value
     */
    public int getAcquireRetryAttempts() {
        return this.acquireRetryAttempts;
    }

    /**
     * After attempting to acquire a connection and failing, try to connect
     * these many times before giving up. Default 5.
     *
     * @param acquireRetryAttempts the acquireRetryAttempts to set
     */
    public void setAcquireRetryAttempts(int acquireRetryAttempts) {
        this.acquireRetryAttempts = acquireRetryAttempts;
    }

    /**
     * Sets the connection hook class name. Consider using setConnectionHook()
     * instead.
     *
     * @param connectionHookClassName the connectionHook class name to set
     */
    public void setConnectionHookClassName(String connectionHookClassName) {
        this.connectionHookClassName = connectionHookClassName;
        if (connectionHookClassName != null) {
            Object hookClass;
            try {
                hookClass = loadClass(connectionHookClassName).newInstance();
                this.objectListener = (ObjectListener) hookClass;
            } catch (Exception e) {
                logger.error("Unable to create an instance of the connection hook class (" + connectionHookClassName + ")");
                this.objectListener = null;
            }
        }
    }

    /**
     * Returns the connection hook class name as passed via the setter
     *
     * @return the connectionHookClassName.
     */
    public String getConnectionHookClassName() {
        return this.connectionHookClassName;
    }

    /**
     * Return true if JMX is disabled.
     *
     * @return the disableJMX.
     */
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
     * Returns the bean being used to return a connection.
     *
     * @return the datasourceBean that was set.
     */
    public DataSource getDatasourceBean() {
        return this.datasourceBean;
    }

    /**
     * If set, use datasourceBean.getConnection() to obtain a new connection
     * instead of Driver.getConnection().
     *
     * @param datasourceBean the datasourceBean to set
     */
    public void setDatasourceBean(DataSource datasourceBean) {
        this.datasourceBean = datasourceBean;
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #getQueryExecuteTimeLimitInMs()} instead.
     * @return the queryTimeLimit
     */
    @Deprecated
    public long getQueryExecuteTimeLimit() {
        logger.warn("Please use getQueryExecuteTimeLimitInMs in place of getQueryExecuteTimeLimit. This method has been deprecated.");
        return this.queryExecuteTimeLimitInMs;
    }

    /**
     * Queries taking longer than this limit to execute are logged.
     *
     * @param queryExecuteTimeLimit the limit to set in milliseconds.
     * @deprecated Use {@link #setQueryExecuteTimeLimitInMs(long)} instead.
     */
    @Deprecated
    public void setQueryExecuteTimeLimit(int queryExecuteTimeLimit) {
        logger.warn("Please use setQueryExecuteTimeLimitInMs in place of setQueryExecuteTimeLimit. This method has been deprecated.");
        setQueryExecuteTimeLimit(queryExecuteTimeLimit, TimeUnit.MILLISECONDS);
    }

    /**
     * Return the query execute time limit.
     *
     * @return the queryTimeLimit
     */
    public long getQueryExecuteTimeLimitInMs() {
        return this.queryExecuteTimeLimitInMs;
    }

    /**
     * Returns the queryExecuteTimeLimit setting with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return queryExecuteTimeLimit period
     */
    public long getQueryExecuteTimeLimit(TimeUnit timeUnit) {
        return timeUnit.convert(this.queryExecuteTimeLimitInMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Queries taking longer than this limit to execute are logged.
     *
     * @param queryExecuteTimeLimit the limit to set in milliseconds.
     */
    public void setQueryExecuteTimeLimitInMs(long queryExecuteTimeLimit) {
        setQueryExecuteTimeLimit(queryExecuteTimeLimit, TimeUnit.MILLISECONDS);
    }

    /**
     * Queries taking longer than this limit to execute are logged.
     *
     * @param queryExecuteTimeLimit the limit to set in milliseconds.
     * @param timeUnit
     */
    public void setQueryExecuteTimeLimit(long queryExecuteTimeLimit, TimeUnit timeUnit) {
        this.queryExecuteTimeLimitInMs = TimeUnit.MILLISECONDS.convert(queryExecuteTimeLimit, timeUnit);
    }

    /**
     * Returns the pool watch connection threshold value.
     *
     * @return the poolAvailabilityThreshold currently set.
     */
    public int getPoolAvailabilityThreshold() {
        return this.poolAvailabilityThreshold;
    }

    /**
     * Sets the Pool Watch thread threshold.
     *
     * The pool watch thread attempts to maintain a number of connections always
     * available (between minConnections and maxConnections). This value sets
     * the percentage value to maintain. For example, setting it to 20 means
     * that if the following condition holds: Free Connections / MaxConnections
     * < poolAvailabilityThreshold
     *
     * new connections will be created. In other words, it tries to keep at
     * least 20% of the pool full of connections. Setting the value to zero will
     * make the pool create new connections when it needs them but it also means
     * your application may have to wait for new connections to be obtained at
     * times.
     *
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
    public boolean isDisableObjectTracking() {
        return this.disableObjectTracking;
    }

    /**
     * If set to true, the pool will not monitor connections for proper closure.
     * Enable this option if you only ever obtain your connections via a
     * mechanism that is guaranteed to release the connection back to the pool
     * (eg Spring's jdbcTemplate, some kind of transaction manager, etc).
     *
     * @param disableObjectTracking set to true to disable. Default: false.
     */
    public void setDisableObjectTracking(boolean disableObjectTracking) {
        this.disableObjectTracking = disableObjectTracking;
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #getConnectionTimeoutInMs()} instead.
     * @return the connectionTimeout
     */
    @Deprecated
    public long getWaitTime() {
        logger.warn("Please use getConnectionTimeoutInMs in place of getConnectionTimeout. This method has been deprecated.");
        return this.waitTimeInMs;
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #setWaitTimeInMs(long) } instead.
     * @param connectionTimeout the connectionTimeout to set
     */
    @Deprecated
    public void setWaitTime(long connectionTimeout) {
        logger.warn("Please use setConnectionTimeoutInMs in place of setConnectionTimeout. This method has been deprecated.");
        this.waitTimeInMs = connectionTimeout;
    }

    /**
     * Returns the maximum time (in milliseconds) to wait before a call to
     * getConnection is timed out.
     *
     * @return the connectionTimeout
     */
    public long getWaitTimeInMs() {
        return this.waitTimeInMs;
    }

    /**
     * Returns the connectionTimeout with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return connectionTimeout period
     */
    public long getWaitTime(TimeUnit timeUnit) {
        return timeUnit.convert(this.waitTimeInMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum time (in milliseconds) to wait before a call to
     * getConnection is timed out.
     *
     * Setting this to zero is similar to setting it to Long.MAX_VALUE
     *
     * Default: 0 ( = wait forever )
     *
     * @param connectionTimeoutinMs the connectionTimeout to set
     */
    public void setWaitTimeInMs(long connectionTimeoutinMs) {
        setWaitTime(connectionTimeoutinMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum time to wait before a call to getConnection is timed
     * out.
     *
     * Setting this to zero is similar to setting it to Long.MAX_VALUE
     *
     * @param connectionTimeout
     * @param timeUnit the unit of the connectionTimeout argument
     */
    public void setWaitTime(long connectionTimeout, TimeUnit timeUnit) {
        this.waitTimeInMs = TimeUnit.MILLISECONDS.convert(connectionTimeout, timeUnit);
    }

    /**
     * Returns the currently configured driver properties.
     *
     * @return the driverProperties handle
     */
    public Properties getDriverProperties() {
        return this.driverProperties;
    }

    /**
     * Sets properties that will be passed on to the driver.
     *
     * The properties handle should contain a list of arbitrary string tag/value
     * pairs as connection arguments; normally at least a "user" and "password"
     * property should be included. Failure to include the user or password
     * properties will make the pool copy the values given in
     * config.setUsername(..) and config.setPassword(..).
     *
     * Note that the pool will make a copy of these properties so as not to risk
     * attempting to create a connection later on with different settings.
     *
     * @param driverProperties the driverProperties to set
     */
    public void setDriverProperties(Properties driverProperties) {
        if (driverProperties != null) {
            // make a copy of the properties so that we don't attempt to create more connections
            // later on and are possibly surprised by having different urls/usernames/etc
            this.driverProperties = new Properties();
            this.driverProperties.putAll(driverProperties);
        }
    }

    /**
     * Deprecated.
     *
     * @deprecated Use {@link #getCloseConnectionWatchTimeoutInMs()} instead
     * @return the watchTimeout currently set.
     */
    @Deprecated
    public long getCloseObjectWatchTimeout() {
        logger.warn("Please use getCloseConnectionWatchTimeoutInMs in place of getCloseConnectionWatchTimeout. This method has been deprecated.");
        return this.closeObjectWatchTimeoutInMs;
    }

    /**
     * Deprecated.
     *
     * @param closeConnectionWatchTimeout the watchTimeout to set
     * @deprecated Use {@link #setCloseConnectionWatchTimeoutInMs(long)} instead
     */
    @Deprecated
    public void setCloseObjectWatchTimeout(long closeConnectionWatchTimeout) {
        logger.warn("Please use setCloseConnectionWatchTimeoutInMs in place of setCloseConnectionWatchTimeout. This method has been deprecated.");
        setCloseObjectWatchTimeoutInMs(closeConnectionWatchTimeout);
    }

    /**
     * Returns the no of ms to wait when close connection watch threads are
     * enabled. 0 = wait forever.
     *
     * @return the watchTimeout currently set.
     */
    @Override
    public long getCloseObjectWatchTimeoutInMs() {
        return this.closeObjectWatchTimeoutInMs;
    }

    /**
     * Returns the closeConnectionWatchTimeout with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return closeConnectionWatchTimeout period
     */
    public long getCloseConnectionWatchTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(this.closeObjectWatchTimeoutInMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the no of ms to wait when close connection watch threads are
     * enabled. 0 = wait forever.
     *
     * @param closeConnectionWatchTimeout the watchTimeout to set
     */
    public void setCloseObjectWatchTimeoutInMs(long closeConnectionWatchTimeout) {
        setCloseConnectionWatchTimeout(closeConnectionWatchTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the time to wait when close connection watch threads are enabled. 0
     * = wait forever.
     *
     * @param closeConnectionWatchTimeout the watchTimeout to set
     * @param timeUnit Time granularity
     */
    public void setCloseConnectionWatchTimeout(long closeConnectionWatchTimeout, TimeUnit timeUnit) {
        this.closeObjectWatchTimeoutInMs = TimeUnit.MILLISECONDS.convert(closeConnectionWatchTimeout, timeUnit);
    }

    /**
     * Deprecated. Please use {@link #getMaxObjectAgeInSeconds()} instead.
     *
     * @return maxConnectionAge
     * @deprecated Please use {@link #getMaxConnectionAgeInSeconds()} instead.
     */
    @Deprecated
    public long getMaxConnectionAge() {
        logger.warn("Please use getMaxConnectionAgeInSeconds in place of getMaxConnectionAge. This method has been deprecated.");
        return this.maxObjectAgeInSeconds;
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
     * Returns the maxConnectionAge with the specified granularity.
     *
     * @param timeUnit time granularity
     * @return maxConnectionAge period
     */
    public long getMaxConnectionAge(TimeUnit timeUnit) {
        return timeUnit.convert(this.maxObjectAgeInSeconds, TimeUnit.SECONDS);
    }

    /**
     * Deprecated. Use {{@link #setMaxObjectAgeInSeconds(long)} instead.
     *
     * @param maxConnectionAgeInSeconds the maxConnectionAge to set
     * @deprecated Please use {{@link #setMaxConnectionAgeInSeconds(long)}
     * instead.
     */
    @Deprecated
    public void setMaxConnectionAge(long maxConnectionAgeInSeconds) {
        logger.warn("Please use setmaxConnectionAgeInSecondsInSeconds in place of setMaxConnectionAge. This method has been deprecated.");
        this.maxObjectAgeInSeconds = maxConnectionAgeInSeconds;
    }

    /**
     * Sets the maxConnectionAge in seconds. Any connections older than this
     * setting will be closed off whether it is idle or not. Connections
     * currently in use will not be affected until they are returned to the
     * pool.
     *
     * @param maxObjectAgeInSeconds the maxConnectionAge to set
     */
    public void setMaxObjectAgeInSeconds(long maxObjectAgeInSeconds) {
        setMaxConnectionAge(maxObjectAgeInSeconds, TimeUnit.SECONDS);
    }

    /**
     * Sets the maxConnectionAge. Any connections older than this setting will
     * be closed off whether it is idle or not. Connections currently in use
     * will not be affected until they are returned to the pool.
     *
     * @param maxConnectionAge the maxConnectionAge to set.
     * @param timeUnit the unit of the maxConnectionAge argument.
     */
    public void setMaxConnectionAge(long maxConnectionAge, TimeUnit timeUnit) {
        this.maxObjectAgeInSeconds = TimeUnit.SECONDS.convert(maxConnectionAge, timeUnit);
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
     * Sets the configFile. If configured, this will cause the pool to
     * initialise using the config file in the same way as if calling new
     * BoneCPConfig(filename).
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
     * Sets the queue serviceOrder. Values currently understood are FIFO and
     * LIFO.
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
     * If set to true, keep track of some more statistics for exposure via JMX.
     * Will slow down the pool operation.
     *
     * @param statisticsEnabled set to true to enable
     */
    public void setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }

    /**
     * Returns the defaultAutoCommit field.
     *
     * @return defaultAutoCommit
     */
    public boolean getDefaultAutoCommit() {
        return this.defaultAutoCommit;
    }

    /**
     * Sets the defaultAutoCommit setting for newly created connections. If not
     * set, use driver default.
     *
     * @param defaultAutoCommit the defaultAutoCommit to set
     */
    public void setDefaultAutoCommit(boolean defaultAutoCommit) {
        this.defaultAutoCommit = defaultAutoCommit;
    }

    /**
     * Returns the defaultReadOnly field.
     *
     * @return defaultReadOnly
     */
    public Boolean getDefaultReadOnly() {
        return this.defaultReadOnly;
    }

    /**
     * Sets the defaultReadOnly setting for newly created connections. If not
     * set, use driver default.
     *
     * @param defaultReadOnly the defaultReadOnly to set
     */
    public void setDefaultReadOnly(Boolean defaultReadOnly) {
        this.defaultReadOnly = defaultReadOnly;
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
     * Sets the defaultCatalog setting for newly created connections. If not
     * set, use driver default.
     *
     * @param defaultCatalog the defaultCatalog to set
     */
    public void setDefaultCatalog(String defaultCatalog) {
        this.defaultCatalog = defaultCatalog;
    }

    /**
     * Returns the defaultTransactionIsolation field.
     *
     * @return defaultTransactionIsolation
     */
    public String getDefaultTransactionIsolation() {
        return this.defaultTransactionIsolation;
    }

    /**
     * Sets the defaultTransactionIsolation. Should be set to one of: NONE,
     * READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ or SERIALIZABLE. If not
     * set, will use driver default.
     *
     * @param defaultTransactionIsolation the defaultTransactionIsolation to set
     */
    public void setDefaultTransactionIsolation(String defaultTransactionIsolation) {
        this.defaultTransactionIsolation = defaultTransactionIsolation;
    }

    /**
     * Returns the defaultTransactionIsolationValue field.
     *
     * @return defaultTransactionIsolationValue
     */
    protected int getDefaultTransactionIsolationValue() {
        return this.defaultTransactionIsolationValue;
    }

    /**
     * Sets the defaultTransactionIsolationValue.
     *
     * @param defaultTransactionIsolationValue the
     * defaultTransactionIsolationValue to set
     */
    protected void setDefaultTransactionIsolationValue(int defaultTransactionIsolationValue) {
        this.defaultTransactionIsolationValue = defaultTransactionIsolationValue;
    }

    /**
     * Default constructor. Attempts to fill settings in this order: 1.
     * bonecp-default-config.xml file, usually found in the pool jar 2.
     * bonecp-config.xml file, usually found in your application's classpath 3.
     * Other hardcoded defaults in BoneCPConfig class.
     */
    public BoneOPConfig() {
        // try to load the default config file, if available from somewhere in the classpath
        loadProperties("/bonecp-default-config.xml");
        // try to override with app specific config, if available
        loadProperties("/bonecp-config.xml");
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
     * Initialize the configuration by loading bonecp-config.xml containing the
     * settings.
     *
     * @param sectionName section to load
     * @throws Exception on parse errors
     */
    public BoneOPConfig(String sectionName) throws Exception {
        this(BoneOPConfig.class.getResourceAsStream("/bonecp-config.xml"), sectionName);
    }

    /**
     * Initialise the configuration by loading an XML file containing the
     * settings.
     *
     * @param xmlConfigFile file to load
     * @param sectionName section to load
     * @throws Exception
     */
    public BoneOPConfig(InputStream xmlConfigFile, String sectionName) throws Exception {
        this();
        setXMLProperties(xmlConfigFile, sectionName);
    }

    /**
     * @param xmlConfigFile
     * @param sectionName
     * @throws Exception
     */
    private void setXMLProperties(InputStream xmlConfigFile, String sectionName)
            throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db;
        // ugly XML parsing, but this is built-in the JDK.
        try {
            db = dbf.newDocumentBuilder();
            Document doc = db.parse(xmlConfigFile);
            doc.getDocumentElement().normalize();

            // get the default settings
            Properties settings = parseXML(doc, null);
            if (sectionName != null) {
                // override with custom settings
                settings.putAll(parseXML(doc, sectionName));
            }
            // set the properties
            setProperties(settings);

        } catch (Exception e) {
            throw e;
        }
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
     * Sets the properties by reading off entries in the given parameter (where
     * each key is equivalent to the field name)
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
                    val = props.getProperty("bonecp." + tmp); // hibernate provider style
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
                    val = props.getProperty("bonecp." + tmp); // hibernate provider style
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
                    val = props.getProperty("bonecp." + tmp); // hibernate provider style
                }
                if (val != null) {
                    method.invoke(this, val);
                }
            }
            if (method.getParameterTypes().length == 1 && method.getParameterTypes()[0].equals(boolean.class)) {
                String val = props.getProperty(tmp);
                if (val == null) {
                    val = props.getProperty("bonecp." + tmp); // hibernate provider style
                }
                if (val != null) {
                    method.invoke(this, Boolean.parseBoolean(val));
                }
            }
        }
    }

    /**
     * Parses the given XML doc to extract the properties and return them into a
     * java.util.Properties.
     *
     * @param doc to parse
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
                logger.warn("Did not find " + sectionName + " section in config file. Reverting to defaults.");
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
     * Returns the current externalAuth setting.
     *
     * @return externalAuth setting
     */
    public boolean isExternalAuth() {
        return this.externalAuth;
    }

    /**
     * If set to true, no attempts at passing in a username/password will be
     * attempted when trying to obtain a raw (driver) connection. Useful for
     * cases when you already have another mechanism on authentication eg NTLM.
     *
     * @param externalAuth True to enable external auth.
     */
    public void setExternalAuth(boolean externalAuth) {
        this.externalAuth = externalAuth;
    }

    /**
     * Performs validation on the config object.
     *
     */
    public void sanitize() {
        if (this.configFile != null) {
            loadProperties(this.configFile);
        }

        if (this.poolStrategy == null || !(this.poolStrategy.equalsIgnoreCase("DEFAULT") || this.poolStrategy.equalsIgnoreCase("CACHED"))) {
            logger.warn("Unrecognised pool strategy. Allowed values are DEFAULT and CACHED. Setting to DEFAULT.");
            this.poolStrategy = "DEFAULT";
        }

        this.poolStrategy = this.poolStrategy.toUpperCase();

        if ((this.poolAvailabilityThreshold < 0) || (this.poolAvailabilityThreshold > 100)) {
            this.poolAvailabilityThreshold = 20;
        }

        if (this.defaultTransactionIsolation != null) {
            this.defaultTransactionIsolation = this.defaultTransactionIsolation.trim().toUpperCase();

            if (this.defaultTransactionIsolation.equals("NONE")) {
                this.defaultTransactionIsolationValue = Connection.TRANSACTION_NONE;
            } else if (this.defaultTransactionIsolation.equals("READ_COMMITTED") || this.defaultTransactionIsolation.equals("READ COMMITTED")) {
                this.defaultTransactionIsolationValue = Connection.TRANSACTION_READ_COMMITTED;
            } else if (this.defaultTransactionIsolation.equals("REPEATABLE_READ") || this.defaultTransactionIsolation.equals("REPEATABLE READ")) {
                this.defaultTransactionIsolationValue = Connection.TRANSACTION_REPEATABLE_READ;
            } else if (this.defaultTransactionIsolation.equals("READ_UNCOMMITTED") || this.defaultTransactionIsolation.equals("READ UNCOMMITTED")) {
                this.defaultTransactionIsolationValue = Connection.TRANSACTION_READ_UNCOMMITTED;
            } else if (this.defaultTransactionIsolation.equals("SERIALIZABLE")) {
                this.defaultTransactionIsolationValue = Connection.TRANSACTION_SERIALIZABLE;
            } else {
                logger.warn("Unrecognized defaultTransactionIsolation value. Using driver default.");
                this.defaultTransactionIsolationValue = -1;
            }
        }
        if (this.maxObjectsPerPartition < 1) {
            logger.warn("Max Connections < 1. Setting to 20");
            this.maxObjectsPerPartition = 20;
        }
        if (this.minObjectsPerPartition < 0) {
            logger.warn("Min Connections < 0. Setting to 1");
            this.minObjectsPerPartition = 1;
        }

        if (this.minObjectsPerPartition > this.maxObjectsPerPartition) {
            logger.warn("Min Connections > max connections");
            this.minObjectsPerPartition = this.maxObjectsPerPartition;
        }
        if (this.acquireIncrement <= 0) {
            logger.warn("acquireIncrement <= 0. Setting to 1.");
            this.acquireIncrement = 1;
        }
        if (this.partitionCount < 1) {
            logger.warn("partitions < 1! Setting to 1");
            this.partitionCount = 1;
        }

        if (this.releaseHelperThreads < 0) {
            logger.warn("releaseHelperThreads < 0! Setting to 3");
            this.releaseHelperThreads = 3;
        }

        if (this.acquireRetryDelayInMs <= 0) {
            this.acquireRetryDelayInMs = 1000;
        }
        this.serviceOrder = this.serviceOrder != null ? this.serviceOrder.toUpperCase() : "FIFO";

        if (!(this.serviceOrder.equals("FIFO") || this.serviceOrder.equals("LIFO"))) {
            logger.warn("Queue service order is not set to FIFO or LIFO. Defaulting to FIFO.");
            this.serviceOrder = "FIFO";
        }

    }

    /**
     * Loads the given properties file using the classloader.
     *
     * @param filename Config filename to load
     *
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
     * Loads the given class, respecting the given classloader.
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
     * Sets the classloader to use to load JDBC driver and hooks (set to null to
     * use default).
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
        if (that != null && Objects.equals(this.acquireIncrement, that.getAcquireIncrement())
                && Objects.equals(this.acquireRetryDelayInMs, that.getAcquireRetryDelayInMs())
                && Objects.equals(this.closeConnectionWatch, that.isCloseConnectionWatch())
                && Objects.equals(this.logStatementsEnabled, that.isLogStatementsEnabled())
                && Objects.equals(this.objectListener, that.getObjectListener())
                && Objects.equals(this.objectTestStatement, that.getConnectionTestStatement())
                && Objects.equals(this.idleObjectTestPeriodInSeconds, that.getIdleConnectionTestPeriod(TimeUnit.SECONDS))
                && Objects.equals(this.idleMaxAgeInSeconds, that.getIdleMaxAge(TimeUnit.SECONDS))
                && Objects.equals(this.initSQL, that.getInitSQL())
                && Objects.equals(this.maxObjectsPerPartition, that.getMaxObjectsPerPartition())
                && Objects.equals(this.minObjectsPerPartition, that.getMinObjectsPerPartition())
                && Objects.equals(this.partitionCount, that.getPartitionCount())
                && Objects.equals(this.releaseHelperThreads, that.getReleaseHelperThreads())
                && Objects.equals(this.lazyInit, that.isLazyInit())
                && Objects.equals(this.transactionRecoveryEnabled, that.isTransactionRecoveryEnabled())
                && Objects.equals(this.acquireRetryAttempts, that.getAcquireRetryAttempts())
                && Objects.equals(this.closeObjectWatchTimeoutInMs, that.getCloseObjectWatchTimeout())
                && Objects.equals(this.waitTimeInMs, that.getWaitTimeInMs())
                && Objects.equals(this.datasourceBean, that.getDatasourceBean())
                && Objects.equals(this.getQueryExecuteTimeLimitInMs(), that.getQueryExecuteTimeLimitInMs())
                && Objects.equals(this.poolAvailabilityThreshold, that.getPoolAvailabilityThreshold())
                && Objects.equals(this.poolName, that.getPoolName())
                && Objects.equals(this.disableObjectTracking, that.isDisableObjectTracking())) {
            return true;
        }

        return false;
    }

    /**
     * Returns the deregisterDriverOnClose setting.
     *
     * @return deregisterDriverOnClose
     */
    public boolean isDeregisterDriverOnClose() {
        return this.deregisterDriverOnClose;
    }

    /**
     * If set to true, try to unregister the JDBC driver when pool is shutdown.
     *
     * @param deregisterDriverOnClose the deregisterDriverOnClose setting.
     */
    public void setDeregisterDriverOnClose(boolean deregisterDriverOnClose) {
        this.deregisterDriverOnClose = deregisterDriverOnClose;
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
     *
     * If true, return null on connection timeout rather than throw an
     * exception. This performs better but must be handled differently in your
     * application. This only makes sense when using the connectionTimeout
     * config option.
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
     * If true, issue a reset (rollback) on connection close in case client
     * forgot it.
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
     * Sets the poolStrategy. Currently supported strategies are DEFAULT and
     * CACHED.
     *
     * DEFAULT strategy operates in a manner that has been used in the pool
     * since the very first version: it tries to obtain a connection from a
     * queue.
     *
     * CACHED stores each connection in a thread-local variable so that next
     * time the same thread asks for a connection, it gets the same one assigned
     * to it (if it asks for more than one, it will be allocated a new one).
     * This is very fast but you must ensure that the number of threads asking
     * for a connection is less than or equal to the number of connections you
     * have made available. Should you exceed this limit, the pool will switch
     * back (permanently) to the DEFAULT strategy which will cause a one-time
     * performance hit. Use this strategy if your threads are managed eg in a
     * Tomcat environment where you can limit the number of threads that it can
     * handle. A typical use case would be a web service that always requires
     * some form of database access, therefore a service would have little point
     * in accepting a new incoming socket connection if it still has to wait in
     * order to obtain a connection.
     *
     * Essentially this means that you are pushing back the lock down to the
     * socket or thread layer. While the first few thread hits will be slower
     * than in the DEFAULT strategy, significant performance gains are to be
     * expected as the thread gets increasingly re-used (i.e. initially you
     * should expect the first few rounds to be measurably slower than the
     * DEFAULT strategy but once the caches get more hits you should get >2x
     * better performance).
     *
     * Threads that are killed off are detected during the next garbage
     * collection and result in their allocated connections from being taken
     * back though since GC timing is not guaranteed you should ideally set your
     * minimum pool size to be equal to the maximum pool size.
     *
     * Therefore for best results, make sure that the configured
     * minConnectionPerPartition = maxConnectionPerPartition = min Threads = max
     * Threads.
     *
     *
     * @param poolStrategy the poolStrategy to set
     */
    public void setPoolStrategy(String poolStrategy) {
        this.poolStrategy = poolStrategy;
    }

    /**
     * Returns the detectUnclosedStatements field.
     *
     * @return detectUnclosedStatements
     */
    public boolean isDetectUnclosedStatements() {
        return this.detectUnclosedStatements;
    }

    /**
     * Sets the detectUnclosedStatements. If true, print out a stack trace of
     * where a statement was opened but not closed before the connection was
     * closed. {@link BoneCPConfig#closeOpenStatements}.
     *
     * @param detectUnclosedStatements the detectUnclosedStatements to set
     */
    public void setDetectUnclosedStatements(boolean detectUnclosedStatements) {
        this.detectUnclosedStatements = detectUnclosedStatements;
    }
}
