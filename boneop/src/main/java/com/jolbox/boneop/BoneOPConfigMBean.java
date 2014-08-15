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
/**
 *
 */
package com.jolbox.boneop;

import com.jolbox.boneop.listener.ObjectListener;

/**
 * MBean interface for config.
 *
 * @author Wallace
 */
public interface BoneOPConfigMBean {

    /**
     * Returns the name of the pool for JMX and thread names.
     *
     * @return a pool name.
     */
    String getPoolName();

    /**
     * Gets the minimum number of connections that will be contained in every
     * partition.
     *
     * @return minObjectsPerPartition
     */
    int getMinObjectsPerPartition();

    /**
     * Gets the maximum number of connections that will be contained in every
     * partition.
     *
     * @return maxConnectionsPerPartition
     */
    int getMaxObjectsPerPartition();

    /**
     * Gets the acquireIncrement property.
     * <p/>
     * Gets the current value of the number of connections to add every time the
     * number of available connections is about to run out (up to the
     * maxConnectionsPerPartition).
     *
     * @return acquireIncrement number of connections to add.
     */
    int getAcquireIncrement();

    /**
     * Gets the number of currently defined partitions.
     *
     * @return partitionCount
     */
    int getPartitionCount();

    /**
     * Gets the currently set idleConnectionTestPeriodInMinutes.
     *
     * @return idleConnectionTestPeriod
     */
    long getIdleObjectTestPeriodInMinutes();

    /**
     * Gets idleMaxAge (time in min).
     *
     * @return idleMaxAge
     */
    long getIdleMaxAgeInMinutes();

    /**
     * Gets number of release-connection helper threads to create per partition.
     *
     * @return number of threads
     */
    int getReleaseHelperThreads();

    /**
     * Returns the connection hook class.
     *
     * @return the connectionHook
     */
    ObjectListener getObjectListener();

    /**
     * Returns the number of ms to wait before attempting to obtain a connection
     * again after a failure. Default: 7000.
     *
     * @return the acquireRetryDelay
     */
    long getAcquireRetryDelayInMillis();

    /**
     * Returns true if connection pool is to be initialized lazily.
     *
     * @return lazyInit setting
     */
    boolean isLazyInit();

    /**
     * After attempting to acquire a connection and failing, try to connect
     * these many times before giving up. Default 5.
     *
     * @return the acquireRetryAttempts value
     */
    int getAcquireRetryAttempts();

    /**
     * Returns the connection hook class name as passed via the setter
     *
     * @return the connectionHookClassName.
     */
    String getObjectListenerClassName();

    /**
     * Return true if JMX is disabled.
     *
     * @return the disableJMX.
     */
    boolean isDisableJMX();

    /**
     * Return the query execute time limit in ms.
     *
     * @return the queryTimeLimit
     */
    long getObjectOccupyTimeLimitInMillis();

    /**
     * Returns the pool watch connection threshold value.
     *
     * @return the poolAvailabilityThreshold currently set.
     */
    int getPoolAvailabilityThreshold();

    /**
     * Returns true if connection tracking has been disabled.
     *
     * @return the disableConnectionTracking
     */
    boolean isDisableObjectTracking();

    /**
     * Returns the maximum time (in milliseconds) to wait before a call to
     * getConnection is timed out.
     *
     * @return the connectionTimeout
     */
    long getWaitTimeInMillis();

    /**
     * Returns the no of ms to wait when close connection watch threads are
     * enabled. 0 = wait forever.
     *
     * @return the watchTimeout currently set.
     */
    long getCloseObjectWatchTimeoutInMillis();

    /**
     * Returns the maxConnectionAge field.
     *
     * @return maxConnectionAge
     */
    long getMaxObjectAgeInSeconds();

    /**
     * Returns the configFile field.
     *
     * @return configFile
     */
    String getConfigFile();

    /**
     * Returns the serviceOrder field.
     *
     * @return serviceOrder
     */
    String getServiceOrder();

    /**
     * Returns the statisticsEnabled field.
     *
     * @return statisticsEnabled
     */
    boolean isStatisticsEnabled();
}
