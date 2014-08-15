/**
 *  Copyright 2010 Wallace Wadge
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.jolbox.boneop;


import com.jolbox.boneop.listener.AbstractObjectListener;
import com.jolbox.boneop.listener.ObjectListener;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

/**
 * Tests config object.
 *
 * @author wwadge
 */
public class TestBoneCPConfig {
    /**
     * Config handle.
     */
    static BoneOPConfig config;

    /**
     * Stub out any calls to logger.
     *
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws CloneNotSupportedException
     */
    @BeforeClass
    public static void setup() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, CloneNotSupportedException {
        config = CommonTestUtils.getConfigClone();
    }

    /**
     * Tests configs using xml setups.
     *
     * @throws Exception
     */
    @Test
    public void testXMLConfig() throws Exception {
        // read off from the default boneop-config.xml
//		System.out
//				.println(BoneCPConfig.class.getResource("/boneop-config.xml"));
        BoneOPConfig config = new BoneOPConfig("specialApp");
        assertEquals(99, config.getMinObjectsPerPartition());
    }

    /**
     * Tests configs using xml setups.
     *
     * @throws Exception
     */
    @Test
    public void testXMLConfig2() throws Exception {
        // read off from the default boneop-config.xml
        BoneOPConfig config = new BoneOPConfig("specialApp2");
        assertEquals(123, config.getMinObjectsPerPartition());
    }

    /**
     * Load properties via a given stream.
     *
     * @throws Exception
     */
    @Test
    public void testXmlConfigViaInputStream() throws Exception {
        // read off from an input stream
        BoneOPConfig config = new BoneOPConfig(this.getClass().getResourceAsStream("/boneop-config.xml"), "specialApp");
        assertEquals(99, config.getMinObjectsPerPartition());
    }

    /**
     * XML based config.
     *
     * @throws Exception
     */
    @Test
    public void testXMLConfigWithUnfoundSection() throws Exception {
        BoneOPConfig config = new BoneOPConfig("non-existant");
        assertEquals(20, config.getMinObjectsPerPartition());
    }

    /**
     * Test error condition for xml config.
     */
    @Test
    public void testXmlConfigWithInvalidStream() {
        // throw errors
        try {
            new BoneOPConfig(null, "specialApp");
            fail("Should have thrown an exception");
        } catch (Exception e) {
            // do nothing
        }
    }

    /**
     * Tests configs using xml setups.
     *
     * @throws Exception
     */
    @Test
    public void testPropertyBasedConfig() throws Exception {
        Properties props = new Properties();
        props.setProperty("minObjectsPerPartition", "123");
        props.setProperty("boneop.maxObjectsPerPartition", "456");
        props.setProperty("idleObjectTestPeriodInSeconds", "999");
        props.setProperty("username", "test");
        props.setProperty("partitionCount", "an int which is invalid");
        props.setProperty("idleMaxAgeInSeconds", "a long which is invalid");
        BoneOPConfig config = new BoneOPConfig(props);
        assertEquals(123, config.getMinObjectsPerPartition());
        assertEquals(456, config.getMaxObjectsPerPartition());
        assertEquals(1, config.getPartitionCount());
        assertEquals(999, config.getIdleObjectTestPeriod(TimeUnit.SECONDS));
        assertEquals(3600, config.getIdleMaxAge(TimeUnit.SECONDS));
    }

    /**
     * Property get/set
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testGettersSetters() {
        Properties driverProperties = new Properties();
        config.setIdleObjectTestPeriodInSeconds(60);
        config.setIdleMaxAgeInSeconds(60);
        config.setReleaseHelperThreads(3);
        config.setMaxObjectsPerPartition(5);
        config.setMinObjectsPerPartition(5);
        config.setPartitionCount(1);
        config.setAcquireIncrement(6);
        config.setAcquireRetryDelay(60, TimeUnit.SECONDS);
        config.setWaitTime(60, TimeUnit.SECONDS);
        config.setIdleMaxAge(60, TimeUnit.SECONDS);
        config.setIdleMaxAgeInSeconds(60);
        config.setIdleObjectTestPeriod(60, TimeUnit.SECONDS);
        config.setMaxObjectAge(60, TimeUnit.SECONDS);
        config.setDefaultCatalog("foo");
        config.setStatisticsEnabled(true);
        config.setNullOnObjectTimeout(true);
        config.setResetObjectOnClose(true);
        config.setAcquireRetryAttempts(2);
        config.setCloseObjectWatch(true);
        assertTrue(config.isNullOnObjectTimeout());
        assertTrue(config.isResetObjectOnClose());
        assertEquals("foo", config.getDefaultCatalog());
        assertTrue(config.isStatisticsEnabled());
        assertEquals(2, config.getAcquireRetryAttempts());
        config.setMaxObjectAgeInSeconds(60);
        assertEquals(60, config.getMaxObjectAgeInSeconds());
        assertEquals(1, config.getIdleObjectTestPeriodInMinutes());
        assertEquals(1, config.getIdleMaxAgeInMinutes());
        assertEquals(60000, config.getWaitTimeInMillis());
        assertEquals(60, config.getWaitTime(TimeUnit.SECONDS));

        assertEquals(60000, config.getAcquireRetryDelayInMillis());
        assertEquals(60, config.getMaxObjectAgeInSeconds());
        assertEquals(60, config.getMaxObjectAge(TimeUnit.SECONDS));
        assertEquals(1, config.getMaxObjectAge(TimeUnit.MINUTES));
        assertTrue(config.isCloseObjectWatch());
        ObjectListener hook = new AbstractObjectListener() {
            // do nothing
        };
        config.setObjectListener(hook);

        config.setPoolName("foo");
        config.setDisableJMX(false);
        config.setObjectOccupyTimeLimitInMillis(123);
        config.setObjectOccupyTimeLimitInMillis(123);
        config.setDisableObjectTracking(true);
        config.setWaitTimeInMillis(9999);
        config.setCloseObjectWatchTimeoutInMillis(Long.MAX_VALUE);
        String lifo = "LIFO";
        config.setServiceOrder(lifo);
        config.setConfigFile("abc");
        config.setIdleObjectTestPeriodInMinutes(1);
        config.setWaitTimeInMillis(1000);
        config.setCloseObjectWatchTimeoutInMillis(1000);
        config.setIdleMaxAgeInMinutes(2);

        assertEquals(hook, config.getObjectListener());
        assertEquals(1000, config.getWaitTimeInMillis());
        assertEquals(123, config.getObjectOccupyTimeLimit(TimeUnit.MILLISECONDS));
        assertEquals(1000, config.getCloseConnectionWatchTimeout(TimeUnit.MILLISECONDS));
        assertEquals(120, config.getIdleMaxAge(TimeUnit.SECONDS));
        assertEquals(1000, config.getCloseObjectWatchTimeoutInMillis());
        assertEquals(1, config.getIdleObjectTestPeriodInMinutes());
        assertEquals(lifo, config.getServiceOrder());
        assertEquals("abc", config.getConfigFile());
        assertEquals(1000, config.getCloseObjectWatchTimeoutInMillis());
        assertEquals("foo", config.getPoolName());
        assertEquals(3, config.getReleaseHelperThreads());
        assertEquals(5, config.getMaxObjectsPerPartition());
        assertEquals(5, config.getMinObjectsPerPartition());
        assertEquals(6, config.getAcquireIncrement());
        assertEquals(1000, config.getWaitTimeInMillis());
        assertEquals(true, config.isDisableObjectTracking());
        assertEquals(123, config.getObjectOccupyTimeLimitInMillis());
        assertEquals(1, config.getPartitionCount());
    }

    /**
     * Config file scrubbing
     */
    @Test
    public void testConfigSanitize() {
        config.setMaxObjectsPerPartition(-1);
        config.setMinObjectsPerPartition(-1);
        config.setPartitionCount(-1);

        config.setAcquireIncrement(0);

        config.setPoolAvailabilityThreshold(-50);
        config.setWaitTimeInMillis(0);
        config.setServiceOrder("something non-sensical");
        config.setAcquireRetryDelayInMillis(-1);

        config.setReleaseHelperThreads(-1);
        config.setLazyInit(false);
        config.sanitize();

        assertEquals(1000, config.getAcquireRetryDelay(TimeUnit.MILLISECONDS));
        assertEquals(1000, config.getAcquireRetryDelayInMillis());
        assertEquals("FIFO", config.getServiceOrder());
        assertEquals(Long.MAX_VALUE, config.getWaitTimeInMillis());
        assertNotNull(config.toString());
        assertFalse(config.getAcquireIncrement() == 0);
        assertFalse(config.getReleaseHelperThreads() == -1);
        assertFalse(config.getMaxObjectsPerPartition() == -1);
        assertFalse(config.getMinObjectsPerPartition() == -1);
        assertFalse(config.getPartitionCount() == -1);
        assertFalse(config.isLazyInit());
        config.setLazyInit(true);
        config.setMinObjectsPerPartition(config.getMaxObjectsPerPartition() + 1);
        config.setServiceOrder(null);
        config.setPoolStrategy("CACHED");
        config.sanitize();
        assertEquals("FIFO", config.getServiceOrder());
        assertEquals("CACHED", config.getPoolStrategy());
        assertEquals(config.getMinObjectsPerPartition(), config.getMaxObjectsPerPartition());
        assertEquals(20, config.getPoolAvailabilityThreshold());
        assertTrue(config.isLazyInit());

        config.sanitize();
        config.setPoolStrategy("DEFAULT");
        config.sanitize();
        assertEquals("DEFAULT", config.getPoolStrategy());
        // coverage
        BoneOPConfig config = new BoneOPConfig();
        config.setPoolStrategy(UUID.randomUUID().toString());
        config.sanitize();
        assertEquals("DEFAULT", config.getPoolStrategy());
    }


    /**
     * Tests general methods.
     *
     * @throws CloneNotSupportedException
     */
    @Test
    public void testCloneEqualsConfigHashCode() throws CloneNotSupportedException {
        BoneOPConfig clone = config.clone();
        assertTrue(clone.hasSameConfiguration(config));
        assertFalse(clone.hasSameConfiguration(null));
        assertTrue(clone.hasSameConfiguration(clone));
        clone.setPoolName("different pool name.");
        assertFalse(clone.hasSameConfiguration(config));
    }

    /**
     * Tries to load an invalid property file.
     *
     * @throws CloneNotSupportedException
     * @throws IOException
     */
    @Test
    public void testLoadPropertyFileInvalid() throws CloneNotSupportedException, IOException {
        BoneOPConfig config = new BoneOPConfig();
        BoneOPConfig clone = config.clone();

        config.loadProperties("invalid-property-file.xml");
        assertTrue(config.hasSameConfiguration(clone));
    }

    /**
     * Tries to load an invalid property file.
     *
     * @throws CloneNotSupportedException
     * @throws IOException
     * @throws URISyntaxException
     */
    @Test
    public void testLoadPropertyFileValid() throws CloneNotSupportedException, IOException, URISyntaxException {
        BoneOPConfig config = new BoneOPConfig();
        //coverage
        config.loadProperties("boneop-config.xml");
    }

    /**
     * See how the config handles a garbage filled file.
     *
     * @throws CloneNotSupportedException
     * @throws IOException
     */
    @Test
    public void testLoadPropertyFileInvalid2() throws CloneNotSupportedException, IOException {
        BoneOPConfig config = new BoneOPConfig();
        BoneOPConfig clone = config.clone();

        config.loadProperties("java/lang/String.class");
        assertTrue(config.hasSameConfiguration(clone));
    }

    @Test
    public void testSetObjectListenerClassName() throws Exception {
        BoneOPConfig config = new BoneOPConfig();
        String className = "java/lang/String.class";
        config.setObjectListenerClassName(className);
        assertEquals(className, config.getObjectListenerClassName());
        assertTrue(null == config.getObjectListener());
    }

    @Test
    public void testSetClassLoader() throws Exception {
        BoneOPConfig config = new BoneOPConfig();
        config.setClassLoader(getClass().getClassLoader());
        assertEquals(getClass().getClassLoader(), config.getClassLoader());
    }

    @Test
    public void testLoadClass() throws Exception {
        BoneOPConfig conf = new BoneOPConfig();
        Class<?> clazz = conf.loadClass(String.class.getName());
        assertNotNull(clazz);
        conf.setClassLoader(getClass().getClassLoader());
        clazz = conf.loadClass(String.class.getName());
        assertNotNull(clazz);
    }
}