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
/**
 *
 */
package com.jolbox.boneop.listener;

import com.jolbox.boneop.BoneOP;
import com.jolbox.boneop.BoneOPConfig;
import org.easymock.IAnswer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.easymock.EasyMock.*;
import static org.testng.Assert.*;

/**
 * Tests the connection hooks.
 *
 * @author wallacew
 */
public class TestConnectionHook {

    /**
     * Mock support.
     */
    private static BoneOPConfig mockConfig;
    /**
     * Pool handle.
     */
    private static BoneOP poolClass;
    /**
     * Helper class.
     */
    private static CustomHook hookClass;

    /**
     * Setups all mocks.
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @BeforeClass
    public static void setup() throws Exception, ClassNotFoundException {
        ConnectionState.valueOf(ConnectionState.NOP.toString()); // coverage BS.

        hookClass = new CustomHook();
        Class.forName("com.jolbox.bonecp.MockJDBCDriver");
        mockConfig = createNiceMock(BoneOPConfig.class);
        expect(mockConfig.getPartitionCount()).andReturn(1).anyTimes();
        expect(mockConfig.getMaxObjectsPerPartition()).andReturn(5).anyTimes();
        expect(mockConfig.getAcquireIncrement()).andReturn(0).anyTimes();
        expect(mockConfig.getMinObjectsPerPartition()).andReturn(5).anyTimes();
        expect(mockConfig.getIdleConnectionTestPeriodInMinutes()).andReturn(10000L).anyTimes();
        expect(mockConfig.getReleaseHelperThreads()).andReturn(0).anyTimes();
        expect(mockConfig.isDisableObjectTracking()).andReturn(true).anyTimes();
        expect(mockConfig.getObjectListener()).andReturn(hookClass).anyTimes();
        replay(mockConfig);

        // once for no release threads, once with release threads....
        poolClass = new BoneOP(mockConfig, null);

    }

    /**
     * Unload the driver.
     *
     * @throws SQLException
     */
    @AfterClass
    public static void destroy() throws SQLException {

        poolClass.shutdown();
        poolClass = null;
    }

    /**
     * Test method for
     * {@link com.jolbox.bonecp.hooks.AbstractConnectionHook#onAcquire(com.jolbox.bonecp.ConnectionHandle)}.
     */
    @Test
    public void testOnAcquire() {
        assertEquals(5, hookClass.acquire);
    }

    /**
     * Test method for
     * {@link com.jolbox.bonecp.hooks.AbstractConnectionHook#onCheckOut(com.jolbox.bonecp.ConnectionHandle)}.
     *
     * @throws SQLException
     */
    @Test
    public void testOnCheckOutAndOnCheckin() throws Exception {
        poolClass.returnObject(poolClass.getObject());
        assertEquals(1, hookClass.checkout);
        assertEquals(1, hookClass.checkin);
    }

    /**
     * Test method for
     * {@link com.jolbox.bonecp.hooks.AbstractConnectionHook#onDestroy(com.jolbox.bonecp.ConnectionHandle)}.
     */
    @Test
    public void testOnDestroy() {
        poolClass.close();
        assertEquals(5, hookClass.destroy);
    }

    /**
     * Just to do code coverage of abstract class.
     *
     * @throws SQLException
     */
    @Test
    public void dummyCoverage() throws Exception {
        CoverageHook hook = new CoverageHook();
        reset(mockConfig);
        mockConfig.sanitize();
        expectLastCall().anyTimes();

        expect(mockConfig.getPartitionCount()).andReturn(1).anyTimes();
        expect(mockConfig.getMaxObjectsPerPartition()).andReturn(5).anyTimes();
        expect(mockConfig.getMinObjectsPerPartition()).andReturn(5).anyTimes();
        expect(mockConfig.getIdleConnectionTestPeriodInMinutes()).andReturn(10000L).anyTimes();
        expect(mockConfig.getReleaseHelperThreads()).andReturn(0).anyTimes();
        expect(mockConfig.isDisableObjectTracking()).andReturn(true).anyTimes();
        expect(mockConfig.getObjectListener()).andReturn(hook).anyTimes();
        replay(mockConfig);

        poolClass = new BoneOP(mockConfig, null);

        poolClass.returnObject(poolClass.getObject());
        poolClass.close();

        reset(mockConfig);
        expect(mockConfig.getPartitionCount()).andReturn(1).anyTimes();
        expect(mockConfig.getMaxObjectsPerPartition()).andReturn(5).anyTimes();
        expect(mockConfig.getMinObjectsPerPartition()).andReturn(5).anyTimes();
        expect(mockConfig.getIdleConnectionTestPeriodInMinutes()).andReturn(10000L).anyTimes();
        expect(mockConfig.getReleaseHelperThreads()).andReturn(0).anyTimes();
        expect(mockConfig.getObjectListener()).andReturn(hook).anyTimes();
        expect(mockConfig.isDisableObjectTracking()).andReturn(true).anyTimes();
        replay(mockConfig);
        try {
            poolClass = new BoneOP(mockConfig, null);
            // should throw an exception
        } catch (Exception e) {
            // do nothing
        }

        poolClass.close();
    }

    /**
     * Test method for
     * {@link com.jolbox.bonecp.hooks.AbstractConnectionHook#onDestroy(com.jolbox.bonecp.ConnectionHandle)}.
     *
     * @throws SQLException
     */
    @Test
    public void testOnAcquireFail() throws SQLException {
        hookClass = new CustomHook();
        reset(mockConfig);
        mockConfig.sanitize();
        expectLastCall().anyTimes();

        expect(mockConfig.getPartitionCount()).andReturn(1).anyTimes();
        expect(mockConfig.getMaxObjectsPerPartition()).andReturn(5).anyTimes();
        expect(mockConfig.getMinObjectsPerPartition()).andReturn(5).anyTimes();
        expect(mockConfig.getIdleConnectionTestPeriodInMinutes()).andReturn(10000L).anyTimes();
        expect(mockConfig.getReleaseHelperThreads()).andReturn(0).anyTimes();
        expect(mockConfig.isDisableObjectTracking()).andReturn(true).anyTimes();
        expect(mockConfig.getObjectListener()).andReturn(hookClass).anyTimes();
        replay(mockConfig);

        try {
            poolClass = new BoneOP(mockConfig, null);
            poolClass.getObject();
        } catch (Exception e) {
            // do nothing
        }
        assertEquals(1, hookClass.fail);

    }

    /**
     * @throws SQLException
     */
    @Test
    public void testOnAcquireFailDefault() throws SQLException {
        ObjectListener hook = new AbstractObjectListener() {
            // do nothing
        };
        AcquireFailConfig fail = createNiceMock(AcquireFailConfig.class);
        expect(fail.getAcquireRetryDelayInMs()).andReturn(0L).times(5).andThrow(new RuntimeException()).once();
        expect(fail.getAcquireRetryAttempts()).andReturn(new AtomicInteger(2)).times(3).andReturn(new AtomicInteger(1)).times(3);
        replay(fail);
        assertTrue(hook.onAcquireFail(new SQLException(), fail));
        assertFalse(hook.onAcquireFail(new SQLException(), fail));
        assertFalse(hook.onAcquireFail(new SQLException(), fail));

    }

    /**
     * Test method.
     *
     * @throws SQLException
     */
    @Test
    public void testonQueryExecuteTimeLimitExceeded() throws Exception {
        reset(mockConfig);
        expect(mockConfig.getPartitionCount()).andReturn(1).anyTimes();
        expect(mockConfig.getMaxObjectsPerPartition()).andReturn(5).anyTimes();
        expect(mockConfig.getMinObjectsPerPartition()).andReturn(5).anyTimes();
        expect(mockConfig.getIdleConnectionTestPeriodInMinutes()).andReturn(10000L).anyTimes();
        expect(mockConfig.getReleaseHelperThreads()).andReturn(0).anyTimes();
        expect(mockConfig.isDisableObjectTracking()).andReturn(true).anyTimes();
        expect(mockConfig.getObjectListener()).andReturn(hookClass).anyTimes();
        expect(mockConfig.getQueryExecuteTimeLimitInMs()).andReturn(200L).anyTimes();
        expect(mockConfig.getWaitTimeInMs()).andReturn(Long.MAX_VALUE).anyTimes();
        expect(mockConfig.isDeregisterDriverOnClose()).andReturn(false).anyTimes();

        PreparedStatement mockPreparedStatement = createNiceMock(PreparedStatement.class);
        Connection mockConnection = createNiceMock(Connection.class);
        expect(mockConnection.prepareStatement("")).andReturn(mockPreparedStatement).anyTimes();
        expect(mockPreparedStatement.execute()).andAnswer(new IAnswer<Boolean>() {

            public Boolean answer() throws Throwable {
                Thread.sleep(300); // something that exceeds our limit
                return false;
            }
        }).once();
        replay(mockConfig, mockPreparedStatement, mockConnection);

        poolClass = new BoneOP(mockConfig, null);
        Object con = poolClass.getObject();

        assertEquals(1, hookClass.queryTimeout);
        reset(mockPreparedStatement, mockConnection);
        poolClass.close();
    }

    /**
     * Test method.
     *
     * @throws SQLException
     */
    @Test
    public void testonQueryExecuteTimeLimitExceededCoverage() throws Exception {
        Connection mockConnection = createNiceMock(Connection.class);
        reset(mockConfig, mockConnection);
        expect(mockConfig.getPartitionCount()).andReturn(1).anyTimes();
        expect(mockConfig.getMaxObjectsPerPartition()).andReturn(5).anyTimes();
        expect(mockConfig.getMinObjectsPerPartition()).andReturn(5).anyTimes();
        expect(mockConfig.getIdleConnectionTestPeriodInMinutes()).andReturn(10000L).anyTimes();
        expect(mockConfig.getReleaseHelperThreads()).andReturn(0).anyTimes();
        expect(mockConfig.isDisableObjectTracking()).andReturn(true).anyTimes();
        expect(mockConfig.getObjectListener()).andReturn(new CoverageHook()).anyTimes();
        expect(mockConfig.getQueryExecuteTimeLimitInMs()).andReturn(200L).anyTimes();

        PreparedStatement mockPreparedStatement = createNiceMock(PreparedStatement.class);
        expect(mockConnection.prepareStatement("")).andReturn(mockPreparedStatement).anyTimes();
        expect(mockPreparedStatement.execute()).andAnswer(new IAnswer<Boolean>() {

            public Boolean answer() throws Throwable {
                Thread.sleep(300); // something that exceeds our limit
                return false;
            }
        }).once();
        replay(mockConfig, mockPreparedStatement, mockConnection);

        poolClass = new BoneOP(mockConfig, null);
        Object con = poolClass.getObject();
        assertEquals(1, hookClass.queryTimeout);
    }
}
