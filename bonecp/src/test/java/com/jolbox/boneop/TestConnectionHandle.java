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

import com.google.common.base.FinalizableReferenceQueue;
import com.jolbox.boneop.listener.AcquireFailConfig;
import com.jolbox.boneop.listener.CoverageHook;
import com.jolbox.boneop.listener.CustomHook;
import com.jolbox.boneop.listener.ObjectListener;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.Thread.State;
import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.easymock.EasyMock.*;
import static org.testng.Assert.*;

/**
 * Mock unit testing for Connection Handle class.
 *
 * @author wwadge
 */
public class TestConnectionHandle {

    /**
     * Test class handle.
     */
    private ObjectHandle<TestObject> testClass;
    /**
     * Mock handle.
     */
    private ObjectHandle<TestObject> mockConnection = createNiceMock(ObjectHandle.class);
    private TestObject mockObject = createNiceMock(TestObject.class);
    /**
     * Mock handle.
     */
    private BoneOP<TestObject> mockPool = createNiceMock(BoneOP.class);
    /**
     * Mock handle.
     */
    private Logger mockLogger;
    /**
     * Config clone.
     */
    private BoneOPConfig config;

    /**
     * Reset everything.
     *
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    @BeforeMethod
    public void before() throws Exception {
        this.config = CommonTestUtils.getConfigClone();
        this.mockPool.connectionStrategy = new DefaultObjectStrategy(this.mockPool);
        expect(this.mockPool.getConfig()).andReturn(this.config).anyTimes();

        expect(this.mockConnection.getPool()).andReturn(this.mockPool).anyTimes();
        replay(this.mockConnection, this.mockPool);

        this.config.setReleaseHelperThreads(0);
        this.config.setTransactionRecoveryEnabled(false);
        this.config.setStatisticsEnabled(true);

        this.testClass = ObjectHandle.<TestObject>createTestObjectHandle(this.mockConnection.getInternalObject(), this.mockPool);

        this.mockLogger = TestUtils.mockLogger(testClass.getClass());

        Field field = this.testClass.getClass().getDeclaredField("logicallyClosed");
        field.setAccessible(true);
        field.set(this.testClass, false);

        field = this.testClass.getClass().getDeclaredField("statisticsEnabled");
        field.setAccessible(true);
        field.set(this.testClass, true);

        field = this.testClass.getClass().getDeclaredField("statistics");
        field.setAccessible(true);
        field.set(this.testClass, new Statistics(this.mockPool));

        reset(this.mockConnection, this.mockPool);
    }

    /**
     * For test.
     */
    static int count = 1;

    /**
     * Tests obtaining internal connection.
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void testObtainInternalConnection() throws Exception {
        expect(this.mockPool.getConfig()).andReturn(this.config).anyTimes();

        this.config.setAcquireRetryDelayInMs(1);
        CustomHook testHook = new CustomHook();
        this.config.setObjectListener(testHook);
        // make it fail the first time and succeed the second time
        expect(this.mockPool.getDown()).andReturn(new AtomicBoolean()).anyTimes();
        expect(this.mockPool.obtainRawInternalObject()).andThrow(new SQLException()).once().andReturn(this.mockObject).once();
        replay(this.mockPool);
        // get counts on our hooks

        assertEquals(1, testHook.fail);
        assertEquals(1, testHook.acquire);

        // Test 2: Same thing but without the hooks
        reset(this.mockPool);
        expect(this.mockPool.getDown()).andReturn(new AtomicBoolean()).anyTimes();
        expect(this.mockPool.getConfig()).andReturn(this.config).anyTimes();
        expect(this.mockPool.obtainRawInternalObject()).andThrow(new SQLException()).once().andReturn(this.mockObject).once();
        count = 1;
        this.config.setObjectListener(null);
        replay(this.mockPool);

        // Test 3: Keep failing
        reset(this.mockPool);
        expect(this.mockPool.getConfig()).andReturn(this.config).anyTimes();
        expect(this.mockPool.obtainRawInternalObject()).andThrow(new SQLException()).anyTimes();
        replay(this.mockPool);
        count = 99;
        this.config.setAcquireRetryAttempts(2);
        try {
            fail("Should have thrown an exception");
        } catch (Exception e) {
            // expected behaviour
        }

        //	Test 4: Get signalled to interrupt fail delay
        count = 99;
        this.config.setAcquireRetryAttempts(2);
        this.config.setAcquireRetryDelayInMs(7000);
        final Thread currentThread = Thread.currentThread();

        try {
            new Thread(new Runnable() {

                //				@Override
                public void run() {
                    while (!currentThread.getState().equals(State.TIMED_WAITING)) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    currentThread.interrupt();
                }
            }).start();
            this.testClass.obtainInternalObject(this.mockPool);
            fail("Should have thrown an exception");
        } catch (Exception e) {
            // expected behaviour
        }
        this.config.setAcquireRetryDelayInMs(10);

    }

    /**
     * Test bounce of inner connection.
     *
     * @throws IllegalArgumentException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void testStandardMethods() throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException {
        Set<String> skipTests = new HashSet<String>();
        skipTests.add("close");
        skipTests.add("getConnection");
        skipTests.add("markPossiblyBroken");
        skipTests.add("trackStatement");
        skipTests.add("checkClosed");
        skipTests.add("isClosed");
        skipTests.add("internalClose");
        skipTests.add("prepareCall");
        skipTests.add("prepareStatement");
        skipTests.add("setClientInfo");
        skipTests.add("getConnectionLastUsed");
        skipTests.add("setConnectionLastUsed");
        skipTests.add("getConnectionLastReset");
        skipTests.add("setConnectionLastReset");
        skipTests.add("isPossiblyBroken");
        skipTests.add("getOriginatingPartition");
        skipTests.add("setOriginatingPartition");
        skipTests.add("renewConnection");
        skipTests.add("clearStatementCaches");
        skipTests.add("obtainInternalConnection");
        skipTests.add("refreshConnection");
        skipTests.add("recreateConnectionHandle");
        skipTests.add("fillConnectionFields");
        skipTests.add("createConnectionHandle");

        skipTests.add("sendInitSQL");
        skipTests.add("$VRi"); // this only comes into play when code coverage is started. Eclemma bug?
        expect(this.mockPool.getConfig()).andReturn(this.config).anyTimes();
        replay(this.mockPool);
        CommonTestUtils.testStatementBounceMethod(this.mockConnection, this.testClass, skipTests, this.mockConnection);
    }

    /**
     * Test marking of possibly broken status.
     *
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    @Test
    public void testMarkPossiblyBroken() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field field = this.testClass.getClass().getDeclaredField("possiblyBroken");
        field.setAccessible(true);
        field.set(this.testClass, false);
        this.testClass.markPossiblyBroken(new PoolException());
        Assert.assertTrue(field.getBoolean(this.testClass));

        // Test that a db fatal error will lead to the pool being instructed to terminate all connections (+ log)
        expect(this.mockPool.getDown()).andReturn(new AtomicBoolean()).anyTimes();
        this.mockPool.connectionStrategy.destroyAllObjects();
        this.mockLogger.error((String) anyObject(), anyObject());
        replay(this.mockPool);
        this.testClass.markPossiblyBroken(new PoolException("test", "08001"));
        verify(this.mockPool);

    }

    /**
     * Test.
     */
    boolean interrupted = false;
    /**
     * Test.
     */
    boolean started = false;

    /**
     * Closing a connection handle should release that connection back in the pool and mark it as closed.
     *
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testClose() throws Exception {

        Field field = this.testClass.getClass().getDeclaredField("doubleCloseCheck");
        field.setAccessible(true);
        field.set(this.testClass, true);

        this.testClass.renewObject();
        this.mockPool.releaseObject(anyObject(TestObject.class));
        expectLastCall().once().andThrow(new PoolException()).once();
        expect(this.mockPool.getConfig()).andReturn(this.config).anyTimes();
        replay(this.mockPool);

        // create a thread so that we can check that thread.interrupt was called during connection close.
//		final Thread thisThread = Thread.currentThread();
        Thread testThread = new Thread(new Runnable() {

            //			@Override
            public void run() {
                try {
                    TestConnectionHandle.this.started = true;
                    while (true) {
                        Thread.sleep(20);
                    }
//				} catch (InterruptedException e) {
//					TestConnectionHandle.this.interrupted = true;
                } catch (Exception e) {
                    TestConnectionHandle.this.interrupted = true;

                }
            }
        });
        testThread.start();
        while (!this.started) {
            Thread.sleep(20);
        }
        this.testClass.setThreadWatch(testThread);
        this.testClass.close();
        testThread.join();
        assertTrue(this.interrupted); // thread should have been interrupted 

        // logically mark the connection as closed
        field = this.testClass.getClass().getDeclaredField("logicallyClosed");

        field.setAccessible(true);
        Assert.assertTrue(field.getBoolean(this.testClass));
        assertTrue(this.testClass.isClosed());

        this.testClass.renewObject();
        try {
            this.testClass.close(); // 2nd time should throw an exception
            fail("Should have thrown an exception");
        } catch (Throwable t) {
            // do nothing.
        }

    }

    /**
     * Tests sendInitialSQL method.
     *
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws SQLException
     */
    @Test
    public void testSendInitialSQL() throws Exception, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, SQLException {

        BoneOPConfig mockConfig = createNiceMock(BoneOPConfig.class);
        expect(this.mockPool.getConfig()).andReturn(mockConfig).anyTimes();

        this.testClass.setInternalObject(this.mockObject);

        Statement mockStatement = createNiceMock(Statement.class);
        expect(mockStatement.execute("test")).andReturn(true).once();

        replay(mockConfig, this.mockPool, this.mockConnection, mockStatement);
        verify(mockConfig, this.mockPool, this.mockConnection, mockStatement);

    }

    /**
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws SQLException
     */
    @Test
    public void testDoubleClose() throws Exception {
        Field field = this.testClass.getClass().getDeclaredField("doubleCloseCheck");
        field.setAccessible(true);
        field.set(this.testClass, true);

        field = this.testClass.getClass().getDeclaredField("logicallyClosed");
        field.setAccessible(true);
        field.set(this.testClass, true);

        field = this.testClass.getClass().getDeclaredField("doubleCloseException");
        field.setAccessible(true);
        field.set(this.testClass, "fakeexception");
        this.mockLogger.error((String) anyObject(), anyObject());
        expectLastCall().once();

        this.mockPool.releaseObject(anyObject(TestObject.class));
        expectLastCall().once().andThrow(new PoolException()).once();
        replay(this.mockLogger, this.mockPool);

        this.testClass.close();

    }

    /**
     * Closing a connection handle should release that connection back in the pool and mark it as closed.
     *
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testInternalClose() throws Exception, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, SQLException {
        ConcurrentLinkedQueue<Statement> mockStatementHandles = createNiceMock(ConcurrentLinkedQueue.class);

        this.mockConnection.close();
        expectLastCall().once().andThrow(new SQLException()).once();

        Map<TestObject, Reference<ObjectHandle<TestObject>>> refs = new HashMap<>();
        expect(this.mockPool.getFinalizableRefs()).andReturn(refs).anyTimes();
        FinalizableReferenceQueue finalizableRefQueue = new FinalizableReferenceQueue();

        expect(this.mockPool.getFinalizableRefQueue()).andReturn(finalizableRefQueue).anyTimes();
        expect(this.mockConnection.getPool()).andReturn(this.mockPool).anyTimes();

        Field f = this.testClass.getClass().getDeclaredField("finalizableRefs");
        f.setAccessible(true);
        f.set(this.testClass, refs);
        this.testClass.internalClose();
        try {
            this.testClass.internalClose(); //2nd time should throw exception
            fail("Should have thrown an exception");
        } catch (Throwable t) {
            // do nothing.
        }
    }

    /**
     * Test for check closed routine.
     *
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     */
    @Test
    public void testCheckClosed() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

        this.testClass.renewObject();

        // call the method (should not throw an exception)
        Method method = this.testClass.getClass().getDeclaredMethod("checkClosed");
        method.setAccessible(true);
        method.invoke(this.testClass);

        // logically mark the connection as closed
        Field field = this.testClass.getClass().getDeclaredField("logicallyClosed");

        field.setAccessible(true);
        field.set(this.testClass, true);
        try {
            method.invoke(this.testClass);
            fail("Should have thrown an exception");
        } catch (Throwable t) {
            // do nothing.
        }
    }

    /**
     * Test renewal of connection.
     *
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    @Test
    public void testRenewConnection() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field field = this.testClass.getClass().getDeclaredField("doubleCloseCheck");
        field.setAccessible(true);
        field.set(this.testClass, true);

        field = this.testClass.getClass().getDeclaredField("logicallyClosed");
        field.setAccessible(true);
        field.set(this.testClass, true);

        this.testClass.renewObject();
        assertFalse(field.getBoolean(this.testClass));

    }

    /**
     * Tests various getter/setters.
     *
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws SecurityException
     * @throws NoSuchFieldException
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testSettersGetters() throws IllegalArgumentException, IllegalAccessException, SecurityException, NoSuchFieldException {
        ObjectPartition mockPartition = createNiceMock(ObjectPartition.class);
        this.testClass.setOriginatingPartition(mockPartition);
        assertEquals(mockPartition, this.testClass.getOriginatingPartition());

        this.testClass.setObjectLastResetInMs(123);
        assertEquals(this.testClass.getObjectLastResetInMs(), 123);
        assertEquals(this.testClass.getObjectLastReset(), 123);

        this.testClass.setObjectLastUsedInMs(456);
        assertEquals(this.testClass.getObjectLastUsedInMs(), 456);
        assertEquals(this.testClass.getObjectLastUsed(), 456);

        Field field = this.testClass.getClass().getDeclaredField("possiblyBroken");
        field.setAccessible(true);
        field.setBoolean(this.testClass, true);
        assertTrue(this.testClass.isPossiblyBroken());

        field = this.testClass.getClass().getDeclaredField("connectionCreationTimeInMs");
        field.setAccessible(true);
        field.setLong(this.testClass, 1234L);
        assertEquals(1234L, this.testClass.getObjectCreationTimeInMs());

        Object debugHandle = new Object();
        this.testClass.setDebugHandle(debugHandle);
        assertEquals(debugHandle, this.testClass.getDebugHandle());

        this.testClass.setInternalObject(this.mockObject);
        assertEquals(this.mockConnection, this.testClass.getInternalObject());
        assertEquals(this.mockConnection, this.testClass.getRawObject());

        field = this.testClass.getClass().getDeclaredField("logicallyClosed");
        field.setAccessible(true);
        field.setBoolean(this.testClass, true);
        assertTrue(this.testClass.isClosed());

        assertEquals(this.testClass.getPool(), this.mockPool);

        this.testClass.threadUsingConnection = Thread.currentThread();
        assertEquals(Thread.currentThread(), this.testClass.getThreadUsingConnection());

        this.testClass.setThreadWatch(Thread.currentThread());
        assertEquals(Thread.currentThread(), this.testClass.getThreadWatch());

    }

    /**
     * Simple test.
     */
    @Test
    public void testIsConnectionHandleAlive() {
        // just make sure this is bounced off to the right place
        reset(this.mockPool);
        expect(this.mockPool.isObjectHandleAlive(this.testClass)).andReturn(true).once();
        replay(this.mockPool);
        this.testClass.isObjectAlive();
        verify(this.mockPool);
    }

    /**
     * Tests isExpired method.
     *
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    @Test
    public void testIsExpired() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field field = this.testClass.getClass().getDeclaredField("maxConnectionAgeInMs");
        field.setAccessible(true);
        field.setLong(this.testClass, 1234L);
        assertTrue(this.testClass.isExpired(System.currentTimeMillis() + 9999L));
    }

    /**
     * Tests that a thrown exception will call the onAcquireFail hook.
     *
     * @throws SQLException
     */
    @Test

    public void testConstructorFail() throws SQLException {
        BoneOPConfig mockConfig = createNiceMock(BoneOPConfig.class);
        ObjectListener mockConnectionHook = createNiceMock(CoverageHook.class);
        expect(this.mockPool.getConfig()).andReturn(mockConfig).anyTimes();
//		expect(mockConfig.getReleaseHelperThreads()).andReturn(1).once();
        expect(mockConfig.getObjectListener()).andReturn(mockConnectionHook).once();
        expect(mockConnectionHook.onAcquireFail((Throwable) anyObject(), (AcquireFailConfig) anyObject())).andReturn(false).once();
        replay(this.mockPool, mockConfig, mockConnectionHook);
        try {
            ObjectHandle.createObjectHandle(mockPool);
            fail("Should throw an exception");
        } catch (Throwable t) {
            // do nothing.
        }
        verify(this.mockPool, mockConfig, this.mockPool);
    }

    /**
     *
     */
    @Test
    public void testStackTraceAndTxResolve() {
        this.testClass.setAutoCommitStackTrace("foo");
        this.testClass.getAutoCommitStackTrace();
        assertEquals("foo", this.testClass.getAutoCommitStackTrace());
        this.testClass.txResolved = true;
        assertTrue(this.testClass.isTxResolved());
    }

}
