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
import org.easymock.EasyMock;
import org.slf4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.management.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

import static org.easymock.EasyMock.*;
import static org.testng.Assert.*;


/**
 * @author wwadge
 */
public class TestBoneCP {

    /**
     * Class under test.
     */
    private static BoneOP testClass;
    /**
     * Mock handle.
     */
    private static BoneOPConfig mockConfig;
    /**
     * Mock handle.
     */
    private static ObjectPartition mockPartition;
    /**
     * Mock handle.
     */
    private static ScheduledExecutorService mockKeepAliveScheduler;
    /**
     * Mock handle.
     */
    private static ExecutorService mockConnectionsScheduler;
    /**
     * Mock handle.
     */
    private static BoundedLinkedTransferQueue<ObjectHandle> mockConnectionHandles;
    /**
     * Mock handle.
     */
    private static ObjectHandle mockConnection;
    /**
     * Mock handle.
     */
    private static Lock mockLock;
    /**
     * Mock handle.
     */
    private static Logger mockLogger;
    /**
     * Mock handle.
     */
    private static DatabaseMetaData mockDatabaseMetadata;

    /**
     * Mock setups.
     *
     * @throws Exception
     * @throws ClassNotFoundException
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws CloneNotSupportedException
     */
    @BeforeClass
    public static void setup() throws Exception, ClassNotFoundException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, CloneNotSupportedException {

    }

    /**
     * Reset the mocks.
     *
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws Exception
     * @throws NoSuchFieldException
     * @throws SecurityException
     */
    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void before() throws Exception {
        mockConfig = createNiceMock(BoneOPConfig.class);
        expect(mockConfig.getPartitionCount()).andReturn(2).anyTimes();
        expect(mockConfig.getMaxObjectsPerPartition()).andReturn(1).anyTimes();
        expect(mockConfig.getMinObjectsPerPartition()).andReturn(1).anyTimes();
        expect(mockConfig.getIdleObjectTestPeriodInMinutes()).andReturn(0L).anyTimes();
        expect(mockConfig.getIdleMaxAgeInMinutes()).andReturn(1000L).anyTimes();
        expect(mockConfig.getReleaseHelperThreads()).andReturn(1).once().andReturn(0).anyTimes();
        expect(mockConfig.isCloseObjectWatch()).andReturn(true).anyTimes();
        expect(mockConfig.getWaitTimeInMillis()).andReturn(Long.MAX_VALUE).anyTimes();
        expect(mockConfig.getServiceOrder()).andReturn("LIFO").anyTimes();

        expect(mockConfig.getAcquireRetryDelayInMillis()).andReturn(1000L).anyTimes();
        expect(mockConfig.getPoolName()).andReturn("poolName").anyTimes();
        expect(mockConfig.getPoolAvailabilityThreshold()).andReturn(20).anyTimes();

        replay(mockConfig);

        // once for no {statement, connection} release threads, once with release threads....
        testClass = new BoneOP(mockConfig, new TestObjectFactory());
        testClass = new BoneOP(mockConfig, new TestObjectFactory());

        Field field = testClass.getClass().getDeclaredField("partitions");
        field.setAccessible(true);
        List<ObjectPartition> partitions = (List<ObjectPartition>) field.get(testClass);

        // if all ok 
        assertEquals(2, partitions.size());
        // switch to our mock version now
        mockPartition = createNiceMock(ObjectPartition.class);
        partitions.add(0, mockPartition);
        partitions.add(1, mockPartition);

        mockKeepAliveScheduler = createNiceMock(ScheduledExecutorService.class);
        field = testClass.getClass().getDeclaredField("keepAliveScheduler");
        field.setAccessible(true);
        field.set(testClass, mockKeepAliveScheduler);

        field = testClass.getClass().getDeclaredField("objectScheduler");
        field.setAccessible(true);
        mockConnectionsScheduler = createNiceMock(ExecutorService.class);
        field.set(testClass, mockConnectionsScheduler);

        mockConnectionHandles = createNiceMock(BoundedLinkedTransferQueue.class);
        mockConnection = createNiceMock(ObjectHandle.class);
        mockLock = createNiceMock(Lock.class);
        mockLogger = TestUtils.mockLogger(testClass.getClass());
        makeThreadSafe(mockLogger, true);
        mockDatabaseMetadata = createNiceMock(DatabaseMetaData.class);
        mockLogger.error((String) anyObject(), anyObject());
        expectLastCall().anyTimes();

        reset(mockConfig, mockKeepAliveScheduler, mockConnectionsScheduler, mockPartition,
                mockConnectionHandles, mockConnection, mockLock);
    }

    /**
     * Test method for {@link com.jolbox.boneop.BoneOP#shutdown()}.
     *
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws SecurityException
     */
    @Test
    public void testShutdown() throws SecurityException, IllegalArgumentException, NoSuchFieldException, IllegalAccessException {
        testShutdownClose(true);
    }

    /**
     * Tests shutdown/close method.
     *
     * @param doShutdown call shutdown or call close
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    private void testShutdownClose(boolean doShutdown) throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        expect(mockKeepAliveScheduler.shutdownNow()).andReturn(null).once();
        expect(mockConnectionsScheduler.shutdownNow()).andReturn(null).once();

        expect(mockConnectionHandles.poll()).andReturn(null).once();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        Field field = testClass.getClass().getDeclaredField("releaseHelperThreadsConfigured");
        field.setAccessible(true);
        field.setBoolean(testClass, true);

        ExecutorService mockReleaseHelper = createNiceMock(ExecutorService.class);
        testClass.setReleaseHelper(mockReleaseHelper);
        expect(mockReleaseHelper.shutdownNow()).andReturn(null).once();

        ExecutorService mockStatementCloseHelperExecutor = createNiceMock(ExecutorService.class);
        testClass.setStatementCloseHelperExecutor(mockStatementCloseHelperExecutor);

        expect(mockStatementCloseHelperExecutor.shutdownNow()).andReturn(null).once();

        replay(mockConfig, mockConnectionsScheduler, mockStatementCloseHelperExecutor, mockKeepAliveScheduler, mockPartition, mockConnectionHandles, mockReleaseHelper);

        if (doShutdown) {
            testClass.shutdown();
        } else {
            testClass.close();
        }
        verify(mockConnectionsScheduler, mockStatementCloseHelperExecutor, mockKeepAliveScheduler, mockPartition, mockConnectionHandles, mockReleaseHelper);
    }

    /**
     * Test method for {@link com.jolbox.boneop.BoneOP#close()}.
     *
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws SecurityException
     */
    @Test
    public void testClose() throws SecurityException, IllegalArgumentException, NoSuchFieldException, IllegalAccessException {
        testClass.poolShuttingDown = false;
        testShutdownClose(false);
    }

    /**
     * Test method.
     *
     * @throws Exception
     */
    @Test
    public void testTerminateAllConnections() throws Exception {
        expect(mockConnectionHandles.poll()).andReturn(mockConnection).times(2).andReturn(null).once();
        mockConnection.internalClose();
        expectLastCall().once().andThrow(new Exception()).once();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockConnection.getOriginatingPartition()).andReturn(mockPartition).anyTimes();
        Connection mockRealConnection = createNiceMock(Connection.class);
        expect(mockConnection.getInternalObject()).andReturn(mockRealConnection).anyTimes();
        replay(mockRealConnection, mockConnectionsScheduler, mockKeepAliveScheduler, mockPartition, mockConnectionHandles, mockConnection);

        // test.
        testClass.objectStrategy.destroy();
        verify(mockConnectionsScheduler, mockKeepAliveScheduler, mockPartition, mockConnectionHandles, mockConnection);
    }

    /**
     * Test method for.
     *
     * @throws Exception
     */
    @Test
    public void testTerminateAllConnections2() throws Exception {
        Connection mockRealConnection = createNiceMock(Connection.class);

        // same test but to cover the finally section
        reset(mockRealConnection, mockConnectionsScheduler, mockKeepAliveScheduler, mockPartition, mockConnectionHandles, mockConnection);
        expect(mockConnectionHandles.poll()).andReturn(mockConnection).anyTimes();
        expect(mockConnection.getInternalObject()).andReturn(mockRealConnection).anyTimes();
        mockConnection.internalClose();
        expectLastCall().once().andThrow(new RuntimeException()).once();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockConnection.getOriginatingPartition()).andReturn(mockPartition).anyTimes();
        replay(mockRealConnection, mockConnectionsScheduler, mockKeepAliveScheduler, mockPartition, mockConnectionHandles, mockConnection);

        // test.
        try {
            testClass.objectStrategy.destroy();
            fail("Should throw exception");
        } catch (RuntimeException e) {
            // do nothing
        }
        verify(mockConnectionsScheduler, mockKeepAliveScheduler, mockPartition, mockConnectionHandles, mockConnection);
    }

    /**
     * Mostly for coverage.
     */
    @Test
    public void testPostDestroyConnection() {
        Connection mockRealConnection = createNiceMock(Connection.class);

        reset(mockConnection);
        expect(mockConnection.getOriginatingPartition()).andReturn(mockPartition).anyTimes();
        mockPartition.updateCreatedObjects(-1);
        expectLastCall().once();
        mockPartition.setUnableToCreateMoreObjects(false);
        expectLastCall().once();
        ObjectListener mockConnectionHook = createNiceMock(ObjectListener.class);
        expect(mockConnection.getObjectListener()).andReturn(mockConnectionHook).anyTimes();
        expect(mockConnection.getInternalObject()).andReturn(mockRealConnection).anyTimes();

        mockConnectionHook.onDestroy(mockConnection);
        expectLastCall().once();
        replay(mockRealConnection, mockConnectionHook, mockConnection);
        testClass.postDestroyObject(mockConnection);
        verify(mockConnectionHook, mockConnection);
    }

    /**
     * Test method for {@link BoneOP#getObject()}.
     *
     * @throws Exception
     * @throws InterruptedException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws NoSuchFieldException
     * @throws SecurityException
     */
    @Test
    public void testGetConnection() throws Exception {
        // Test 1: Get connection - normal state

        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(true).once();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockPartition.getAvailableObjects()).andReturn(1).anyTimes();
        expect(mockConnectionHandles.poll()).andReturn(mockConnection).once();
        mockConnection.renewObject();
        expectLastCall().once();

        replay(mockPartition, mockConnectionHandles, mockConnection);
        assertEquals(mockConnection, testClass.getObject());
        verify(mockPartition, mockConnectionHandles, mockConnection);
    }

    /**
     * Test method for {@link BoneOP#getObject()} .
     *
     * @throws Exception
     * @throws InterruptedException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws NoSuchFieldException
     * @throws SecurityException
     */
    @Test
    public void testGetConnectionWithTimeout() throws Exception {

        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(true).once();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockConnectionHandles.poll()).andReturn(null).anyTimes();

        replay(mockPartition, mockConnectionHandles, mockConnection);
        try {
            testClass.getObject();
            fail("Should have thrown an exception");
        } catch (Exception e) {
            // do nothing
        }
        verify(mockPartition, mockConnectionHandles, mockConnection);
    }

    /**
     * Attempting to fetch a connection from a pool that is marked as being shut down should throw an exception
     */
    @Test
    public void testGetConnectionOnShutdownPool() {
        // Test #8: 
        testClass.poolShuttingDown = true;
        try {
            testClass.getObject();
            fail("Should have thrown an exception");
        } catch (Exception e) {
            // do nothing
        }
    }

    /**
     * Like test 6, except we fake an unchecked exception to make sure our locks are released.
     *
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    @Test
    public void testGetConnectionUncheckedExceptionTriggeredWhileWaiting()
            throws NoSuchFieldException, IllegalAccessException {
        reset(mockPartition, mockConnectionHandles, mockConnection);
        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(false).anyTimes();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockPartition.getMaxObjects()).andReturn(0).anyTimes(); // cause a division by zero error
        replay(mockPartition, mockConnectionHandles, mockConnection, mockLock);
        try {
            testClass.getObject();
            fail("Should have thrown an exception");
        } catch (Throwable t) {
            // do nothing
        }
        verify(mockPartition, mockConnectionHandles, mockConnection, mockLock);
    }

    /**
     * If we hit our limit, we should signal for more connections to be created on the fly
     *
     * @throws Exception
     */
    @Test
    public void testGetConnectionLimitsHit() throws Exception {
        reset(mockPartition, mockConnectionHandles, mockConnection);
        expect(mockConfig.getPoolAvailabilityThreshold()).andReturn(0).anyTimes();
        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(false).anyTimes();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockPartition.getMaxObjects()).andReturn(10).anyTimes();
        expect(mockPartition.getAvailableObjects()).andReturn(1).anyTimes();
        BlockingQueue<Object> bq = new ArrayBlockingQueue<Object>(1);
        bq.add(new Object());
        expect(mockPartition.getPoolWatchThreadSignalQueue()).andReturn(bq);

        //		mockPartition.almostFullSignal();
        //		expectLastCall().once();
        expect(mockConnectionHandles.poll()).andReturn(mockConnection).once();
        mockConnection.renewObject();
        expectLastCall().once();

        replay(mockPartition, mockConnectionHandles, mockConnection);
        testClass.getObject();
        verify(mockPartition, mockConnectionHandles, mockConnection);
    }

    /**
     * Connection queues are starved of free connections. Should block and wait on one without spin-locking.
     *
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     * @throws InterruptedException
     * @throws Exception
     */
    @Test
    public void testGetConnectionConnectionQueueStarved()
            throws Exception {
        reset(mockPartition, mockConnectionHandles, mockConnection);
        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(true).once();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockConnectionHandles.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS)).andReturn(mockConnection).once();

        mockConnection.renewObject();
        expectLastCall().once();

        replay(mockPartition, mockConnectionHandles, mockConnection);
        assertEquals(mockConnection, testClass.getObject());
        verify(mockPartition, mockConnectionHandles, mockConnection);
    }

    /**
     * Simulate an interrupted exception elsewhere.
     *
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     * @throws InterruptedException
     */
    @Test
    public void testGetConnectionSimulateInterruptedException2()
            throws NoSuchFieldException, IllegalAccessException,
            InterruptedException {
        reset(mockPartition, mockConnectionHandles, mockConnection);
        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(true).once();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockConnectionHandles.poll()).andReturn(null).once();
        expect(mockConnectionHandles.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS)).andThrow(new InterruptedException()).once();

        replay(mockPartition, mockConnectionHandles, mockConnection);
        try {
            testClass.getObject();
            fail("Should have throw an SQL Exception");
        } catch (Exception e) {
            // do nothing
        }
        verify(mockPartition, mockConnectionHandles, mockConnection);
    }

    /**
     * Simulate an interrupted exception elsewhere.
     *
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     * @throws InterruptedException
     */
    @Test
    public void testGetConnectionSimulateInterruptedExceptionWithNullReturn()
            throws NoSuchFieldException, IllegalAccessException,
            InterruptedException {
        reset(mockPartition, mockConnectionHandles, mockConnection);
        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(true).once();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        testClass.nullOnObjectTimeout = true;
        expect(mockConnectionHandles.poll()).andReturn(null).once();
        expect(mockConnectionHandles.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS)).andThrow(new InterruptedException()).once();

        replay(mockPartition, mockConnectionHandles, mockConnection);
        try {
            assertNull(testClass.getObject());
        } catch (Exception e) {
            fail("Should not have throw an SQL Exception");
            // do nothing
        }
        verify(mockPartition, mockConnectionHandles, mockConnection);
    }

    /**
     * Test #3: Like test #2 but simulate an interrupted exception
     *
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     * @throws InterruptedException
     */
    @Test
    public void testGetConnectionSimulateInterruptedException()
            throws NoSuchFieldException, IllegalAccessException,
            InterruptedException {
        reset(mockPartition, mockConnectionHandles, mockConnection);
        expect(mockPartition.getMaxObjects()).andReturn(100).anyTimes();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockConnectionHandles.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS)).andThrow(new InterruptedException()).once();
        BlockingQueue<Object> bq = new ArrayBlockingQueue<Object>(1);
        bq.add(new Object());
        expect(mockPartition.getPoolWatchThreadSignalQueue()).andReturn(bq);
        replay(mockPartition, mockConnectionHandles, mockConnection);
        try {
            testClass.getObject();
            fail("Should have throw an SQL Exception");
        } catch (Exception e) {
            // do nothing
        }
        verify(mockPartition, mockConnectionHandles, mockConnection);
    }

    /**
     * Test #3: Like test #2 but simulate an interrupted exception
     *
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     * @throws InterruptedException
     */
    @Test
    public void testGetConnectionWithNullReturn()
            throws NoSuchFieldException, IllegalAccessException,
            InterruptedException {
        reset(mockPartition, mockConnectionHandles, mockConnection);
        expect(mockPartition.getMaxObjects()).andReturn(100).anyTimes();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockConnectionHandles.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS)).andReturn(null).once();
        BlockingQueue<Object> bq = new ArrayBlockingQueue<Object>(1);
        bq.add(new Object());
        testClass.nullOnObjectTimeout = true;
        expect(mockPartition.getPoolWatchThreadSignalQueue()).andReturn(bq);
        replay(mockPartition, mockConnectionHandles, mockConnection);
        try {
            assertNull(testClass.getObject());
        } catch (Exception e) {
            fail("Should have throw an SQL Exception");
            // do nothing
        }
        verify(mockPartition, mockConnectionHandles, mockConnection);
    }

    /**
     * Get connection, not finding any available block to wait for one
     *
     * @throws InterruptedException
     * @throws Exception
     */
    @Test
    public void testGetConnectionBlockOnUnavailable()
            throws Exception {

        reset(mockPartition, mockConnectionHandles, mockConnection);
        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(true).once();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockPartition.getAvailableObjects()).andReturn(1).anyTimes();
        expect(mockConnectionHandles.poll()).andReturn(null).once();
        expect(mockConnectionHandles.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS)).andReturn(mockConnection).once();

        mockConnection.renewObject();
        expectLastCall().once();

        replay(mockPartition, mockConnectionHandles, mockConnection);
        assertEquals(mockConnection, testClass.getObject());
        verify(mockPartition, mockConnectionHandles, mockConnection);
    }

    /**
     * Test obtaining a connection asynchronously.
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void testGetAsyncConnection() throws InterruptedException, ExecutionException {
        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(true).once();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockPartition.getAvailableObjects()).andReturn(1).anyTimes();
        expect(mockConnectionHandles.poll()).andReturn(mockConnection).once();
        mockConnection.renewObject();
        expectLastCall().once();

        replay(mockPartition, mockConnectionHandles, mockConnection);
        assertEquals(mockConnection, testClass.getAsyncObject().get());
        verify(mockPartition, mockConnectionHandles, mockConnection);

    }

    /**
     * Test method for {@link com.jolbox.boneop.BoneOP#releaseObject(Object)} .
     *
     * @throws Exception
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws InterruptedException
     */
    @Test
    public void testReleaseConnection() throws Exception, IllegalArgumentException, IllegalAccessException, SecurityException, NoSuchFieldException, InterruptedException {
        // Test #1: Test releasing connections directly (without helper threads)
        Field field = testClass.getClass().getDeclaredField("releaseHelperThreadsConfigured");
        field.setAccessible(true);
        field.setBoolean(testClass, false);

        expect(mockConnection.isPossiblyBroken()).andReturn(false).once();
        expect(mockConnection.getOriginatingPartition()).andReturn(mockPartition).anyTimes();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockPartition.getAvailableObjects()).andReturn(1).anyTimes();

        expect(mockConnectionHandles.offer(mockConnection)).andReturn(false).anyTimes();
        //		expect(mockConnectionHandles.offer(mockObject)).andReturn(true).once();

        // should reset last connection use
        mockConnection.setObjectLastUsedInMillis(anyLong());
        expectLastCall().once();
        Connection mockRealConnection = createNiceMock(Connection.class);
        expect(mockConnection.getInternalObject()).andReturn(mockRealConnection).anyTimes();

        replay(mockRealConnection, mockConnection, mockPartition, mockConnectionHandles);
        testClass.releaseObject(mockConnection);
        verify(mockConnection, mockPartition, mockConnectionHandles);

        reset(mockConnection, mockPartition, mockConnectionHandles);
    }

    /**
     * @throws Exception
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testReleaseConnectionThrowingException() throws Exception, IllegalArgumentException, IllegalAccessException, SecurityException, NoSuchFieldException, InterruptedException {

        // Test #2: Same test as testReleaseConnection but throw an exception instead
//		Field field = testClass.getClass().getDeclaredField("releaseHelperThreadsConfigured");
//		field.setAccessible(true);
//		field.setBoolean(testClass, false);
        reset(mockConnection, mockPartition, mockConnectionHandles);

        // Test #2: Test with releaser helper threads configured
        Field field = testClass.getClass().getDeclaredField("releaseHelperThreadsConfigured");
        field.setAccessible(true);
        field.setBoolean(testClass, true);

        LinkedTransferQueue<ObjectHandle> mockPendingRelease = createNiceMock(LinkedTransferQueue.class);
        expect(mockConnection.getOriginatingPartition()).andReturn(mockPartition).anyTimes();
        expect(mockPartition.getObjectsPendingRelease()).andReturn(mockPendingRelease).anyTimes();
        mockPendingRelease.put(mockConnection);
        expectLastCall().once();

        replay(mockConnection, mockPartition, mockConnectionHandles, mockPendingRelease);
        testClass.releaseObject(mockConnection);
        verify(mockConnection, mockPartition, mockConnectionHandles, mockPendingRelease);

    }

    /**
     * Test method for {@link com.jolbox.boneop.BoneOP#internalReleaseObject(ObjectHandle)} .
     *
     * @throws InterruptedException
     * @throws Exception
     */
    @Test
    public void testInternalReleaseConnection() throws InterruptedException, Exception {
        // Test #1: Test normal case where connection is considered to be not broken
        // should reset last connection use
        mockConnection.setObjectLastUsedInMillis(anyLong());
        expectLastCall().once();
        expect(mockConnection.getOriginatingPartition()).andReturn(mockPartition).anyTimes();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockPartition.getAvailableObjects()).andReturn(1).anyTimes();
        Connection mockRealConnection = createNiceMock(Connection.class);
        expect(mockConnection.getInternalObject()).andReturn(mockRealConnection).anyTimes();

        //		expect(mockConnectionHandles.offer(mockObject)).andReturn(false).anyTimes();
        expect(mockConnectionHandles.offer(mockConnection)).andReturn(true).once();

        replay(mockRealConnection, mockConnection, mockPartition, mockConnectionHandles);
        testClass.internalReleaseObject(mockConnection);
        verify(mockConnection, mockPartition, mockConnectionHandles);
    }

    /**
     * Test method for {@link com.jolbox.boneop.BoneOP#internalReleaseObject(ObjectHandle)} .
     *
     * @throws InterruptedException
     * @throws Exception
     */
    @Test
    public void testInternalReleaseConnectionWhereConnectionIsBroken() throws InterruptedException, Exception {
        // Test case where connection is broken
        reset(mockConnection, mockPartition, mockConnectionHandles);
        expect(mockConnection.isPossiblyBroken()).andReturn(true).once();
        expect(mockConnection.getOriginatingPartition()).andReturn(mockPartition).anyTimes();
        // we're about to destroy this connection, so we can create new ones.
        mockPartition.setUnableToCreateMoreObjects(false);
        expectLastCall().once();

        // break out from the next method, we're not interested in it
        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(true).once();

        Connection mockRealConnection = createNiceMock(Connection.class);
        expect(mockConnection.getInternalObject()).andReturn(mockRealConnection).anyTimes();

        replay(mockPartition, mockConnection);
        testClass.internalReleaseObject(mockConnection);
        verify(mockPartition, mockConnection);
    }

    /**
     * Test method for {@link com.jolbox.boneop.BoneOP#internalReleaseObject(ObjectHandle)} .
     *
     * @throws InterruptedException
     * @throws Exception
     */
    @Test
    public void testInternalReleaseConnectionWhereConnectionIsExpired() throws InterruptedException, Exception {
        // Test case where connection is expired
        reset(mockConnection, mockPartition, mockConnectionHandles);

        Connection mockRealConnection = createNiceMock(Connection.class);
        expect(mockConnection.getInternalObject()).andReturn(mockRealConnection).anyTimes();

        // return a partition
        expect(mockConnection.getOriginatingPartition()).andReturn(mockPartition).anyTimes();
        // break out from this method, we're not interested in it
        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(true).once();

        expect(mockConnection.isExpired()).andReturn(true).anyTimes();
        mockConnection.internalClose();
        expectLastCall();

        replay(mockPartition, mockConnection, mockRealConnection);
        testClass.internalReleaseObject(mockConnection);
        verify(mockPartition, mockConnection, mockRealConnection);
    }

    /**
     * Test method for
     * {@link com.jolbox.boneop.BoneOP#putObjectBackInPartition(ObjectHandle)} .
     *
     * @throws InterruptedException
     * @throws Exception
     */
    @Test
    public void testPutConnectionBackInPartition() throws InterruptedException, Exception {
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockPartition.getAvailableObjects()).andReturn(1).anyTimes();
        Connection mockRealConnection = createNiceMock(Connection.class);
        expect(mockConnection.getInternalObject()).andReturn(mockRealConnection).anyTimes();

        expect(mockConnection.getOriginatingPartition()).andReturn(mockPartition).anyTimes();
        expect(mockConnectionHandles.tryTransfer(mockConnection)).andReturn(false).anyTimes();
        expect(mockConnectionHandles.offer(mockConnection)).andReturn(true).once();
        replay(mockRealConnection, mockPartition, mockConnectionHandles, mockConnection);
        testClass.putObjectBackInPartition(mockConnection);
        // FIXME
        //		assertEquals(2, ai.get());
        verify(mockPartition, mockConnectionHandles);

    }

    /**
     * Test method for
     * {@link com.jolbox.boneop.BoneOP#putObjectBackInPartition(ObjectHandle)} .
     *
     * @throws InterruptedException
     * @throws Exception
     */
    @Test
    public void testPutConnectionBackInPartitionWithResetConnectionOnClose() throws InterruptedException, Exception {
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockPartition.getAvailableObjects()).andReturn(1).anyTimes();

        expect(mockConnection.getOriginatingPartition()).andReturn(mockPartition).anyTimes();
        expect(mockConnectionHandles.tryTransfer(mockConnection)).andReturn(false).anyTimes();
        expect(mockConnectionHandles.offer(mockConnection)).andReturn(true).once();
        Connection mockInternalConnection = createNiceMock(Connection.class);
        expect(mockInternalConnection.getAutoCommit()).andReturn(false).once();
        expect(mockConnection.getInternalObject()).andReturn(mockInternalConnection).anyTimes();
        mockInternalConnection.rollback();
        mockInternalConnection.setAutoCommit(true);
        replay(mockInternalConnection, mockPartition, mockConnectionHandles, mockConnection);
        testClass.resetObjectOnClose = true;
        testClass.putObjectBackInPartition(mockConnection);
        verify(mockPartition, mockConnectionHandles);

    }

    /**
     * Test method for com.jolbox.bonecp.BoneCP isConnectionHandleAlive.
     *
     * @throws Exception
     */
    @Test
    public void testIsConnectionHandleAlive() throws Exception {
        expectLastCall().once();
        assertTrue(testClass.isObjectHandleAlive(mockConnection));
    }

    /**
     * Test method for com.jolbox.bonecp.BoneCP isConnectionHandleAlive.
     *
     * @throws Exception
     */
    @Test
    public void testIsConnectionHandleAliveNormalCaseWithConnectionTestStatement() throws Exception {

        // Test 3: Normal case (+ with connection test statement)
        Statement mockStatement = createNiceMock(Statement.class);
        expect(mockStatement.execute((String) anyObject())).andReturn(true).once();
        //		mockResultSet.close();
        //		expectLastCall().once();

        replay(mockConfig, mockConnection, mockDatabaseMetadata, mockStatement);
        assertTrue(testClass.isObjectHandleAlive(mockConnection));
        verify(mockConfig, mockConnection, mockDatabaseMetadata, mockStatement);
    }

    /**
     * Test method for com.jolbox.bonecp.BoneCP isConnectionHandleAlive.
     *
     * @throws Exception
     */
    @Test
    public void testIsConnectionHandleAliveNormalCaseWithConnectionTestTriggerException() throws Exception {
        Statement mockStatement = createNiceMock(Statement.class);
        // Test 4: Same like test testIsConnectionHandleAliveNormalCaseWithConnectionTestStatement but triggers exception

        expect(mockStatement.execute((String) anyObject())).andThrow(new RuntimeException()).once();
        mockStatement.close();
        expectLastCall().once();

        //		assertFalse(testClass.isConnectionHandleAlive(mockObject));
        try {
            mockConnection.logicallyClosed = true;// (code coverage)
            testClass.isObjectHandleAlive(mockConnection);
            fail("Should have thrown an exception");
        } catch (RuntimeException e) {
            // do nothing 
        }
    }

    /**
     * Test method for com.jolbox.bonecp.BoneCP isConnectionHandleAlive.
     *
     * @throws Exception
     */
    @Test
    public void testIsConnectionHandleAliveNormalCaseWithConnectionTestTriggerExceptionInFinally() throws Exception {
        Statement mockStatement = createNiceMock(Statement.class);

        // Test 5: Same like test testIsConnectionHandleAliveNormalCaseWithConnectionTestTriggerException
        // but triggers exception in finally block
        expect(mockStatement.execute((String) anyObject())).andThrow(new RuntimeException()).once();
        mockStatement.close();
        expectLastCall().andThrow(new Exception()).once();

        try {
            testClass.isObjectHandleAlive(mockConnection);
            fail("Should have thrown an exception");
        } catch (RuntimeException e) {
            // do nothing
        }

    }

    /**
     * Test method for maybeSignalForMoreConnections(com.jolbox.bonecp.ConnectionPartition)}.
     *
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void testMaybeSignalForMoreConnections() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(false).once();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        //		expect(mockConnectionHandles.size()).andReturn(1).anyTimes();
        expect(mockPartition.getAvailableObjects()).andReturn(1).anyTimes();
        expect(mockPartition.getMaxObjects()).andReturn(10).anyTimes();
        BlockingQueue<Object> bq = new ArrayBlockingQueue<Object>(1);
        expect(mockPartition.getPoolWatchThreadSignalQueue()).andReturn(bq).anyTimes();
        //		mockPartition.lockAlmostFullLock();
        //		expectLastCall().once();
        //		mockPartition.almostFullSignal();
        //		expectLastCall().once();
        //		mockPartition.unlockAlmostFullLock();
        //		expectLastCall().once();
        replay(mockPartition, mockConnectionHandles);
        Method method = testClass.getClass().getDeclaredMethod("maybeSignalForMoreConnections", ObjectPartition.class);
        method.setAccessible(true);
        method.invoke(testClass, new Object[]{mockPartition});
        verify(mockPartition, mockConnectionHandles);
    }

    /**
     * Test method for maybeSignalForMoreConnections(com.jolbox.bonecp.ConnectionPartition)}.
     *
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void testMaybeSignalForMoreConnectionsWithException() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        BlockingQueue<Object> bq = new ArrayBlockingQueue<Object>(1);
        Method method = testClass.getClass().getDeclaredMethod("maybeSignalForMoreConnections", ObjectPartition.class);
        method.setAccessible(true);

        // Test 2, same test but fake an exception
        reset(mockPartition, mockConnectionHandles);
        expect(mockPartition.getPoolWatchThreadSignalQueue()).andReturn(bq).anyTimes();
        expect(mockPartition.isUnableToCreateMoreObjects()).andReturn(false).anyTimes();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockConnectionHandles.size()).andReturn(1).anyTimes();
        expect(mockPartition.getMaxObjects()).andReturn(10).anyTimes();
        //		mockPartition.lockAlmostFullLock();
        //		expectLastCall().once();
        //		mockPartition.almostFullSignal();
        //		expectLastCall().andThrow(new RuntimeException()).once();
        //		mockPartition.unlockAlmostFullLock();
        //		expectLastCall().once(); 
        replay(mockPartition, mockConnectionHandles);
        try {
            method.invoke(testClass, new Object[]{mockPartition});
            fail("Should have thrown an exception");
        } catch (Throwable t) {
            // do nothing
        }
        verify(mockPartition, mockConnectionHandles);

    }

    /**
     * Test method for {@link BoneOP#getTotalLeased()} .
     */
    @Test
    public void testGetTotalLeased() {
        expect(mockPartition.getCreatedObjects()).andReturn(5).anyTimes();
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockPartition.getAvailableObjects()).andReturn(3).anyTimes();
        replay(mockPartition, mockConnectionHandles);
        assertEquals(4, testClass.getTotalLeased());
        verify(mockPartition, mockConnectionHandles);

    }

    /**
     * Test method for {@link BoneOP#getTotalFree()} .
     */
    @Test
    public void testGetTotalFree() {
        expect(mockPartition.getFreeObjects()).andReturn(mockConnectionHandles).anyTimes();
        expect(mockPartition.getAvailableObjects()).andReturn(3).anyTimes();

        // expect(mockConnectionHandles.size()).andReturn(3).anyTimes();
        replay(mockPartition, mockConnectionHandles);
        assertEquals(6, testClass.getTotalFree());
        verify(mockPartition, mockConnectionHandles);

    }

    /**
     * Test method for {@link BoneOP#getTotalCreatedObjects()} .
     */
    @Test
    public void testGetTotalCreatedConnections() {
        expect(mockPartition.getCreatedObjects()).andReturn(5).anyTimes();
        replay(mockPartition);
        assertEquals(10, testClass.getTotalCreatedObjects());
        verify(mockPartition);

    }

    /**
     * Test method for {@link BoneOP#getConfig()}.
     */
    @Test
    public void testGetConfig() {
        assertEquals(mockConfig, testClass.getConfig());
    }

    /**
     * A coverage test.
     */
    @Test
    public void testIsReleaseHelperThreadsConfigured() {
        // coverage
        assertFalse(testClass.isReleaseHelperThreadsConfigured());
    }

    /**
     * Coverage.
     *
     * @throws Exception
     */
    @Test
    public void testCoverage() throws Exception {
        BoneOPConfig config = createNiceMock(BoneOPConfig.class);
        expect(config.getMinObjectsPerPartition()).andReturn(2).anyTimes();
        expect(config.getMaxObjectsPerPartition()).andReturn(20).anyTimes();
        expect(config.getPartitionCount()).andReturn(1).anyTimes();
        expect(config.getServiceOrder()).andReturn("LIFO").anyTimes();
        expect(config.getMaxObjectAgeInSeconds()).andReturn(100000L).anyTimes();
        replay(config);
        try {
            BoneOP pool = new BoneOP(config, new TestObjectFactory());
            // just for more coverage
            ExecutorService statementCloseHelper = Executors.newFixedThreadPool(1);
            pool.setStatementCloseHelperExecutor(statementCloseHelper);
            assertEquals(statementCloseHelper, pool.getStatementCloseHelperExecutor());

        } catch (Throwable t) {
            // do nothing
        }

    }

    /**
     * Test method for {@link BoneOP#setReleaseHelper(java.util.concurrent.ExecutorService)}.
     */
    @Test
    public void testSetReleaseHelper() {
        ExecutorService mockReleaseHelper = createNiceMock(ExecutorService.class);
        testClass.setReleaseHelper(mockReleaseHelper);
        assertEquals(mockReleaseHelper, testClass.getReleaseHelper());
    }

    /**
     * JMX setup test
     *
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InstanceAlreadyExistsException
     * @throws MBeanRegistrationException
     * @throws NotCompliantMBeanException
     */
    @Test
    public void testJMX() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
        MBeanServer mockMbs = EasyMock.createNiceMock(MBeanServer.class);
        Field field = testClass.getClass().getDeclaredField("mbs");
        field.setAccessible(true);
        field.set(testClass, mockMbs);
        expect(mockConfig.getPoolName()).andReturn(null).anyTimes();
        ObjectInstance mockInstance = createNiceMock(ObjectInstance.class);
        expect(mockMbs.isRegistered((ObjectName) anyObject())).andReturn(false).anyTimes();
        expect(mockMbs.registerMBean(anyObject(), (ObjectName) anyObject())).andReturn(mockInstance).once().andThrow(new InstanceAlreadyExistsException()).once();
        replay(mockMbs, mockInstance, mockConfig);
        testClass.registerUnregisterJMX(true);
        verify(mockMbs);
    }

    /**
     * Test for different pool names.
     *
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InstanceAlreadyExistsException
     * @throws MBeanRegistrationException
     * @throws NotCompliantMBeanException
     */
    @Test
    public void testJMXWithName() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
        MBeanServer mockMbs = EasyMock.createNiceMock(MBeanServer.class);
        Field field = testClass.getClass().getDeclaredField("mbs");
        field.setAccessible(true);
        field.set(testClass, mockMbs);
        expect(mockConfig.getPoolName()).andReturn("poolName").anyTimes();
        ObjectInstance mockInstance = createNiceMock(ObjectInstance.class);
        expect(mockMbs.isRegistered((ObjectName) anyObject())).andReturn(false).anyTimes();
        expect(mockMbs.registerMBean(anyObject(), (ObjectName) anyObject())).andReturn(mockInstance).once().andThrow(new InstanceAlreadyExistsException()).once();
        replay(mockMbs, mockInstance, mockConfig);
        testClass.registerUnregisterJMX(true);
        verify(mockMbs);
    }

    /**
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void testCaptureException() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        Method method = testClass.getClass().getDeclaredMethod("captureStackTrace", String.class);
        method.setAccessible(true);
        try {
            method.invoke(testClass);
            fail("Should throw an exception");
        } catch (Exception e) {
            // do nothing
        }
    }

    /**
     * Tests for watch connection.
     *
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws NoSuchFieldException
     */
    @Test
    public void testWatchConnection() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, NoSuchFieldException {
        Field field = testClass.getClass().getDeclaredField("closeConnectionExecutor");
        field.setAccessible(true);

        ExecutorService mockExecutor = createNiceMock(ExecutorService.class);
        field.set(testClass, mockExecutor);

        Method method = testClass.getClass().getDeclaredMethod("watchConnection", ObjectHandle.class);
        method.setAccessible(true);
        expect(mockExecutor.submit((CloseThreadMonitor) anyObject())).andReturn(null).once();
        replay(mockExecutor);
        method.invoke(testClass, mockConnection);
        verify(mockExecutor);

        // Test #2: Code coverage
        method.invoke(testClass, new Object[]{null});
    }
}
