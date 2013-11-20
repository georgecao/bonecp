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

import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import com.google.common.base.FinalizableReferenceQueue;
import com.jolbox.boneop.proxy.ConnectionProxy;
import org.junit.Test;
import org.slf4j.Logger;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author wwadge
 *
 */
public class TestConnectionPartition {

    /**
     * mock handle.
     */
    private BoneOP mockPool = createNiceMock(BoneOP.class);
    /**
     * mock handle.
     */
    private static Logger mockLogger;
    /**
     * mock handle.
     */
    private static BoneOPConfig mockConfig;
    /**
     * mock handle.
     */
    private static ObjectPartition testClass;

    /**
     * Tests the constructor. Makes sure release helper threads are launched (+ setup other config items).
     *
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Test
    public void testConstructor() throws Exception {
        mockConfig = createNiceMock(BoneOPConfig.class);
        expect(mockConfig.getAcquireIncrement()).andReturn(1).anyTimes();
        expect(mockConfig.getMaxObjectsPerPartition()).andReturn(1).anyTimes();
        expect(mockConfig.getMinObjectsPerPartition()).andReturn(1).anyTimes();
        expect(mockConfig.getReleaseHelperThreads()).andReturn(3).anyTimes();
        expect(mockConfig.getPoolName()).andReturn("Junit test").anyTimes();
        expect(mockConfig.isDisableObjectTracking()).andReturn(false).anyTimes();
        Map<Connection, Reference<ObjectHandle>> refs = new HashMap<Connection, Reference<ObjectHandle>>();
        expect(this.mockPool.getFinalizableRefs()).andReturn(refs).anyTimes();
        expect(this.mockPool.getConfig()).andReturn(mockConfig).anyTimes();
        ExecutorService mockReleaseHelper = createNiceMock(ExecutorService.class);
        expect(this.mockPool.getReleaseHelper()).andReturn(mockReleaseHelper).anyTimes();
        mockReleaseHelper.execute((Runnable) anyObject());
        expectLastCall().times(3);
        replay(this.mockPool, mockReleaseHelper, mockConfig);
        testClass = new ObjectPartition(this.mockPool);
        mockLogger = TestUtils.mockLogger(testClass.getClass());
        makeThreadSafe(mockLogger, true);
        mockLogger.error((String) anyObject());
        expectLastCall().anyTimes();
        replay(mockLogger);
        verify(this.mockPool, mockReleaseHelper, mockConfig);
        reset(this.mockPool, mockReleaseHelper, mockConfig);
    }

    /**
     * Test method for created connections.
     *
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Test
    public void testUpdateCreatedConnections() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        int count = testClass.getCreatedObjects();
        testClass.updateCreatedObjects(5);
        assertEquals(count + 5, testClass.getCreatedObjects());
    }

    /**
     * Test method for created connections.
     *
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Test
    public void testUpdateCreatedConnectionsWithException() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {

        // Test #2: Same test but fake an exception 
        ReentrantReadWriteLock mockLock = createNiceMock(ReentrantReadWriteLock.class);
        WriteLock mockWriteLock = createNiceMock(WriteLock.class);

        Field field = testClass.getClass().getDeclaredField("statsLock");
        field.setAccessible(true);
        ReentrantReadWriteLock oldLock = (ReentrantReadWriteLock) field.get(testClass);
        field.set(testClass, mockLock);
        expect(mockLock.writeLock()).andThrow(new RuntimeException()).once().andReturn(mockWriteLock).once();
        mockWriteLock.lock();
        expectLastCall().once();
        replay(mockLock, mockWriteLock);

        try {
            testClass.updateCreatedObjects(5);
            fail("Should have thrown an exception");
        } catch (Throwable t) {
            //do nothing
        }
        verify(mockLock);
        field.set(testClass, oldLock);

    }

    /**
     * Test method for freeConnections
     *
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testFreeConnection() throws Exception {
        int count = testClass.getCreatedObjects();

        BoundedLinkedTransferQueue<ObjectHandle> freeConnections = createNiceMock(BoundedLinkedTransferQueue.class);
        makeThreadSafe(freeConnections, true);
        testClass.setFreeObjects(freeConnections);
        assertEquals(freeConnections, testClass.getFreeObjects());
        reset(this.mockPool);
        Map<Connection, Reference<ObjectHandle>> refs = new HashMap<Connection, Reference<ObjectHandle>>();
        expect(this.mockPool.getFinalizableRefs()).andReturn(refs).anyTimes();
        FinalizableReferenceQueue finalizableRefQueue = new FinalizableReferenceQueue();
        expect(this.mockPool.getFinalizableRefQueue()).andReturn(finalizableRefQueue).anyTimes();

        ObjectHandle mockConnectionHandle = createNiceMock(ObjectHandle.class);
        expect(mockConnectionHandle.getPool()).andReturn(this.mockPool).anyTimes();
        expect(freeConnections.offer(mockConnectionHandle)).andReturn(true).anyTimes();
        replay(mockConnectionHandle, freeConnections, this.mockPool);
        testClass.addFreeObject(mockConnectionHandle);
        verify(mockConnectionHandle, freeConnections);
        assertEquals(count + 1, testClass.getCreatedObjects());
        assertEquals(0, testClass.getRemainingCapacity());

    }

    /**
     * fail to offer a new connection.
     *
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testFreeConnectionFailing() throws Exception {
        int count = testClass.getCreatedObjects();

        BoundedLinkedTransferQueue<ObjectHandle> freeConnections = createNiceMock(BoundedLinkedTransferQueue.class);
        makeThreadSafe(freeConnections, true);

        testClass.setFreeObjects(freeConnections);
        assertEquals(freeConnections, testClass.getFreeObjects());
        reset(this.mockPool);
        Map<Connection, Reference<ObjectHandle>> refs = new HashMap<Connection, Reference<ObjectHandle>>();
        expect(this.mockPool.getFinalizableRefs()).andReturn(refs).anyTimes();

        FinalizableReferenceQueue finalizableRefQueue = new FinalizableReferenceQueue();
        expect(this.mockPool.getFinalizableRefQueue()).andReturn(finalizableRefQueue).anyTimes();
        ObjectHandle mockConnectionHandle = createNiceMock(ObjectHandle.class);
        expect(mockConnectionHandle.getPool()).andReturn(this.mockPool).anyTimes();
        expect(freeConnections.offer(mockConnectionHandle)).andReturn(false);

        mockConnectionHandle.internalClose();
        expectLastCall().once();

        expect(freeConnections.remainingCapacity()).andReturn(1).anyTimes();
        Connection mockRealConnection = createNiceMock(Connection.class);
        expect(mockConnectionHandle.getInternalObject()).andReturn(mockRealConnection).anyTimes();
        testClass.pool = this.mockPool;
        replay(mockConnectionHandle, mockRealConnection, freeConnections, this.mockPool);
        testClass.addFreeObject(mockConnectionHandle);
        verify(mockConnectionHandle, freeConnections);
        assertEquals(count, testClass.getCreatedObjects());
        assertEquals(1, testClass.getRemainingCapacity());

    }

    /**
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    @Test
    public void testGetCreatedConnections() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        ReentrantReadWriteLock mockLock = createNiceMock(ReentrantReadWriteLock.class);
        ReadLock mockReadLock = createNiceMock(ReadLock.class);

        Field field = testClass.getClass().getDeclaredField("statsLock");
        field.setAccessible(true);
        ReentrantReadWriteLock oldLock = (ReentrantReadWriteLock) field.get(testClass);
        field.set(testClass, mockLock);
        expect(mockLock.readLock()).andThrow(new RuntimeException()).once().andReturn(mockReadLock).once();
        mockReadLock.lock();
        expectLastCall().once();
        replay(mockLock, mockReadLock);

        try {
            testClass.getCreatedObjects();
            fail("Should have thrown an exception");
        } catch (Throwable t) {
            //do nothing
        }
        verify(mockLock);
        field.set(testClass, oldLock);
    }

    /**
     * Test method for config related stuff.
     */
    @Test
    public void testConfigStuff() {
        assertEquals(1, testClass.getMaxObjects());
        assertEquals(1, testClass.getMinObjects());
        assertEquals(1, testClass.getAcquireIncrement());
        assertEquals(0, testClass.getObjectsPendingRelease().size());
    }

    /**
     * Test method for unable to create more transactions.
     */
    @Test
    public void testUnableToCreateMoreTransactionsFlag() {
        testClass.setUnableToCreateMoreTransactions(true);
        assertEquals(testClass.isUnableToCreateMoreTransactions(), true);
    }

    /**
     * Test finalizer.
     *
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testFinalizer() throws SQLException, InterruptedException {
        ObjectHandle mockConnectionHandle = createNiceMock(ObjectHandle.class);
        Connection mockConnection = createNiceMock(Connection.class);
        expect(mockConnectionHandle.getInternalObject()).andReturn(mockConnection).anyTimes();
        mockConnection.close();
        expectLastCall().once();
        reset(this.mockPool);
        Map<Connection, Reference<ObjectHandle>> refs = new HashMap<Connection, Reference<ObjectHandle>>();
        expect(this.mockPool.getFinalizableRefs()).andReturn(refs).anyTimes();
        FinalizableReferenceQueue finalizableRefQueue = new FinalizableReferenceQueue();
        expect(this.mockPool.getFinalizableRefQueue()).andReturn(finalizableRefQueue).anyTimes();
        expect(mockConnectionHandle.getPool()).andReturn(this.mockPool).anyTimes();
        expect(this.mockPool.getConfig()).andReturn(mockConfig).anyTimes();
        expect(mockConfig.getPoolName()).andReturn("foo").once();
        makeThreadSafe(this.mockPool, true);

        replay(mockConnection, mockConnectionHandle, this.mockPool, mockConfig);

        testClass.trackObjectFinalizer(mockConnectionHandle);
        reset(mockConnectionHandle);

        mockConnectionHandle = null; // prompt GC to kick in
        for (int i = 0; i < 500; i++) {
            System.gc();
            System.gc();
            System.gc();
            Thread.sleep(20);
            try {
                verify(mockConnection);
                break; // we succeeded
            } catch (Throwable t) {
					//				t.printStackTrace();
                // do nothing, try again
                Thread.sleep(20);
            }
        }
    }

    /**
     * Test finalizer with error.
     *
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testFinalizerCoverageException() throws SQLException, InterruptedException {
        ObjectHandle mockConnectionHandle = createNiceMock(ObjectHandle.class);
        Connection mockConnection = createNiceMock(Connection.class);
        expect(mockConnectionHandle.getInternalObject()).andReturn(mockConnection).anyTimes();
        mockConnection.close();
        expectLastCall().andThrow(new SQLException("fake reason")).once();
        reset(this.mockPool);
        Map<Connection, Reference<ObjectHandle>> refs = new HashMap<Connection, Reference<ObjectHandle>>();
        expect(this.mockPool.getFinalizableRefs()).andReturn(refs).anyTimes();
        FinalizableReferenceQueue finalizableRefQueue = new FinalizableReferenceQueue();
        expect(this.mockPool.getFinalizableRefQueue()).andReturn(finalizableRefQueue).anyTimes();
        expect(mockConnectionHandle.getPool()).andReturn(this.mockPool).anyTimes();

        replay(mockConnectionHandle, mockConnection, this.mockPool);
        testClass.trackObjectFinalizer(mockConnectionHandle);
        testClass.statsLock = null; // this makes it blow up.
        reset(mockLogger);
        mockLogger.error((String) anyObject());
        expectLastCall().anyTimes();
        replay(mockLogger);
        reset(mockConnectionHandle);
        mockConnectionHandle = null; // prompt GC to kick in
        for (int i = 0; i < 100; i++) {
            System.gc();
            System.gc();
            System.gc();
            Thread.sleep(20);
            try {
                verify(mockLogger);
                break; // we succeeded
            } catch (Throwable t) {
                // do nothing, try again
                Thread.sleep(20);
            }
        }
    }

    /**
     * Test finalizer.
     *
     * @throws SQLException
     * @throws InterruptedException
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Test
    public void testFinalizerException2() throws SQLException, InterruptedException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        ObjectHandle mockConnectionHandle = createNiceMock(ObjectHandle.class);
        Connection mockConnection = createNiceMock(Connection.class);
        expect(mockConnectionHandle.getInternalObject()).andReturn(mockConnection).anyTimes();
        makeThreadSafe(mockConnectionHandle, true);
        makeThreadSafe(mockConnection, true);
        reset(mockLogger);
        makeThreadSafe(mockLogger, true);
        reset(this.mockPool);
        Map<Connection, Reference<ObjectHandle>> refs = new HashMap<Connection, Reference<ObjectHandle>>();
        expect(this.mockPool.getFinalizableRefs()).andReturn(refs).anyTimes();
        FinalizableReferenceQueue finalizableRefQueue = new FinalizableReferenceQueue();
        expect(this.mockPool.getFinalizableRefQueue()).andReturn(finalizableRefQueue).anyTimes();
        expect(mockConnectionHandle.getPool()).andReturn(this.mockPool).anyTimes();

        replay(mockConnection, mockConnectionHandle, this.mockPool);

        testClass.trackObjectFinalizer(mockConnectionHandle);
        reset(mockConnectionHandle);
        mockConnectionHandle = null; // prompt GC to kick in
        for (int i = 0; i < 100; i++) {
            System.gc();
            System.gc();
            System.gc();
            Thread.sleep(20);
            try {
                verify(mockConnection);
                break; // we succeeded
            } catch (Throwable t) {
                t.printStackTrace();
                // do nothing, try again
                Thread.sleep(20);
            }
        }
    }

}
