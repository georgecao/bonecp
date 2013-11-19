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

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import com.jolbox.boneop.listener.CoverageHook;
import org.easymock.IAnswer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import static org.easymock.EasyMock.*;

/**
 * Tests the functionality of the pool watch thread.
 *
 * @author wwadge
 *
 */
public class TestPoolWatchThread {

    /**
     * Mock handle.
     */
    private static ObjectPartition mockPartition;
    /**
     * Mock handle.
     */
    private static BoneOP mockPool;
    /**
     * Class under test.
     */
    static PoolWatchThread testClass;
    /**
     * Mock handle.
     */
    private static Logger mockLogger;
    /**
     * Break out from infinite loop.
     */
    static boolean first = true;
    /**
     * Mock handle.
     */
    private static BoneOPConfig mockConfig;

    /**
     * Test class setup.
     *
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @BeforeClass
    public static void setup() throws IllegalArgumentException, IllegalAccessException, SecurityException, NoSuchFieldException, ClassNotFoundException, SQLException {
        mockPartition = createNiceMock(ObjectPartition.class);
        mockConfig = createNiceMock(BoneOPConfig.class);
        expect(mockConfig.getAcquireRetryDelayInMs()).andReturn(1000L).anyTimes();
        expect(mockConfig.getAcquireRetryAttempts()).andReturn(0).anyTimes();
        expect(mockConfig.getDefaultTransactionIsolationValue()).andReturn(-1).anyTimes();
        expect(mockConfig.getDefaultAutoCommit()).andReturn(false).anyTimes();

        expect(mockConfig.getObjectListener()).andReturn(new CoverageHook()).anyTimes();
        expect(mockConfig.isLazyInit()).andReturn(false).anyTimes();

        mockPool = createNiceMock(BoneOP.class);
        expect(mockPool.getDbIsDown()).andReturn(new AtomicBoolean()).anyTimes();
        expect(mockPool.getConfig()).andReturn(mockConfig).anyTimes();
        replay(mockPool, mockConfig);
        testClass = new PoolWatchThread(mockPartition, mockPool);

        mockLogger = TestUtils.mockLogger(testClass.getClass());
        makeThreadSafe(mockLogger, true);

        mockLogger.error((String) anyObject(), anyObject());
        expectLastCall().anyTimes();
        expect(mockLogger.isDebugEnabled()).andReturn(true).anyTimes();

        mockLogger.debug((String) anyObject(), anyObject());
        expectLastCall().anyTimes();
    }

    /**
     * AfterClass cleanup.
     *
     * @throws SQLException
     */
    @AfterClass
    public static void teardown() throws SQLException {

    }

    /**
     * Rest the mocks.
     */
    @Before
    public void doReset() {
        reset(mockPartition, mockPool, mockLogger);
    }

    /**
     * Tests the case where we cannot create more transactions.
     *
     * @throws InterruptedException
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testRunFullConnections() throws InterruptedException {
//		mockPartition.lockAlmostFullLock();
//		expectLastCall().once();
//		mockPartition.unlockAlmostFullLock();
//		expectLastCall().anyTimes();
        BlockingQueue<Object> bq = new ArrayBlockingQueue<Object>(1);
        bq.add(new Object());
        expect(mockPartition.getPoolWatchThreadSignalQueue()).andReturn(bq);
        expect(mockPartition.getMaxObjects()).andReturn(5).once();
        expect(mockPartition.getCreatedObjects()).andReturn(5).once();
        mockPartition.setUnableToCreateMoreTransactions(true);
        expectLastCall().once();

        // just to break out of the loop
        BlockingQueue<?> mockQueue = createNiceMock(BlockingQueue.class);
        expect(mockPartition.getPoolWatchThreadSignalQueue()).andReturn((BlockingQueue) mockQueue);
        expect(mockQueue.take()).andThrow(new InterruptedException());

        replay(mockPartition, mockPool, mockLogger, mockQueue);
        testClass.run();
        verify(mockPartition);

    }

    /**
     * Tests the normal state.
     *
     * @throws InterruptedException
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws SQLException
     */
    @Test
    public void testRunCreateConnections() throws Exception {
        expect(mockLogger.isDebugEnabled()).andReturn(true).anyTimes();

        BoundedLinkedTransferQueue<ObjectHandle> fakeConnections = new BoundedLinkedTransferQueue<ObjectHandle>(100);
//		mockPartition.almostFullWait();
//		expectLastCall().anyTimes();
        expect(mockPartition.getMaxObjects()).andAnswer(new IAnswer<Integer>() {

            // @Override
            public Integer answer() throws Throwable {
                if (first) {
                    first = false;
                    return 4;
                }
                Field field = testClass.getClass().getDeclaredField("signalled");
                field.setAccessible(true);
                field.setBoolean(testClass, true);
                return 4;

            }
        }).anyTimes();

        expect(mockPartition.getFreeObjects()).andReturn(fakeConnections).anyTimes();

        expect(mockPartition.getAcquireIncrement()).andReturn(1);

        expect(mockPartition.getAvailableConnections()).andReturn(fakeConnections.size()).anyTimes();

        mockPartition.addFreeObject((ObjectHandle) anyObject());
        expectLastCall().once();
        expect(mockPool.obtainRawInternalObject()).andReturn(createNiceMock(ObjectHandle.class)).anyTimes();
        expect(mockPool.getDbIsDown()).andReturn(new AtomicBoolean()).anyTimes();
        expect(mockPool.getConfig()).andReturn(mockConfig).anyTimes();
        replay(mockPool, mockPartition, mockLogger);
        testClass.run();
        verify(mockPartition);

        // check exceptional cases
        reset(mockPartition, mockPool, mockLogger);
        resetSignalled();

        first = true;
        replay(mockPartition, mockLogger);
        try {
            testClass.run();
            Assert.fail("Exception should have been thrown");
        } catch (RuntimeException e) {
            // do nothing
        }
        verify(mockPartition);

        // check case where creating new ConnectionHandle fails
        reset(mockPool, mockLogger, mockConfig);
        reset(mockPartition);
        resetSignalled();

        first = true;
        expect(mockPartition.getFreeObjects()).andReturn(fakeConnections).anyTimes();

        expect(mockPartition.getMaxObjects()).andAnswer(new IAnswer<Integer>() {

            // @Override
            public Integer answer() throws Throwable {
                if (first) {
                    first = false;
                    return 4;
                }
                Field field = testClass.getClass().getDeclaredField("signalled");
                field.setAccessible(true);
                field.setBoolean(testClass, true);
                return 4;

            }
        }).anyTimes();

        expect(mockPartition.getAcquireIncrement()).andReturn(1).anyTimes();
        expect(mockPartition.getAvailableConnections()).andReturn(fakeConnections.size()).anyTimes();
        expect(mockPool.getConfig()).andReturn(mockConfig).anyTimes();

        expect(mockConfig.getAcquireRetryAttempts()).andReturn(0).anyTimes();

        mockLogger.error((String) anyObject(), anyObject());
        expectLastCall();
        expect(mockPool.getDbIsDown()).andReturn(new AtomicBoolean()).anyTimes();
        replay(mockPartition, mockPool, mockLogger, mockConfig);
        testClass.run();
        verify(mockPartition);

    }

    /**
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    private void resetSignalled() throws NoSuchFieldException,
            IllegalAccessException {
        Field field = testClass.getClass().getDeclaredField("signalled");
        field.setAccessible(true);
        field.setBoolean(testClass, false);
    }
}
