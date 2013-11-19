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

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.fail;


/**
 * Test for connection thread tester
 * @author wwadge
 *
 */
public class TestConnectionMaxAgeTester {
	/** Mock handle. */
	private static BoneOP mockPool;
	/** Mock handle. */
	private static ObjectPartition mockConnectionPartition;
	/** Mock handle. */
	private static ScheduledExecutorService mockExecutor;
	/** Test class handle. */
	private static ObjectMaxAgeThread testClass;
	/** Mock handle. */
	private static BoneOPConfig config;
	/** Mock handle. */
	private static Logger mockLogger;

	/** Mock setup.
	 * @throws ClassNotFoundException
	 */
	@BeforeClass
	public static void setup() throws ClassNotFoundException{
		mockPool = createNiceMock(BoneOP.class);
		mockConnectionPartition = createNiceMock(ObjectPartition.class);
		mockExecutor = createNiceMock(ScheduledExecutorService.class);
		
		mockLogger = createNiceMock(Logger.class);
		
		makeThreadSafe(mockLogger, true);
		config = new BoneOPConfig();
		config.setMaxObjectAgeInSeconds(1);
		
		testClass = new ObjectMaxAgeThread(mockConnectionPartition, mockExecutor, mockPool, 5000, false);
	}

	/**
	 * Reset all mocks.
	 */
	@Before
	public void resetMocks(){
		reset(mockPool, mockConnectionPartition, mockExecutor, mockLogger);
	}
	
	/**
	 * Tests that a partition with expired connections should those connections killed off.
	 * @throws SQLException 
	 */
	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testConnectionExpired() throws SQLException{
		 
		TransferQueue<ObjectHandle> mockQueue = createNiceMock(TransferQueue.class);
		expect(mockConnectionPartition.getAvailableConnections()).andReturn(1);
		expect(mockConnectionPartition.getFreeObjects()).andReturn(mockQueue).anyTimes();
		ObjectHandle mockConnectionExpired = createNiceMock(ObjectHandle.class);
		ObjectHandle mockConnection = createNiceMock(ObjectHandle.class);
		expect(mockQueue.poll()).andReturn(mockConnectionExpired).once();
			
		expect(mockConnectionExpired.isExpired(anyLong())).andReturn(true).once();

		expect(mockExecutor.isShutdown()).andReturn(false).once();
		
		mockConnectionExpired.internalClose();
		expectLastCall().once();
		
		mockPool.postDestroyConnection(mockConnectionExpired);
		expectLastCall().once();
		
		
		expect(mockExecutor.schedule((Callable)anyObject(), anyLong(), (TimeUnit)anyObject())).andReturn(null).once();
		replay(mockQueue, mockExecutor, mockConnectionPartition, mockConnection, mockPool, mockConnectionExpired);
		testClass.run();
		verify(mockConnectionExpired);
	}


	/**
	 * Tests that a partition with expired connections should those connections killed off.
	 * @throws SQLException 
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testConnectionNotExpiredLifoMode() throws SQLException{
		 
		LIFOQueue<ObjectHandle> mockQueue = createNiceMock(LIFOQueue.class);
		expect(mockConnectionPartition.getAvailableConnections()).andReturn(1);
		expect(mockConnectionPartition.getFreeObjects()).andReturn(mockQueue).anyTimes();
		ObjectHandle mockConnection = createNiceMock(ObjectHandle.class);
		expect(mockQueue.poll()).andReturn(mockConnection).once();
			
		expect(mockConnection.isExpired(anyLong())).andReturn(false).once();

		expect(mockExecutor.isShutdown()).andReturn(false).once();
		

		expect(mockConnection.getOriginatingPartition()).andReturn(mockConnectionPartition).anyTimes();
		expect(mockQueue.offerLast(mockConnection)).andReturn(false).anyTimes();
		mockConnection.internalClose();
		
		replay(mockQueue, mockExecutor, mockConnectionPartition, mockConnection, mockPool);
		ObjectMaxAgeThread testClass2 = new ObjectMaxAgeThread(mockConnectionPartition, mockExecutor, mockPool, 5000, true);
		testClass2.run();

		verify(mockConnection, mockPool);
		
	}
	
	/**
	 * Tests that a partition with expired connections should those connections killed off.
	 * @throws SQLException 
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testConnectionNotExpired() throws SQLException{
		 
		TransferQueue<ObjectHandle> mockQueue = createNiceMock(TransferQueue.class);
		expect(mockConnectionPartition.getAvailableConnections()).andReturn(1);
		expect(mockConnectionPartition.getFreeObjects()).andReturn(mockQueue).anyTimes();
		ObjectHandle mockConnection = createNiceMock(ObjectHandle.class);
		expect(mockQueue.poll()).andReturn(mockConnection).once();
			
		expect(mockConnection.isExpired(anyLong())).andReturn(false).once();

		expect(mockExecutor.isShutdown()).andReturn(false).once();
		
		mockPool.putObjectBackInPartition(mockConnection);
		expectLastCall().once();
		
		
		replay(mockQueue, mockExecutor, mockConnectionPartition, mockConnection, mockPool);
		testClass.run();
		verify(mockConnection, mockPool);
		
	}
	
	/**
	 * @throws SQLException
	 */
	@Test
	@SuppressWarnings({ "unchecked" })
	public void testExceptionsCase() throws SQLException{
		 
		TransferQueue<ObjectHandle> mockQueue = createNiceMock(TransferQueue.class);
		expect(mockConnectionPartition.getAvailableConnections()).andReturn(2);
		expect(mockConnectionPartition.getFreeObjects()).andReturn(mockQueue).anyTimes();
		ObjectHandle mockConnectionException = createNiceMock(ObjectHandle.class);
		expect(mockQueue.poll()).andReturn(mockConnectionException).times(2);
		expect(mockConnectionException.isExpired(anyLong())).andThrow(new RuntimeException()).anyTimes();
		expect(mockExecutor.isShutdown()).andReturn(false).once().andReturn(true).once();
		
		replay(mockQueue,mockConnectionException, mockExecutor, mockConnectionPartition, mockPool);
		testClass.run();
		verify(mockExecutor, mockConnectionException);
		
	}
	
	/**
	 * @throws SQLException
	 */
	@Test
	@SuppressWarnings( "unchecked")
	public void testExceptionsCaseWherePutInPartitionFails() throws SQLException{
		 
		TransferQueue<ObjectHandle> mockQueue = createNiceMock(TransferQueue.class);
		expect(mockConnectionPartition.getAvailableConnections()).andReturn(1);
		expect(mockConnectionPartition.getFreeObjects()).andReturn(mockQueue).anyTimes();
		ObjectHandle mockConnectionException = createNiceMock(ObjectHandle.class);
		expect(mockQueue.poll()).andReturn(mockConnectionException).times(1);
		expect(mockConnectionException.isExpired(anyLong())).andReturn(false).anyTimes();
		expect(mockExecutor.isShutdown()).andReturn(false).anyTimes();
		mockPool.putObjectBackInPartition(mockConnectionException);
		expectLastCall().andThrow(new SQLException()).once();
		
		// we should be able to reschedule
		expect(mockExecutor.schedule((Runnable)anyObject(), anyLong(), (TimeUnit)anyObject())).andReturn(null).once();
		
		replay(mockQueue,mockConnectionException, mockExecutor, mockConnectionPartition, mockPool);
		testClass.run();
		verify(mockExecutor, mockConnectionException);
	}
	
	/**
	 * @throws SQLException
	 */
	@Test
	public void testCloseConnectionNormalCase() throws SQLException{
		ObjectHandle mockConnection = createNiceMock(ObjectHandle.class);
		mockPool.postDestroyConnection(mockConnection);
		expectLastCall().once();
		
		mockConnection.internalClose();
		expectLastCall().once();
		
		replay(mockConnection, mockPool);
		testClass.closeConnection(mockConnection);
		verify(mockConnection, mockPool);
	}
	
	/**
	 * @throws SQLException
	 */
	@Test
	public void testCloseConnectionWithException() throws SQLException{
		ObjectHandle mockConnection = createNiceMock(ObjectHandle.class);
		mockPool.postDestroyConnection(mockConnection);
		expectLastCall().once();
		
		mockConnection.internalClose();
		expectLastCall().andThrow(new SQLException());
		
		replay(mockConnection, mockPool);
		testClass.closeConnection(mockConnection);
		verify(mockConnection, mockPool);
	}
	
	/**
	 * @throws SQLException
	 */
	@Test
	public void testCloseConnectionWithExceptionCoverage() throws Exception{
		ObjectHandle mockConnection = createNiceMock(ObjectHandle.class);
		mockPool.postDestroyConnection(mockConnection);
		expectLastCall().once();
    // set logger to null so that exception will be thrown in catch clause
    Field field = ObjectMaxAgeThread.class.getDeclaredField("logger");
    TestUtils.setFinalStatic(field, null);
		mockConnection.internalClose();
		expectLastCall().andThrow(new SQLException());
		
		replay(mockConnection, mockPool);
		try{
			testClass.closeConnection(mockConnection);
      fail("Expecting NPE because logger was set to null");
		} catch (Exception e){
			// do nothing
		}
		verify(mockConnection, mockPool);
	}
}