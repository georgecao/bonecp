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


import static org.easymock.EasyMock.*;

import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;

import org.easymock.IAnswer;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jolbox.boneop.BoneOP;
import com.jolbox.boneop.ObjectHandle;
import com.jolbox.boneop.ObjectReleaseHelperThread;

/**
 * Mock tester for release helper thread
 * @author wwadge
 *
 */
public class TestReleaseHelperThread {
	/** Mock handle. */
	private static BoneOP mockPool;
	/** Mock handle. */
	private static BlockingQueue<ObjectHandle> mockQueue;
	/** Mock handle. */
	static ObjectHandle mockConnection;
	/** temp. */
	static boolean first = true;

	/** Mock setup
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	@BeforeClass
	public static void setup() throws ClassNotFoundException{
		mockPool = createNiceMock(BoneOP.class);
		mockConnection = createNiceMock(ObjectHandle.class);
		mockQueue = createNiceMock(BlockingQueue.class);
		
	}
	
	/** Normal case test
	 * @throws InterruptedException
	 * @throws SQLException 
	 */
	@Test
	public void testNormalCycle() throws Exception {
		expect(mockQueue.take()).andAnswer(new IAnswer<ObjectHandle>() {

			// @Override
			public ObjectHandle answer() throws Throwable {
				if (first){
					first = false;
					return mockConnection;
				} 
					throw new InterruptedException();
				
			}
		}).times(2);

		mockPool.internalReleaseObject(mockConnection);
		expectLastCall().times(1).andThrow(new SQLException()).once();
		expect(mockQueue.poll()).andReturn(mockConnection).times(2).andReturn(null).once();
		mockPool.poolShuttingDown = true;
			
		
		replay(mockPool, mockQueue);
		ObjectReleaseHelperThread clazz = new ObjectReleaseHelperThread(mockQueue, mockPool);
		clazz.run();
		verify(mockPool, mockQueue);
		reset(mockPool, mockQueue);
		
	
	}
	
	/** Normal case test
	 * @throws InterruptedException
	 * @throws SQLException 
	 */
	@Test
	public void testSQLExceptionCycle() throws Exception {
		first = true;
		expect(mockQueue.take()).andReturn(mockConnection);
		mockPool.internalReleaseObject(mockConnection);
		expectLastCall().andThrow(new SQLException());
		
		
		replay(mockPool, mockQueue);
		ObjectReleaseHelperThread clazz = new ObjectReleaseHelperThread(mockQueue, mockPool);
		clazz.run();
		verify(mockPool, mockQueue);
		reset(mockPool, mockQueue);
		
	
	}
}
