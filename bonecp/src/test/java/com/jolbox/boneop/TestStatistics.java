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

import com.jolbox.boneop.BoneOP;
import com.jolbox.boneop.Statistics;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;


/** Testclass for statistics module
 * @author wallacew
 *
 */
public class TestStatistics {

	/** pool handle. */
	private BoneOP mockPool = createNiceMock(BoneOP.class);
	/** stats handle. */
	private Statistics stats = new Statistics(this.mockPool);

	

	/**	Test that the values start off at zero initially
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	@Test
	public void testStatsStartAtZero() throws IllegalArgumentException, IllegalAccessException{
		
		// test that the values start off at zero initially
		checkValuesSetToZero(this.stats);
	}

	/** Main methods.
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	@Test
	public void testStatsFunctionality() throws IllegalArgumentException, IllegalAccessException{
		assertEquals(0, this.stats.getObjectWaitTimeAvg(), 0.5);
		assertEquals(0, this.stats.getStatementExecuteTimeAvg(), 0.5);
		assertEquals(0, this.stats.getStatementPrepareTimeAvg(), 0.5);
		assertEquals(0, this.stats.getCacheHitRatio(), 0.05);
		
		this.stats.addCumulativeObjectWaitTime(1000000);
		this.stats.addStatementExecuteTime(1000000);
		this.stats.addStatementPrepareTime(TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS));
		this.stats.incrementCacheHits();
		this.stats.incrementCacheMiss();
		this.stats.incrementObjectsRequested();
		this.stats.incrementStatementsCached();
		this.stats.incrementStatementsExecuted();
		this.stats.incrementStatementsPrepared();
		
		expect(this.mockPool.getTotalLeased()).andReturn(1).once();
		expect(this.mockPool.getTotalFree()).andReturn(1).once();
		expect(this.mockPool.getTotalCreatedObjects()).andReturn(1).once();
		replay(this.mockPool);
		
		assertEquals(1, this.stats.getCumulativeObjectWaitTime());
		assertEquals(1000, this.stats.getCumulativeStatementPrepareTime());
		assertEquals(1, this.stats.getStatementExecuteTimeAvg(), 0.5);
		assertEquals(1000, this.stats.getStatementPrepareTimeAvg(), 0.5);
		assertEquals(1, this.stats.getCumulativeStatementExecutionTime());
		assertEquals(1, this.stats.getObjectWaitTimeAvg(), 0.5);
		assertEquals(1, this.stats.getStatementsCached());
		assertEquals(1, this.stats.getStatementsExecuted());
		assertEquals(1, this.stats.getStatementsPrepared());
		assertEquals(1, this.stats.getObjectsRequested());
		assertEquals(1, this.stats.getCacheHits());
		assertEquals(1, this.stats.getCacheMiss());
		assertEquals(1, this.stats.getTotalFree());
		assertEquals(1, this.stats.getTotalCreatedObjects());
		assertEquals(1, this.stats.getTotalLeased());
		assertEquals(0.5, this.stats.getCacheHitRatio(), 0.05);
		
	}
	/**
	 * @param stats
	 * @throws IllegalAccessException
	 */
	private void checkValuesSetToZero(Statistics stats)
			throws IllegalAccessException {
		for (Field field: Statistics.class.getDeclaredFields()){
			if (field.getType().equals(AtomicLong.class) ){
				field.setAccessible(true);
				assertEquals(0, ((AtomicLong)field.get(stats)).get());
			}
			
		}
	}
	
	/** Tests that values are reset properly when instructed to do so.
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	@Test
	public void testStatsReset() throws IllegalArgumentException, IllegalAccessException{
		
		this.stats.resetStats();
		// test that the values start off at zero initially
		checkValuesSetToZero(this.stats);
	}

	
}


