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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Statistics class.
 *
 * @author wallacew
 *
 */
public class Statistics implements StatisticsMBean {

    /**
     * No of cache hits.
     */
    private final AtomicLong cacheHits = new AtomicLong(0);
    /**
     * No of cache misses.
     */
    private final AtomicLong cacheMiss = new AtomicLong(0);
    /**
     * No of statements cached.
     */
    private final AtomicLong statementsCached = new AtomicLong(0);
    /**
     * Connections obtained.
     */
    private final AtomicLong objectsRequested = new AtomicLong(0);
    /**
     * Time taken to give a connection to the application.
     */
    private final AtomicLong cumulativeObjectWaitTime = new AtomicLong(0);
    /**
     * Time taken to execute statements.
     */
    private final AtomicLong cumulativeStatementExecuteTime = new AtomicLong(0);
    /**
     * Time taken to prepare statements (or obtain from cache).
     */
    private final AtomicLong cumulativeStatementPrepareTime = new AtomicLong(0);
    /**
     * Number of statements that have been executed.
     */
    private final AtomicLong statementsExecuted = new AtomicLong(0);
    /**
     * Number of statements that have been prepared.
     */
    private final AtomicLong statementsPrepared = new AtomicLong(0);

    /**
     * Pool handle.
     */
    private BoneOP pool;

    /**
     * BoneCP handle.
     *
     * @param pool
     */
    public Statistics(BoneOP pool) {
        this.pool = pool;
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#resetStats()
     */
    public void resetStats() {
        this.cacheHits.set(0);
        this.cacheMiss.set(0);
        this.statementsCached.set(0);
        this.objectsRequested.set(0);
        this.cumulativeObjectWaitTime.set(0);
        this.cumulativeStatementExecuteTime.set(0);
        this.cumulativeStatementPrepareTime.set(0);
        this.statementsExecuted.set(0);
        this.statementsPrepared.set(0);
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getConnectionWaitTimeAvg()
     */
    public double getObjectWaitTimeAvg() {
        return this.objectsRequested.get() == 0 ? 0 : this.cumulativeObjectWaitTime.get() / (1.0 * this.objectsRequested.get()) / 1000000.0;
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getStatementWaitTimeAvg()
     */
    public double getStatementExecuteTimeAvg() {
        return this.statementsExecuted.get() == 0 ? 0 : this.cumulativeStatementExecuteTime.get() / (1.0 * this.statementsExecuted.get()) / 1000000.0;
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getStatementPrepareTimeAvg()
     */
    public double getStatementPrepareTimeAvg() {
        return this.cumulativeStatementPrepareTime.get() == 0 ? 0 : this.cumulativeStatementPrepareTime.get() / (1.0 * this.statementsPrepared.get()) / 1000000.0;
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getTotalLeased()
     */
    public int getTotalLeased() {
        return this.pool.getTotalLeased();
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getTotalFree()
     */
    public int getTotalFree() {
        return this.pool.getTotalFree();
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getTotalCreatedConnections()
     */
    public int getTotalCreatedObjects() {
        return this.pool.getTotalCreatedObjects();
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getCacheHits()
     */
    public long getCacheHits() {
        return this.cacheHits.get();
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getCacheMiss()
     */
    public long getCacheMiss() {
        return this.cacheMiss.get();
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getStatementsCached()
     */
    public long getStatementsCached() {
        return this.statementsCached.get();
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getConnectionsRequested()
     */
    public long getObjectsRequested() {
        return this.objectsRequested.get();
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getCumulativeConnectionWaitTime()
     */
    public long getCumulativeObjectWaitTime() {
        return this.cumulativeObjectWaitTime.get() / 1000000;
    }

    /**
     * Adds connection wait time.
     *
     * @param increment
     */
    protected void addCumulativeObjectWaitTime(long increment) {
        this.cumulativeObjectWaitTime.addAndGet(increment);
    }

    /**
     * Adds statements executed.
     */
    protected void incrementStatementsExecuted() {
        this.statementsExecuted.incrementAndGet();
    }

    /**
     * Adds statements executed.
     */
    protected void incrementStatementsPrepared() {
        this.statementsPrepared.incrementAndGet();
    }

    /**
     * Accessor method.
     */
    protected void incrementStatementsCached() {
        this.statementsCached.incrementAndGet();
    }

    /**
     * Accessor method.
     */
    protected void incrementCacheMiss() {
        this.cacheMiss.incrementAndGet();
    }

    /**
     * Accessor method.
     */
    protected void incrementCacheHits() {
        this.cacheHits.incrementAndGet();
    }

    /**
     * Accessor method.
     */
    protected void incrementObjectsRequested() {
        this.objectsRequested.incrementAndGet();
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getCacheHitRatio()
     */
    @Override
    public double getCacheHitRatio() {
        return this.cacheHits.get() + this.cacheMiss.get() == 0 ? 0 : this.cacheHits.get() / (1.0 * this.cacheHits.get() + this.cacheMiss.get());
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getStatementsExecuted()
     */
    public long getStatementsExecuted() {
        return this.statementsExecuted.get();
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getCumulativeStatementExecutionTime()
     */
    public long getCumulativeStatementExecutionTime() {
        return this.cumulativeStatementExecuteTime.get() / 1000000;
    }

    /**
     * Accessor method
     *
     * @param time
     */
    protected void addStatementExecuteTime(long time) {
        this.cumulativeStatementExecuteTime.addAndGet(time);
    }

    /**
     * Accessor method
     *
     * @param time
     */
    protected void addStatementPrepareTime(long time) {
        this.cumulativeStatementPrepareTime.addAndGet(time);
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getCumulativeStatementPrepareTime()
     */
    public long getCumulativeStatementPrepareTime() {
        return this.cumulativeStatementPrepareTime.get() / 1000000;
    }

    /* (non-Javadoc)
     * @see com.jolbox.bonecp.StatisticsMBean#getStatementsPrepared()
     */
    public long getStatementsPrepared() {
        return this.statementsPrepared.get();
    }

}
