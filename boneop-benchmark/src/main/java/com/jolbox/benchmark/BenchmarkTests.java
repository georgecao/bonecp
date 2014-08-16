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
package com.jolbox.benchmark;

import com.jolbox.boneop.BoneOP;
import com.jolbox.boneop.BoneOPConfig;
import com.jolbox.boneop.PoolException;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks of Apache Commons Pool and BoneOP.
 *
 * @author wwadge
 * @version $Revision$
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2)
@State(Scope.Thread)
@Fork(2)
public class BenchmarkTests {

    /**
     * config setting.
     */
    @Param({"1", "2", "5"})
    private int acquireIncrement;

    @Param({"1", "2", "4", "8"})
    private int partitionCount;

    @Param({"20", "50", "100", "300", "500"})
    private int poolSize;

    @Param({"1", "2", "4", "8"})
    private int releaseHelperThreads;

    private BoneOP<BenchmarkObject> pool;

    @Setup
    public void createPool() throws PoolException {
        BoneOPConfig conf = new BoneOPConfig();
        conf.setIdleObjectTestPeriodInSeconds(0);
        conf.setIdleMaxAgeInSeconds(0);
        conf.setMinObjectsPerPartition(poolSize);
        conf.setMaxObjectsPerPartition(poolSize);
        conf.setPartitionCount(partitionCount);
        conf.setAcquireIncrement(acquireIncrement);
        conf.setDisableObjectTracking(true);
        conf.setReleaseHelperThreads(releaseHelperThreads);
        conf.setWaitTimeInMillis(200);
        conf.setNullOnObjectTimeout(true);
        conf.setDisableJMX(true);
        this.pool = new BoneOP<>(conf, new BenchmarkObjectFactory());
    }

    @Benchmark
    @Fork(2)
    public void getObject() throws PoolException {
        BenchmarkObject object = pool.getObject();
        if (null == object) {
            return;
        }
        pool.releaseObject(object);
    }

    public static void main(String... args) throws RunnerException {
        Options options = new OptionsBuilder()
                .include(".*" + BenchmarkTests.class.getSimpleName() + ".*")
                .warmupIterations(2)
                .forks(2)
                .threads(20)
                .measurementIterations(4)
                .build();
        Runner runner = new Runner(options);
        runner.run();
    }
}
