/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.perf;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
/**
 * Hotspot:
 * Benchmark                                             Mode  Cnt   Score    Error  Units
 * BenchmarkThreadMXBean.benchThreadMXBeanMem            avgt    5  ≈ 10⁻⁴           ms/op
 * BenchmarkThreadMXBean.benchThreadMXBeanThreadCPUTime  avgt    5   0.001 ±  0.001  ms/op
 *
 * OpenJ9 does not even support getThreadAllocatedBytes, so it is always returning 0
 * meanwhile JMH doesn't support OpenJ9, so the benchmark is not accurate
 * Benchmark                                             Mode  Cnt  Score    Error  Units
 * BenchmarkThreadMXBean.benchThreadMXBeanMem            avgt    5  0.001 ±  0.001  ms/op
 * BenchmarkThreadMXBean.benchThreadMXBeanThreadCPUTime  avgt    5  0.003 ±  0.001  ms/op
 */
public class BenchmarkThreadResourceUsageProvider {

  public static void main(String[] args)
      throws RunnerException {
    Options opt =
        new OptionsBuilder().include(BenchmarkThreadResourceUsageProvider.class.getSimpleName())
            .warmupTime(TimeValue.seconds(5))
            .warmupIterations(3).measurementTime(TimeValue.seconds(10)).measurementIterations(5).forks(1).build();

    new Runner(opt).run();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchThreadMXBeanThreadCPUTime(MyState myState, Blackhole bh) {
    bh.consume(myState._threadResourceUsageProvider.getThreadTimeNs());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchThreadMXBeanMem(MyState myState, Blackhole bh) {
    bh.consume(myState._threadResourceUsageProvider.getThreadAllocatedBytes());
  }

  @State(Scope.Benchmark)
  public static class MyState {
    ThreadResourceUsageProvider _threadResourceUsageProvider;
    long[] _allocation;

    @Setup(Level.Iteration)
    public void doSetup() {
      _threadResourceUsageProvider = new ThreadResourceUsageProvider();
    }

    @Setup(Level.Invocation)
    public void allocateMemory() {
      _allocation = new long[1000];
    }

    static {
      ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
      ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    }
  }
}
