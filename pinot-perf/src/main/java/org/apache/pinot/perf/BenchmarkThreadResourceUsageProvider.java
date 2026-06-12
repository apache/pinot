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
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
/// OpenJDK 21.0.2
/// Benchmark                                                                  Mode  Cnt    Score    Error  Units
/// BenchmarkThreadResourceUsageProvider.benchmarkCurrentThreadAllocatedBytes  avgt    5    9.940 ±  0.193  ns/op
/// BenchmarkThreadResourceUsageProvider.benchmarkCurrentThreadCpuTime         avgt    5  556.737 ± 16.349  ns/op
public class BenchmarkThreadResourceUsageProvider {
  static {
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
  }

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
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void benchmarkCurrentThreadCpuTime(Blackhole bh) {
    bh.consume(ThreadResourceUsageProvider.getCurrentThreadCpuTime());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void benchmarkCurrentThreadAllocatedBytes(Blackhole bh) {
    bh.consume(ThreadResourceUsageProvider.getCurrentThreadAllocatedBytes());
  }
}
