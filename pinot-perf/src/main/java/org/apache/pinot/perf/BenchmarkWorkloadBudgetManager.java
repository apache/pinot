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
import org.apache.pinot.spi.accounting.WorkloadBudgetManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BenchmarkWorkloadBudgetManager {

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(BenchmarkWorkloadBudgetManager.class.getSimpleName()) // Match class name
        .warmupIterations(3).measurementIterations(3).forks(1).shouldFailOnError(true).build();

    new Runner(opt).run();
  }

  @State(Scope.Thread)
  public static class BenchmarkState {
    static final String WORKLOAD = "benchmark";
    static final long CPU = 10L;
    static final long MEM = 10L;

    static WorkloadBudgetManager _manager;

    @Setup(Level.Iteration)
    public void setup() {
      PinotConfiguration config = new PinotConfiguration();
      config.setProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_COLLECTION, true);
      config.setProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENFORCEMENT_WINDOW_MS, Long.MAX_VALUE);

      _manager = new WorkloadBudgetManager(config);
      _manager.addOrUpdateWorkload(WORKLOAD, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
      // TODO(Vivek): Check if there are stale threads after each benchmark.
      _manager.shutdown();
    }

    public WorkloadBudgetManager manager() {
      return _manager;
    }
  }

  @Benchmark
  @Threads(1)
  public WorkloadBudgetManager.BudgetStats tryCharge1Thread(BenchmarkState state) {
    return state.manager().tryCharge(BenchmarkState.WORKLOAD, BenchmarkState.CPU, BenchmarkState.MEM);
  }

  @Benchmark
  @Threads(10)
  public WorkloadBudgetManager.BudgetStats tryCharge10Threads(BenchmarkState state) {
    return state.manager().tryCharge(BenchmarkState.WORKLOAD, BenchmarkState.CPU, BenchmarkState.MEM);
  }

  @Benchmark
  @Threads(20)
  public WorkloadBudgetManager.BudgetStats tryCharge20Threads(BenchmarkState state) {
    return state.manager().tryCharge(BenchmarkState.WORKLOAD, BenchmarkState.CPU, BenchmarkState.MEM);
  }

  @Benchmark
  @Threads(40)
  public WorkloadBudgetManager.BudgetStats tryCharge40Threads(BenchmarkState state) {
    return state.manager().tryCharge(BenchmarkState.WORKLOAD, BenchmarkState.CPU, BenchmarkState.MEM);
  }
}
