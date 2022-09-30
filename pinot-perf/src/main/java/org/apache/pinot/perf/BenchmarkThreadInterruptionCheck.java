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
public class BenchmarkThreadInterruptionCheck {

  static final int MAX_ROWS_UPSERT_PER_INTERRUPTION_CHECK_MASK = 0b111_11111_11111;

  public static void main(String[] args)
      throws RunnerException {
    Options opt =
        new OptionsBuilder().include(BenchmarkThreadInterruptionCheck.class.getSimpleName())
            .warmupTime(TimeValue.seconds(5))
            .warmupIterations(3).measurementTime(TimeValue.seconds(5)).measurementIterations(5).forks(1).build();

    new Runner(opt).run();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchMaskingTime(Blackhole bh) {
    for (int i = 0; i < 1000000; i++) {
      bh.consume((i & MAX_ROWS_UPSERT_PER_INTERRUPTION_CHECK_MASK) == 0);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchModuloTime(Blackhole bh) {
    for (int i = 0; i < 1000000; i++) {
      bh.consume((i % 16321) == 0);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchLoopTilingTime(Blackhole bh) {
    for (int i = 0; i < 1000000; i += 16321) {
      bh.consume(Math.min(i + 16321, 1000000));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchInterruptionCheckTime(Blackhole bh) {
    for (int i = 0; i < 1000000; i++) {
      bh.consume(Thread.interrupted());
    }
  }
}
