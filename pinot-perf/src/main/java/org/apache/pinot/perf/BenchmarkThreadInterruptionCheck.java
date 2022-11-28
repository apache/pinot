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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.trace.Tracing;
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


/**
 * TOTAL_LOOPS = 10_000_000
 * BenchmarkThreadInterruptionCheck.benchInterruptionCheckTime    avgt    8   86.242 ±  5.375  ms/op
 * BenchmarkThreadInterruptionCheck.benchMaskingTime              avgt    8  170.389 ± 18.277  ms/op
 * BenchmarkThreadInterruptionCheck.benchModuloTime               avgt    8  181.029 ± 20.284  ms/op
 * BenchmarkThreadInterruptionCheck.benchmarkWorkload             avgt    8  164.734 ± 18.122  ms/op
 * BenchmarkThreadInterruptionCheck.benchmarkWorkloadWithTiling1  avgt    8  169.854 ±  9.057  ms/op
 * BenchmarkThreadInterruptionCheck.benchmarkWorkloadWithTiling2  avgt    8  181.711 ± 21.245  ms/op
 *
 * TOTAL_LOOPS = 100_000
 * Benchmark                                                      Mode  Cnt  Score   Error  Units
 * BenchmarkThreadInterruptionCheck.benchInterruptionCheckTime    avgt    8  0.822 ± 0.014  ms/op
 * BenchmarkThreadInterruptionCheck.benchMaskingTime              avgt    8  1.618 ± 0.037  ms/op
 * BenchmarkThreadInterruptionCheck.benchModuloTime               avgt    8  1.678 ± 0.012  ms/op
 * BenchmarkThreadInterruptionCheck.benchmarkWorkload             avgt    8  1.590 ± 0.020  ms/op
 * BenchmarkThreadInterruptionCheck.benchmarkWorkloadWithTiling1  avgt    8  2.089 ± 0.231  ms/op
 * BenchmarkThreadInterruptionCheck.benchmarkWorkloadWithTiling2  avgt    8  1.776 ± 0.044  ms/op
 *
 * TOTAL_LOOPS = 10_000
 * Benchmark                                                      Mode  Cnt  Score   Error  Units
 * BenchmarkThreadInterruptionCheck.benchInterruptionCheckTime    avgt    8  0.092 ± 0.004  ms/op
 * BenchmarkThreadInterruptionCheck.benchMaskingTime              avgt    8  0.139 ± 0.005  ms/op
 * BenchmarkThreadInterruptionCheck.benchModuloTime               avgt    8  0.168 ± 0.034  ms/op
 * BenchmarkThreadInterruptionCheck.benchmarkWorkload             avgt    8  0.141 ± 0.003  ms/op
 * BenchmarkThreadInterruptionCheck.benchmarkWorkloadWithTiling1  avgt    8  0.155 ± 0.008  ms/op
 * BenchmarkThreadInterruptionCheck.benchmarkWorkloadWithTiling2  avgt    8  0.182 ± 0.011  ms/op
 */
@State(Scope.Benchmark)
public class BenchmarkThreadInterruptionCheck {

  static final int MAX_ROWS_UPSERT_PER_INTERRUPTION_CHECK_MASK = 0b1_1111_1111_1111;
  static final int TOTAL_LOOPS = 100_000;
  static final int LOOP_TILE_SIZE = 10_000;

  public static void main(String[] args)
      throws RunnerException {
    Options opt = new OptionsBuilder().include(BenchmarkThreadInterruptionCheck.class.getSimpleName())
        .warmupTime(TimeValue.seconds(5)).warmupIterations(3).measurementTime(TimeValue.seconds(5))
        .measurementIterations(8).forks(1).build();

    new Runner(opt).run();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchInterruptionCheckTime(Blackhole bh) {
    for (int i = 0; i < TOTAL_LOOPS; i++) {
      bh.consume(Tracing.ThreadAccountantOps.isInterrupted());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchmarkWorkload(Blackhole bh) {
    AtomicInteger atm = new AtomicInteger(0);
    for (int i = 0; i < TOTAL_LOOPS; i++) {
      atm.getAndAdd(String.valueOf(i % 16321 + 100).hashCode() % 1000);
    }
    bh.consume(atm); //647375024
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchModuloTime(Blackhole bh) {
    AtomicInteger atm = new AtomicInteger(0);
    int count = 0;
    for (int i = 0; i < TOTAL_LOOPS; i++) {
      if ((count % LOOP_TILE_SIZE) == 0 && Tracing.ThreadAccountantOps.isInterrupted()) {
        throw new EarlyTerminationException();
      }
      atm.getAndAdd(String.valueOf(i % 16321 + 100).hashCode() % 1000);
      count++;
    }
    bh.consume(atm); //647375024
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchMaskingTime(Blackhole bh) {
    AtomicInteger atm = new AtomicInteger(0);
    int count = 0;
    for (int i = 0; i < TOTAL_LOOPS; i++) {
      if ((count & MAX_ROWS_UPSERT_PER_INTERRUPTION_CHECK_MASK) == 0 && Tracing.ThreadAccountantOps.isInterrupted()) {
        throw new EarlyTerminationException();
      }
      atm.getAndAdd(String.valueOf(i % 16321 + 100).hashCode() % 1000);
      count++;
    }
    bh.consume(atm); //647375024
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchmarkWorkloadWithTiling1(Blackhole bh) {
    AtomicInteger atm = new AtomicInteger(0);
    for (int i = 0; i < TOTAL_LOOPS; i += LOOP_TILE_SIZE) {
      if (Tracing.ThreadAccountantOps.isInterrupted()) {
        throw new EarlyTerminationException();
      }
      for (int j = i; j < Math.min(i + LOOP_TILE_SIZE, TOTAL_LOOPS); j++) {
        atm.getAndAdd(String.valueOf(j % 16321 + 100).hashCode() % 1000);
      }
    }
    bh.consume(atm); //647375024
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchmarkWorkloadWithTiling2(Blackhole bh) {
    AtomicInteger atm = new AtomicInteger(0);
    LoopUtils.tiledLoopExecution(TOTAL_LOOPS, LOOP_TILE_SIZE,
        i -> atm.getAndAdd(String.valueOf(i % 16321 + 100).hashCode() % 1000));
    bh.consume(atm); //647375024
  }

  public static class LoopUtils {

    private LoopUtils() {
    }

    public static void tiledLoopExecution(int totalSize, int tileSize, Consumer<Integer> consumer) {
      try {
        for (int i = 0; i < totalSize; i += tileSize) {
          int upper = Math.min(i + tileSize, totalSize);
          if (Tracing.ThreadAccountantOps.isInterrupted()) {
            throw new EarlyTerminationException();
          }
          for (int j = i; j < upper; j++) {
            consumer.accept(j);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
