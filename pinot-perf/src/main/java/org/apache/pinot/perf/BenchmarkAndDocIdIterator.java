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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.operator.filter.AndFilterOperator;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapBasedFilterOperator;
import org.apache.pinot.segment.spi.datasource.NullMode;
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
import org.roaringbitmap.buffer.MutableRoaringBitmap;


@State(Scope.Benchmark)
public class BenchmarkAndDocIdIterator {
  private static final int NUM_DOCS = 10_000;
  private static final int NUM_FILTERS = 5;

  public static void main(String[] args)
      throws RunnerException {
    Options opt =
        new OptionsBuilder().include(BenchmarkAndDocIdIterator.class.getSimpleName()).warmupTime(TimeValue.seconds(5))
            .warmupIterations(3).measurementTime(TimeValue.seconds(10)).measurementIterations(5).forks(1).build();

    new Runner(opt).run();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchAndFilterOperator(MyState myState, Blackhole bh) {
    for (int i = 0; i < 100; i++) {
      bh.consume(new AndFilterOperator(myState._childOperators, null, NUM_DOCS, NullMode.NONE_NULLABLE)
          .nextBlock()
          .getBlockDocIdSet()
          .iterator());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchAndFilterOperatorDegenerate(MyState myState, Blackhole bh) {
    for (int i = 0; i < 100; i++) {
      bh.consume(
          new AndFilterOperator(myState._childOperatorsNoOrdering, null, NUM_DOCS, NullMode.NONE_NULLABLE)
              .nextBlock()
              .getBlockDocIdSet()
              .iterator());
    }
  }

  @State(Scope.Benchmark)
  public static class MyState {
    public List<BaseFilterOperator> _childOperators = new ArrayList<>();
    public List<BaseFilterOperator> _childOperatorsNoOrdering = new ArrayList<>();

    @Setup(Level.Trial)
    public void doSetup() {
      MutableRoaringBitmap mutableRoaringBitmap;
      Random r = new Random();
      r.setSeed(42);
      for (int i = 0; i < NUM_FILTERS; i++) {
        mutableRoaringBitmap = new MutableRoaringBitmap();
        double selectedPortion = (NUM_FILTERS - i) / (2.0 * NUM_FILTERS);
        for (int j = 0; j < NUM_DOCS; j++) {
          if (r.nextDouble() < selectedPortion) {
            mutableRoaringBitmap.add(j);
          }
        }
        _childOperators.add(new BitmapBasedFilterOperator(mutableRoaringBitmap.toImmutableRoaringBitmap(),
            false, NUM_DOCS));

        mutableRoaringBitmap = new MutableRoaringBitmap();
        selectedPortion = 0.1;
        for (int j = 0; j < NUM_DOCS; j++) {
          if (r.nextDouble() < selectedPortion) {
            mutableRoaringBitmap.add(j);
          }
        }
        _childOperatorsNoOrdering.add(new BitmapBasedFilterOperator(mutableRoaringBitmap.toImmutableRoaringBitmap(),
            false, NUM_DOCS));
      }
    }
  }
}
