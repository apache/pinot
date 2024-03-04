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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.ModeAggregationFunction;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.RoaringBitmap;

@Fork(1)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 50, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 50, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class BenchmarkModeAggregation extends AbstractAggregationFunctionBenchmark.Stable {
  private static final ExpressionContext EXPR = ExpressionContext.forIdentifier("col");
  @Param({"100", "50", "0"})
  public int _nullHandlingEnabledPerCent;
  private boolean _nullHandlingEnabled;
  @Param({"2", "4", "8", "16", "32", "64", "128"})
  protected int _nullPeriod;
  private final Random _segmentNullRandomGenerator = new Random(42);
  private double _modeIgnoringNull;
  private double _modeNullAware;
  private final int _numDocs = DocIdSetPlanNode.MAX_DOC_PER_CALL;

  public static void main(String[] args)
      throws RunnerException {
    Options opt = new OptionsBuilder().include(BenchmarkModeAggregation.class.getSimpleName())
        //                .addProfiler(LinuxPerfAsmProfiler.class)
        .build();

    new Runner(opt).run();
  }

  @Override
  protected Map<ExpressionContext, BlockValSet> createBlockValSetMap() {
    Random valueRandom = new Random(42);
    int tries = 3;
    LongSupplier longSupplier = () -> {
      // just a simple distribution that will have some kind of normal but limited distribution
      int sum = 0;
      for (int i = 0; i < tries; i++) {
        sum += valueRandom.nextInt(_numDocs);
      }
      return sum / tries;
    };
    RoaringBitmap nullBitmap = SyntheticNullBitmapFactories.Periodic.randomInPeriod(_numDocs, _nullPeriod);

    BlockValSet block = SyntheticBlockValSets.Long.create(_numDocs, nullBitmap, longSupplier);
    return Map.of(EXPR, block);
  }

  @Override
  public void setupTrial() {
    super.setupTrial();

    HashMap<Long, Integer> ignoringNullDistribution = new HashMap<>();
    HashMap<Long, Integer> nullAwareDistribution = new HashMap<>();

    BlockValSet blockValSet = getBlockValSetMap().get(EXPR);
    long[] longValuesSV = blockValSet.getLongValuesSV();
    RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
    if (nullBitmap != null) {
      for (int i = 0; i < _numDocs; i++) {
        long value = longValuesSV[i];
        ignoringNullDistribution.merge(value, 1, Integer::sum);
        if (!nullBitmap.contains(i)) {
          nullAwareDistribution.merge(value, 1, Integer::sum);
        }
      }
    } else {
      for (int i = 0; i < _numDocs; i++) {
        long value = longValuesSV[i];
        ignoringNullDistribution.merge(value, 1, Integer::sum);
        nullAwareDistribution.merge(value, 1, Integer::sum);
      }
    }
    _modeIgnoringNull = ignoringNullDistribution.entrySet().stream()
        .max(Comparator.comparingInt(Map.Entry::getValue))
        .get()
        .getKey()
        .doubleValue();
    _modeNullAware = nullAwareDistribution.entrySet().stream()
        .max(Comparator.comparingInt(Map.Entry::getValue))
        .get()
        .getKey()
        .doubleValue();
  }

  @Override
  public void setupIteration() {
    _nullHandlingEnabled = _segmentNullRandomGenerator.nextInt(100) < _nullHandlingEnabledPerCent;
    super.setupIteration();
  }

  @Override
  protected Level getAggregationFunctionLevel() {
    return Level.Iteration;
  }

  @Override
  protected AggregationFunction<?, ?> createAggregationFunction() {
    return new ModeAggregationFunction(Collections.singletonList(EXPR), _nullHandlingEnabled);
  }

  @Override
  protected Level getResultHolderLevel() {
    return Level.Iteration;
  }

  @Override
  protected AggregationResultHolder createResultHolder() {
    return getAggregationFunction().createAggregationResultHolder();
  }

  @Override
  protected void resetResultHolder(AggregationResultHolder resultHolder) {
    Map<? extends Number, Long> result = resultHolder.getResult();
    if (result != null) {
      result.clear();
    }
  }

  @Override
  protected Level getExpectedResultLevel() {
    return Level.Iteration;
  }

  @Override
  protected Object createExpectedResult(Map<ExpressionContext, BlockValSet> map) {
    return _nullHandlingEnabled ? _modeNullAware : _modeIgnoringNull;
  }
}
