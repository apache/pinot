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
package org.apache.pinot.perf.aggregation;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctCountBitmapAggregationFunction;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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


/// Benchmarks the serialized-bitmap (`BYTES`) aggregation path of DISTINCT_COUNT_BITMAP, i.e. the path used when
/// unioning pre-aggregated bitmaps such as star-tree DISTINCT_COUNT_BITMAP columns. `_valuesPerBitmap` controls the
/// input bitmap size and `_maxValue` the value universe: a small universe produces dense accumulator containers
/// (bitmap containers), a large one sparse accumulators (array containers).
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 30, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 30, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class BenchmarkDistinctCountBitmapAggregation extends AbstractAggregationFunctionBenchmark.Stable {
  private static final ExpressionContext EXPR = ExpressionContext.forIdentifier("col");

  @Param({"200", "2000"})
  private int _valuesPerBitmap;

  @Param({"2000000", "100000000"})
  private int _maxValue;

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder().include(BenchmarkDistinctCountBitmapAggregation.class.getSimpleName()).build();
    new Runner(opt).run();
  }

  @Override
  protected AggregationFunction<?, ?> createAggregationFunction() {
    return new DistinctCountBitmapAggregationFunction(List.of(EXPR), false);
  }

  @Override
  protected AggregationResultHolder createResultHolder() {
    return getAggregationFunction().createAggregationResultHolder();
  }

  @Override
  protected Map<ExpressionContext, BlockValSet> createBlockValSetMap() {
    Random random = new Random(420);
    int numDocs = DocIdSetPlanNode.MAX_DOC_PER_CALL;
    BlockValSet block = SyntheticBlockValSets.Bytes.create(numDocs, null, () -> {
      RoaringBitmap bitmap = new RoaringBitmap();
      for (int i = 0; i < _valuesPerBitmap; i++) {
        bitmap.add(random.nextInt(_maxValue));
      }
      return RoaringBitmapUtils.serialize(bitmap);
    });
    return Map.of(EXPR, block);
  }

  @Override
  protected Object createExpectedResult(Map<ExpressionContext, BlockValSet> map) {
    RoaringBitmap expected = new RoaringBitmap();
    for (byte[] serialized : map.get(EXPR).getBytesValuesSV()) {
      expected.or(RoaringBitmapUtils.deserialize(serialized));
    }
    return expected.getCardinality();
  }

  @Override
  protected Comparable extractFinalResult(AggregationResultHolder resultHolder) {
    // Route through extractAggregationResult(): the holder's accumulator is not directly consumable (it may be in
    // the lazy union state that extraction repairs), matching how the query engine reads aggregation results
    DistinctCountBitmapAggregationFunction function =
        (DistinctCountBitmapAggregationFunction) getAggregationFunction();
    return function.extractFinalResult(function.extractAggregationResult(resultHolder));
  }

  @Override
  protected void resetResultHolder(AggregationResultHolder resultHolder) {
    resultHolder.setValue(null);
  }
}
