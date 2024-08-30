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

import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.MinAggregationFunction;
import org.apache.pinot.perf.SyntheticBlockValSets;
import org.apache.pinot.perf.SyntheticNullBitmapFactories;
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


@Fork(1)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 50, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 50, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class BenchmarkMinAggregation extends AbstractAggregationFunctionBenchmark.Stable {
  private static final ExpressionContext EXPR = ExpressionContext.forIdentifier("col");

  @Param({"false", "true"})
  private boolean _nullHandlingEnabled;

  @Param({"1", "2", "4", "8", "16", "32", "64", "128"})
  protected int _nullPeriod;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(BenchmarkMinAggregation.class.getSimpleName()).build();
    new Runner(opt).run();
  }

  @Override
  protected AggregationFunction<?, ?> createAggregationFunction() {
    return new MinAggregationFunction(Collections.singletonList(EXPR), _nullHandlingEnabled);
  }

  @Override
  protected AggregationResultHolder createResultHolder() {
    return getAggregationFunction().createAggregationResultHolder();
  }

  @Override
  protected Map<ExpressionContext, BlockValSet> createBlockValSetMap() {
    Random valueRandom = new Random(420);
    int numDocs = DocIdSetPlanNode.MAX_DOC_PER_CALL;
    RoaringBitmap nullBitmap = SyntheticNullBitmapFactories.Periodic.randomInPeriod(numDocs, _nullPeriod);
    BlockValSet block = SyntheticBlockValSets.Double.create(numDocs, _nullHandlingEnabled ? nullBitmap : null,
        valueRandom::nextInt);
    return Map.of(EXPR, block);
  }

  @Override
  protected Object createExpectedResult(Map<ExpressionContext, BlockValSet> map) {
    Double min = null;
    BlockValSet blockValSet = getBlockValSetMap().get(EXPR);
    double[] doubleValuesSV = blockValSet.getDoubleValuesSV();
    RoaringBitmap nullBitmap = blockValSet.getNullBitmap();

    for (int i = 0; i < doubleValuesSV.length; i++) {
      if (nullBitmap != null && nullBitmap.contains(i)) {
        continue;
      }
      min = (min == null) ? doubleValuesSV[i] : Math.min(min, doubleValuesSV[i]);
    }

    return min;
  }

  @Override
  protected void resetResultHolder(AggregationResultHolder resultHolder) {
    if (_nullHandlingEnabled) {
      resultHolder.setValue(null);
    } else {
      resultHolder.setValue(Double.POSITIVE_INFINITY);
    }
  }

  @Override
  protected Comparable extractFinalResult(AggregationResultHolder resultHolder) {
    if (_nullHandlingEnabled) {
      return resultHolder.getResult();
    } else {
      return resultHolder.getDoubleResult();
    }
  }
}
