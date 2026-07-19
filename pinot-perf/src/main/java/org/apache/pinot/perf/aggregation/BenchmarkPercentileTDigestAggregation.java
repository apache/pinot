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

import com.tdunning.math.stats.TDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/// Measures percentile TDigest aggregation over 100 million rows on one machine.
///
/// Each invocation creates a fresh result holder and feeds 100 million deterministic pseudo-random values through the
/// production aggregation function in Pinot-sized 10,000-row blocks. The result is checked for its row count and
/// accuracy against an exact precomputed 75th percentile.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx8g"})
@Threads(1)
public class BenchmarkPercentileTDigestAggregation {
  private static final int NUM_ROWS = 100_000_000;
  private static final int BLOCK_SIZE = DocIdSetPlanNode.MAX_DOC_PER_CALL;
  private static final int NUM_BLOCKS = NUM_ROWS / BLOCK_SIZE;
  private static final int PERCENTILE_INDEX = (int) ((long) NUM_ROWS * 75 / 100);
  private static final double MAX_ABSOLUTE_ERROR = 0.001;
  private static final ExpressionContext EXPRESSION = ExpressionContext.forIdentifier("col");

  /// State for the end-to-end aggregation-function benchmark.
  @State(Scope.Benchmark)
  public static class AggregationState {
    private PercentileTDigestAggregationFunction _function;
    private List<Map<ExpressionContext, BlockValSet>> _blockValSetMaps;
    private double _expectedResult;

    @Setup(Level.Trial)
    public void setUp() {
      if (NUM_ROWS % BLOCK_SIZE != 0) {
        throw new IllegalStateException("NUM_ROWS must be divisible by BLOCK_SIZE");
      }
      SplittableRandom random = new SplittableRandom(42);
      _blockValSetMaps = new ArrayList<>(NUM_BLOCKS);
      double[] expectedValues = new double[NUM_ROWS];
      for (int blockId = 0; blockId < NUM_BLOCKS; blockId++) {
        double[] values = new double[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
          values[i] = random.nextDouble();
        }
        _blockValSetMaps.add(Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(null, values)));
        System.arraycopy(values, 0, expectedValues, blockId * BLOCK_SIZE, BLOCK_SIZE);
      }
      Arrays.sort(expectedValues);
      _expectedResult = expectedValues[PERCENTILE_INDEX];
      _function = new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, false);
    }
  }

  @Benchmark
  public double aggregatePercentileTDigest75(AggregationState state) {
    AggregationResultHolder resultHolder = state._function.createAggregationResultHolder();
    for (int i = 0; i < NUM_BLOCKS; i++) {
      state._function.aggregate(BLOCK_SIZE, resultHolder, state._blockValSetMaps.get(i));
    }
    TDigest result = state._function.extractAggregationResult(resultHolder);
    if (result.size() != NUM_ROWS) {
      throw new IllegalStateException("Unexpected TDigest size: " + result.size() + ", expected: " + NUM_ROWS);
    }
    return verify(state._function.extractFinalResult(result), state._expectedResult);
  }

  private static double verify(double actual, double expected) {
    if (!Double.isFinite(actual) || Math.abs(actual - expected) > MAX_ABSOLUTE_ERROR) {
      throw new IllegalStateException("Unexpected percentile: " + actual + ", expected: " + expected);
    }
    return actual;
  }
}
