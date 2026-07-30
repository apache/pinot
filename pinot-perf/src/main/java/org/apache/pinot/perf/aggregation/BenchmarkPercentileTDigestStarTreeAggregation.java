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
import java.util.Arrays;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.roaringbitmap.RoaringBitmap;

/// Measures percentile TDigest aggregation over serialized star-tree entries representing 100 million source rows.
///
/// The benchmark creates 10,000 deterministic serialized digests, each summarizing 10,000 source values, and feeds one
/// Pinot-sized block through the production `BYTES` aggregation path. The group-by workload distributes those entries
/// round-robin across 100, 1,000, or 10,000 result groups. It measures query-time deserialization, merging, and result
/// extraction, but deliberately excludes segment generation and forward-index I/O.
///
/// `NATIVE` measures the production end-to-end layout, including the dependency version's source compression.
/// `FIXED_VERBOSE` serializes the same raw values into a deterministic verbose centroid layout, including singleton
/// endpoint centroids, so a TDigest 3.2 build and a 3.3 build receive byte-for-byte identical query inputs.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx8g"})
@Threads(1)
public class BenchmarkPercentileTDigestStarTreeAggregation {
  private static final int NUM_SOURCE_ROWS = 100_000_000;
  private static final int NUM_STAR_TREE_ROWS = DocIdSetPlanNode.MAX_DOC_PER_CALL;
  private static final int SOURCE_ROWS_PER_DIGEST = NUM_SOURCE_ROWS / NUM_STAR_TREE_ROWS;
  private static final double EXPECTED_PERCENTILE_75 = 0.7499315463846341;
  private static final double MAX_ABSOLUTE_ERROR = 0.001;
  private static final ExpressionContext EXPRESSION = ExpressionContext.forIdentifier("col");

  /// Selects dependency-native serialized inputs or byte-identical verbose control inputs.
  public enum SourceLayout {
    NATIVE,
    FIXED_VERBOSE
  }

  /// State for the serialized star-tree aggregation benchmark.
  @State(Scope.Benchmark)
  public static class AggregationState {
    @Param({"NATIVE", "FIXED_VERBOSE"})
    private SourceLayout _sourceLayout;

    private PercentileTDigestAggregationFunction _function;
    private Map<ExpressionContext, BlockValSet> _blockValSetMap;

    @Setup(Level.Trial)
    public void setUp() {
      if (NUM_SOURCE_ROWS % NUM_STAR_TREE_ROWS != 0) {
        throw new IllegalStateException("NUM_SOURCE_ROWS must be divisible by NUM_STAR_TREE_ROWS");
      }
      _blockValSetMap = Map.of(EXPRESSION, new BytesBlockValSet(createSerializedDigests(_sourceLayout)));
      _function = new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, false);
    }
  }

  /// State for the serialized star-tree group-by aggregation benchmark.
  @State(Scope.Benchmark)
  public static class GroupByAggregationState {
    @Param({"NATIVE", "FIXED_VERBOSE"})
    private SourceLayout _sourceLayout;

    @Param({"100", "1000", "10000"})
    private int _numGroups;

    private PercentileTDigestAggregationFunction _function;
    private Map<ExpressionContext, BlockValSet> _blockValSetMap;
    private int[] _groupKeys;

    @Setup(Level.Trial)
    public void setUp() {
      if (NUM_STAR_TREE_ROWS % _numGroups != 0) {
        throw new IllegalStateException("NUM_STAR_TREE_ROWS must be divisible by the number of groups");
      }
      _blockValSetMap = Map.of(EXPRESSION, new BytesBlockValSet(createSerializedDigests(_sourceLayout)));
      _function = new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, false);
      _groupKeys = new int[NUM_STAR_TREE_ROWS];
      for (int rowId = 0; rowId < NUM_STAR_TREE_ROWS; rowId++) {
        _groupKeys[rowId] = rowId % _numGroups;
      }
    }
  }

  @Benchmark
  public double aggregatePercentileTDigest75(AggregationState state) {
    AggregationResultHolder resultHolder = state._function.createAggregationResultHolder();
    state._function.aggregate(NUM_STAR_TREE_ROWS, resultHolder, state._blockValSetMap);
    TDigest result = state._function.extractAggregationResult(resultHolder);
    if (result.size() != NUM_SOURCE_ROWS) {
      throw new IllegalStateException("Unexpected TDigest size: " + result.size() + ", expected: " + NUM_SOURCE_ROWS);
    }
    double percentile = state._function.extractFinalResult(result);
    if (!Double.isFinite(percentile) || Math.abs(percentile - EXPECTED_PERCENTILE_75) > MAX_ABSOLUTE_ERROR) {
      throw new IllegalStateException(
          "Unexpected percentile: " + percentile + ", expected: " + EXPECTED_PERCENTILE_75);
    }
    return percentile;
  }

  @Benchmark
  public double aggregateGroupByPercentileTDigest75(GroupByAggregationState state) {
    GroupByResultHolder resultHolder = state._function.createGroupByResultHolder(state._numGroups, state._numGroups);
    state._function.aggregateGroupBySV(NUM_STAR_TREE_ROWS, state._groupKeys, resultHolder, state._blockValSetMap);

    long totalSize = 0L;
    double percentileChecksum = 0.0;
    long expectedGroupSize = NUM_SOURCE_ROWS / state._numGroups;
    for (int groupKey = 0; groupKey < state._numGroups; groupKey++) {
      TDigest result = state._function.extractGroupByResult(resultHolder, groupKey);
      if (result.size() != expectedGroupSize) {
        throw new IllegalStateException(
            "Unexpected TDigest size: " + result.size() + ", expected: " + expectedGroupSize);
      }
      double percentile = state._function.extractFinalResult(result);
      if (!Double.isFinite(percentile) || Math.abs(percentile - 0.75) > 0.05) {
        throw new IllegalStateException("Unexpected percentile for group " + groupKey + ": " + percentile);
      }
      totalSize += result.size();
      percentileChecksum += percentile;
    }
    if (totalSize != NUM_SOURCE_ROWS) {
      throw new IllegalStateException("Unexpected total TDigest size: " + totalSize + ", expected: " + NUM_SOURCE_ROWS);
    }
    return percentileChecksum;
  }

  private static byte[][] createSerializedDigests(SourceLayout sourceLayout) {
    SplittableRandom random = new SplittableRandom(42);
    byte[][] serializedDigests = new byte[NUM_STAR_TREE_ROWS][];
    for (int rowId = 0; rowId < NUM_STAR_TREE_ROWS; rowId++) {
      if (sourceLayout == SourceLayout.NATIVE) {
        TDigest digest = TDigestBenchmarkUtils.usePinotScaleFunction(
            TDigest.createMergingDigest(PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION));
        for (int i = 0; i < SOURCE_ROWS_PER_DIGEST; i++) {
          digest.add(random.nextDouble());
        }
        serializedDigests[rowId] = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(digest);
      } else {
        double[] values = new double[SOURCE_ROWS_PER_DIGEST];
        for (int i = 0; i < SOURCE_ROWS_PER_DIGEST; i++) {
          values[i] = random.nextDouble();
        }
        Arrays.sort(values);
        serializedDigests[rowId] = TDigestBenchmarkUtils.createFixedVerboseBytes(values,
            PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);
      }
    }
    return serializedDigests;
  }

  private static final class BytesBlockValSet extends SyntheticBlockValSets.Base {
    private final byte[][] _values;

    private BytesBlockValSet(byte[][] values) {
      _values = values;
    }

    @Override
    public DataType getValueType() {
      return DataType.BYTES;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public byte[][] getBytesValuesSV() {
      return _values;
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return null;
    }
  }
}
