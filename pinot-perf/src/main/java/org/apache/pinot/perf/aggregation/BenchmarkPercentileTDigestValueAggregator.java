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
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.local.aggregator.PercentileTDigestValueAggregator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/// Measures the raw-value and pre-aggregated TDigest kernels used while constructing a star-tree index.
///
/// The default models one million source rows split into groups of 1,000 rows with identical dimension keys. Each
/// leaf group is serialized because every aggregate eventually becomes a star-tree forward-index entry. The second
/// workload merges ten serialized leaves into each parent entry. Use `-p _numRows=100000000` for a 100-million-row
/// run and vary `_rowsPerGroup` to model the table's dimension cardinality. This deliberately excludes dimension
/// sorting and forward-index I/O so it isolates [PercentileTDigestValueAggregator].
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx8g"})
@Threads(1)
public class BenchmarkPercentileTDigestValueAggregator {
  private static final int VALUE_BLOCK_SIZE = 1 << 16;
  private static final int LEAVES_PER_PARENT = 10;

  @Param({"1000000"})
  int _numRows;

  @Param({"1000"})
  int _rowsPerGroup;

  private double[] _values;
  private byte[][] _serializedLeaves;

  @Setup
  public void setUp() {
    if (_numRows <= 0) {
      throw new IllegalArgumentException("numRows must be positive");
    }
    if (_rowsPerGroup <= 0) {
      throw new IllegalArgumentException("rowsPerGroup must be positive");
    }
    _values = new double[VALUE_BLOCK_SIZE];
    SplittableRandom random = new SplittableRandom(42);
    for (int i = 0; i < VALUE_BLOCK_SIZE; i++) {
      _values[i] = random.nextDouble();
    }
    PercentileTDigestValueAggregator aggregator = new PercentileTDigestValueAggregator(List.of());
    _serializedLeaves = new byte[(_numRows + _rowsPerGroup - 1) / _rowsPerGroup][];
    for (int leafId = 0; leafId < _serializedLeaves.length; leafId++) {
      int from = leafId * _rowsPerGroup;
      int to = Math.min(_numRows, from + _rowsPerGroup);
      TDigest digest = aggregator.getInitialAggregatedValue(_values[from & (VALUE_BLOCK_SIZE - 1)]);
      for (int i = from + 1; i < to; i++) {
        aggregator.applyRawValue(digest, _values[i & (VALUE_BLOCK_SIZE - 1)]);
      }
      _serializedLeaves[leafId] = aggregator.serializeAggregatedValue(digest);
    }
  }

  @Benchmark
  public long aggregateAndSerializeRawValues() {
    PercentileTDigestValueAggregator aggregator = new PercentileTDigestValueAggregator(List.of());
    long totalAggregatedRows = 0;
    long totalSerializedBytes = 0;
    for (int from = 0; from < _numRows; from += _rowsPerGroup) {
      int to = Math.min(_numRows, from + _rowsPerGroup);
      TDigest digest = aggregator.getInitialAggregatedValue(_values[from & (VALUE_BLOCK_SIZE - 1)]);
      for (int i = from + 1; i < to; i++) {
        aggregator.applyRawValue(digest, _values[i & (VALUE_BLOCK_SIZE - 1)]);
      }
      int maxByteSize = aggregator.getMaxAggregatedValueByteSize();
      byte[] serialized = aggregator.serializeAggregatedValue(digest);
      if (serialized.length > maxByteSize) {
        throw new IllegalStateException(
            "Serialized TDigest exceeds registered maximum: " + serialized.length + " > " + maxByteSize);
      }
      totalAggregatedRows += digest.size();
      totalSerializedBytes += serialized.length;
    }
    if (totalAggregatedRows != _numRows) {
      throw new IllegalStateException("Unexpected aggregated row count: " + totalAggregatedRows);
    }
    return totalSerializedBytes;
  }

  @Benchmark
  public long mergeAndSerializePreAggregatedValues() {
    PercentileTDigestValueAggregator aggregator = new PercentileTDigestValueAggregator(List.of());
    long totalAggregatedRows = 0L;
    long totalSerializedBytes = 0L;
    for (int from = 0; from < _serializedLeaves.length; from += LEAVES_PER_PARENT) {
      int to = Math.min(_serializedLeaves.length, from + LEAVES_PER_PARENT);
      TDigest digest = aggregator.deserializeAggregatedValue(_serializedLeaves[from]);
      for (int i = from + 1; i < to; i++) {
        aggregator.applyAggregatedValue(digest, aggregator.deserializeAggregatedValue(_serializedLeaves[i]));
      }
      double median = digest.quantile(0.5);
      if (!Double.isFinite(median) || median < 0.0 || median > 1.0) {
        throw new IllegalStateException("Invalid merged median: " + median);
      }
      totalAggregatedRows += digest.size();
      totalSerializedBytes += aggregator.serializeAggregatedValue(digest).length;
    }
    if (totalAggregatedRows != _numRows) {
      throw new IllegalStateException("Unexpected merged row count: " + totalAggregatedRows);
    }
    return totalSerializedBytes;
  }
}
