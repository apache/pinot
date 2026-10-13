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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.function.PercentileAggregationFunction;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Compares two ways to merge a serialized PERCENTILE intermediate result (a DoubleArrayList) into a merged list that
/// already holds `size` values:
/// - `deserializeAndAddAll`: reads the incoming list one value at a time, then calls `merge` (`addAll`).
/// - `mergeSerialized`: [PercentileAggregationFunction#mergeSerializedIntermediateResult], which reads the values
///   straight into the merged list (in bulk from PrimitiveArrayOutput.MIN_BULK_VALUES values up).
///
/// Each operation first truncates the merged list back to `size` values, so it never reallocates.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkPercentileSerializedMerge {
  @Param({"1", "4", "16", "64", "1024", "65536"})
  private int _size;

  private PercentileAggregationFunction _function;
  private ByteBuffer _serialized;
  private DoubleArrayList _merged;

  @Setup
  public void setUp() {
    _function = new PercentileAggregationFunction(ExpressionContext.forIdentifier("col"), 50, false);
    double[] values = new Random(42).doubles(_size).toArray();
    _serialized = ByteBuffer.wrap(ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.serialize(new DoubleArrayList(values)));
    _merged = new DoubleArrayList(2 * _size);
    _merged.addElements(0, values);
  }

  @Benchmark
  public DoubleArrayList deserializeAndAddAll() {
    _merged.size(_size);
    ByteBuffer buffer = _serialized.duplicate();
    int numValues = buffer.getInt();
    DoubleArrayList incoming = new DoubleArrayList(numValues);
    for (int i = 0; i < numValues; i++) {
      incoming.add(buffer.getDouble());
    }
    return _function.merge(_merged, incoming);
  }

  @Benchmark
  public DoubleArrayList mergeSerialized() {
    _merged.size(_size);
    CustomObject serialized =
        new CustomObject(ObjectSerDeUtils.ObjectType.DoubleArrayList.getValue(), _serialized.duplicate());
    return _function.mergeSerializedIntermediateResult(_merged, serialized);
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkPercentileSerializedMerge.class.getSimpleName()).build()).run();
  }
}
