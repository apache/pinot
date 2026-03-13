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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.segment.processing.aggregator.DistinctCountCPCSketchAggregator;
import org.apache.pinot.core.segment.processing.aggregator.DistinctCountThetaSketchAggregator;
import org.apache.pinot.core.segment.processing.aggregator.IntegerTupleSketchAggregator;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Benchmark comparing pairwise vs batch aggregation for sketch ValueAggregators.
 *
 * <p>Measures the performance improvement from batch aggregation which:
 * <ul>
 *   <li>Reduces serialization overhead from O(N-1) to O(1)</li>
 *   <li>Uses zero-copy sketch wrapping via Memory.wrap()</li>
 *   <li>Applies theta-based sorting for early termination (Theta/Tuple sketches)</li>
 * </ul>
 *
 * <p>Run with: java -jar pinot-perf/target/benchmarks.jar BenchmarkSketchBatchAggregation
 */
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class BenchmarkSketchBatchAggregation {

  private static final int SKETCH_LG_K = 12;  // 4096 nominal entries
  private static final int SKETCH_CARDINALITY = 1000;
  private static final Map<String, String> EMPTY_PARAMS = Collections.emptyMap();

  @Param({"10", "50", "100", "500"})
  private int _numSketchesPerKey;

  // Theta sketch data
  private DistinctCountThetaSketchAggregator _thetaAggregator;
  private List<Object> _thetaSketches;

  // Tuple sketch data
  private IntegerTupleSketchAggregator _tupleAggregator;
  private List<Object> _tupleSketches;

  // CPC sketch data
  private DistinctCountCPCSketchAggregator _cpcAggregator;
  private List<Object> _cpcSketches;

  public static void main(String[] args)
      throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(BenchmarkSketchBatchAggregation.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setUp() {
    Random random = new Random(42);

    // Initialize Theta sketch aggregator and data
    _thetaAggregator = new DistinctCountThetaSketchAggregator();
    _thetaSketches = new ArrayList<>(_numSketchesPerKey);
    for (int i = 0; i < _numSketchesPerKey; i++) {
      UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(1 << SKETCH_LG_K).build();
      int offset = random.nextInt(SKETCH_CARDINALITY * 10);
      for (int j = 0; j < SKETCH_CARDINALITY; j++) {
        sketch.update(offset + j);
      }
      _thetaSketches.add(ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(sketch.compact()));
    }

    // Initialize Tuple sketch aggregator and data
    _tupleAggregator = new IntegerTupleSketchAggregator(IntegerSummary.Mode.Sum);
    _tupleSketches = new ArrayList<>(_numSketchesPerKey);
    for (int i = 0; i < _numSketchesPerKey; i++) {
      IntegerSketch sketch = new IntegerSketch(SKETCH_LG_K, IntegerSummary.Mode.Sum);
      int offset = random.nextInt(SKETCH_CARDINALITY * 10);
      for (int j = 0; j < SKETCH_CARDINALITY; j++) {
        sketch.update(offset + j, 1);
      }
      _tupleSketches.add(ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(sketch.compact()));
    }

    // Initialize CPC sketch aggregator and data
    _cpcAggregator = new DistinctCountCPCSketchAggregator();
    _cpcSketches = new ArrayList<>(_numSketchesPerKey);
    for (int i = 0; i < _numSketchesPerKey; i++) {
      CpcSketch sketch = new CpcSketch(SKETCH_LG_K);
      int offset = random.nextInt(SKETCH_CARDINALITY * 10);
      for (int j = 0; j < SKETCH_CARDINALITY; j++) {
        sketch.update(offset + j);
      }
      _cpcSketches.add(ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(sketch));
    }
  }

  // ==================== Theta Sketch Benchmarks ====================

  @Benchmark
  public void thetaPairwise(Blackhole bh) {
    Object result = _thetaSketches.get(0);
    for (int i = 1; i < _thetaSketches.size(); i++) {
      result = _thetaAggregator.aggregate(result, _thetaSketches.get(i), EMPTY_PARAMS);
    }
    bh.consume(result);
  }

  @Benchmark
  public void thetaBatch(Blackhole bh) {
    bh.consume(_thetaAggregator.aggregateBatch(_thetaSketches, EMPTY_PARAMS));
  }

  // ==================== Tuple Sketch Benchmarks ====================

  @Benchmark
  public void tuplePairwise(Blackhole bh) {
    Object result = _tupleSketches.get(0);
    for (int i = 1; i < _tupleSketches.size(); i++) {
      result = _tupleAggregator.aggregate(result, _tupleSketches.get(i), EMPTY_PARAMS);
    }
    bh.consume(result);
  }

  @Benchmark
  public void tupleBatch(Blackhole bh) {
    bh.consume(_tupleAggregator.aggregateBatch(_tupleSketches, EMPTY_PARAMS));
  }

  // ==================== CPC Sketch Benchmarks ====================

  @Benchmark
  public void cpcPairwise(Blackhole bh) {
    Object result = _cpcSketches.get(0);
    for (int i = 1; i < _cpcSketches.size(); i++) {
      result = _cpcAggregator.aggregate(result, _cpcSketches.get(i), EMPTY_PARAMS);
    }
    bh.consume(result);
  }

  @Benchmark
  public void cpcBatch(Blackhole bh) {
    bh.consume(_cpcAggregator.aggregateBatch(_cpcSketches, EMPTY_PARAMS));
  }
}
