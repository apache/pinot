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
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.theta.CompactThetaSketch;
import org.apache.datasketches.theta.ThetaSetOperationBuilder;
import org.apache.datasketches.theta.ThetaUnion;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.tuple.TupleUnion;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.apache.datasketches.tuple.aninteger.IntegerTupleSketch;
import org.apache.pinot.core.common.ObjectSerDeUtils;
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


/// Benchmarks the sketch [ObjectSerDeUtils.ObjectSerDe] deserialize + merge hot paths that run once per row on
/// serialized-sketch BYTES columns during aggregation. Used to compare the datasketches-java memory access layer
/// across library upgrades (e.g. 6.2.0 Unsafe-based reads vs 9.0.0 java.lang.foreign MemorySegment reads).
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
public class BenchmarkSketchSerDe {
  private static final int NUM_SKETCHES = 64;
  private static final int NOMINAL_ENTRIES = 4096;
  private static final int LG_K = 12;

  @Param({"1000", "50000"})
  private int _valuesPerSketch;

  private byte[][] _thetaBytes;
  private byte[][] _tupleBytes;
  private byte[][] _kllBytes;
  private byte[][] _cpcBytes;
  private CompactThetaSketch _thetaSketch;

  @Setup
  public void setUp() {
    _thetaBytes = new byte[NUM_SKETCHES][];
    _tupleBytes = new byte[NUM_SKETCHES][];
    _kllBytes = new byte[NUM_SKETCHES][];
    _cpcBytes = new byte[NUM_SKETCHES][];
    for (int i = 0; i < NUM_SKETCHES; i++) {
      UpdatableThetaSketch theta = UpdatableThetaSketch.builder().setNominalEntries(NOMINAL_ENTRIES).build();
      IntegerTupleSketch tuple = new IntegerTupleSketch(LG_K, IntegerSummary.Mode.Sum);
      KllDoublesSketch kll = KllDoublesSketch.newHeapInstance();
      CpcSketch cpc = new CpcSketch(LG_K);
      // Overlapping key ranges so unions do real merge work
      long base = (long) i * _valuesPerSketch / 2;
      for (int j = 0; j < _valuesPerSketch; j++) {
        long key = base + j;
        theta.update(key);
        tuple.update(key, 1);
        kll.update(key);
        cpc.update(key);
      }
      _thetaBytes[i] = theta.compact().toByteArray();
      _tupleBytes[i] = tuple.compact().toByteArray();
      _kllBytes[i] = kll.toByteArray();
      _cpcBytes[i] = cpc.toByteArray();
    }
    UpdatableThetaSketch serializeSource = UpdatableThetaSketch.builder().setNominalEntries(NOMINAL_ENTRIES).build();
    for (int j = 0; j < _valuesPerSketch; j++) {
      serializeSource.update(j);
    }
    _thetaSketch = serializeSource.compact();
  }

  @Benchmark
  public double thetaDeserializeAndUnion() {
    ThetaUnion union = new ThetaSetOperationBuilder().setNominalEntries(NOMINAL_ENTRIES).buildUnion();
    for (byte[] bytes : _thetaBytes) {
      union.union(ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(bytes));
    }
    return union.getResult().getEstimate();
  }

  @Benchmark
  public double tupleDeserializeAndUnion() {
    TupleUnion<IntegerSummary> union =
        new TupleUnion<>(NOMINAL_ENTRIES, new IntegerSummarySetOperations(IntegerSummary.Mode.Sum,
            IntegerSummary.Mode.Sum));
    for (byte[] bytes : _tupleBytes) {
      union.union(ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(bytes));
    }
    return union.getResult().getEstimate();
  }

  @Benchmark
  public double kllDeserializeAndMerge() {
    KllDoublesSketch merged = KllDoublesSketch.newHeapInstance();
    for (byte[] bytes : _kllBytes) {
      merged.merge(ObjectSerDeUtils.KLL_SKETCH_SER_DE.deserialize(bytes));
    }
    return merged.getQuantile(0.5);
  }

  @Benchmark
  public double cpcDeserializeAndUnion() {
    CpcUnion union = new CpcUnion(LG_K);
    for (byte[] bytes : _cpcBytes) {
      union.update(ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(bytes));
    }
    return union.getResult().getEstimate();
  }

  @Benchmark
  public int thetaSerialize() {
    return ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(_thetaSketch).length;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkSketchSerDe.class.getSimpleName()).build()).run();
  }
}
