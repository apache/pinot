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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.segment.local.aggregator.DistinctCountCPCSketchValueAggregator;
import org.apache.pinot.segment.local.aggregator.DistinctCountThetaSketchValueAggregator;
import org.apache.pinot.segment.local.aggregator.IntegerTupleSketchValueAggregator;
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


/**
 * No-regression / parity check for the byte[] vs ByteBuffer entry points of the sketch
 * {@link org.apache.pinot.segment.local.aggregator.ValueAggregator} family. Confirms that
 * {@code applyRawValueFromBuffer} is not measurably slower than {@code applyRawValue(byte[])}.
 *
 * <p><b>This benchmark does NOT, and cannot, demonstrate the allocation win.</b> Two reasons:
 * <ul>
 *   <li>The byte[] arm passes a pre-built {@code byte[]} from trial setup, so it does no per-row
 *       allocation here — the allocation that the ByteBuffer path eliminates in production never
 *       happens in this harness. The win is at the forward-index read level (where
 *       {@code getBytes} allocates a fresh array per row but {@code getBytesView} returns a
 *       view), not at this aggregator boundary.</li>
 *   <li>The {@code Union.union} / {@code heapify} work dominates per-row time and is identical
 *       on both arms, so any allocation delta is in the noise. For theta specifically, the byte[]
 *       path already deserializes zero-copy via {@code Sketch.wrap(Memory.wrap(bytes))}, so the
 *       only thing the buffer path saves is the array allocation itself.</li>
 * </ul>
 *
 * <p>The end-to-end win (read path + star-tree wiring + this aggregator) is measured separately:
 * {@code BenchmarkRawForwardIndexReader} for the read path, and a star-tree-build benchmark for
 * the composed path once the {@code BaseSingleTreeBuilder} wiring is in place.
 *
 * <p>The benchmark unions {@code numSketches} pre-serialized sketches into a single accumulator
 * per invocation. The serialized sketch bytes are prepared once at trial setup.
 */
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class BenchmarkValueAggregatorBufferApi {

  @Param({"100", "1000", "10000"})
  private int _numSketches;

  // Sketch hash cardinality per sketch. Kept small relative to nominal entries so sketches are
  // in exact mode — the wrap vs heapify cost asymmetry shows clearest before the hashtable spills.
  private static final int _ENTRIES_PER_SKETCH = 1000;

  // Theta nominal entries (default Helix is 4096 = 2^12).
  private static final int _THETA_NOMINAL_ENTRIES = 4096;

  // CPC lgK (default Helix is 12 → K = 4096).
  private static final int _CPC_LG_K = 12;

  // Tuple sketch nominal entries (lgK = 4 → 16 entries; matches existing test fixture).
  private static final int _TUPLE_NOMINAL_ENTRIES = 16;

  private byte[][] _thetaBytes;
  private ByteBuffer[] _thetaBuffers;
  private byte[][] _cpcBytes;
  private ByteBuffer[] _cpcBuffers;
  private byte[][] _tupleBytes;
  private ByteBuffer[] _tupleBuffers;

  private DistinctCountThetaSketchValueAggregator _thetaAgg;
  private DistinctCountCPCSketchValueAggregator _cpcAgg;
  private IntegerTupleSketchValueAggregator _tupleAgg;

  @Setup(Level.Trial)
  public void setUp() {
    _thetaAgg = new DistinctCountThetaSketchValueAggregator(Collections.emptyList());
    _cpcAgg = new DistinctCountCPCSketchValueAggregator(Collections.emptyList());
    _tupleAgg = new IntegerTupleSketchValueAggregator(Collections.emptyList(), IntegerSummary.Mode.Sum);

    _thetaBytes = new byte[_numSketches][];
    _thetaBuffers = new ByteBuffer[_numSketches];
    _cpcBytes = new byte[_numSketches][];
    _cpcBuffers = new ByteBuffer[_numSketches];
    _tupleBytes = new byte[_numSketches][];
    _tupleBuffers = new ByteBuffer[_numSketches];

    for (int s = 0; s < _numSketches; s++) {
      int baseHash = s * _ENTRIES_PER_SKETCH;

      UpdateSketch theta = Sketches.updateSketchBuilder().setNominalEntries(_THETA_NOMINAL_ENTRIES).build();
      IntStream.range(baseHash, baseHash + _ENTRIES_PER_SKETCH).forEach(theta::update);
      _thetaBytes[s] = _thetaAgg.serializeAggregatedValue(theta.compact());
      _thetaBuffers[s] = ByteBuffer.wrap(_thetaBytes[s]);

      CpcSketch cpc = new CpcSketch(_CPC_LG_K);
      IntStream.range(baseHash, baseHash + _ENTRIES_PER_SKETCH).forEach(cpc::update);
      _cpcBytes[s] = _cpcAgg.serializeAggregatedValue(cpc);
      _cpcBuffers[s] = ByteBuffer.wrap(_cpcBytes[s]);

      IntegerSketch tuple = new IntegerSketch(_TUPLE_NOMINAL_ENTRIES, IntegerSummary.Mode.Sum);
      for (int v = baseHash; v < baseHash + _ENTRIES_PER_SKETCH; v++) {
        tuple.update(Integer.toString(v), 1);
      }
      _tupleBytes[s] = _tupleAgg.serializeAggregatedValue(tuple);
      _tupleBuffers[s] = ByteBuffer.wrap(_tupleBytes[s]);
    }
  }

  // -- Theta ---------------------------------------------------------------------------------

  @Benchmark
  public Object thetaApplyRawByteArray() {
    Object acc = null;
    for (int i = 0; i < _numSketches; i++) {
      acc = _thetaAgg.applyRawValue(acc, _thetaBytes[i]);
    }
    return acc;
  }

  @Benchmark
  public Object thetaApplyRawByteBuffer() {
    Object acc = null;
    for (int i = 0; i < _numSketches; i++) {
      // duplicate() resets position/limit each call. ByteBuffer state is otherwise mutated by
      // the union path (Memory.wrap honours the buffer's position cursor on read).
      acc = _thetaAgg.applyRawValueFromBuffer(acc, _thetaBuffers[i].duplicate());
    }
    return acc;
  }

  // -- CPC -----------------------------------------------------------------------------------

  @Benchmark
  public Object cpcApplyRawByteArray() {
    Object acc = null;
    for (int i = 0; i < _numSketches; i++) {
      acc = _cpcAgg.applyRawValue(acc, _cpcBytes[i]);
    }
    return acc;
  }

  @Benchmark
  public Object cpcApplyRawByteBuffer() {
    Object acc = null;
    for (int i = 0; i < _numSketches; i++) {
      acc = _cpcAgg.applyRawValueFromBuffer(acc, _cpcBuffers[i].duplicate());
    }
    return acc;
  }

  // -- Tuple ---------------------------------------------------------------------------------

  @Benchmark
  public Object tupleApplyRawByteArray() {
    Object acc = null;
    for (int i = 0; i < _numSketches; i++) {
      acc = _tupleAgg.applyRawValue(acc, _tupleBytes[i]);
    }
    return acc;
  }

  @Benchmark
  public Object tupleApplyRawByteBuffer() {
    Object acc = null;
    for (int i = 0; i < _numSketches; i++) {
      acc = _tupleAgg.applyRawValueFromBuffer(acc, _tupleBuffers[i].duplicate());
    }
    return acc;
  }
}
