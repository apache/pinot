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

import java.io.File;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.dociditerators.SVScanDocIdIterator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitSVForwardIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitSVForwardIndexReaderV2;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkScanDocIdIterators {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkScanDocIdIterators");

  @Param("10000000")
  int _numDocs;

  // how selective the bitmap is
  @Param({"1", "2"})
  private int _bitmapQuantile;

  // how selective the predicate evaluator is
  @Param({"1", "2"})
  private int _thresholdQuantile;

  @Param("42")
  long _seed;

  @Param({"UNIFORM(0,10000000)"})
  String _distribution;

  private DummyPredicateEvaluator _predicateEvaluator;
  private FixedBitSVForwardIndexReaderV2 _readerV2;
  private ImmutableRoaringBitmap _bitmap;
  private PinotDataBuffer _dataBuffer;

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    FileUtils.forceMkdir(INDEX_DIR);
    File indexFile = new File(INDEX_DIR, "index-file");
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    LongSupplier supplier = Distribution.createLongSupplier(_seed, _distribution);
    int[] values = new int[_numDocs];
    int max = Integer.MIN_VALUE;
    for (int i = 0; i < values.length; i++) {
      values[i] = (int) supplier.getAsLong();
      max = Math.max(values[i], max);
    }
    int numBits = 32 - Integer.numberOfLeadingZeros(max);
    int[] sorted = Arrays.copyOf(values, values.length);
    Arrays.sort(sorted);
    try (FixedBitSVForwardIndexWriter indexWriter = new FixedBitSVForwardIndexWriter(indexFile, _numDocs, numBits)) {
      for (int i = 0; i < _numDocs; i++) {
        indexWriter.putDictId(values[i]);
      }
    }
    _dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    for (int i = 0; i < values.length; i++) {
      if (values[i] < sorted[_bitmapQuantile * sorted.length / 10]) {
        writer.add(i);
      }
    }
    _bitmap = writer.get();
    _predicateEvaluator = new DummyPredicateEvaluator(sorted[_thresholdQuantile * sorted.length / 10]);
    _readerV2 = new FixedBitSVForwardIndexReaderV2(_dataBuffer, values.length, numBits);
  }

  @TearDown(Level.Trial)
  public void tearDown()
      throws Exception {
    _dataBuffer.close();
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Benchmark
  public MutableRoaringBitmap benchmarkSVLong() {
    return new SVScanDocIdIterator(_predicateEvaluator, _readerV2, _numDocs).applyAnd(_bitmap);
  }

  public static class DummyPredicateEvaluator implements PredicateEvaluator {

    private final int _threshold;

    public DummyPredicateEvaluator(int threshold) {
      _threshold = threshold;
    }

    @Override
    public Predicate getPredicate() {
      return null;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return null;
    }

    @Override
    public boolean isDictionaryBased() {
      return true;
    }

    @Override
    public FieldSpec.DataType getDataType() {
      return null;
    }

    @Override
    public boolean isExclusive() {
      return false;
    }

    @Override
    public boolean isAlwaysTrue() {
      return false;
    }

    @Override
    public boolean isAlwaysFalse() {
      return false;
    }

    @Override
    public boolean applySV(int value) {
      return value < _threshold;
    }

    @Override
    public boolean applyMV(int[] values, int length) {
      return false;
    }

    @Override
    public int getNumMatchingDictIds() {
      return 0;
    }

    @Override
    public int[] getMatchingDictIds() {
      return new int[0];
    }

    @Override
    public int getNumNonMatchingDictIds() {
      return 0;
    }

    @Override
    public int[] getNonMatchingDictIds() {
      return new int[0];
    }

    @Override
    public boolean applySV(long value) {
      return false;
    }

    @Override
    public boolean applyMV(long[] values, int length) {
      return false;
    }

    @Override
    public boolean applySV(float value) {
      return false;
    }

    @Override
    public boolean applyMV(float[] values, int length) {
      return false;
    }

    @Override
    public boolean applySV(double value) {
      return false;
    }

    @Override
    public boolean applySV(BigDecimal value) {
      return false;
    }

    @Override
    public boolean applyMV(double[] values, int length) {
      return false;
    }

    @Override
    public boolean applySV(String value) {
      return false;
    }

    @Override
    public boolean applyMV(String[] values, int length) {
      return false;
    }

    @Override
    public boolean applySV(byte[] value) {
      return false;
    }

    @Override
    public boolean applyMV(byte[][] values, int length) {
      return false;
    }
  }
}
