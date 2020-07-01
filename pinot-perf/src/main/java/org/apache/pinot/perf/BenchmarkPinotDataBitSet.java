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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import org.apache.pinot.core.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.core.io.util.FixedBitIntReaderWriterV2;
import org.apache.pinot.core.io.util.PinotDataBitSet;
import org.apache.pinot.core.io.util.PinotDataBitSetFactory;
import org.apache.pinot.core.io.util.PinotDataBitSetV2;
import org.apache.pinot.core.io.writer.impl.v1.FixedBitSingleValueWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkPinotDataBitSet {

  private static final int CARDINALITY_2_BITS = 3;
  private static final int CARDINALITY_4_BITS = 12;
  private static final int CARDINALITY_8_BITS = 190;
  private static final int CARDINALITY_16_BITS = 40000;
  private static final int CARDINALITY_32_BITS = 100000;

  private File twoBitEncodedFile = new File("/Users/steotia/2-bit-encoded");
  private File fourBitEncodedFile = new File("/Users/steotia/4-bit-encoded");
  private File eightBitEncodedFile = new File("/Users/steotia/8-bit-encoded");
  private File sixteenBitEncodedFile = new File("/Users/steotia/16-bit-encoded");
  private File thirtyTwoBitEncodedFile = new File("/Users/steotia/32-bit-encoded");

  private File rawFile2 = new File("/Users/steotia/raw2");
  private File rawFile4 = new File("/Users/steotia/raw4");
  private File rawFile8 = new File("/Users/steotia/raw8");
  private File rawFile16 = new File("/Users/steotia/raw16");
  private File rawFile32 = new File("/Users/steotia/raw32");

  static int ROWS = 20_000_000;
  static int NUM_DOCIDS_WITH_GAPS = 9000;

  private int[] rawValues2 = new int[ROWS];
  private int[] rawValues4 = new int[ROWS];
  private int[] rawValues8 = new int[ROWS];
  private int[] rawValues16 = new int[ROWS];
  private int[] rawValues32 = new int[ROWS];

  PinotDataBuffer _pinotDataBuffer2;
  private FixedBitIntReaderWriter _bit2Reader;
  private FixedBitIntReaderWriterV2 _bit2ReaderFast;

  PinotDataBuffer _pinotDataBuffer4;
  private FixedBitIntReaderWriter _bit4Reader;
  private FixedBitIntReaderWriterV2 _bit4ReaderFast;

  PinotDataBuffer _pinotDataBuffer8;
  private FixedBitIntReaderWriter _bit8Reader;
  private FixedBitIntReaderWriterV2 _bit8ReaderFast;

  PinotDataBuffer _pinotDataBuffer16;
  private FixedBitIntReaderWriter _bit16Reader;
  private FixedBitIntReaderWriterV2 _bit16ReaderFast;

  PinotDataBuffer _pinotDataBuffer32;
  private FixedBitIntReaderWriter _bit32Reader;
  private FixedBitIntReaderWriterV2 _bit32ReaderFast;

  int[] unpacked = new int[32];
  int[] unalignedUnpacked2Bit = new int[50];
  int[] unalignedUnpacked4Bit = new int[48];
  int[] unalignedUnpacked8Bit = new int[51];
  int[] unalignedUnpacked16Bit = new int[49];

  int[] docIdsWithGaps = new int[NUM_DOCIDS_WITH_GAPS];
  int[] docIdsWithSparseGaps = new int[NUM_DOCIDS_WITH_GAPS];
  int[] unpackedWithGaps = new int[NUM_DOCIDS_WITH_GAPS];

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    generateRawFile();
    generateBitEncodedFwdIndex();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    rawFile2.delete();
    rawFile4.delete();
    rawFile8.delete();
    rawFile16.delete();
    rawFile32.delete();

    twoBitEncodedFile.delete();
    fourBitEncodedFile.delete();
    eightBitEncodedFile.delete();
    sixteenBitEncodedFile.delete();
    thirtyTwoBitEncodedFile.delete();

    _pinotDataBuffer2.close();
    _pinotDataBuffer4.close();
    _pinotDataBuffer8.close();
    _pinotDataBuffer16.close();
    _pinotDataBuffer32.close();
  }

  private void generateRawFile()
      throws IOException {
    BufferedWriter bw2 = new BufferedWriter(new FileWriter(rawFile2));
    BufferedWriter bw4 = new BufferedWriter(new FileWriter(rawFile4));
    BufferedWriter bw8 = new BufferedWriter(new FileWriter(rawFile8));
    BufferedWriter bw16 = new BufferedWriter(new FileWriter(rawFile16));
    BufferedWriter bw32 = new BufferedWriter(new FileWriter(rawFile32));
    Random r = new Random();
    for (int i = 0; i < ROWS; i++) {
      rawValues2[i] =  r.nextInt(CARDINALITY_2_BITS);
      rawValues4[i] =  r.nextInt(CARDINALITY_4_BITS);
      rawValues8[i] =  r.nextInt(CARDINALITY_8_BITS);
      rawValues16[i] =  r.nextInt(CARDINALITY_16_BITS);
      rawValues32[i] =  r.nextInt(CARDINALITY_32_BITS);
      bw2.write("" + rawValues2[i]);
      bw2.write("\n");
      bw4.write("" + rawValues4[i]);
      bw4.write("\n");
      bw8.write("" + rawValues8[i]);
      bw8.write("\n");
      bw16.write("" + rawValues16[i]);
      bw16.write("\n");
      bw32.write("" + rawValues32[i]);
      bw32.write("\n");
    }
    bw2.close();
    bw4.close();
    bw8.close();
    bw16.close();
    bw32.close();
  }

  private void generateBitEncodedFwdIndex() throws Exception {
    generateFwdIndexHelper(rawFile2, twoBitEncodedFile, 2);
    generateFwdIndexHelper(rawFile4, fourBitEncodedFile, 4);
    generateFwdIndexHelper(rawFile8, eightBitEncodedFile, 8);
    generateFwdIndexHelper(rawFile16, sixteenBitEncodedFile, 16);
    generateFwdIndexHelper(rawFile32, thirtyTwoBitEncodedFile, 32);

    _pinotDataBuffer2 = PinotDataBuffer.loadBigEndianFile(twoBitEncodedFile);
    _bit2ReaderFast = new FixedBitIntReaderWriterV2(_pinotDataBuffer2, ROWS, 2);
    _bit2Reader = new FixedBitIntReaderWriter(_pinotDataBuffer2, ROWS, 2);

    _pinotDataBuffer4 = PinotDataBuffer.loadBigEndianFile(fourBitEncodedFile);
    _bit4ReaderFast = new FixedBitIntReaderWriterV2(_pinotDataBuffer4, ROWS, 4);
    _bit4Reader = new FixedBitIntReaderWriter(_pinotDataBuffer4, ROWS, 4);

    _pinotDataBuffer8 = PinotDataBuffer.loadBigEndianFile(eightBitEncodedFile);
    _bit8ReaderFast = new FixedBitIntReaderWriterV2(_pinotDataBuffer8, ROWS, 8);
    _bit8Reader = new FixedBitIntReaderWriter(_pinotDataBuffer8, ROWS, 8);

    _pinotDataBuffer16 = PinotDataBuffer.loadBigEndianFile(sixteenBitEncodedFile);
    _bit16ReaderFast = new FixedBitIntReaderWriterV2(_pinotDataBuffer16, ROWS, 16);
    _bit16Reader = new FixedBitIntReaderWriter(_pinotDataBuffer16, ROWS, 16);

    _pinotDataBuffer32 = PinotDataBuffer.loadBigEndianFile(thirtyTwoBitEncodedFile);
    _bit32ReaderFast = new FixedBitIntReaderWriterV2(_pinotDataBuffer32, ROWS, 32);
    _bit32Reader = new FixedBitIntReaderWriter(_pinotDataBuffer32, ROWS, 32);

    docIdsWithGaps[0] = 0;
    docIdsWithSparseGaps[0] = 0;
    for (int i = 1; i < NUM_DOCIDS_WITH_GAPS; i++) {
      docIdsWithGaps[i] = docIdsWithGaps[i - 1] + 2;
      docIdsWithSparseGaps[i] = docIdsWithSparseGaps[i - 1] + 10;
    }
  }

  private void generateFwdIndexHelper(File rawFile, File encodedFile, int numBits) throws Exception {
    BufferedReader bfr = new BufferedReader(new FileReader(rawFile));
    FixedBitSingleValueWriter fixedBitSingleValueWriter = new FixedBitSingleValueWriter(encodedFile, ROWS, numBits);
    String line;
    int rowId = 0;
    while ((line = bfr.readLine()) != null) {
      fixedBitSingleValueWriter.setInt(rowId++, Integer.parseInt(line));
    }
    fixedBitSingleValueWriter.close();
  }

  // 2-bit: test single integer decode in a contiguous manner one docId at a time
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit2Contiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bit2Reader.readInt(startIndex);
    }
  }

  // 2-bit: test single integer decode in a contiguous manner one docId at a time
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit2ContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bit2ReaderFast.readInt(startIndex);
    }
  }

  // 2-bit: test multi integer decode for a range of contiguous docIds
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit2BulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bit2Reader.readInt(startIndex, 32, unpacked);
    }
  }

  // 2-bit: test multi integer decode for a range of contiguous docIds
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit2BulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bit2ReaderFast.readInt(startIndex, 32, unpacked);
    }
  }

  // 2-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit2BulkWithGaps() {
    _bit2Reader.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 2-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit2BulkWithGapsFast() {
    _bit2ReaderFast.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 2-bit: test multi integer decode for a set of monotonically
  // increasing docIds with sparse gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit2BulkWithSparseGaps() {
    _bit2Reader.readValues(docIdsWithSparseGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 2-bit: test multi integer decode for a set of monotonically
  // increasing docIds with sparse gaps with optimized API
  // this won't use the vectorized bulk read due to sparseness
  // but will still leverage the efficient single read unpacking API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit2BulkWithSparseGapsFast() {
    _bit2ReaderFast.readValues(docIdsWithSparseGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 2-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit2BulkContiguousUnaligned() {
    for (int startIndex = 1; startIndex < ROWS - 50; startIndex += 50) {
      _bit2Reader.readInt(startIndex, 50, unalignedUnpacked2Bit);
    }
  }

  // 2-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit2BulkContiguousUnalignedFast() {
    for (int startIndex = 1; startIndex < ROWS - 50; startIndex += 50) {
      _bit2ReaderFast.readInt(startIndex, 50, unalignedUnpacked2Bit);
    }
  }

  // 4-bit: test single integer decode in a contiguous manner one docId at a time
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit4Contiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bit4Reader.readInt(startIndex);
    }
  }

  // 4-bit: test single integer decode in a contiguous manner one docId at a time
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit4ContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bit4ReaderFast.readInt(startIndex);
    }
  }

  // 4-bit: test multi integer decode for a range of contiguous docIds
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit4BulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bit4Reader.readInt(startIndex, 32, unpacked);
    }
  }

  // 4-bit: test multi integer decode for a range of contiguous docIds
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit4BulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bit4ReaderFast.readInt(startIndex, 32, unpacked);
    }
  }

  // 4-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit4BulkWithGaps() {
    _bit4Reader.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 4-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit4BulkWithGapsFast() {
    _bit4ReaderFast.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 4-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit4BulkWithSparseGaps() {
    _bit4Reader.readValues(docIdsWithSparseGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 4-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  // this won't use the vectorized bulk read due to sparseness
  // but will still leverage the efficient single read unpacking API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit4BulkWithSparseGapsFast() {
    _bit4ReaderFast.readValues(docIdsWithSparseGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 4-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit4BulkContiguousUnaligned() {
    for (int startIndex = 1; startIndex < ROWS - 48; startIndex += 48) {
      _bit4Reader.readInt(startIndex, 48, unalignedUnpacked4Bit);
    }
  }

  // 4-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit4BulkContiguousUnalignedFast() {
    for (int startIndex = 1; startIndex < ROWS - 48; startIndex += 48) {
      _bit4ReaderFast.readInt(startIndex, 48, unalignedUnpacked8Bit);
    }
  }

  // 8-bit: test single integer decode in a contiguous manner one docId at a time
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit8Contiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bit8Reader.readInt(startIndex);
    }
  }

  // 8-bit: test single integer decode in a contiguous manner one docId at a time
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit8ContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bit8ReaderFast.readInt(startIndex);
    }
  }

  // 8-bit: test multi integer decode for a range of contiguous docIds
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit8BulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bit8Reader.readInt(startIndex, 32, unpacked);
    }
  }

  // 8-bit: test multi integer decode for a range of contiguous docIds
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit8BulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bit8ReaderFast.readInt(startIndex, 32, unpacked);
    }
  }

  // 8-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit8BulkWithGaps() {
    _bit8Reader.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 8-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit8BulkWithGapsFast() {
    _bit8ReaderFast.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 8-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit8BulkWithSparseGaps() {
    _bit8Reader.readValues(docIdsWithSparseGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 8-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  // this won't use the vectorized bulk read due to sparseness
  // but will still leverage the efficient single read unpacking API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit8BulkWithSparseGapsFast() {
    _bit8ReaderFast.readValues(docIdsWithSparseGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 8-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit8BulkContiguousUnaligned() {
    for (int startIndex = 1; startIndex < ROWS - 51; startIndex += 51) {
      _bit8Reader.readInt(startIndex, 51, unalignedUnpacked8Bit);
    }
  }

  // 8-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit8BulkContiguousUnalignedFast() {
    for (int startIndex = 1; startIndex < ROWS - 51; startIndex += 51) {
      _bit8ReaderFast.readInt(startIndex, 51, unalignedUnpacked8Bit);
    }
  }

  // 16-bit: test single integer decode in a contiguous manner one docId at a time
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit16Contiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bit16Reader.readInt(startIndex);
    }
  }

  // 16-bit: test single integer decode in a contiguous manner one docId at a time
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit16ContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bit16ReaderFast.readInt(startIndex);
    }
  }

  // 16-bit: test multi integer decode for a range of contiguous docIds
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit16BulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bit16Reader.readInt(startIndex, 32, unpacked);
    }
  }

  // 16-bit: test multi integer decode for a range of contiguous docIds
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit16BulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bit16ReaderFast.readInt(startIndex, 32, unpacked);
    }
  }

  // 16-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit16BulkWithGaps() {
    _bit16Reader.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 16-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit16BulkWithGapsFast() {
    _bit16ReaderFast.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 16-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit16BulkWithSparseGaps() {
    _bit16Reader.readValues(docIdsWithSparseGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 16-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit16BulkWithSparseGapsFast() {
    _bit16ReaderFast.readValues(docIdsWithSparseGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 16-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit16BulkContiguousUnaligned() {
    for (int startIndex = 1; startIndex < ROWS - 49; startIndex += 49) {
      _bit16Reader.readInt(startIndex, 49, unalignedUnpacked16Bit);
    }
  }

  // 16-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit16BulkContiguousUnalignedFast() {
    for (int startIndex = 1; startIndex < ROWS - 49; startIndex += 49) {
      _bit16ReaderFast.readInt(startIndex, 49, unalignedUnpacked16Bit);
    }
  }

  // 32-bit: test single integer decode in a contiguous manner one docId at a time
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit32Contiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bit32Reader.readInt(startIndex);
    }
  }

  // 32-bit: test single integer decode in a contiguous manner one docId at a time
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit32ContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bit32ReaderFast.readInt(startIndex);
    }
  }

  // 32-bit: test multi integer decode for a range of contiguous docIds
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit32BulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bit32Reader.readInt(startIndex, 32, unpacked);
    }
  }

  // 32-bit: test multi integer decode for a range of contiguous docIds
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit32BulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bit32ReaderFast.readInt(startIndex, 32, unpacked);
    }
  }

  // 32-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit32BulkWithGaps() {
    _bit32Reader.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 32-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit32BulkWithGapsFast() {
    _bit32ReaderFast.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 32-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit32BulkWithSparseGaps() {
    _bit16Reader.readValues(docIdsWithSparseGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 32-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void bit32BulkWithSparseGapsFast() {
    _bit16ReaderFast.readValues(docIdsWithSparseGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkPinotDataBitSet.class.getSimpleName())
        .warmupIterations(1).measurementIterations(1);
    new Runner(opt.build()).run();
  }
}