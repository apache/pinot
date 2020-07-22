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
import org.apache.pinot.core.io.util.FixedBitIntReaderWriterV2;
import org.apache.pinot.core.io.util.PinotDataBitSet;
import org.apache.pinot.core.io.util.PinotDataBitSetV2;
import org.apache.pinot.core.io.writer.impl.FixedBitSVForwardIndexWriter;
import org.apache.pinot.core.segment.index.readers.forward.FixedBitSVForwardIndexReader;
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

  private File twoBitEncodedFile = new File("/Users/steotia/two-bit-encoded");
  private File fourBitEncodedFile = new File("/Users/steotia/four-bit-encoded");
  private File eightBitEncodedFile = new File("/Users/steotia/eight-bit-encoded");
  private File sixteenBitEncodedFile = new File("/Users/steotia/sixteen-bit-encoded");

  private File rawFile2 = new File("/Users/steotia/raw2");
  private File rawFile4 = new File("/Users/steotia/raw4");
  private File rawFile8 = new File("/Users/steotia/raw8");
  private File rawFile16 = new File("/Users/steotia/raw16");

  static int ROWS = 20_000_000;
  static int NUM_DOCIDS_WITH_GAPS = 9000;

  private int[] rawValues2 = new int[ROWS];
  private int[] rawValues4 = new int[ROWS];
  private int[] rawValues8 = new int[ROWS];
  private int[] rawValues16 = new int[ROWS];

  PinotDataBuffer _pinotDataBuffer2;
  private PinotDataBitSet _bitSet2;
  private PinotDataBitSetV2 _bitSet2Fast;
  private FixedBitSVForwardIndexReader _bit2Reader;
  private FixedBitIntReaderWriterV2 _bit2ReaderFast;

  PinotDataBuffer _pinotDataBuffer4;
  private PinotDataBitSet _bitSet4;
  private PinotDataBitSetV2 _bitSet4Fast;
  private FixedBitSVForwardIndexReader _bit4Reader;
  private FixedBitIntReaderWriterV2 _bit4ReaderFast;

  PinotDataBuffer _pinotDataBuffer8;
  private PinotDataBitSet _bitSet8;
  private PinotDataBitSetV2 _bitSet8Fast;
  private FixedBitSVForwardIndexReader _bit8Reader;
  private FixedBitIntReaderWriterV2 _bit8ReaderFast;

  PinotDataBuffer _pinotDataBuffer16;
  private PinotDataBitSet _bitSet16;
  private PinotDataBitSetV2 _bitSet16Fast;
  private FixedBitSVForwardIndexReader _bit16Reader;
  private FixedBitIntReaderWriterV2 _bit16ReaderFast;

  int[] unpacked = new int[32];
  int[] unalignedUnpacked2Bit = new int[50];
  int[] unalignedUnpacked4Bit = new int[48];
  int[] unalignedUnpacked8Bit = new int[51];
  int[] unalignedUnpacked16Bit = new int[49];

  int[] docIdsWithGaps = new int[NUM_DOCIDS_WITH_GAPS];
  int[] unpackedWithGaps = new int[NUM_DOCIDS_WITH_GAPS];

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    generateRawFile();
    generateBitEncodedFwdIndex();
  }

  @TearDown(Level.Trial)
  public void tearDown()
      throws Exception {
    rawFile2.delete();
    rawFile4.delete();
    rawFile8.delete();
    rawFile16.delete();

    twoBitEncodedFile.delete();
    fourBitEncodedFile.delete();
    eightBitEncodedFile.delete();
    sixteenBitEncodedFile.delete();

    _pinotDataBuffer2.close();
    _pinotDataBuffer4.close();
    _pinotDataBuffer8.close();
    _pinotDataBuffer16.close();
  }

  private void generateRawFile()
      throws IOException {
    BufferedWriter bw2 = new BufferedWriter(new FileWriter(rawFile2));
    BufferedWriter bw4 = new BufferedWriter(new FileWriter(rawFile4));
    BufferedWriter bw8 = new BufferedWriter(new FileWriter(rawFile8));
    BufferedWriter bw16 = new BufferedWriter(new FileWriter(rawFile16));
    Random r = new Random();
    for (int i = 0; i < ROWS; i++) {
      rawValues2[i] = r.nextInt(CARDINALITY_2_BITS);
      rawValues4[i] = r.nextInt(CARDINALITY_4_BITS);
      rawValues8[i] = r.nextInt(CARDINALITY_8_BITS);
      rawValues16[i] = r.nextInt(CARDINALITY_16_BITS);
      bw2.write("" + rawValues2[i]);
      bw2.write("\n");
      bw4.write("" + rawValues4[i]);
      bw4.write("\n");
      bw8.write("" + rawValues8[i]);
      bw8.write("\n");
      bw16.write("" + rawValues16[i]);
      bw16.write("\n");
    }
    bw2.close();
    bw4.close();
    bw8.close();
    bw16.close();
  }

  private void generateBitEncodedFwdIndex()
      throws Exception {
    generateFwdIndexHelper(rawFile2, twoBitEncodedFile, 2);
    generateFwdIndexHelper(rawFile4, fourBitEncodedFile, 4);
    generateFwdIndexHelper(rawFile8, eightBitEncodedFile, 8);
    generateFwdIndexHelper(rawFile16, sixteenBitEncodedFile, 16);

    _pinotDataBuffer2 = PinotDataBuffer.loadBigEndianFile(twoBitEncodedFile);
    _bitSet2Fast = PinotDataBitSetV2.createBitSet(_pinotDataBuffer2, 2);
    _bit2ReaderFast = new FixedBitIntReaderWriterV2(_pinotDataBuffer2, ROWS, 2);
    _bitSet2 = new PinotDataBitSet(_pinotDataBuffer2);
    _bit2Reader = new FixedBitSVForwardIndexReader(_pinotDataBuffer2, ROWS, 2);

    _pinotDataBuffer4 = PinotDataBuffer.loadBigEndianFile(fourBitEncodedFile);
    _bitSet4Fast = PinotDataBitSetV2.createBitSet(_pinotDataBuffer4, 4);
    _bit4ReaderFast = new FixedBitIntReaderWriterV2(_pinotDataBuffer4, ROWS, 4);
    _bitSet4 = new PinotDataBitSet(_pinotDataBuffer4);
    _bit4Reader = new FixedBitSVForwardIndexReader(_pinotDataBuffer4, ROWS, 4);

    _pinotDataBuffer8 = PinotDataBuffer.loadBigEndianFile(eightBitEncodedFile);
    _bitSet8Fast = PinotDataBitSetV2.createBitSet(_pinotDataBuffer8, 8);
    _bit8ReaderFast = new FixedBitIntReaderWriterV2(_pinotDataBuffer8, ROWS, 8);
    _bitSet8 = new PinotDataBitSet(_pinotDataBuffer8);
    _bit8Reader = new FixedBitSVForwardIndexReader(_pinotDataBuffer8, ROWS, 8);

    _pinotDataBuffer16 = PinotDataBuffer.loadBigEndianFile(sixteenBitEncodedFile);
    _bitSet16Fast = PinotDataBitSetV2.createBitSet(_pinotDataBuffer16, 16);
    _bit16ReaderFast = new FixedBitIntReaderWriterV2(_pinotDataBuffer16, ROWS, 16);
    _bitSet16 = new PinotDataBitSet(_pinotDataBuffer16);
    _bit16Reader = new FixedBitSVForwardIndexReader(_pinotDataBuffer16, ROWS, 16);

    docIdsWithGaps[0] = 0;
    for (int i = 1; i < NUM_DOCIDS_WITH_GAPS; i++) {
      docIdsWithGaps[i] = docIdsWithGaps[i - 1] + 2;
    }
  }

  private void generateFwdIndexHelper(File rawFile, File encodedFile, int numBits)
      throws Exception {
    BufferedReader bfr = new BufferedReader(new FileReader(rawFile));
    FixedBitSVForwardIndexWriter forwardIndexWriter = new FixedBitSVForwardIndexWriter(encodedFile, ROWS, numBits);
    String line;
    while ((line = bfr.readLine()) != null) {
      forwardIndexWriter.putDictId(Integer.parseInt(line));
    }
    forwardIndexWriter.close();
  }

  // 2-bit: test single integer decode in a contiguous manner one docId at a time
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void twoBitContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bitSet2.readInt(startIndex, 2);
    }
  }

  // 2-bit: test single integer decode in a contiguous manner one docId at a time
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void twoBitContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bitSet2Fast.readInt(startIndex);
    }
  }

  // 2-bit: test multi integer decode for a range of contiguous docIds
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void twoBitBulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bitSet2.readInt(startIndex, 2, 32, unpacked);
    }
  }

  // 2-bit: test multi integer decode for a range of contiguous docIds
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void twoBitBulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bitSet2Fast.readInt(startIndex, 32, unpacked);
    }
  }

  // 2-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void twoBitBulkWithGaps() {
    _bit2Reader.readDictIds(docIdsWithGaps, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, null);
  }

  // 2-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void twoBitBulkWithGapsFast() {
    _bit2ReaderFast.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 2-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void twoBitBulkContiguousUnaligned() {
    for (int startIndex = 1; startIndex < ROWS - 50; startIndex += 50) {
      _bitSet2.readInt(startIndex, 2, 50, unalignedUnpacked2Bit);
    }
  }

  // 2-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void twoBitBulkContiguousUnalignedFast() {
    for (int startIndex = 1; startIndex < ROWS - 50; startIndex += 50) {
      _bitSet2Fast.readInt(startIndex, 50, unalignedUnpacked2Bit);
    }
  }

  // 4-bit: test single integer decode in a contiguous manner one docId at a time
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void fourBitContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bitSet4.readInt(startIndex, 4);
    }
  }

  // 4-bit: test single integer decode in a contiguous manner one docId at a time
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void fourBitContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bitSet4Fast.readInt(startIndex);
    }
  }

  // 4-bit: test multi integer decode for a range of contiguous docIds
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void fourBitBulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bitSet4.readInt(startIndex, 4, 32, unpacked);
    }
  }

  // 4-bit: test multi integer decode for a range of contiguous docIds
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void fourBitBulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bitSet4Fast.readInt(startIndex, 32, unpacked);
    }
  }

  // 4-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void fourBitBulkWithGaps() {
    _bit4Reader.readDictIds(docIdsWithGaps, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, null);
  }

  // 4-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void fourBitBulkWithGapsFast() {
    _bit4ReaderFast.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 4-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void fourBitBulkContiguousUnaligned() {
    for (int startIndex = 1; startIndex < ROWS - 48; startIndex += 48) {
      _bitSet4.readInt(startIndex, 4, 48, unalignedUnpacked4Bit);
    }
  }

  // 4-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void fourBitBulkContiguousUnalignedFast() {
    for (int startIndex = 1; startIndex < ROWS - 48; startIndex += 48) {
      _bitSet4Fast.readInt(startIndex, 48, unalignedUnpacked8Bit);
    }
  }

  // 8-bit: test single integer decode in a contiguous manner one docId at a time
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void eightBitContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bitSet8.readInt(startIndex, 8);
    }
  }

  // 8-bit: test single integer decode in a contiguous manner one docId at a time
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void eightBitContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bitSet8Fast.readInt(startIndex);
    }
  }

  // 8-bit: test multi integer decode for a range of contiguous docIds
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void eightBitBulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bitSet8.readInt(startIndex, 8, 32, unpacked);
    }
  }

  // 8-bit: test multi integer decode for a range of contiguous docIds
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void eightBitBulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bitSet8Fast.readInt(startIndex, 32, unpacked);
    }
  }

  // 8-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void eightBitBulkWithGaps() {
    _bit8Reader.readDictIds(docIdsWithGaps, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, null);
  }

  // 8-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void eightBitBulkWithGapsFast() {
    _bit8ReaderFast.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 8-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void eightBitBulkContiguousUnaligned() {
    for (int startIndex = 1; startIndex < ROWS - 51; startIndex += 51) {
      _bitSet8.readInt(startIndex, 8, 51, unalignedUnpacked8Bit);
    }
  }

  // 8-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void eightBitBulkContiguousUnalignedFast() {
    for (int startIndex = 1; startIndex < ROWS - 51; startIndex += 51) {
      _bitSet8Fast.readInt(startIndex, 51, unalignedUnpacked8Bit);
    }
  }

  // 16-bit: test single integer decode in a contiguous manner one docId at a time
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sixteenBitContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bitSet16.readInt(startIndex, 16);
    }
  }

  // 16-bit: test single integer decode in a contiguous manner one docId at a time
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sixteenBitContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      _bitSet16Fast.readInt(startIndex);
    }
  }

  // 16-bit: test multi integer decode for a range of contiguous docIds
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sixteenBitBulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bitSet16.readInt(startIndex, 16, 32, unpacked);
    }
  }

  // 2-bit: test multi integer decode for a range of contiguous docIds
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sixteenBitBulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      _bitSet16Fast.readInt(startIndex, 32, unpacked);
    }
  }

  // 16-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sixteenBitBulkWithGaps() {
    _bit16Reader.readDictIds(docIdsWithGaps, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, null);
  }

  // 16-bit: test multi integer decode for a set of monotonically
  // increasing docIds with gaps with optimized API
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sixteenBitBulkWithGapsFast() {
    _bit16ReaderFast.readValues(docIdsWithGaps, 0, NUM_DOCIDS_WITH_GAPS, unpackedWithGaps, 0);
  }

  // 16-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sixteenBitBulkContiguousUnaligned() {
    for (int startIndex = 1; startIndex < ROWS - 49; startIndex += 49) {
      _bitSet16.readInt(startIndex, 16, 51, unalignedUnpacked16Bit);
    }
  }

  // 16-bit: test multi integer decode for a range of contiguous docIds
  // starting at unaligned boundary and spilling over to unaligned boundary
  // with optimized API
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sixteenBitBulkContiguousUnalignedFast() {
    for (int startIndex = 1; startIndex < ROWS - 49; startIndex += 49) {
      _bitSet16Fast.readInt(startIndex, 49, unalignedUnpacked16Bit);
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkPinotDataBitSet.class.getSimpleName()).warmupIterations(1)
            .measurementIterations(3);
    new Runner(opt.build()).run();
  }
}