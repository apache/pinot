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
import org.apache.pinot.core.io.util.PinotDataBitSet;
import org.apache.pinot.core.io.util.PinotDataBitSetV2;
import org.apache.pinot.core.io.writer.impl.v1.FixedBitSingleValueWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
public class FwdIndexBenchmarkJMH {

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

  private int[] rawValues2 = new int[ROWS];
  private int[] rawValues4 = new int[ROWS];
  private int[] rawValues8 = new int[ROWS];
  private int[] rawValues16 = new int[ROWS];

  private PinotDataBitSet bitSet2;
  private PinotDataBitSetV2 bitSet2Fast;
  private PinotDataBitSet bitSet4;
  private PinotDataBitSetV2 bitSet4Fast;
  private PinotDataBitSet bitSet8;
  private PinotDataBitSetV2 bitSet8Fast;
  private PinotDataBitSet bitSet16;
  private PinotDataBitSetV2 bitSet16Fast;

  int[] unpacked = new int[32];

  @Setup
  public void setUp() throws Exception {
    generateRawFile();
    generateBitEncodedFwdIndex();
  }

  @TearDown
  public void tearDown() {
    rawFile2.delete();
    rawFile4.delete();
    rawFile8.delete();
    rawFile16.delete();
    twoBitEncodedFile.delete();
    fourBitEncodedFile.delete();
    eightBitEncodedFile.delete();
    sixteenBitEncodedFile.delete();
  }

  private void generateRawFile()
      throws IOException {
    BufferedWriter bw2 = new BufferedWriter(new FileWriter(rawFile2));
    BufferedWriter bw4 = new BufferedWriter(new FileWriter(rawFile4));
    BufferedWriter bw8 = new BufferedWriter(new FileWriter(rawFile8));
    BufferedWriter bw16 = new BufferedWriter(new FileWriter(rawFile16));
    Random r = new Random();
    for (int i = 0; i < ROWS; i++) {
      rawValues2[i] =  r.nextInt(CARDINALITY_2_BITS);
      rawValues4[i] =  r.nextInt(CARDINALITY_4_BITS);
      rawValues8[i] =  r.nextInt(CARDINALITY_8_BITS);
      rawValues16[i] =  r.nextInt(CARDINALITY_16_BITS);
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

  private void generateBitEncodedFwdIndex() throws Exception {
    generateFwdIndexHelper(rawFile2, twoBitEncodedFile, 2);
    generateFwdIndexHelper(rawFile4, fourBitEncodedFile, 4);
    generateFwdIndexHelper(rawFile8, eightBitEncodedFile, 8);
    generateFwdIndexHelper(rawFile16, sixteenBitEncodedFile, 16);

    PinotDataBuffer pinotDataBuffer2 = PinotDataBuffer.loadBigEndianFile(twoBitEncodedFile);
    bitSet2Fast = PinotDataBitSetV2.createBitSet(pinotDataBuffer2, 2);
    bitSet2 = new PinotDataBitSet(pinotDataBuffer2);
    PinotDataBuffer pinotDataBuffer4 = PinotDataBuffer.loadBigEndianFile(fourBitEncodedFile);
    bitSet4Fast = PinotDataBitSetV2.createBitSet(pinotDataBuffer4, 4);
    bitSet4 = new PinotDataBitSet(pinotDataBuffer4);
    PinotDataBuffer pinotDataBuffer8 = PinotDataBuffer.loadBigEndianFile(eightBitEncodedFile);
    bitSet8Fast = PinotDataBitSetV2.createBitSet(pinotDataBuffer8, 8);
    bitSet8 = new PinotDataBitSet(pinotDataBuffer8);
    PinotDataBuffer pinotDataBuffer16 = PinotDataBuffer.loadBigEndianFile(sixteenBitEncodedFile);
    bitSet16Fast = PinotDataBitSetV2.createBitSet(pinotDataBuffer16, 16);
    bitSet16 = new PinotDataBitSet(pinotDataBuffer16);
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

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void twoBitContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      bitSet2.readInt(startIndex, 2);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void twoBitContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      bitSet2Fast.readInt(startIndex);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void twoBitBulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      bitSet2.readInt(startIndex, 2, 32, unpacked);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void twoBitBulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      bitSet2Fast.readInt(startIndex, 32, unpacked);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void fourBitContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      bitSet4.readInt(startIndex, 4);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void fourBitContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      bitSet4Fast.readInt(startIndex);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void fourBitBulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      bitSet4.readInt(startIndex, 4, 32, unpacked);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void fourBitBulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      bitSet4Fast.readInt(startIndex, 32, unpacked);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void eightBitContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      bitSet8.readInt(startIndex, 8);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void eightBitContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      bitSet8Fast.readInt(startIndex);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void eightBitBulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      bitSet8.readInt(startIndex, 8, 32, unpacked);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void eightBitBulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      bitSet8Fast.readInt(startIndex, 32, unpacked);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sixteenBitContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      bitSet16.readInt(startIndex, 16);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sixteenBitContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      bitSet16Fast.readInt(startIndex);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sixteenBitBulkContiguous() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      bitSet16.readInt(startIndex, 16, 32, unpacked);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void sixteenBitBulkContiguousFast() {
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      bitSet16Fast.readInt(startIndex, 32, unpacked);
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(FwdIndexBenchmarkJMH.class.getSimpleName())
        .warmupIterations(1).measurementIterations(1);
    new Runner(opt.build()).run();
  }
}