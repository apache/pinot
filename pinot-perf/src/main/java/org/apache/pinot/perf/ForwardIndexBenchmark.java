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

import com.google.common.base.Stopwatch;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import me.lemire.integercompression.BitPacking;
import org.apache.commons.math.util.MathUtils;
import org.apache.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import org.apache.pinot.core.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.core.io.util.FixedBitIntReaderWriterV2;
import org.apache.pinot.core.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.core.io.util.PinotDataBitSet;
import org.apache.pinot.core.io.writer.impl.v1.FixedBitSingleValueWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;

public class ForwardIndexBenchmark {

  static int ROWS = 4_000_000;
  static int MAX_VALUE = 11;
  static int NUM_BITS = PinotDataBitSet.getNumBitsPerValue(MAX_VALUE);
  static File rawFile = new File("/Users/steotia/fwd-index.test");
  static File pinotOutFile = new File(rawFile.getAbsolutePath() + ".pinot.fwd");
  static File bitPackedFile = new File(rawFile.getAbsolutePath() + ".fast.fwd");
  static int[] rawValues = new int[ROWS];

  static {
    rawFile.delete();
    pinotOutFile.delete();
    bitPackedFile.delete();
  }

  static void generateRawFile()
      throws IOException {

    rawFile.delete();
    BufferedWriter bw = new BufferedWriter(new FileWriter(rawFile));
    Random r = new Random();
    for (int i = 0; i < ROWS; i++) {
      rawValues[i] =  r.nextInt(MAX_VALUE);
      bw.write("" + rawValues[i]);
      bw.write("\n");
    }
    bw.close();
  }

  static void generatePinotFwdIndex()
      throws Exception {
    BufferedReader bfr = new BufferedReader(new FileReader(rawFile));
    FixedBitSingleValueWriter fixedBitSingleValueWriter = new FixedBitSingleValueWriter(pinotOutFile, ROWS, NUM_BITS);
    String line;
    int rowId = 0;
    while ((line = bfr.readLine()) != null) {
      fixedBitSingleValueWriter.setInt(rowId++, Integer.parseInt(line));
    }
    fixedBitSingleValueWriter.close();
    System.out.println("pinotOutFile.length = " + pinotOutFile.length());
  }

  static void generatePFORFwdIndex()
      throws Exception {
    BufferedReader bfr = new BufferedReader(new FileReader(rawFile));
    String line;
    int rowId = 0;
    int[] data = new int[ROWS];
    while ((line = bfr.readLine()) != null) {
      data[rowId++] = Integer.parseInt(line);
    }
    int bits = MathUtils.lcm(32, NUM_BITS);
    int inputSize = 32;
    int outputSize = 32 * NUM_BITS / 32;
    int[] raw = new int[inputSize];
    int[] bitPacked = new int[outputSize];

    int totalNum = (NUM_BITS * ROWS + 31) / Integer.SIZE;

    PinotDataBuffer pinotDataBuffer = PinotDataBuffer
        .mapFile(bitPackedFile, false, 0, (long) totalNum * Integer.BYTES, ByteOrder.BIG_ENDIAN, "bitpacking");

    FixedByteValueReaderWriter readerWriter = new FixedByteValueReaderWriter(pinotDataBuffer);
    int counter = 0;
    int outputCount = 0;
    for (int i = 0; i < data.length; i++) {
      raw[counter] = data[i];
      if (counter == raw.length - 1 || i == data.length - 1) {
        BitPacking.fastpack(raw, 0, bitPacked, 0, NUM_BITS);
        for (int j = 0; j < outputSize; j++) {
          readerWriter.writeInt(outputCount++, bitPacked[j]);
        }
        Arrays.fill(bitPacked, 0);
        counter = 0;
      } else {
        counter = counter + 1;
      }
    }
    readerWriter.close();
    System.out.println("bitPackedFile.length = " + bitPackedFile.length());
  }

  private static void compareFormats() throws Exception {
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.loadBigEndianFile(pinotOutFile);
    FileChannel fileChannel = new RandomAccessFile(bitPackedFile, "r").getChannel();
    ByteBuffer buffer =
        fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, bitPackedFile.length()).order(ByteOrder.BIG_ENDIAN);
    long length = pinotDataBuffer.size();
    for (long i = 0; i < length; i += 4) {
      int val1 = pinotDataBuffer.getInt(i);
      int val2 = buffer.getInt((int)i);
      int v1 = val1 >>> 16;
      int v2 = val1 & (-1 >>> 16);
      int v3 = val2 >>> 16;
      int v4 = val2 & (-1 >>> 16);
      //35 -98 71 111 -- pinot buffer
      //71 111 35 -98 -- pfor buffer
      if (v1 != v4 || v2 != v3) {
        throw new IllegalStateException("detected different bytes at : " + i);
      }
    }
  }

  static void readRawFile()
      throws IOException {
    BufferedReader bfr = new BufferedReader(new FileReader(rawFile));
    String line;
    int rowId = 0;
    while ((line = bfr.readLine()) != null) {
      rawValues[rowId++] = Integer.parseInt(line);
    }
  }

  static void readPinotFwdIndexSequentialContiguous()
      throws IOException {
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.loadBigEndianFile(pinotOutFile);
    FixedBitIntReaderWriter reader = new FixedBitIntReaderWriter(pinotDataBuffer, ROWS, NUM_BITS);
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    stopwatch.start();
    // sequentially unpack 1 integer at a time
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      reader.readInt(startIndex);
    }
    stopwatch.stop();
    System.out.println("pinot took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
  }

  static void readPinotFwdIndexSequentialContiguousBulk()
      throws IOException {
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.loadBigEndianFile(pinotOutFile);
    FixedBitIntReaderWriter reader = new FixedBitIntReaderWriter(pinotDataBuffer, ROWS, NUM_BITS);
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    int[] unpacked = new int[32];
    stopwatch.start();
    // sequentially unpack 32 integers at a time
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      reader.readInt(startIndex, 32, unpacked);
      //checkUnpackedValues(startIndex, unpacked);
    }
    stopwatch.stop();
    System.out.println("pinot took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
  }

  static void readFastPinotFwdIndexSequentialContiguous()
      throws IOException {
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.loadBigEndianFile(pinotOutFile);
    FixedBitIntReaderWriterV2 reader = new FixedBitIntReaderWriterV2(pinotDataBuffer, ROWS, NUM_BITS);
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    stopwatch.start();
    // sequentially unpack 1 integer at a time
    for (int startIndex = 0; startIndex < ROWS; startIndex++) {
      reader.readInt(startIndex);
    }
    stopwatch.stop();
    System.out.println("fast pinot took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
  }

  static void readFastPinotFwdIndexSequentialContiguousBulk()
      throws IOException {
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.loadBigEndianFile(pinotOutFile);
    FixedBitIntReaderWriterV2 reader = new FixedBitIntReaderWriterV2(pinotDataBuffer, ROWS, NUM_BITS);
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    int[] unpacked = new int[32];
    stopwatch.start();
    // sequentially unpack 32 integers at a time
    for (int startIndex = 0; startIndex < ROWS; startIndex += 32) {
      reader.readInt(startIndex, 32, unpacked);
      //checkUnpackedValues(startIndex, unpacked);
    }
    stopwatch.stop();
    System.out.println("fast pinot took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
  }

  private static void checkUnpackedValues(int startIndex, int[] out) {
    for (int i = 0; i < 32; i++) {
      if (out[i] != rawValues[startIndex + i]) {
        throw new IllegalStateException("incorrect data: startIndex:" +startIndex + " i:" + i + " actual:"+out[i] + " expected:"+rawValues[startIndex +i]);
      }
    }
  }

  static void readPFORFwdIndex()
      throws IOException {
    FileChannel fileChannel = new RandomAccessFile(bitPackedFile, "r").getChannel();
    ByteBuffer buffer =
        fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, bitPackedFile.length());
    int[] compressed = new int[NUM_BITS];
    int[] unpacked = new int[32];
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    stopwatch.start();
    // sequentially unpack 32 integers at a time
    for (int i = 0; i < ROWS; i += 32) {
      for (int j = 0; j < compressed.length; j++) {
        compressed[j] = buffer.getInt();
      }
      BitPacking.fastunpack(compressed, 0, unpacked, 0, NUM_BITS);
      //checkUnpackedValues(i, unpacked);
    }
    stopwatch.stop();
    System.out.println("PFOR took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS)+ " ms");
  }

  public static void main(String[] args)
      throws Exception {
    System.out.println("ROWS = " + ROWS);
    System.out.println("NUM_BITS = " + NUM_BITS);
    generateRawFile();
    generatePinotFwdIndex();
    generatePFORFwdIndex();
    //compareFormats();
    readRawFile();
    readPinotFwdIndexSequentialContiguous();
    readFastPinotFwdIndexSequentialContiguous();
    readPinotFwdIndexSequentialContiguousBulk();
    readFastPinotFwdIndexSequentialContiguousBulk();
    readPFORFwdIndex();
  }
}