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
package org.apache.pinot.index.reader;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.util.Random;
import org.apache.pinot.core.io.reader.ForwardIndexReader;
import org.apache.pinot.core.io.reader.ReaderContext;
import org.apache.pinot.core.io.reader.impl.FixedBitMVForwardIndexReader;
import org.apache.pinot.core.io.writer.ForwardIndexWriter;
import org.apache.pinot.core.io.writer.impl.FixedBitMVForwardIndexWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FixedBitMVForwardIndexTest {

  @Test
  public void testSingleColMultiValue()
      throws Exception {
    testSingleColMultiValue(FixedBitMVForwardIndexWriter.class, FixedBitMVForwardIndexReader.class);
    testSingleColMultiValueWithContext(FixedBitMVForwardIndexWriter.class, FixedBitMVForwardIndexReader.class);
  }

  public void testSingleColMultiValue(Class<? extends ForwardIndexWriter> writerClazz,
      Class<? extends ForwardIndexReader<?>> readerClazz)
      throws Exception {
    Constructor<? extends ForwardIndexWriter> writerClazzConstructor =
        writerClazz.getConstructor(File.class, int.class, int.class, int.class);
    Constructor<? extends ForwardIndexReader<?>> readerClazzConstructor =
        readerClazz.getConstructor(PinotDataBuffer.class, int.class, int.class, int.class);
    int maxBits = 1;
    while (maxBits < 32) {
      final String fileName = getClass().getName() + "_test_single_col_mv_fixed_bit.dat";
      final File f = new File(fileName);
      f.delete();
      int numDocs = 10;
      int maxNumValues = 100;
      final int[][] data = new int[numDocs][];
      final Random r = new Random();
      final int maxValue = (int) Math.pow(2, maxBits);
      int totalNumValues = 0;
      int[] startOffsets = new int[numDocs];
      int[] lengths = new int[numDocs];
      for (int i = 0; i < data.length; i++) {
        final int numValues = r.nextInt(maxNumValues) + 1;
        data[i] = new int[numValues];
        for (int j = 0; j < numValues; j++) {
          data[i][j] = r.nextInt(maxValue);
        }
        startOffsets[i] = totalNumValues;
        lengths[i] = numValues;
        totalNumValues = totalNumValues + numValues;
      }

      ForwardIndexWriter writer = writerClazzConstructor.newInstance(f, numDocs, totalNumValues, maxBits);

      for (int i = 0; i < data.length; i++) {
        writer.setIntArray(i, data[i]);
      }
      writer.close();

      final RandomAccessFile raf = new RandomAccessFile(f, "rw");
      raf.close();

      int[] readValues = new int[maxNumValues];

      // Test heap mode
      try (ForwardIndexReader<?> heapReader = readerClazzConstructor
          .newInstance(PinotDataBuffer.loadBigEndianFile(f), numDocs, totalNumValues, maxBits)) {
        for (int i = 0; i < data.length; i++) {
          final int numValues = heapReader.getIntArray(i, readValues);
          Assert.assertEquals(numValues, data[i].length);
          for (int j = 0; j < numValues; j++) {
            Assert.assertEquals(readValues[j], data[i][j]);
          }
        }
      }

      // Test mmap mode
      try (ForwardIndexReader<?> mmapReader = readerClazzConstructor
          .newInstance(PinotDataBuffer.mapReadOnlyBigEndianFile(f), numDocs, totalNumValues, maxBits)) {
        for (int i = 0; i < data.length; i++) {
          final int numValues = mmapReader.getIntArray(i, readValues);
          Assert.assertEquals(numValues, data[i].length);
          for (int j = 0; j < numValues; j++) {
            Assert.assertEquals(readValues[j], data[i][j]);
          }
        }
      }

      f.delete();
      maxBits = maxBits + 1;
    }
  }

  public void testSingleColMultiValueWithContext(Class<? extends ForwardIndexWriter> writerClazz,
      Class<? extends ForwardIndexReader<?>> readerClazz)
      throws Exception {
    Constructor<? extends ForwardIndexWriter> writerClazzConstructor =
        writerClazz.getConstructor(File.class, int.class, int.class, int.class);
    Constructor<? extends ForwardIndexReader<?>> readerClazzConstructor =
        readerClazz.getConstructor(PinotDataBuffer.class, int.class, int.class, int.class);
    int maxBits = 1;
    while (maxBits < 32) {
      final String fileName = getClass().getName() + "_test_single_col_mv_fixed_bit.dat";
      final File f = new File(fileName);
      f.delete();
      int numDocs = 10;
      int maxNumValues = 100;
      final int[][] data = new int[numDocs][];
      final Random r = new Random();
      final int maxValue = (int) Math.pow(2, maxBits);
      int totalNumValues = 0;
      int[] startOffsets = new int[numDocs];
      int[] lengths = new int[numDocs];
      for (int i = 0; i < data.length; i++) {
        final int numValues = r.nextInt(maxNumValues) + 1;
        data[i] = new int[numValues];
        for (int j = 0; j < numValues; j++) {
          data[i][j] = r.nextInt(maxValue);
        }
        startOffsets[i] = totalNumValues;
        lengths[i] = numValues;
        totalNumValues = totalNumValues + numValues;
      }

      ForwardIndexWriter writer = writerClazzConstructor.newInstance(f, numDocs, totalNumValues, maxBits);

      for (int i = 0; i < data.length; i++) {
        writer.setIntArray(i, data[i]);
      }
      writer.close();

      final RandomAccessFile raf = new RandomAccessFile(f, "rw");
      raf.close();

      int[] readValues = new int[maxNumValues];

      // Test heap mode
      try (ForwardIndexReader heapReader = readerClazzConstructor
          .newInstance(PinotDataBuffer.loadBigEndianFile(f), numDocs, totalNumValues, maxBits)) {
        ReaderContext context = heapReader.createContext();
        for (int i = 0; i < data.length; i++) {
          final int numValues = heapReader.getIntArray(i, readValues, context);
          if (numValues != data[i].length) {
            System.err.println("Failed Expected:" + data[i].length + " Actual:" + numValues);
            int length = heapReader.getIntArray(i, readValues, context);
          }
          Assert.assertEquals(numValues, data[i].length);
          for (int j = 0; j < numValues; j++) {
            Assert.assertEquals(readValues[j], data[i][j]);
          }
        }
      }

      // Test mmap mode
      try (ForwardIndexReader<?> mmapReader = readerClazzConstructor
          .newInstance(PinotDataBuffer.mapReadOnlyBigEndianFile(f), numDocs, totalNumValues, maxBits)) {
        for (int i = 0; i < data.length; i++) {
          final int numValues = mmapReader.getIntArray(i, readValues);
          Assert.assertEquals(numValues, data[i].length);
          for (int j = 0; j < numValues; j++) {
            Assert.assertEquals(readValues[j], data[i][j]);
          }
        }
      }

      f.delete();
      maxBits = maxBits + 1;
    }
  }
}
