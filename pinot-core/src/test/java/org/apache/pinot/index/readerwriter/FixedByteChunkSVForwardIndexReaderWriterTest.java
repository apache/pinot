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
package org.apache.pinot.index.readerwriter;

import java.io.File;
import java.net.URL;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.io.compression.ChunkCompressorFactory;
import org.apache.pinot.core.io.reader.impl.ChunkReaderContext;
import org.apache.pinot.core.io.reader.impl.FixedByteChunkSVForwardIndexReader;
import org.apache.pinot.core.io.writer.impl.BaseChunkSVForwardIndexWriter;
import org.apache.pinot.core.io.writer.impl.FixedByteChunkSVForwardIndexWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link FixedByteChunkSVForwardIndexReader} and {@link FixedByteChunkSVForwardIndexWriter} classes.
 *
 * This test writes {@link #NUM_VALUES} using {@link FixedByteChunkSVForwardIndexWriter}. It then reads
 * the values using {@link FixedByteChunkSVForwardIndexReader}, and asserts that what was written is the same as
 * what was read in.
 *
 * Number of docs and docs per chunk are chosen to generate complete as well partial chunks.
 *
 */
public class FixedByteChunkSVForwardIndexReaderWriterTest {
  private static final int NUM_VALUES = 10009;
  private static final int NUM_DOCS_PER_CHUNK = 5003;
  private static final String TEST_FILE = System.getProperty("java.io.tmpdir") + File.separator + "FixedByteSVRTest";
  private static final Random RANDOM = new Random();

  @Test
  public void testWithCompression()
      throws Exception {
    ChunkCompressorFactory.CompressionType compressionType = ChunkCompressorFactory.CompressionType.SNAPPY;
    testInt(compressionType);
    testLong(compressionType);
    testFloat(compressionType);
    testDouble(compressionType);
  }

  @Test
  public void testWithoutCompression()
      throws Exception {
    ChunkCompressorFactory.CompressionType compressionType = ChunkCompressorFactory.CompressionType.PASS_THROUGH;
    testInt(compressionType);
    testLong(compressionType);
    testFloat(compressionType);
    testDouble(compressionType);
  }

  public void testInt(ChunkCompressorFactory.CompressionType compressionType)
      throws Exception {
    int[] expected = new int[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = RANDOM.nextInt();
    }

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    FixedByteChunkSVForwardIndexWriter fourByteOffsetWriter =
        new FixedByteChunkSVForwardIndexWriter(outFileFourByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Integer.BYTES, BaseChunkSVForwardIndexWriter.DEFAULT_VERSION);
    FixedByteChunkSVForwardIndexWriter eightByteOffsetWriter =
        new FixedByteChunkSVForwardIndexWriter(outFileEightByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Integer.BYTES, BaseChunkSVForwardIndexWriter.CURRENT_VERSION);

    for (int i = 0; i < NUM_VALUES; i++) {
      fourByteOffsetWriter.setInt(i, expected[i]);
      eightByteOffsetWriter.setInt(i, expected[i]);
    }

    fourByteOffsetWriter.close();
    eightByteOffsetWriter.close();

    try (FixedByteChunkSVForwardIndexReader fourByteOffsetReader = new FixedByteChunkSVForwardIndexReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), DataType.INT);
        FixedByteChunkSVForwardIndexReader eightByteOffsetReader = new FixedByteChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), DataType.INT)) {

      ChunkReaderContext context1 = fourByteOffsetReader.createContext();
      ChunkReaderContext context2 = eightByteOffsetReader.createContext();

      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getInt(i, context1), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getInt(i, context2), expected[i]);
        if (compressionType.equals(ChunkCompressorFactory.CompressionType.PASS_THROUGH)) {
          Assert.assertEquals(fourByteOffsetReader.getInt(i), expected[i]);
          Assert.assertEquals(eightByteOffsetReader.getInt(i), expected[i]);
        }
      }
    }

    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);
  }

  public void testLong(ChunkCompressorFactory.CompressionType compressionType)
      throws Exception {
    long[] expected = new long[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = RANDOM.nextLong();
    }

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    FixedByteChunkSVForwardIndexWriter fourByteOffsetWriter =
        new FixedByteChunkSVForwardIndexWriter(outFileFourByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Long.BYTES, BaseChunkSVForwardIndexWriter.DEFAULT_VERSION);
    FixedByteChunkSVForwardIndexWriter eightByteOffsetWriter =
        new FixedByteChunkSVForwardIndexWriter(outFileEightByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Long.BYTES, BaseChunkSVForwardIndexWriter.CURRENT_VERSION);

    for (int i = 0; i < NUM_VALUES; i++) {
      fourByteOffsetWriter.setLong(i, expected[i]);
      eightByteOffsetWriter.setLong(i, expected[i]);
    }

    fourByteOffsetWriter.close();
    eightByteOffsetWriter.close();

    try (FixedByteChunkSVForwardIndexReader fourByteOffsetReader = new FixedByteChunkSVForwardIndexReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), DataType.LONG);
        FixedByteChunkSVForwardIndexReader eightByteOffsetReader = new FixedByteChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), DataType.LONG)) {

      ChunkReaderContext context1 = fourByteOffsetReader.createContext();
      ChunkReaderContext context2 = eightByteOffsetReader.createContext();

      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getLong(i, context1), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getLong(i, context2), expected[i]);
        if (compressionType.equals(ChunkCompressorFactory.CompressionType.PASS_THROUGH)) {
          Assert.assertEquals(fourByteOffsetReader.getLong(i), expected[i]);
          Assert.assertEquals(eightByteOffsetReader.getLong(i), expected[i]);
        }
      }
    }

    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);
  }

  public void testFloat(ChunkCompressorFactory.CompressionType compressionType)
      throws Exception {
    float[] expected = new float[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = RANDOM.nextFloat();
    }

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    FixedByteChunkSVForwardIndexWriter fourByteOffsetWriter =
        new FixedByteChunkSVForwardIndexWriter(outFileFourByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Float.BYTES, BaseChunkSVForwardIndexWriter.DEFAULT_VERSION);
    FixedByteChunkSVForwardIndexWriter eightByteOffsetWriter =
        new FixedByteChunkSVForwardIndexWriter(outFileEightByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Float.BYTES, BaseChunkSVForwardIndexWriter.CURRENT_VERSION);

    for (int i = 0; i < NUM_VALUES; i++) {
      fourByteOffsetWriter.setFloat(i, expected[i]);
      eightByteOffsetWriter.setFloat(i, expected[i]);
    }

    fourByteOffsetWriter.close();
    eightByteOffsetWriter.close();

    try (FixedByteChunkSVForwardIndexReader fourByteOffsetReader = new FixedByteChunkSVForwardIndexReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), DataType.FLOAT);
        FixedByteChunkSVForwardIndexReader eightByteOffsetReader = new FixedByteChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), DataType.FLOAT)) {

      ChunkReaderContext context1 = fourByteOffsetReader.createContext();
      ChunkReaderContext context2 = eightByteOffsetReader.createContext();

      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getFloat(i, context1), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getFloat(i, context2), expected[i]);
        if (compressionType.equals(ChunkCompressorFactory.CompressionType.PASS_THROUGH)) {
          Assert.assertEquals(fourByteOffsetReader.getFloat(i), expected[i]);
          Assert.assertEquals(eightByteOffsetReader.getFloat(i), expected[i]);
        }
      }
    }

    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);
  }

  public void testDouble(ChunkCompressorFactory.CompressionType compressionType)
      throws Exception {
    double[] expected = new double[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = RANDOM.nextDouble();
    }

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    FixedByteChunkSVForwardIndexWriter fourByteOffsetWriter =
        new FixedByteChunkSVForwardIndexWriter(outFileFourByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Double.BYTES, BaseChunkSVForwardIndexWriter.DEFAULT_VERSION);
    FixedByteChunkSVForwardIndexWriter eightByteOffsetWriter =
        new FixedByteChunkSVForwardIndexWriter(outFileEightByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Double.BYTES, BaseChunkSVForwardIndexWriter.CURRENT_VERSION);

    for (int i = 0; i < NUM_VALUES; i++) {
      fourByteOffsetWriter.setDouble(i, expected[i]);
      eightByteOffsetWriter.setDouble(i, expected[i]);
    }

    fourByteOffsetWriter.close();
    eightByteOffsetWriter.close();

    try (FixedByteChunkSVForwardIndexReader fourByteOffsetReader = new FixedByteChunkSVForwardIndexReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), DataType.DOUBLE);
        FixedByteChunkSVForwardIndexReader eightByteOffsetReader = new FixedByteChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), DataType.DOUBLE)) {

      ChunkReaderContext context1 = fourByteOffsetReader.createContext();
      ChunkReaderContext context2 = eightByteOffsetReader.createContext();

      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getDouble(i, context1), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getDouble(i, context2), expected[i]);
        if (compressionType.equals(ChunkCompressorFactory.CompressionType.PASS_THROUGH)) {
          Assert.assertEquals(fourByteOffsetReader.getDouble(i), expected[i]);
          Assert.assertEquals(eightByteOffsetReader.getDouble(i), expected[i]);
        }
      }
    }

    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);
  }

  /**
   * This test ensures that the reader can read in an data file from version 1.
   */
  @Test
  public void testBackwardCompatibilityV1()
      throws Exception {
    testBackwardCompatibilityHelper("data/fixedByteSVRDoubles.v1", 10009, 0);
  }

  /**
   * This test ensures that the reader can read in an data file from version 2.
   */
  @Test
  public void testBackwardCompatibilityV2()
      throws Exception {
    testBackwardCompatibilityHelper("data/fixedByteCompressed.v2", 2000, 100.2356);
    testBackwardCompatibilityHelper("data/fixedByteRaw.v2", 2000, 100.2356);
  }

  private void testBackwardCompatibilityHelper(String fileName, int numDocs, double startValue)
      throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL resource = classLoader.getResource(fileName);
    if (resource == null) {
      throw new RuntimeException("Input file not found: " + fileName);
    }
    File file = new File(resource.getFile());
    try (FixedByteChunkSVForwardIndexReader reader = new FixedByteChunkSVForwardIndexReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(file), DataType.DOUBLE)) {
      ChunkReaderContext context = reader.createContext();
      for (int i = 0; i < numDocs; i++) {
        double actual = reader.getDouble(i, context);
        Assert.assertEquals(actual, i + startValue);
      }
    }
  }
}
