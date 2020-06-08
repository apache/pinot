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
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.core.io.compression.ChunkCompressorFactory;
import org.apache.pinot.core.io.reader.impl.ChunkReaderContext;
import org.apache.pinot.core.io.reader.impl.v1.FixedByteChunkSingleValueReader;
import org.apache.pinot.core.io.writer.impl.v1.BaseChunkSingleValueWriter;
import org.apache.pinot.core.io.writer.impl.v1.FixedByteChunkSingleValueWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link FixedByteChunkSingleValueReader} and {@link FixedByteChunkSingleValueWriter} classes.
 *
 * This test writes {@link #NUM_VALUES} using {@link FixedByteChunkSingleValueWriter}. It then reads
 * the values using {@link FixedByteChunkSingleValueReader}, and asserts that what was written is the same as
 * what was read in.
 *
 * Number of docs and docs per chunk are chosen to generate complete as well partial chunks.
 *
 */
public class FixedByteChunkSingleValueReaderWriteTest {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private static final int NUM_VALUES = 10009;
  private static final int NUM_DOCS_PER_CHUNK = 5003;
  private static final String TEST_FILE = System.getProperty("java.io.tmpdir") + File.separator + "FixedByteSVRTest";
  private static final Random _random = new Random();
  private static final int BYTES_LENGTH = 101;

  @Test
  public void testWithCompression()
      throws Exception {
    ChunkCompressorFactory.CompressionType compressionType = ChunkCompressorFactory.CompressionType.SNAPPY;
    testInt(compressionType);
    testLong(compressionType);
    testFloat(compressionType);
    testDouble(compressionType);
    testBytes(compressionType);
  }

  @Test
  public void testWithoutCompression()
      throws Exception {
    ChunkCompressorFactory.CompressionType compressionType = ChunkCompressorFactory.CompressionType.PASS_THROUGH;
    testInt(compressionType);
    testLong(compressionType);
    testFloat(compressionType);
    testDouble(compressionType);
    testBytes(compressionType);
  }

  public void testInt(ChunkCompressorFactory.CompressionType compressionType)
      throws Exception {
    int[] expected = new int[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = _random.nextInt();
    }

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    FixedByteChunkSingleValueWriter fourByteOffsetWriter =
        new FixedByteChunkSingleValueWriter(outFileFourByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Integer.BYTES, BaseChunkSingleValueWriter.DEFAULT_VERSION);
    FixedByteChunkSingleValueWriter eightByteOffsetWriter =
        new FixedByteChunkSingleValueWriter(outFileEightByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Integer.BYTES, BaseChunkSingleValueWriter.CURRENT_VERSION);

    for (int i = 0; i < NUM_VALUES; i++) {
      fourByteOffsetWriter.setInt(i, expected[i]);
      eightByteOffsetWriter.setInt(i, expected[i]);
    }

    fourByteOffsetWriter.close();
    eightByteOffsetWriter.close();

    try (FixedByteChunkSingleValueReader fourByteOffsetReader = new FixedByteChunkSingleValueReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte));
        FixedByteChunkSingleValueReader eightByteOffsetReader = new FixedByteChunkSingleValueReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte))) {

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
      expected[i] = _random.nextLong();
    }

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    FixedByteChunkSingleValueWriter fourByteOffsetWriter =
        new FixedByteChunkSingleValueWriter(outFileFourByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Long.BYTES,
            BaseChunkSingleValueWriter.DEFAULT_VERSION);
    FixedByteChunkSingleValueWriter eightByteOffsetWriter =
        new FixedByteChunkSingleValueWriter(outFileEightByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Long.BYTES, BaseChunkSingleValueWriter.CURRENT_VERSION);

    for (int i = 0; i < NUM_VALUES; i++) {
      fourByteOffsetWriter.setLong(i, expected[i]);
      eightByteOffsetWriter.setLong(i, expected[i]);
    }

    fourByteOffsetWriter.close();
    eightByteOffsetWriter.close();

    try (FixedByteChunkSingleValueReader fourByteOffsetReader = new FixedByteChunkSingleValueReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte));
        FixedByteChunkSingleValueReader eightByteOffsetReader = new FixedByteChunkSingleValueReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte))) {

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
      expected[i] = _random.nextFloat();
    }

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    FixedByteChunkSingleValueWriter fourByteOffsetWriter =
        new FixedByteChunkSingleValueWriter(outFileFourByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Float.BYTES, BaseChunkSingleValueWriter.DEFAULT_VERSION);
    FixedByteChunkSingleValueWriter eightByteOffsetWriter =
        new FixedByteChunkSingleValueWriter(outFileEightByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Float.BYTES, BaseChunkSingleValueWriter.CURRENT_VERSION);

    for (int i = 0; i < NUM_VALUES; i++) {
      fourByteOffsetWriter.setFloat(i, expected[i]);
      eightByteOffsetWriter.setFloat(i, expected[i]);
    }

    fourByteOffsetWriter.close();
    eightByteOffsetWriter.close();

    try (FixedByteChunkSingleValueReader fourByteOffsetReader = new FixedByteChunkSingleValueReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte));
        FixedByteChunkSingleValueReader eightByteOffsetReader = new FixedByteChunkSingleValueReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte))) {

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
      expected[i] = _random.nextDouble();
    }

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    FixedByteChunkSingleValueWriter fourByteOffsetWriter =
        new FixedByteChunkSingleValueWriter(outFileFourByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Double.BYTES, BaseChunkSingleValueWriter.DEFAULT_VERSION);
    FixedByteChunkSingleValueWriter eightByteOffsetWriter =
        new FixedByteChunkSingleValueWriter(outFileEightByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            Double.BYTES, BaseChunkSingleValueWriter.CURRENT_VERSION);

    for (int i = 0; i < NUM_VALUES; i++) {
      fourByteOffsetWriter.setDouble(i, expected[i]);
      eightByteOffsetWriter.setDouble(i, expected[i]);
    }

    fourByteOffsetWriter.close();
    eightByteOffsetWriter.close();

    try (FixedByteChunkSingleValueReader fourByteOffsetReader = new FixedByteChunkSingleValueReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte));
        FixedByteChunkSingleValueReader eightByteOffsetReader = new FixedByteChunkSingleValueReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte))) {

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

  public void testBytes(ChunkCompressorFactory.CompressionType compressionType)
      throws Exception {
    byte[][] expected = new byte[NUM_VALUES][BYTES_LENGTH];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = RandomStringUtils.randomAscii(50).getBytes(UTF_8);
    }

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    FixedByteChunkSingleValueWriter fourByteOffsetWriter =
        new FixedByteChunkSingleValueWriter(outFileFourByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, 50,
            BaseChunkSingleValueWriter.DEFAULT_VERSION);
    FixedByteChunkSingleValueWriter eightByteOffsetWriter =
        new FixedByteChunkSingleValueWriter(outFileEightByte, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, 50,
            BaseChunkSingleValueWriter.CURRENT_VERSION);

    for (int i = 0; i < NUM_VALUES; i++) {
      fourByteOffsetWriter.setBytes(i, expected[i]);
      eightByteOffsetWriter.setBytes(i, expected[i]);
    }

    fourByteOffsetWriter.close();
    eightByteOffsetWriter.close();

    try (FixedByteChunkSingleValueReader fourByteOffsetReader = new FixedByteChunkSingleValueReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte));
        FixedByteChunkSingleValueReader eightByteOffsetReader = new FixedByteChunkSingleValueReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte))) {

      ChunkReaderContext context1 = fourByteOffsetReader.createContext();
      ChunkReaderContext context2 = eightByteOffsetReader.createContext();

      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getBytes(i, context1), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getBytes(i, context2), expected[i]);
        if (compressionType.equals(ChunkCompressorFactory.CompressionType.PASS_THROUGH)) {
          Assert.assertEquals(fourByteOffsetReader.getBytes(i), expected[i]);
          Assert.assertEquals(eightByteOffsetReader.getBytes(i), expected[i]);
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
    try (FixedByteChunkSingleValueReader reader = new FixedByteChunkSingleValueReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(file))) {
      ChunkReaderContext context = reader.createContext();
      for (int i = 0; i < numDocs; i++) {
        double actual = reader.getDouble(i, context);
        Assert.assertEquals(actual, i + startValue);
      }
    }
  }
}
