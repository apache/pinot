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
package org.apache.pinot.segment.local.segment.index.forward;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBytePower2ChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit test for {@link FixedByteChunkSVForwardIndexReader} and {@link FixedByteChunkForwardIndexWriter} classes.
 *
 * This test writes {@link #NUM_VALUES} using {@link FixedByteChunkForwardIndexWriter}. It then reads
 * the values using {@link FixedByteChunkSVForwardIndexReader}, and asserts that what was written is the same as
 * what was read in.
 *
 * Number of docs and docs per chunk are chosen to generate complete as well partial chunks.
 */
public class FixedByteChunkSVForwardIndexTest {
  private static final int NUM_VALUES = 10009;
  private static final int NUM_DOCS_PER_CHUNK = 5003;
  private static final String TEST_FILE = System.getProperty("java.io.tmpdir") + File.separator + "FixedByteSVRTest";
  private static final Random RANDOM = new Random();

  @DataProvider(name = "combinations")
  public static Object[][] combinations() {
    return Arrays.stream(ChunkCompressionType.values())
        .flatMap(chunkCompressionType -> IntStream.of(2, 3, 4)
            .mapToObj(version -> new Object[]{chunkCompressionType, version}))
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "combinations")
  public void testInt(ChunkCompressionType compressionType, int version)
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
    try (FixedByteChunkForwardIndexWriter fourByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileFourByte,
        compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Integer.BYTES, version);
        FixedByteChunkForwardIndexWriter eightByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileEightByte,
            compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Integer.BYTES, version)) {
      for (int value : expected) {
        fourByteOffsetWriter.putInt(value);
        eightByteOffsetWriter.putInt(value);
      }
    }

    try (ForwardIndexReader<ChunkReaderContext> fourByteOffsetReader = version >= 4
        ? new FixedBytePower2ChunkSVForwardIndexReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), DataType.INT)
        : new FixedByteChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), DataType.INT);
        ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader
            .createContext();
        ForwardIndexReader<ChunkReaderContext> eightByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), DataType.INT)
            : new FixedByteChunkSVForwardIndexReader(
                PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), DataType.INT);
        ChunkReaderContext eightByteOffsetReaderContext = eightByteOffsetReader
            .createContext()) {
      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getInt(i, fourByteOffsetReaderContext), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getInt(i, eightByteOffsetReaderContext), expected[i]);
      }


      Assert.assertTrue(fourByteOffsetReader.isByteRangeRecordingSupported());
      Assert.assertTrue(eightByteOffsetReader.isByteRangeRecordingSupported());
      // Validate byte range provider behaviour
      if (compressionType == ChunkCompressionType.PASS_THROUGH) {
        // For pass through compression, the buffer is fixed offset mapping type
        Assert.assertTrue(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(fourByteOffsetReader.getDocLength(), Integer.BYTES);
        Assert.assertFalse(fourByteOffsetReader.isDocLengthInBits());

        Assert.assertTrue(eightByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(eightByteOffsetReader.getDocLength(), Integer.BYTES);
        Assert.assertFalse(eightByteOffsetReader.isDocLengthInBits());
      } else {
        Assert.assertFalse(eightByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertFalse(eightByteOffsetReader.isFixedOffsetMappingType());
      }
    }

    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);
  }

  @Test(dataProvider = "combinations")
  public void testLong(ChunkCompressionType compressionType, int version)
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
    try (FixedByteChunkForwardIndexWriter fourByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileFourByte,
        compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Long.BYTES, version);
        FixedByteChunkForwardIndexWriter eightByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileEightByte,
            compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Long.BYTES, version)) {
      for (long value : expected) {
        fourByteOffsetWriter.putLong(value);
        eightByteOffsetWriter.putLong(value);
      }
    }

    try (ForwardIndexReader<ChunkReaderContext> fourByteOffsetReader = version >= 4
        ? new FixedBytePower2ChunkSVForwardIndexReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), DataType.LONG)
        : new FixedByteChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), DataType.LONG);
        ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader
            .createContext();
        ForwardIndexReader<ChunkReaderContext> eightByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), DataType.LONG)
            : new FixedByteChunkSVForwardIndexReader(
                PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), DataType.LONG);
        ChunkReaderContext eightByteOffsetReaderContext = eightByteOffsetReader
            .createContext()) {
      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getLong(i, fourByteOffsetReaderContext), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getLong(i, eightByteOffsetReaderContext), expected[i]);
      }

      // Validate byte range provider behaviour
      Assert.assertTrue(fourByteOffsetReader.isByteRangeRecordingSupported());
      Assert.assertTrue(eightByteOffsetReader.isByteRangeRecordingSupported());
      if (compressionType == ChunkCompressionType.PASS_THROUGH) {
        // For pass through compression, the buffer is fixed offset mapping type
        Assert.assertTrue(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(fourByteOffsetReader.getDocLength(), Long.BYTES);
        Assert.assertFalse(fourByteOffsetReader.isDocLengthInBits());

        Assert.assertTrue(eightByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(eightByteOffsetReader.getDocLength(), Long.BYTES);
        Assert.assertFalse(eightByteOffsetReader.isDocLengthInBits());
      } else {
        Assert.assertFalse(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertFalse(eightByteOffsetReader.isFixedOffsetMappingType());
      }
    }

    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);
  }

  @Test(dataProvider = "combinations")
  public void testFloat(ChunkCompressionType compressionType, int version)
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
    try (FixedByteChunkForwardIndexWriter fourByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileFourByte,
        compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Float.BYTES, version);
        FixedByteChunkForwardIndexWriter eightByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileEightByte,
            compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Float.BYTES, version)) {
      for (float value : expected) {
        fourByteOffsetWriter.putFloat(value);
        eightByteOffsetWriter.putFloat(value);
      }
    }

    try (ForwardIndexReader<ChunkReaderContext> fourByteOffsetReader = version >= 4
        ? new FixedBytePower2ChunkSVForwardIndexReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), DataType.FLOAT)
        : new FixedByteChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), DataType.FLOAT);
        ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader
            .createContext();
        ForwardIndexReader<ChunkReaderContext> eightByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte),
            DataType.FLOAT)
            : new FixedByteChunkSVForwardIndexReader(
                PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), DataType.FLOAT);
        ChunkReaderContext eightByteOffsetReaderContext = eightByteOffsetReader
            .createContext()) {
      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getFloat(i, fourByteOffsetReaderContext), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getFloat(i, eightByteOffsetReaderContext), expected[i]);
      }

      // Validate byte range provider behaviour
      Assert.assertTrue(fourByteOffsetReader.isByteRangeRecordingSupported());
      Assert.assertTrue(eightByteOffsetReader.isByteRangeRecordingSupported());
      if (compressionType == ChunkCompressionType.PASS_THROUGH) {
        // For pass through compression, the buffer is fixed offset mapping type
        Assert.assertTrue(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(fourByteOffsetReader.getDocLength(), Float.BYTES);
        Assert.assertFalse(fourByteOffsetReader.isDocLengthInBits());

        Assert.assertTrue(eightByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(eightByteOffsetReader.getDocLength(), Float.BYTES);
        Assert.assertFalse(eightByteOffsetReader.isDocLengthInBits());
      } else {
        Assert.assertFalse(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertFalse(eightByteOffsetReader.isFixedOffsetMappingType());
      }
    }

    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);
  }

  @Test(dataProvider = "combinations")
  public void testDouble(ChunkCompressionType compressionType, int version)
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
    try (FixedByteChunkForwardIndexWriter fourByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileFourByte,
        compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Double.BYTES, version);
        FixedByteChunkForwardIndexWriter eightByteOffsetWriter = new FixedByteChunkForwardIndexWriter(outFileEightByte,
            compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK, Double.BYTES, version)) {
      for (double value : expected) {
        fourByteOffsetWriter.putDouble(value);
        eightByteOffsetWriter.putDouble(value);
      }
    }

    try (ForwardIndexReader<ChunkReaderContext> fourByteOffsetReader = version >= 4
        ? new FixedBytePower2ChunkSVForwardIndexReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), DataType.DOUBLE)
        : new FixedByteChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte), DataType.DOUBLE);
        ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader
            .createContext();
        ForwardIndexReader<ChunkReaderContext> eightByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(
            PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), DataType.DOUBLE)
            : new FixedByteChunkSVForwardIndexReader(
                PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte), DataType.DOUBLE);
        ChunkReaderContext eightByteOffsetReaderContext = eightByteOffsetReader
            .createContext()) {
      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getDouble(i, fourByteOffsetReaderContext), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getDouble(i, eightByteOffsetReaderContext), expected[i]);
      }

      // Validate byte range provider behaviour
      Assert.assertTrue(fourByteOffsetReader.isByteRangeRecordingSupported());
      Assert.assertTrue(eightByteOffsetReader.isByteRangeRecordingSupported());
      if (compressionType == ChunkCompressionType.PASS_THROUGH) {
        // For pass through compression, the buffer is fixed offset mapping type
        Assert.assertTrue(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(fourByteOffsetReader.getDocLength(), Double.BYTES);
        Assert.assertFalse(fourByteOffsetReader.isDocLengthInBits());

        Assert.assertTrue(eightByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(eightByteOffsetReader.getDocLength(), Double.BYTES);
        Assert.assertFalse(eightByteOffsetReader.isDocLengthInBits());
      } else {
        Assert.assertFalse(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertFalse(eightByteOffsetReader.isFixedOffsetMappingType());
      }

      Assert.assertTrue(fourByteOffsetReader.isByteRangeRecordingSupported());
      Assert.assertTrue(eightByteOffsetReader.isByteRangeRecordingSupported());
      if (compressionType == ChunkCompressionType.PASS_THROUGH) {
        // For pass through compression, the buffer is fixed offset mapping type
        Assert.assertTrue(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(fourByteOffsetReader.getDocLength(), Double.BYTES);
        Assert.assertFalse(fourByteOffsetReader.isDocLengthInBits());

        Assert.assertTrue(eightByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertEquals(eightByteOffsetReader.getDocLength(), Double.BYTES);
        Assert.assertFalse(eightByteOffsetReader.isDocLengthInBits());
      } else {
        Assert.assertFalse(fourByteOffsetReader.isFixedOffsetMappingType());
        Assert.assertFalse(eightByteOffsetReader.isFixedOffsetMappingType());
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
        PinotDataBuffer.mapReadOnlyBigEndianFile(file), DataType.DOUBLE);
        ChunkReaderContext readerContext = reader.createContext()) {
      for (int i = 0; i < numDocs; i++) {
        double actual = reader.getDouble(i, readerContext);
        Assert.assertEquals(actual, i + startValue);
      }
    }
  }
}
