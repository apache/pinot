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
import java.io.RandomAccessFile;
import java.net.URL;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
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


/// Unit test for [FixedByteChunkSVForwardIndexReader] and [FixedByteChunkForwardIndexWriter] classes.
///
/// This test writes [#NUM_VALUES] using [FixedByteChunkForwardIndexWriter]. It then reads
/// the values using [FixedByteChunkSVForwardIndexReader], and asserts that what was written is the same as
/// what was read in.
///
/// Number of docs and docs per chunk are chosen to generate complete as well partial chunks.
public class FixedByteChunkSVForwardIndexTest implements PinotBuffersAfterMethodCheckRule {
  private static final int NUM_VALUES = 10009;
  private static final int NUM_DOCS_PER_CHUNK = 5003;
  private static final String TEST_FILE = System.getProperty("java.io.tmpdir") + File.separator + "FixedByteSVRTest";
  private static final Random RANDOM = new Random();

  @DataProvider(name = "combinations")
  public static Object[][] combinations() {
    return Arrays.stream(ChunkCompressionType.values())
        .filter(t -> t != ChunkCompressionType.DELTA && t != ChunkCompressionType.DELTADELTA)
        .flatMap(chunkCompressionType -> IntStream.of(2, 3, 4)
            .mapToObj(version -> new Object[]{chunkCompressionType, version}))
        .toArray(Object[][]::new);
  }

  @DataProvider(name = "deltaCombinations")
  public static Object[][] deltaCombinations() {
    return Arrays.stream(new ChunkCompressionType[]{ChunkCompressionType.DELTA, ChunkCompressionType.DELTADELTA})
        .flatMap(chunkCompressionType -> IntStream.of(2, 3, 4)
            .mapToObj(version -> new Object[]{chunkCompressionType, version}))
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "deltaCombinations")
  public void testDeltaIntChunkCaching(ChunkCompressionType compressionType, int version)
      throws Exception {
    int[] expected = {101, 103, 107, 109, 211, 223, 227, 229, 307};
    File outputFile = new File(TEST_FILE + "-int-" + compressionType + '-' + version);
    FileUtils.deleteQuietly(outputFile);

    try {
      try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(outputFile,
          compressionType, expected.length, 4, Integer.BYTES, version)) {
        for (int value : expected) {
          writer.putInt(value);
        }
      }

      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(outputFile);
          ForwardIndexReader<ChunkReaderContext> reader = version >= 4
              ? new FixedBytePower2ChunkSVForwardIndexReader(buffer, DataType.INT)
              : new FixedByteChunkSVForwardIndexReader(buffer, DataType.INT);
          ChunkReaderContext context = reader.createContext()) {
        // Same-chunk reads, cross-chunk reads, a partial final chunk, and revisits expose stale context buffers.
        int[] docIds = {0, 1, 4, 5, 8, 2, 3, 6, 7};
        for (int docId : docIds) {
          Assert.assertEquals(reader.getInt(docId, context), expected[docId]);
        }
      }
    } finally {
      FileUtils.deleteQuietly(outputFile);
    }
  }

  @Test(dataProvider = "deltaCombinations")
  public void testDeltaLongChunkCaching(ChunkCompressionType compressionType, int version)
      throws Exception {
    long[] expected = {10_000_000_001L, 10_000_000_003L, 10_000_000_007L, 10_000_000_009L, 20_000_000_011L,
        20_000_000_033L, 20_000_000_039L, 20_000_000_051L, 30_000_000_077L};
    File outputFile = new File(TEST_FILE + "-long-" + compressionType + '-' + version);
    FileUtils.deleteQuietly(outputFile);

    try {
      try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(outputFile,
          compressionType, expected.length, 4, Long.BYTES, version)) {
        for (long value : expected) {
          writer.putLong(value);
        }
      }

      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(outputFile);
          ForwardIndexReader<ChunkReaderContext> reader = version >= 4
              ? new FixedBytePower2ChunkSVForwardIndexReader(buffer, DataType.LONG)
              : new FixedByteChunkSVForwardIndexReader(buffer, DataType.LONG);
          ChunkReaderContext context = reader.createContext()) {
        // Same-chunk reads, cross-chunk reads, a partial final chunk, and revisits expose stale context buffers.
        int[] docIds = {0, 1, 4, 5, 8, 2, 3, 6, 7};
        for (int docId : docIds) {
          Assert.assertEquals(reader.getLong(docId, context), expected[docId]);
        }
      }
    } finally {
      FileUtils.deleteQuietly(outputFile);
    }
  }

  @Test
  public void testFailedDeltaDecodeInvalidatesCachedChunk()
      throws Exception {
    long[] expected = {101L, 103L, 107L, 109L, 211L, 223L, 227L, 229L};
    File outputFile = new File(TEST_FILE + "-failed-delta-decode");
    FileUtils.deleteQuietly(outputFile);

    try {
      try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(outputFile,
          ChunkCompressionType.DELTA, expected.length, 4, Long.BYTES,
          FixedBytePower2ChunkSVForwardIndexReader.VERSION)) {
        for (long value : expected) {
          writer.putLong(value);
        }
      }

      // Corrupt the second chunk's compressed-size field. DELTA writes the first decoded value into the context
      // before validating this field, so reading the malformed chunk partially mutates the cached buffer and fails.
      try (RandomAccessFile file = new RandomAccessFile(outputFile, "rw")) {
        int dataHeaderStart = 7 * Integer.BYTES;
        file.seek(dataHeaderStart + Long.BYTES);
        long secondChunkOffset = file.readLong();
        file.seek(secondChunkOffset + Byte.BYTES + Integer.BYTES + Long.BYTES);
        file.writeInt(Integer.MAX_VALUE);
      }

      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(outputFile);
          ForwardIndexReader<ChunkReaderContext> reader =
              new FixedBytePower2ChunkSVForwardIndexReader(buffer, DataType.LONG);
          ChunkReaderContext context = reader.createContext()) {
        Assert.assertEquals(reader.getLong(0, context), expected[0]);
        Assert.assertEquals(context.getChunkId(), 0);

        Assert.expectThrows(IllegalArgumentException.class, () -> reader.getLong(4, context));
        Assert.assertEquals(context.getChunkId(), -1);

        // With a stale chunk id this would falsely return 211, which the failed decode wrote at buffer offset zero.
        Assert.assertEquals(reader.getLong(0, context), expected[0]);
        Assert.assertEquals(reader.getLong(1, context), expected[1]);
      }
    } finally {
      FileUtils.deleteQuietly(outputFile);
    }
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

    try (PinotDataBuffer buffer1 = PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte);
        ForwardIndexReader<ChunkReaderContext> fourByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(buffer1, DataType.INT)
            : new FixedByteChunkSVForwardIndexReader(buffer1, DataType.INT);
        ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader.createContext();
        PinotDataBuffer buffer2 = PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte);
        ForwardIndexReader<ChunkReaderContext> eightByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(buffer2, DataType.INT)
            : new FixedByteChunkSVForwardIndexReader(buffer2, DataType.INT);
        ChunkReaderContext eightByteOffsetReaderContext = eightByteOffsetReader.createContext()) {

      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getInt(i, fourByteOffsetReaderContext), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getInt(i, eightByteOffsetReaderContext), expected[i]);
      }


      Assert.assertTrue(fourByteOffsetReader.isBufferByteRangeInfoSupported());
      Assert.assertTrue(eightByteOffsetReader.isBufferByteRangeInfoSupported());
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

    try (PinotDataBuffer buffer1 = PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte);
        ForwardIndexReader<ChunkReaderContext> fourByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(buffer1, DataType.LONG)
            : new FixedByteChunkSVForwardIndexReader(buffer1, DataType.LONG);
        ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader.createContext();
        PinotDataBuffer buffer2 = PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte);
        ForwardIndexReader<ChunkReaderContext> eightByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(buffer2, DataType.LONG)
            : new FixedByteChunkSVForwardIndexReader(buffer2, DataType.LONG);
        ChunkReaderContext eightByteOffsetReaderContext = eightByteOffsetReader.createContext()) {

      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getLong(i, fourByteOffsetReaderContext), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getLong(i, eightByteOffsetReaderContext), expected[i]);
      }

      // Validate byte range provider behaviour
      Assert.assertTrue(fourByteOffsetReader.isBufferByteRangeInfoSupported());
      Assert.assertTrue(eightByteOffsetReader.isBufferByteRangeInfoSupported());
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

    try (PinotDataBuffer buffer1 = PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte);
        ForwardIndexReader<ChunkReaderContext> fourByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(buffer1, DataType.FLOAT)
            : new FixedByteChunkSVForwardIndexReader(buffer1, DataType.FLOAT);
        ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader.createContext();
        PinotDataBuffer buffer2 = PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte);
        ForwardIndexReader<ChunkReaderContext> eightByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(buffer2, DataType.FLOAT)
            : new FixedByteChunkSVForwardIndexReader(buffer2, DataType.FLOAT);
        ChunkReaderContext eightByteOffsetReaderContext = eightByteOffsetReader.createContext()) {
      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getFloat(i, fourByteOffsetReaderContext), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getFloat(i, eightByteOffsetReaderContext), expected[i]);
      }

      // Validate byte range provider behaviour
      Assert.assertTrue(fourByteOffsetReader.isBufferByteRangeInfoSupported());
      Assert.assertTrue(eightByteOffsetReader.isBufferByteRangeInfoSupported());
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

    try (PinotDataBuffer buffer1 = PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte);
        ForwardIndexReader<ChunkReaderContext> fourByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(buffer1, DataType.DOUBLE)
            : new FixedByteChunkSVForwardIndexReader(buffer1, DataType.DOUBLE);
        ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader.createContext();
        PinotDataBuffer buffer2 = PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte);
        ForwardIndexReader<ChunkReaderContext> eightByteOffsetReader = version >= 4
            ? new FixedBytePower2ChunkSVForwardIndexReader(buffer2, DataType.DOUBLE)
            : new FixedByteChunkSVForwardIndexReader(buffer2, DataType.DOUBLE);
        ChunkReaderContext eightByteOffsetReaderContext = eightByteOffsetReader
            .createContext()) {
      for (int i = 0; i < NUM_VALUES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getDouble(i, fourByteOffsetReaderContext), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getDouble(i, eightByteOffsetReaderContext), expected[i]);
      }

      // Validate byte range provider behaviour
      Assert.assertTrue(fourByteOffsetReader.isBufferByteRangeInfoSupported());
      Assert.assertTrue(eightByteOffsetReader.isBufferByteRangeInfoSupported());
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

      Assert.assertTrue(fourByteOffsetReader.isBufferByteRangeInfoSupported());
      Assert.assertTrue(eightByteOffsetReader.isBufferByteRangeInfoSupported());
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

  /// This test ensures that the reader can read in an data file from version 1.
  @Test
  public void testBackwardCompatibilityV1()
      throws Exception {
    testBackwardCompatibilityHelper("data/fixedByteSVRDoubles.v1", 10009, 0);
  }

  /// This test ensures that the reader can read in an data file from version 2.
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
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        FixedByteChunkSVForwardIndexReader reader = new FixedByteChunkSVForwardIndexReader(buffer, DataType.DOUBLE);
        ChunkReaderContext readerContext = reader.createContext()) {
      for (int i = 0; i < numDocs; i++) {
        double actual = reader.getDouble(i, readerContext);
        Assert.assertEquals(actual, i + startValue);
      }
    }
  }
}
