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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV5;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV6;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkWriter;
import org.apache.pinot.segment.local.utils.ArraySerDeUtils;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Tests uncompressed serialized-byte tracking across fixed-byte and variable-byte forward-index writers.
public class ForwardIndexWriterUncompressedValueSizeTest {
  // Use power-of-2 aligned counts so V4's chunk normalization doesn't create partial chunks
  private static final int NUM_DOCS = 1024;
  private static final int DOCS_PER_CHUNK = 128; // already power-of-2, no normalization needed
  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(),
        ForwardIndexWriterUncompressedValueSizeTest.class.getSimpleName() + "_" + UUID.randomUUID());
    FileUtils.forceMkdir(_tempDir);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  @DataProvider(name = "compressionTypes")
  public Object[][] compressionTypes() {
    return new Object[][]{
        {ChunkCompressionType.LZ4},
        {ChunkCompressionType.ZSTANDARD},
        {ChunkCompressionType.SNAPPY},
        {ChunkCompressionType.PASS_THROUGH}
    };
  }

  @DataProvider(name = "varByteWriterVersions")
  public Object[][] varByteWriterVersions() {
    return new Object[][]{{4}, {5}, {6}};
  }

  @Test(dataProvider = "compressionTypes")
  public void testFixedByteWriterIntTracksUncompressedValueSize(ChunkCompressionType compressionType)
      throws IOException {
    File file = new File(_tempDir, "fixedInt_" + compressionType.name());
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, compressionType, NUM_DOCS, DOCS_PER_CHUNK, Integer.BYTES, 4)) {
      writer.enableRawForwardIndexUncompressedValueSizeTracking();
      for (int i = 0; i < NUM_DOCS; i++) {
        writer.putInt(i);
      }
      // 1024 docs / 128 per chunk = 8 full chunks, all flushed before close
      long expected = (long) NUM_DOCS * Integer.BYTES;
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), expected,
          "Uncompressed size should equal NUM_DOCS * INT_BYTES for " + compressionType);
    }
  }

  @Test(dataProvider = "compressionTypes")
  public void testFixedByteWriterLongTracksUncompressedValueSize(ChunkCompressionType compressionType)
      throws IOException {
    File file = new File(_tempDir, "fixedLong_" + compressionType.name());
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, compressionType, NUM_DOCS, DOCS_PER_CHUNK, Long.BYTES, 4)) {
      writer.enableRawForwardIndexUncompressedValueSizeTracking();
      for (int i = 0; i < NUM_DOCS; i++) {
        writer.putLong(i * 1000L);
      }
      long expected = (long) NUM_DOCS * Long.BYTES;
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), expected,
          "Uncompressed size should equal NUM_DOCS * LONG_BYTES for " + compressionType);
    }
  }

  @Test(dataProvider = "compressionTypes")
  public void testFixedByteWriterDoubleTracksUncompressedValueSize(ChunkCompressionType compressionType)
      throws IOException {
    File file = new File(_tempDir, "fixedDouble_" + compressionType.name());
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, compressionType, NUM_DOCS, DOCS_PER_CHUNK, Double.BYTES, 4)) {
      writer.enableRawForwardIndexUncompressedValueSizeTracking();
      for (int i = 0; i < NUM_DOCS; i++) {
        writer.putDouble(i * 0.5);
      }
      long expected = (long) NUM_DOCS * Double.BYTES;
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), expected,
          "Uncompressed size should equal NUM_DOCS * DOUBLE_BYTES for " + compressionType);
    }
  }

  @Test(dataProvider = "compressionTypes")
  public void testVarByteV4WriterSVTracksUncompressedValueSize(ChunkCompressionType compressionType)
      throws IOException {
    File file = new File(_tempDir, "varByteSV_" + compressionType.name());
    String[] values = new String[NUM_DOCS];
    long totalRawBytes = 0;
    for (int i = 0; i < NUM_DOCS; i++) {
      values[i] = "test_string_" + i;
      totalRawBytes += values[i].getBytes(StandardCharsets.UTF_8).length;
    }

    try (VarByteChunkForwardIndexWriterV4 writer =
        new VarByteChunkForwardIndexWriterV4(file, compressionType, 1024)) {
      writer.enableRawForwardIndexUncompressedValueSizeTracking();
      for (String value : values) {
        writer.putString(value);
      }
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), totalRawBytes,
          "V4 should report the exact UTF-8 value bytes for " + compressionType);
    }
  }

  @Test
  public void testUncompressedValueSizeConsistentAcrossCompressionTypes()
      throws IOException {
    // Fixed-width INT column: uncompressed size must be EXACTLY the same regardless of compression type
    long[] sizes = new long[4];
    ChunkCompressionType[] types = {
        ChunkCompressionType.LZ4, ChunkCompressionType.ZSTANDARD,
        ChunkCompressionType.SNAPPY, ChunkCompressionType.PASS_THROUGH
    };

    for (int t = 0; t < types.length; t++) {
      File file = new File(_tempDir, "consistency_" + types[t].name());
      try (FixedByteChunkForwardIndexWriter writer =
          new FixedByteChunkForwardIndexWriter(file, types[t], NUM_DOCS, DOCS_PER_CHUNK, Integer.BYTES, 4)) {
        writer.enableRawForwardIndexUncompressedValueSizeTracking();
        for (int i = 0; i < NUM_DOCS; i++) {
          writer.putInt(i * 7);
        }
        sizes[t] = writer.getRawForwardIndexUncompressedValueSizeInBytes();
      }
    }

    for (int t = 1; t < types.length; t++) {
      assertEquals(sizes[t], sizes[0],
          "Uncompressed size should be identical for " + types[t] + " and " + types[0]);
    }
  }

  @Test
  public void testVarByteV4UncompressedValueSizeConsistentAcrossCompressionTypes()
      throws IOException {
    // Variable-width STRING column: uncompressed size must be the same regardless of compression type
    long[] sizes = new long[4];
    ChunkCompressionType[] types = {
        ChunkCompressionType.LZ4, ChunkCompressionType.ZSTANDARD,
        ChunkCompressionType.SNAPPY, ChunkCompressionType.PASS_THROUGH
    };
    String[] values = new String[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      values[i] = "consistent_value_" + i;
    }

    for (int t = 0; t < types.length; t++) {
      File file = new File(_tempDir, "varByteConsistency_" + types[t].name());
      try (VarByteChunkForwardIndexWriterV4 writer =
          new VarByteChunkForwardIndexWriterV4(file, types[t], 1024)) {
        writer.enableRawForwardIndexUncompressedValueSizeTracking();
        for (String value : values) {
          writer.putString(value);
        }
        sizes[t] = writer.getRawForwardIndexUncompressedValueSizeInBytes();
      }
    }

    for (int t = 1; t < types.length; t++) {
      assertEquals(sizes[t], sizes[0],
          "VarByte V4 uncompressed size should be identical for " + types[t] + " and " + types[0]);
    }
  }

  @Test
  public void testPassthroughCompressionRatioIsOne()
      throws IOException {
    // With PASS_THROUGH compression, uncompressed size should still be tracked correctly.
    File file = new File(_tempDir, "passthrough");
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, ChunkCompressionType.PASS_THROUGH, NUM_DOCS,
            DOCS_PER_CHUNK, Integer.BYTES, 4)) {
      writer.enableRawForwardIndexUncompressedValueSizeTracking();
      for (int i = 0; i < NUM_DOCS; i++) {
        writer.putInt(i);
      }
      long uncompressedValueSize = writer.getRawForwardIndexUncompressedValueSizeInBytes();
      assertEquals(uncompressedValueSize, (long) NUM_DOCS * Integer.BYTES,
          "PASS_THROUGH uncompressed size should equal exact data size");
    }
    // The file size should be >= uncompressed size (includes headers + data with no compression savings)
    assertTrue(file.length() >= (long) NUM_DOCS * Integer.BYTES,
        "PASS_THROUGH file size should be >= uncompressed data size");
  }

  @Test
  public void testEmptyWriterHasZeroUncompressedValueSize()
      throws IOException {
    File file = new File(_tempDir, "empty");
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, ChunkCompressionType.LZ4, 0, DOCS_PER_CHUNK, Integer.BYTES, 4)) {
      writer.enableRawForwardIndexUncompressedValueSizeTracking();
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), 0,
          "Empty writer should have 0 uncompressed size");
    }
  }

  @Test
  public void testFixedByteWriterTrackingDisabledReturnsUnavailable()
      throws IOException {
    File file = new File(_tempDir, "fixedDisabled");
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, ChunkCompressionType.LZ4, 1, 1, Integer.BYTES, 4)) {
      writer.putInt(42);
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), -1L);
    }
  }

  @Test(dataProvider = "varByteWriterVersions")
  public void testVarByteWriterTrackingDisabledReturnsUnavailable(int writerVersion)
      throws IOException {
    File file = new File(_tempDir, "varByteDisabled_v" + writerVersion);
    try (VarByteChunkWriter writer = newVarByteWriter(file, writerVersion)) {
      writer.putString("value");
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), -1L,
          "Writer V" + writerVersion + " should keep tracking disabled by default");
    }
  }

  @Test
  public void testSingleDocUncompressedValueSize()
      throws IOException {
    // V4 leaves numDocsPerChunk=1 unchanged because it is already a power of two.
    // Writing one document fills and immediately flushes the chunk, producing four uncompressed bytes.
    File file = new File(_tempDir, "singleDoc");
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, ChunkCompressionType.LZ4, 1, 1, Integer.BYTES, 4)) {
      writer.enableRawForwardIndexUncompressedValueSizeTracking();
      writer.putInt(42);
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), Integer.BYTES,
          "Single INT doc should have uncompressed size = 4");
    }
  }

  @Test
  public void testMultipleChunksAccumulateCorrectly()
      throws IOException {
    // Use power-of-2 docs per chunk so each chunk boundary is predictable
    File file = new File(_tempDir, "multiChunk");
    int docsPerChunk = 16; // power-of-2, no normalization
    int totalDocs = 128;   // 128 / 16 = 8 full chunks
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, ChunkCompressionType.LZ4, totalDocs,
            docsPerChunk, Integer.BYTES, 4)) {
      writer.enableRawForwardIndexUncompressedValueSizeTracking();
      for (int i = 0; i < totalDocs; i++) {
        writer.putInt(i);
        // After each full chunk, verify accumulated size
        if ((i + 1) % docsPerChunk == 0) {
          long expectedSoFar = (long) (i + 1) * Integer.BYTES;
          assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), expectedSoFar,
              "After " + (i + 1) + " docs, uncompressed size should be " + expectedSoFar);
        }
      }
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), (long) totalDocs * Integer.BYTES);
    }
  }

  @Test
  public void testVarByteV4MultiValueTracksUncompressedValueSize()
      throws IOException {
    File file = new File(_tempDir, "varByteMV");
    long expectedRawBytes = 0;
    try (VarByteChunkForwardIndexWriterV4 writer =
        new VarByteChunkForwardIndexWriterV4(file, ChunkCompressionType.LZ4, 4096)) {
      writer.enableRawForwardIndexUncompressedValueSizeTracking();
      for (int i = 0; i < 100; i++) {
        String[] mvValues = {"value_" + i + "_a", "caf\u00e9_" + i, "\u6771\u4eac_" + i};
        expectedRawBytes += ArraySerDeUtils.serializeStringArray(mvValues).length;
        writer.putStringMV(mvValues);
      }
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), expectedRawBytes,
          "MV writer should report the exact serialized array bytes");
    }
  }

  @Test(dataProvider = "varByteWriterVersions")
  public void testMultiValueFixedBytesAreExactForEachWriterVersion(int writerVersion)
      throws IOException {
    File file = new File(_tempDir, "varByteMV_v" + writerVersion);
    long expectedRawBytes = 0;
    try (VarByteChunkWriter writer = newVarByteWriter(file, writerVersion)) {
      writer.enableRawForwardIndexUncompressedValueSizeTracking();
      for (int i = 0; i < 100; i++) {
        int[] values = {i, i + 1, i + 2};
        expectedRawBytes += writerVersion == 4
            ? ArraySerDeUtils.serializeIntArrayWithLength(values).length
            : ArraySerDeUtils.serializeIntArrayWithoutLength(values).length;
        writer.putIntMV(values);
      }
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), expectedRawBytes,
          "Writer V" + writerVersion + " should report its exact serialized MV payload");
    }
  }

  @Test(dataProvider = "compressionTypes")
  public void testVarByteV4HugeValueTracksExactSize(ChunkCompressionType compressionType)
      throws IOException {
    File file = new File(_tempDir, "varByteHuge_" + compressionType.name());
    String value = "\u6771\u4eac-" + "x".repeat(128);
    long expectedRawBytes = value.getBytes(StandardCharsets.UTF_8).length;
    try (VarByteChunkForwardIndexWriterV4 writer =
        new VarByteChunkForwardIndexWriterV4(file, compressionType, 32)) {
      writer.enableRawForwardIndexUncompressedValueSizeTracking();
      writer.putString(value);
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), expectedRawBytes,
          "Huge chunks should report only the original value bytes for " + compressionType);
    }
  }

  @Test
  public void testLegacyVarByteWriterTracksUncompressedValueSize()
      throws IOException {
    // Covers the legacy V2/V3 VarByteChunkForwardIndexWriter.
    File file = new File(_tempDir, "legacyVarByte");
    try (VarByteChunkForwardIndexWriter writer =
        new VarByteChunkForwardIndexWriter(file, ChunkCompressionType.LZ4, 100, 10, 50, 2)) {
      writer.enableRawForwardIndexUncompressedValueSizeTracking();
      long expectedRawBytes = 0;
      for (int i = 0; i < 100; i++) {
        String value = "value_" + i;
        expectedRawBytes += value.getBytes(StandardCharsets.UTF_8).length;
        writer.putString(value);
      }
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), expectedRawBytes,
          "Legacy var-byte writers should report the exact UTF-8 value bytes");
    }
  }

  @Test
  public void testLegacyVarByteWriterTrackingDisabledReturnsUnavailable()
      throws IOException {
    File file = new File(_tempDir, "legacyVarByteDisabled");
    try (VarByteChunkForwardIndexWriter writer =
        new VarByteChunkForwardIndexWriter(file, ChunkCompressionType.LZ4, 100, 10, 50, 2)) {
      // tracking disabled by default
      for (int i = 0; i < 100; i++) {
        writer.putString("value_" + i);
      }
      assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), -1L,
          "Legacy VarByteChunkForwardIndexWriter should return the unavailable sentinel when tracking is disabled");
    }
  }

  @Test
  public void testTrackingCannotBeEnabledAfterWritingStarts()
      throws IOException {
    File legacyFile = new File(_tempDir, "legacyLateTracking");
    try (VarByteChunkForwardIndexWriter writer =
        new VarByteChunkForwardIndexWriter(legacyFile, ChunkCompressionType.LZ4, 10, 10, 50, 2)) {
      writer.putString("value");
      assertThrows(IllegalStateException.class, writer::enableRawForwardIndexUncompressedValueSizeTracking);
    }

    File v4File = new File(_tempDir, "v4LateTracking");
    try (VarByteChunkWriter writer = new VarByteChunkForwardIndexWriterV4(v4File, ChunkCompressionType.LZ4, 128)) {
      writer.putString("value");
      assertThrows(IllegalStateException.class, writer::enableRawForwardIndexUncompressedValueSizeTracking);
    }
  }

  @Test
  public void testPartialChunkAccountedInClose()
      throws IOException {
    // V4 normalizes 100 to 128 documents per chunk: three full chunks plus 116 remaining documents.
    // Tracking is per-chunk-flush: before close only 3 full chunks are counted (384 * 4 = 1536).
    // close() flushes the partial chunk, adding 116 * 4 = 464, giving total 500 * 4 = 2000.
    File file = new File(_tempDir, "partialChunk");
    int totalDocs = 500;
    int requestedDocsPerChunk = 100; // normalized to 128 by V4
    int normalizedDocsPerChunk = 128;
    FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, ChunkCompressionType.LZ4, totalDocs,
            requestedDocsPerChunk, Integer.BYTES, 4);
    writer.enableRawForwardIndexUncompressedValueSizeTracking();
    for (int i = 0; i < totalDocs; i++) {
      writer.putInt(i);
    }

    // getRawForwardIndexUncompressedValueSizeInBytes() includes in-flight bytes from the current unflushed chunk,
    // so it returns the correct total even before close().
    long expectedTotal = (long) totalDocs * Integer.BYTES;
    assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), expectedTotal,
        "Before close, getRawForwardIndexUncompressedValueSizeInBytes() should include in-flight bytes");

    // Closing flushes the partial chunk and clears the in-flight byte count without changing the total.
    writer.close();
    assertEquals(writer.getRawForwardIndexUncompressedValueSizeInBytes(), expectedTotal,
        "After close, total uncompressed size should be unchanged");
  }

  private static VarByteChunkWriter newVarByteWriter(File file, int writerVersion)
      throws IOException {
    switch (writerVersion) {
      case 4:
        return new VarByteChunkForwardIndexWriterV4(file, ChunkCompressionType.LZ4, 128);
      case 5:
        return new VarByteChunkForwardIndexWriterV5(file, ChunkCompressionType.LZ4, 128);
      case 6:
        return new VarByteChunkForwardIndexWriterV6(file, ChunkCompressionType.LZ4, 128);
      default:
        throw new IllegalArgumentException("Unsupported writer version: " + writerVersion);
    }
  }
}
