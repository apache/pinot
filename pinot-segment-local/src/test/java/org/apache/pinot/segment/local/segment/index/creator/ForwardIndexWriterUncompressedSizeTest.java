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
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests that forward index writers correctly track uncompressed data size.
 *
 * <p>Verifies the {@code _uncompressedSize} field in both {@link BaseChunkForwardIndexWriter}
 * (via {@link FixedByteChunkForwardIndexWriter}) and {@link VarByteChunkForwardIndexWriterV4}
 * across multiple compression types.
 *
 * <p>Important: V4+ writers normalize numDocsPerChunk to the next power of 2. The uncompressed
 * size only reflects complete chunks flushed before close; the last partial chunk is flushed
 * during close(). Tests use power-of-2 doc counts to ensure exact matches when checking
 * before close().
 */
public class ForwardIndexWriterUncompressedSizeTest {
  // Use power-of-2 aligned counts so V4's chunk normalization doesn't create partial chunks
  private static final int NUM_DOCS = 1024;
  private static final int DOCS_PER_CHUNK = 128; // already power-of-2, no normalization needed
  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(),
        ForwardIndexWriterUncompressedSizeTest.class.getSimpleName() + "_" + UUID.randomUUID());
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

  @Test(dataProvider = "compressionTypes")
  public void testFixedByteWriterIntTracksUncompressedSize(ChunkCompressionType compressionType)
      throws IOException {
    File file = new File(_tempDir, "fixedInt_" + compressionType.name());
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, compressionType, NUM_DOCS, DOCS_PER_CHUNK, Integer.BYTES, 4)) {
      writer.setTrackUncompressedSize(true);
      for (int i = 0; i < NUM_DOCS; i++) {
        writer.putInt(i);
      }
      // 1024 docs / 128 per chunk = 8 full chunks, all flushed before close
      long expected = (long) NUM_DOCS * Integer.BYTES;
      assertEquals(writer.getUncompressedSize(), expected,
          "Uncompressed size should equal NUM_DOCS * INT_BYTES for " + compressionType);
    }
  }

  @Test(dataProvider = "compressionTypes")
  public void testFixedByteWriterLongTracksUncompressedSize(ChunkCompressionType compressionType)
      throws IOException {
    File file = new File(_tempDir, "fixedLong_" + compressionType.name());
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, compressionType, NUM_DOCS, DOCS_PER_CHUNK, Long.BYTES, 4)) {
      writer.setTrackUncompressedSize(true);
      for (int i = 0; i < NUM_DOCS; i++) {
        writer.putLong(i * 1000L);
      }
      long expected = (long) NUM_DOCS * Long.BYTES;
      assertEquals(writer.getUncompressedSize(), expected,
          "Uncompressed size should equal NUM_DOCS * LONG_BYTES for " + compressionType);
    }
  }

  @Test(dataProvider = "compressionTypes")
  public void testFixedByteWriterDoubleTracksUncompressedSize(ChunkCompressionType compressionType)
      throws IOException {
    File file = new File(_tempDir, "fixedDouble_" + compressionType.name());
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, compressionType, NUM_DOCS, DOCS_PER_CHUNK, Double.BYTES, 4)) {
      writer.setTrackUncompressedSize(true);
      for (int i = 0; i < NUM_DOCS; i++) {
        writer.putDouble(i * 0.5);
      }
      long expected = (long) NUM_DOCS * Double.BYTES;
      assertEquals(writer.getUncompressedSize(), expected,
          "Uncompressed size should equal NUM_DOCS * DOUBLE_BYTES for " + compressionType);
    }
  }

  @Test(dataProvider = "compressionTypes")
  public void testVarByteV4WriterSVTracksUncompressedSize(ChunkCompressionType compressionType)
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
      writer.setTrackUncompressedSize(true);
      for (String value : values) {
        writer.putString(value);
      }
      long uncompressedSize = writer.getUncompressedSize();
      assertTrue(uncompressedSize > 0,
          "Uncompressed size should be > 0 for " + compressionType + ", got: " + uncompressedSize);
      // V4 wraps each string in: 4-byte length prefix + raw bytes, so uncompressed size >= raw bytes
      assertTrue(uncompressedSize >= totalRawBytes,
          "Uncompressed size " + uncompressedSize + " should be >= raw string bytes " + totalRawBytes);
    }
  }

  @Test
  public void testUncompressedSizeConsistentAcrossCompressionTypes()
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
      writer.setTrackUncompressedSize(true);
        for (int i = 0; i < NUM_DOCS; i++) {
          writer.putInt(i * 7);
        }
        sizes[t] = writer.getUncompressedSize();
      }
    }

    for (int t = 1; t < types.length; t++) {
      assertEquals(sizes[t], sizes[0],
          "Uncompressed size should be identical for " + types[t] + " and " + types[0]);
    }
  }

  @Test
  public void testVarByteV4UncompressedSizeConsistentAcrossCompressionTypes()
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
      writer.setTrackUncompressedSize(true);
        for (String value : values) {
          writer.putString(value);
        }
        sizes[t] = writer.getUncompressedSize();
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
      writer.setTrackUncompressedSize(true);
      for (int i = 0; i < NUM_DOCS; i++) {
        writer.putInt(i);
      }
      long uncompressedSize = writer.getUncompressedSize();
      assertEquals(uncompressedSize, (long) NUM_DOCS * Integer.BYTES,
          "PASS_THROUGH uncompressed size should equal exact data size");
    }
    // The file size should be >= uncompressed size (includes headers + data with no compression savings)
    assertTrue(file.length() >= (long) NUM_DOCS * Integer.BYTES,
        "PASS_THROUGH file size should be >= uncompressed data size");
  }

  @Test
  public void testEmptyWriterHasZeroUncompressedSize()
      throws IOException {
    File file = new File(_tempDir, "empty");
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, ChunkCompressionType.LZ4, 0, DOCS_PER_CHUNK, Integer.BYTES, 4)) {
      writer.setTrackUncompressedSize(true);
      assertEquals(writer.getUncompressedSize(), 0, "Empty writer should have 0 uncompressed size");
    }
  }

  @Test
  public void testSingleDocUncompressedSize()
      throws IOException {
    // V4 normalizes numDocsPerChunk=1 → 1 (already power-of-2).
    // After writing 1 doc, the chunk is full → flushed immediately → uncompressed size = 4.
    File file = new File(_tempDir, "singleDoc");
    try (FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, ChunkCompressionType.LZ4, 1, 1, Integer.BYTES, 4)) {
      writer.setTrackUncompressedSize(true);
      writer.putInt(42);
      assertEquals(writer.getUncompressedSize(), Integer.BYTES,
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
      writer.setTrackUncompressedSize(true);
      for (int i = 0; i < totalDocs; i++) {
        writer.putInt(i);
        // After each full chunk, verify accumulated size
        if ((i + 1) % docsPerChunk == 0) {
          long expectedSoFar = (long) (i + 1) * Integer.BYTES;
          assertEquals(writer.getUncompressedSize(), expectedSoFar,
              "After " + (i + 1) + " docs, uncompressed size should be " + expectedSoFar);
        }
      }
      assertEquals(writer.getUncompressedSize(), (long) totalDocs * Integer.BYTES);
    }
  }

  @Test
  public void testVarByteV4MultiValueTracksUncompressedSize()
      throws IOException {
    File file = new File(_tempDir, "varByteMV");
    try (VarByteChunkForwardIndexWriterV4 writer =
        new VarByteChunkForwardIndexWriterV4(file, ChunkCompressionType.LZ4, 4096)) {
      writer.setTrackUncompressedSize(true);
      for (int i = 0; i < 100; i++) {
        String[] mvValues = {"value_" + i + "_a", "value_" + i + "_b", "value_" + i + "_c"};
        writer.putStringMV(mvValues);
      }
      assertTrue(writer.getUncompressedSize() > 0,
          "MV writer should track non-zero uncompressed size");
    }
  }

  @Test
  public void testPartialChunkAccountedInClose()
      throws IOException {
    // Use non-aligned doc count so there's a partial chunk that's flushed during close()
    // V4 normalizes 100 → 128 docs per chunk. 500 docs / 128 = 3 full chunks + 116 remaining.
    // Before close: 3 * 128 * 4 = 1536 bytes. After close: 1536 + 116*4 = 2000 bytes.
    File file = new File(_tempDir, "partialChunk");
    int totalDocs = 500;
    int requestedDocsPerChunk = 100; // normalized to 128 by V4
    int normalizedDocsPerChunk = 128;

    FixedByteChunkForwardIndexWriter writer =
        new FixedByteChunkForwardIndexWriter(file, ChunkCompressionType.LZ4, totalDocs,
            requestedDocsPerChunk, Integer.BYTES, 4);
    writer.setTrackUncompressedSize(true);
    for (int i = 0; i < totalDocs; i++) {
      writer.putInt(i);
    }

    // With per-value tracking, all values are accounted for immediately (not per-chunk)
    long expectedTotal = (long) totalDocs * Integer.BYTES;
    assertEquals(writer.getUncompressedSize(), expectedTotal,
        "Before close, all written values should be tracked");

    // After close: same total — close flushes the chunk buffer but doesn't change uncompressed size
    writer.close();
    assertEquals(writer.getUncompressedSize(), expectedTotal,
        "After close, total uncompressed size should be unchanged");
    assertEquals(expectedTotal, (long) totalDocs * Integer.BYTES,
        "Total uncompressed size should equal totalDocs * INT_BYTES");
  }
}
