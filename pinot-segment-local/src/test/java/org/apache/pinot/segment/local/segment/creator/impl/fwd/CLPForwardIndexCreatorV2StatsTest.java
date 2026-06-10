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
package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV5;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for CLP V2 sub-writer uncompressed size tracking and flag-disabled behavior (T010).
 *
 * <p>Since constructing a full {@code CLPForwardIndexCreatorV2} requires a complex
 * {@code CLPMutableForwardIndexV2} setup, these tests validate the {@code setTrackUncompressedSize}
 * and {@code getUncompressedSize} methods directly on the underlying writers used by CLP:
 * {@link FixedByteChunkForwardIndexWriter} and {@link VarByteChunkForwardIndexWriterV5}.
 */
public class CLPForwardIndexCreatorV2StatsTest {
  private static final int TOTAL_DOCS = 1000;
  private static final int NUM_DOCS_PER_CHUNK = 100;
  private static final int SIZE_OF_INT_ENTRY = Integer.BYTES;
  private static final int WRITER_VERSION = 4;
  private static final int VAR_BYTE_CHUNK_SIZE = 65536;
  private static final ChunkCompressionType COMPRESSION_TYPE = ChunkCompressionType.ZSTANDARD;
  private static final int NUM_ENTRIES_TO_WRITE = 500;

  private File _tempDir;

  @BeforeMethod
  public void setUp() {
    _tempDir = new File(FileUtils.getTempDirectory(), CLPForwardIndexCreatorV2StatsTest.class.getSimpleName());
    FileUtils.deleteQuietly(_tempDir);
    _tempDir.mkdirs();
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  /**
   * Verifies that when tracking is disabled on a {@link FixedByteChunkForwardIndexWriter},
   * {@code getUncompressedSize()} returns 0 after writing data.
   */
  @Test
  public void testFixedByteWriterTrackingDisabled()
      throws IOException {
    File outputFile = new File(_tempDir, "fixed_byte_tracking_disabled.raw");
    try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(
        outputFile, COMPRESSION_TYPE, TOTAL_DOCS, NUM_DOCS_PER_CHUNK, SIZE_OF_INT_ENTRY, WRITER_VERSION)) {
      writer.setTrackUncompressedSize(false);
      for (int i = 0; i < NUM_ENTRIES_TO_WRITE; i++) {
        writer.putInt(i);
      }
      assertEquals(writer.getUncompressedSize(), 0L,
          "Uncompressed size should be 0 when tracking is disabled");
    }
  }

  /**
   * Verifies that with tracking explicitly enabled on a {@link FixedByteChunkForwardIndexWriter},
   * {@code getUncompressedSize()} returns a value greater than 0 after writing data.
   */
  @Test
  public void testFixedByteWriterTrackingEnabled()
      throws IOException {
    File outputFile = new File(_tempDir, "fixed_byte_tracking_enabled.raw");
    try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(
        outputFile, COMPRESSION_TYPE, TOTAL_DOCS, NUM_DOCS_PER_CHUNK, SIZE_OF_INT_ENTRY, WRITER_VERSION)) {
      writer.setTrackUncompressedSize(true);
      for (int i = 0; i < NUM_ENTRIES_TO_WRITE; i++) {
        writer.putInt(i);
      }
      assertTrue(writer.getUncompressedSize() > 0,
          "Uncompressed size should be greater than 0 when tracking is enabled");
    }
  }

  /**
   * Verifies that when tracking is disabled on a {@link VarByteChunkForwardIndexWriterV5},
   * {@code getUncompressedSize()} returns 0 after writing and closing.
   *
   * <p>The VarByte V4/V5 writer only records uncompressed size when a chunk is flushed
   * (either when the chunk buffer fills up or during {@code close()}), so we must close
   * the writer before asserting.
   */
  @Test
  public void testVarByteV5WriterTrackingDisabled()
      throws IOException {
    File outputFile = new File(_tempDir, "var_byte_v5_tracking_disabled.raw");
    VarByteChunkForwardIndexWriterV5 writer = new VarByteChunkForwardIndexWriterV5(
        outputFile, COMPRESSION_TYPE, VAR_BYTE_CHUNK_SIZE);
    try {
      writer.setTrackUncompressedSize(false);
      for (int i = 0; i < NUM_ENTRIES_TO_WRITE; i++) {
        writer.putString("test-string-value-" + i);
      }
    } finally {
      writer.close();
    }
    assertEquals(writer.getUncompressedSize(), 0L,
        "Uncompressed size should be 0 when tracking is disabled");
  }

  /**
   * Verifies that with tracking explicitly enabled on a {@link VarByteChunkForwardIndexWriterV5},
   * {@code getUncompressedSize()} returns a value greater than 0 after writing and closing.
   *
   * <p>The VarByte V4/V5 writer only records uncompressed size when a chunk is flushed
   * (either when the chunk buffer fills up or during {@code close()}), so we must close
   * the writer before asserting.
   */
  @Test
  public void testVarByteV5WriterTrackingEnabled()
      throws IOException {
    File outputFile = new File(_tempDir, "var_byte_v5_tracking_enabled.raw");
    VarByteChunkForwardIndexWriterV5 writer = new VarByteChunkForwardIndexWriterV5(
        outputFile, COMPRESSION_TYPE, VAR_BYTE_CHUNK_SIZE);
    try {
      writer.setTrackUncompressedSize(true);
      for (int i = 0; i < NUM_ENTRIES_TO_WRITE; i++) {
        writer.putString("test-string-value-" + i);
      }
    } finally {
      writer.close();
    }
    assertTrue(writer.getUncompressedSize() > 0,
        "Uncompressed size should be greater than 0 when tracking is enabled");
  }
}
