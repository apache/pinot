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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBytePower2ChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Backward-compatibility test for [ForwardIndexReaderFactory].
///
/// Verifies that segments written with the pre-existing V4 (power-of-2 chunk) format remain
/// readable through [ForwardIndexReaderFactory#createRawIndexReader()] after the V7 dispatch
/// path was added. If the factory's dispatch logic incorrectly misroutes a V4 buffer to the V7
/// reader (or throws), these tests will catch the regression.
public class ForwardIndexReaderFactoryBackwardCompatTest implements PinotBuffersAfterMethodCheckRule {

  private static final int NUM_VALUES = 5_003;
  private static final int NUM_DOCS_PER_CHUNK = 1_024; // power-of-2 for V4
  private static final String TEST_FILE_PREFIX =
      System.getProperty("java.io.tmpdir") + File.separator + "FwdIdxFactoryBackCompatTest";

  @DataProvider(name = "compressionAndType")
  public static Object[][] compressionAndType() {
    return new Object[][]{
        {ChunkCompressionType.PASS_THROUGH, DataType.INT},
        {ChunkCompressionType.LZ4, DataType.INT},
        {ChunkCompressionType.ZSTANDARD, DataType.INT},
        {ChunkCompressionType.PASS_THROUGH, DataType.LONG},
        {ChunkCompressionType.LZ4, DataType.LONG},
        {ChunkCompressionType.ZSTANDARD, DataType.LONG},
    };
  }

  /// Writes a V4 forward index and reads it back through the factory, verifying every value.
  /// This is the core backward-compatibility regression guard: if the factory routes V4 buffers
  /// to the wrong reader after the V7 dispatch was added, reads will return wrong values.
  @Test(dataProvider = "compressionAndType")
  public void testV4SegmentReadableViaFactory(ChunkCompressionType compressionType, DataType dataType)
      throws Exception {
    File file = new File(TEST_FILE_PREFIX + "_" + compressionType + "_" + dataType + ".fwd");
    FileUtils.deleteQuietly(file);

    // Write V4 segment using the legacy writer
    int entryBytes = dataType == DataType.LONG ? Long.BYTES : Integer.BYTES;
    try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(file, compressionType,
        NUM_VALUES, NUM_DOCS_PER_CHUNK, entryBytes, FixedBytePower2ChunkSVForwardIndexReader.VERSION)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        if (dataType == DataType.LONG) {
          writer.putLong((long) i * 1_000_000_000L);
        } else {
          writer.putInt(i);
        }
      }
    }

    // Read back through the factory — this exercises the dispatch logic
    ForwardIndexReaderFactory factory = ForwardIndexReaderFactory.getInstance();
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        ForwardIndexReader<ChunkReaderContext> reader = factory.createRawIndexReader(buffer, dataType, true);
        ChunkReaderContext ctx = reader.createContext()) {
      // Legacy V4 readers must return null from getCodecSpec(): the V7-detection branch in
      // ForwardIndexHandler treats a non-null spec as the positive signal for the codec-pipeline
      // path and would misroute legacy segments if the default ever changed.
      org.testng.Assert.assertNull(reader.getCodecSpec(),
          "Legacy V4 reader must return null for getCodecSpec()");
      for (int i = 0; i < NUM_VALUES; i++) {
        if (dataType == DataType.LONG) {
          assertEquals(reader.getLong(i, ctx), (long) i * 1_000_000_000L,
              "Mismatch at index " + i + " for " + compressionType + "/" + dataType);
        } else {
          assertEquals(reader.getInt(i, ctx), i,
              "Mismatch at index " + i + " for " + compressionType + "/" + dataType);
        }
      }
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  /// Verifies that requesting a non-existent version between V4 and V7 throws rather than silently
  /// misreading. V5 and V6 for fixed-byte SV do not exist; the factory must reject them.
  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testUnknownVersionThrows()
      throws Exception {
    File file = new File(TEST_FILE_PREFIX + "_unknownVersion.fwd");
    FileUtils.deleteQuietly(file);
    // Manually construct a tiny buffer whose first 4 bytes are version 5 (non-existent for fixed SV)
    // Version 5 is > V4 but < V7 — the factory must throw.
    try (org.apache.pinot.segment.spi.memory.PinotDataBuffer buffer =
        org.apache.pinot.segment.spi.memory.PinotDataBuffer.allocateDirect(8,
            java.nio.ByteOrder.BIG_ENDIAN, "test")) {
      buffer.putInt(0, 5); // fake version 5
      ForwardIndexReaderFactory.getInstance().createRawIndexReader(buffer, DataType.INT, true);
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }
}
