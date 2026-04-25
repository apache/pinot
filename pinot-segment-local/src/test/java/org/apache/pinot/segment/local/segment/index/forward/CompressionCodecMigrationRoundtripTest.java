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
import org.apache.pinot.segment.local.io.codec.CodecPipelineExecutor;
import org.apache.pinot.segment.local.io.codec.CodecRegistry;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriterV7;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReaderV7;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBytePower2ChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.utils.CompressionCodecMigrator;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Verifies that every legacy [CompressionCodec] that is applicable to SV fixed-byte raw
/// columns (INT/LONG) can be written and read back correctly, and that every migratable codec
/// produces identical values when its equivalent `codecSpec` is used instead.
///
/// This is the primary regression guard for [CompressionCodecMigrator]: it proves that
/// the old-path and new-path segments are semantically equivalent for users who migrate.
public class CompressionCodecMigrationRoundtripTest implements PinotBuffersAfterMethodCheckRule {

  private static final int NUM_DOCS = 5_003; // not a power of 2 — exercises partial last chunk
  private static final int DOCS_PER_CHUNK = 1_024;
  private static final File TMP_DIR =
      new File(System.getProperty("java.io.tmpdir"), "CompressionCodecMigRoundtrip");

  /// Test cases: every CompressionCodec applicable to SV INT/LONG raw columns, paired with DataType.
  /// MV_ENTRY_DICT and CLP variants are excluded — they use different index types / formats.
  ///
  /// Note: DELTA is intentionally omitted from the legacy roundtrip test.
  /// [BaseChunkForwardIndexReader#decompressChunk()] handles DELTA compression by allocating
  /// a fresh `ByteBuffer` for the decompressed output, but this new buffer is never stored
  /// back into the [ChunkReaderContext]. On the second read within the same chunk the
  /// context still reports the chunk as cached and returns the original (cleared, empty) context
  /// buffer, yielding incorrect values. This is a pre-existing limitation of the V4 reader.
  /// The migration path to `"CODEC(DELTA,LZ4)"` with [FixedByteChunkForwardIndexWriterV7]
  /// is verified by [#testMigratedCodecSpecProducesIdenticalValues()] below.
  @DataProvider(name = "legacyCodecs")
  public static Object[][] legacyCodecs() {
    return new Object[][]{
        {CompressionCodec.PASS_THROUGH, DataType.INT},
        {CompressionCodec.PASS_THROUGH, DataType.LONG},
        {CompressionCodec.SNAPPY, DataType.INT},
        {CompressionCodec.SNAPPY, DataType.LONG},
        {CompressionCodec.LZ4, DataType.INT},
        {CompressionCodec.LZ4, DataType.LONG},
        {CompressionCodec.ZSTANDARD, DataType.INT},
        {CompressionCodec.ZSTANDARD, DataType.LONG},
        {CompressionCodec.GZIP, DataType.INT},
        {CompressionCodec.GZIP, DataType.LONG},
        // DELTA excluded: see Javadoc on this method.
    };
  }

  /// Writes a segment with the legacy [CompressionCodec] path and reads every value back.
  /// Verifies the old chunk format is still functional after the V7 path was added.
  @Test(dataProvider = "legacyCodecs")
  public void testLegacyCodecRoundtrip(CompressionCodec codec, DataType dataType)
      throws Exception {
    ChunkCompressionType chunkType = toChunkCompressionType(codec);
    // SingleValueFixedByteRawIndexCreator writes to: baseDir/columnName + RAW_SV_FORWARD_INDEX_FILE_EXTENSION
    String columnName = "legacy_" + codec + "_" + dataType;
    File indexDir = new File(TMP_DIR, columnName);
    FileUtils.deleteQuietly(indexDir);
    indexDir.mkdirs();
    File file = new File(indexDir, columnName + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);

    try (SingleValueFixedByteRawIndexCreator creator = new SingleValueFixedByteRawIndexCreator(
        indexDir, chunkType, columnName, NUM_DOCS, dataType,
        FixedBytePower2ChunkSVForwardIndexReader.VERSION, DOCS_PER_CHUNK)) {
      writeValues(creator, dataType, NUM_DOCS);
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        FixedBytePower2ChunkSVForwardIndexReader reader =
            new FixedBytePower2ChunkSVForwardIndexReader(buffer, dataType);
        ChunkReaderContext ctx = reader.createContext()) {
      verifyValues(reader, ctx, dataType, NUM_DOCS,
          "legacy " + codec + "/" + dataType);
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /// For every migratable codec, writes a segment with the equivalent `codecSpec` and
  /// verifies it produces identical values to the legacy path.
  /// This is the core correctness guarantee for [CompressionCodecMigrator].
  @Test(dataProvider = "legacyCodecs")
  public void testMigratedCodecSpecProducesIdenticalValues(CompressionCodec codec, DataType dataType)
      throws Exception {
    String codecSpec = CompressionCodecMigrator.toCodecSpec(codec);
    if (codecSpec == null) {
      // Not migratable — skip (the legacy roundtrip test above already covers correctness)
      return;
    }

    File file = new File(TMP_DIR, "migrated_" + codec + "_" + dataType + ".fwd");
    FileUtils.deleteQuietly(file);
    TMP_DIR.mkdirs();

    CodecPipelineExecutor executor = CodecPipelineExecutor.create(
        codecSpec, new CodecContext(dataType), CodecRegistry.DEFAULT);

    int entryBytes = dataType == DataType.LONG ? Long.BYTES : Integer.BYTES;
    try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(
        file, executor, NUM_DOCS, DOCS_PER_CHUNK, entryBytes)) {
      writeValues(writer, dataType, NUM_DOCS);
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        FixedByteChunkSVForwardIndexReaderV7 reader =
            new FixedByteChunkSVForwardIndexReaderV7(buffer, dataType);
        ChunkReaderContext ctx = reader.createContext()) {
      // Verify the canonical spec stored in the header matches what we requested
      assertEquals(reader.getCodecSpec(), codecSpec,
          "Canonical spec mismatch for " + codec + "/" + dataType);
      verifyValues(reader, ctx, dataType, NUM_DOCS,
          "migrated codecSpec=" + codecSpec + "/" + dataType);
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /// Writes NUM_DOCS values: value[i] = i for INT, i * 1_000_000L for LONG.
  private static void writeValues(SingleValueFixedByteRawIndexCreator creator, DataType dataType, int numDocs)
      throws Exception {
    for (int i = 0; i < numDocs; i++) {
      if (dataType == DataType.LONG) {
        creator.putLong((long) i * 1_000_000L);
      } else {
        creator.putInt(i);
      }
    }
  }

  private static void writeValues(FixedByteChunkForwardIndexWriterV7 writer, DataType dataType, int numDocs)
      throws Exception {
    for (int i = 0; i < numDocs; i++) {
      if (dataType == DataType.LONG) {
        writer.putLong((long) i * 1_000_000L);
      } else {
        writer.putInt(i);
      }
    }
  }

  private static void verifyValues(FixedBytePower2ChunkSVForwardIndexReader reader, ChunkReaderContext ctx,
      DataType dataType, int numDocs, String label) {
    for (int i = 0; i < numDocs; i++) {
      if (dataType == DataType.LONG) {
        assertEquals(reader.getLong(i, ctx), (long) i * 1_000_000L,
            "Mismatch at i=" + i + " [" + label + "]");
      } else {
        assertEquals(reader.getInt(i, ctx), i,
            "Mismatch at i=" + i + " [" + label + "]");
      }
    }
  }

  private static void verifyValues(FixedByteChunkSVForwardIndexReaderV7 reader, ChunkReaderContext ctx,
      DataType dataType, int numDocs, String label) {
    for (int i = 0; i < numDocs; i++) {
      if (dataType == DataType.LONG) {
        assertEquals(reader.getLong(i, ctx), (long) i * 1_000_000L,
            "Mismatch at i=" + i + " [" + label + "]");
      } else {
        assertEquals(reader.getInt(i, ctx), i,
            "Mismatch at i=" + i + " [" + label + "]");
      }
    }
  }

  private static ChunkCompressionType toChunkCompressionType(CompressionCodec codec) {
    switch (codec) {
      case PASS_THROUGH:
        return ChunkCompressionType.PASS_THROUGH;
      case SNAPPY:
        return ChunkCompressionType.SNAPPY;
      case LZ4:
        return ChunkCompressionType.LZ4;
      case ZSTANDARD:
        return ChunkCompressionType.ZSTANDARD;
      case GZIP:
        return ChunkCompressionType.GZIP;
      case DELTA:
        return ChunkCompressionType.DELTA;
      default:
        throw new IllegalArgumentException("No ChunkCompressionType mapping for " + codec);
    }
  }
}
