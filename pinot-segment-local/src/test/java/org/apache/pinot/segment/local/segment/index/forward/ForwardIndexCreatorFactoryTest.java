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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.codec.CodecPipelineExecutor;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriterV7;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.CompressionStatsTrackingForwardIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReaderV7;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBytePower2ChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.codec.CodecSpecParser;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests [ForwardIndexCreatorFactory]'s forward-index encoding branch selection. Each test uses an isolated
/// temporary index directory and does not share mutable state.
public class ForwardIndexCreatorFactoryTest {
  private static final String COLUMN_NAME = "testCol";

  @Test
  public void testRawEncodingBuildsRawForwardIndexEvenWithDictionary()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try (ForwardIndexCreator creator = ForwardIndexCreatorFactory.createIndexCreator(newContext(indexDir, true),
        new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW).build())) {
      assertFalse(creator.isDictionaryEncoded());
      assertEquals(creator.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4);
      creator.putInt(1);
      creator.seal();
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @Test
  public void testCreatorReportsExplicitCompressionType()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
        .withCompressionType(ChunkCompressionType.SNAPPY)
        .build();
    try (ForwardIndexCreator creator = ForwardIndexCreatorFactory.createIndexCreator(newContext(indexDir, true),
        config)) {
      assertEquals(creator.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.SNAPPY);
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @Test
  public void testDefaultEncodingBuildsDictionaryForwardIndex()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try (ForwardIndexCreator creator =
        ForwardIndexCreatorFactory.createIndexCreator(newContext(indexDir, true),
            ForwardIndexConfig.getDefault(FieldConfig.EncodingType.DICTIONARY))) {
      assertTrue(creator.isDictionaryEncoded());
      creator.putDictId(0);
      creator.seal();
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @Test
  public void testTableConfigConstructorPropagatesCompressionStatsFlag()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
      tableConfig.getIndexingConfig().setCompressionStatsEnabled(true);
      assertTrue(newContext(indexDir, true, tableConfig).isCompressionStatsEnabled());
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @DataProvider(name = "v7CodecSpecs")
  public Object[][] v7CodecSpecs() {
    return new Object[][]{
        {"LZ4", DataType.INT, false}, {"LZ4", DataType.INT, true},
        {"DELTA,LZ4", DataType.INT, false}, {"DELTA,LZ4", DataType.INT, true},
        {"ZSTD(3)", DataType.LONG, false}, {"ZSTD(3)", DataType.LONG, true},
        {"DELTADELTA,GORILLA,ZSTD(3)", DataType.LONG, false},
        {"DELTADELTA,GORILLA,ZSTD(3)", DataType.LONG, true}
    };
  }

  /// Both compression-only and transform pipelines use V7, including a partial final chunk.
  @Test(dataProvider = "v7CodecSpecs")
  public void testCodecSpecRoundTripUsesV7Format(String codecSpec, DataType storedType, boolean compressionStatsEnabled)
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
          .withCodecSpec(codecSpec)
          .withTargetDocsPerChunk(2)
          .build();
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
      tableConfig.getIndexingConfig().setCompressionStatsEnabled(compressionStatsEnabled);
      long[] values = storedType == DataType.INT
          ? new long[]{11, 13, 21}
          : new long[]{Long.MIN_VALUE, (long) Integer.MAX_VALUE + 1, Long.MAX_VALUE};
      try (ForwardIndexCreator creator = ForwardIndexCreatorFactory.createIndexCreator(
          newContext(indexDir, false, tableConfig, values.length, storedType), config)) {
        assertFalse(creator.isDictionaryEncoded());
        assertNull(creator.getRawForwardIndexChunkCompressionType());
        assertEquals(creator.getRawForwardIndexUncompressedValueSizeInBytes(), -1L);
        for (long value : values) {
          if (storedType == DataType.INT) {
            creator.putInt((int) value);
          } else {
            creator.putLong(value);
          }
          assertEquals(creator.getRawForwardIndexUncompressedValueSizeInBytes(), -1L);
        }
        creator.seal();
        assertEquals(creator.getRawForwardIndexUncompressedValueSizeInBytes(), -1L);
      }
      File indexFile = new File(indexDir, COLUMN_NAME + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
      assertTrue(indexFile.exists());
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
          ForwardIndexReader<?> reader =
              ForwardIndexReaderFactory.getInstance().createRawIndexReader(buffer, storedType, true)) {
        assertTrue(reader instanceof FixedByteChunkSVForwardIndexReaderV7,
            "codecSpec was routed to " + reader.getClass().getSimpleName());
        assertEquals(buffer.getInt(0), FixedByteChunkSVForwardIndexReaderV7.VERSION);
        assertEquals(FixedByteChunkSVForwardIndexReaderV7.readCodecSpec(buffer), codecSpec);
        FixedByteChunkSVForwardIndexReaderV7 v7Reader = (FixedByteChunkSVForwardIndexReaderV7) reader;
        try (FixedByteChunkSVForwardIndexReaderV7.Context context = v7Reader.createContext()) {
          for (int i = 0; i < values.length; i++) {
            assertEquals(storedType == DataType.INT ? v7Reader.getInt(i, context) : v7Reader.getLong(i, context),
                values[i]);
          }
        }
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /// Realistic-volume round trip: many chunks, a non-power-of-two target normalized to 1024 docs per
  /// chunk, a partial final chunk, and reads through both a sequential and a random-access context.
  @Test(dataProvider = "v7CodecSpecs")
  public void testCodecSpecMultiChunkRoundTrip(String codecSpec, DataType storedType, boolean compressionStatsEnabled)
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      int numDocs = 10009;
      Random random = new Random(42);
      long[] values = new long[numDocs];
      for (int i = 0; i < numDocs; i++) {
        values[i] = storedType == DataType.INT ? random.nextInt() : random.nextLong();
      }
      ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
          .withCodecSpec(codecSpec)
          .withTargetDocsPerChunk(1000)
          .build();
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
      tableConfig.getIndexingConfig().setCompressionStatsEnabled(compressionStatsEnabled);
      try (ForwardIndexCreator creator = ForwardIndexCreatorFactory.createIndexCreator(
          newContext(indexDir, false, tableConfig, numDocs, storedType), config)) {
        for (long value : values) {
          if (storedType == DataType.INT) {
            creator.putInt((int) value);
          } else {
            creator.putLong(value);
          }
        }
        creator.seal();
      }
      File indexFile = new File(indexDir, COLUMN_NAME + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
          ForwardIndexReader<?> reader =
              ForwardIndexReaderFactory.getInstance().createRawIndexReader(buffer, storedType, true)) {
        FixedByteChunkSVForwardIndexReaderV7 v7Reader = (FixedByteChunkSVForwardIndexReaderV7) reader;
        // Header ints: version, magic, numChunks, numDocsPerChunk, sizeOfEntry, totalDocs
        assertEquals(buffer.getInt(3 * Integer.BYTES), 1024, "targetDocsPerChunk=1000 should normalize to 1024");
        assertEquals(buffer.getInt(2 * Integer.BYTES), (numDocs + 1023) / 1024);
        assertEquals(buffer.getInt(5 * Integer.BYTES), numDocs);
        try (FixedByteChunkSVForwardIndexReaderV7.Context sequential = v7Reader.createContext();
            FixedByteChunkSVForwardIndexReaderV7.Context randomAccess = v7Reader.createContext()) {
          for (int docId = 0; docId < numDocs; docId++) {
            assertEquals(readValue(v7Reader, storedType, docId, sequential), values[docId], "docId " + docId);
          }
          for (int i = 0; i < 2000; i++) {
            int docId = random.nextInt(numDocs);
            assertEquals(readValue(v7Reader, storedType, docId, randomAccess), values[docId], "docId " + docId);
          }
        }
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  private static long readValue(FixedByteChunkSVForwardIndexReaderV7 reader, DataType storedType, int docId,
      FixedByteChunkSVForwardIndexReaderV7.Context context) {
    return storedType == DataType.INT ? reader.getInt(docId, context) : reader.getLong(docId, context);
  }

  /// The V7 writer rejects shapes it cannot represent and refuses to seal a file whose declared document
  /// count does not match what was written; the reader rejects a mismatched stored type and reads after
  /// its context is closed.
  @Test
  public void testV7WriterAndReaderGuards()
      throws Exception {
    assertTrue(CodecSpecParser.MAX_SPEC_LENGTH <= FixedByteChunkForwardIndexWriterV7.MAX_CODEC_SPEC_LENGTH_BYTES,
        "The DSL parser limit must not exceed the frozen V7 header limit");
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      File indexFile = new File(indexDir, COLUMN_NAME + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
      CodecPipelineExecutor intExecutor = CodecPipelineExecutor.create("DELTA,LZ4", DataType.INT);
      expectThrows(IllegalArgumentException.class,
          () -> new FixedByteChunkForwardIndexWriterV7(indexFile, intExecutor, 3, 2, Long.BYTES));
      try (FixedByteChunkForwardIndexWriterV7 writer =
          new FixedByteChunkForwardIndexWriterV7(indexFile, intExecutor, 3, 2, Integer.BYTES)) {
        expectThrows(UnsupportedOperationException.class, () -> writer.putFloat(1.0f));
        expectThrows(UnsupportedOperationException.class, () -> writer.putDouble(1.0));
        expectThrows(IllegalStateException.class, () -> writer.putLong(1L));
        writer.putInt(1);
        writer.putInt(2);
        writer.putInt(3);
        expectThrows(IllegalStateException.class, () -> writer.putInt(4));
      }
      FixedByteChunkForwardIndexWriterV7 shortWriter =
          new FixedByteChunkForwardIndexWriterV7(indexFile, intExecutor, 3, 2, Integer.BYTES);
      shortWriter.putInt(1);
      expectThrows(IllegalStateException.class, shortWriter::close);

      try (FixedByteChunkForwardIndexWriterV7 writer = new FixedByteChunkForwardIndexWriterV7(indexFile,
          CodecPipelineExecutor.create("LZ4", DataType.LONG), 2, 2, Long.BYTES)) {
        writer.putLong(1L);
        writer.putLong(2L);
      }
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
        expectThrows(IllegalArgumentException.class,
            () -> new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT));
        expectThrows(UnsupportedOperationException.class,
            () -> ForwardIndexReaderFactory.getInstance().createRawIndexReader(buffer, DataType.DOUBLE, true));
        FixedByteChunkSVForwardIndexReaderV7 reader = new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.LONG);
        FixedByteChunkSVForwardIndexReaderV7.Context context = reader.createContext();
        assertEquals(reader.getLong(0, context), 1L);
        context.close();
        expectThrows(IllegalStateException.class, () -> reader.getLong(0, context));
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @DataProvider(name = "compressionStatsEnabled")
  public Object[][] compressionStatsEnabled() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "compressionStatsEnabled")
  public void testLegacyCompressionStatsRemainOptIn(boolean compressionStatsEnabled)
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      int[] values = {11, 13, 21};
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
      tableConfig.getIndexingConfig().setCompressionStatsEnabled(compressionStatsEnabled);
      ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
          .withCompressionType(ChunkCompressionType.LZ4).withRawIndexWriterVersion(4).withTargetDocsPerChunk(2).build();
      try (ForwardIndexCreator creator = ForwardIndexCreatorFactory.createIndexCreator(
          newContext(indexDir, false, tableConfig, values.length), config)) {
        CompressionStatsTrackingForwardIndexCreator trackingCreator =
            (CompressionStatsTrackingForwardIndexCreator) creator;
        assertEquals(creator.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4);
        assertEquals(creator.getRawForwardIndexUncompressedValueSizeInBytes(), compressionStatsEnabled ? 0L : -1L);
        for (int i = 0; i < values.length; i++) {
          creator.putInt(values[i]);
          assertEquals(creator.getRawForwardIndexUncompressedValueSizeInBytes(),
              compressionStatsEnabled ? (long) (i + 1) * Integer.BYTES : -1L);
          expectThrows(IllegalStateException.class,
              trackingCreator::enableRawForwardIndexUncompressedValueSizeTracking);
        }
        creator.seal();
        assertEquals(creator.getRawForwardIndexUncompressedValueSizeInBytes(),
            compressionStatsEnabled ? (long) values.length * Integer.BYTES : -1L);
      }
      File indexFile = new File(indexDir, COLUMN_NAME + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
          FixedBytePower2ChunkSVForwardIndexReader reader = new FixedBytePower2ChunkSVForwardIndexReader(buffer,
              DataType.INT);
          ChunkReaderContext context = reader.createContext()) {
        for (int i = 0; i < values.length; i++) {
          assertEquals(reader.getInt(i, context), values[i]);
        }
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /// Columns: mutation, expected failure type, expected message fragment, and whether the corruption is
  /// only detected on first read of the affected chunk (header-resident corruption fails at load).
  @DataProvider(name = "malformedV7Files")
  public Object[][] malformedV7Files() {
    return new Object[][]{
        {"metadata", IllegalArgumentException.class, "does not match segment metadata", false},
        {"chunkSize", IllegalArgumentException.class, "positive power of two", false},
        {"specLength", IllegalArgumentException.class, "Invalid specLength", false},
        {"offset", IllegalArgumentException.class, "Corrupt chunkOffsets[0]", false},
        {"offsetOrder", IllegalArgumentException.class, "Corrupt chunkOffsets[1]", false},
        {"truncated", IllegalArgumentException.class, "Corrupt per-chunk header", false},
        {"trailing", IllegalArgumentException.class, "Corrupt V7 data section", false},
        {"encodedSize", IllegalStateException.class, "Corrupt per-chunk header", true},
        {"gap", IllegalStateException.class, "exactPayloadBytes", true},
        {"decodedSize", IllegalStateException.class, "decodedSize", true}
    };
  }

  @Test(dataProvider = "malformedV7Files")
  public void testMalformedV7FileIsRejected(String mutation, Class<? extends RuntimeException> failureType,
      String message, boolean rejectedAtRead)
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
          .withCodecSpec("LZ4").withTargetDocsPerChunk(2).build();
      try (ForwardIndexCreator creator =
          ForwardIndexCreatorFactory.createIndexCreator(newContext(indexDir, false, 3), config)) {
        creator.putInt(11);
        creator.putInt(13);
        creator.putInt(21);
        creator.seal();
      }
      File indexFile = new File(indexDir, COLUMN_NAME + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
      byte[] bytes = Files.readAllBytes(indexFile.toPath());
      ByteBuffer header = ByteBuffer.wrap(bytes);
      int offsetTable = header.getInt(7 * Integer.BYTES);
      int firstFrame = Math.toIntExact(header.getLong(offsetTable));
      switch (mutation) {
        case "metadata":
          break;
        case "chunkSize":
          header.putInt(3 * Integer.BYTES, 3);
          break;
        case "specLength":
          header.putInt(6 * Integer.BYTES, 0);
          break;
        case "offset":
          header.putLong(offsetTable, bytes.length);
          break;
        case "offsetOrder":
          // Duplicate the first offset: chunkOffsets[1] no longer leaves room for chunk 0's header.
          header.putLong(offsetTable + Long.BYTES, firstFrame);
          break;
        case "truncated":
          bytes = Arrays.copyOf(bytes, bytes.length - 1);
          break;
        case "trailing":
          bytes = Arrays.copyOf(bytes, bytes.length + 1);
          break;
        case "encodedSize":
          header.putInt(firstFrame, -1);
          break;
        case "gap":
          // Shrink the first (non-final) frame by one byte; only the lazy exact-size check can see it.
          header.putInt(firstFrame, header.getInt(firstFrame) - 1);
          break;
        case "decodedSize":
          header.putInt(firstFrame + Integer.BYTES, Integer.MAX_VALUE);
          break;
        default:
          throw new AssertionError(mutation);
      }
      Files.write(indexFile.toPath(), bytes);
      ColumnMetadataImpl metadata = new ColumnMetadataImpl.Builder()
          .setFieldSpec(new DimensionFieldSpec(COLUMN_NAME, DataType.INT, true))
          .setTotalDocs(mutation.equals("metadata") ? 4 : 3).setHasDictionary(false).build();
      RuntimeException failure;
      if (rejectedAtRead) {
        // Construction must succeed: frame-level corruption is only detected when the chunk is read.
        try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
            ForwardIndexReader<?> reader =
                ForwardIndexReaderFactory.getInstance().createIndexReader(buffer, metadata)) {
          FixedByteChunkSVForwardIndexReaderV7 v7Reader = (FixedByteChunkSVForwardIndexReaderV7) reader;
          try (FixedByteChunkSVForwardIndexReaderV7.Context context = v7Reader.createContext()) {
            failure = expectThrows(failureType, () -> v7Reader.getInt(0, context));
          }
        }
      } else {
        failure = expectThrows(failureType, () -> {
          try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
              ForwardIndexReader<?> reader =
                  ForwardIndexReaderFactory.getInstance().createIndexReader(buffer, metadata)) {
            assertTrue(reader instanceof FixedByteChunkSVForwardIndexReaderV7);
          }
        });
      }
      assertTrue(failure.getMessage().contains(message), failure.getMessage());
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /// Version 7 alone does not select the codec-pipeline reader: legacy fixed-byte writers accept
  /// arbitrary versions greater than or equal to 4 and lack the V7 format magic.
  @Test
  public void testLegacyVersionSevenStillUsesLegacyReader()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      File indexFile = new File(indexDir, COLUMN_NAME + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
      try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(indexFile,
          ChunkCompressionType.LZ4, 3, 2, Integer.BYTES, FixedByteChunkSVForwardIndexReaderV7.VERSION)) {
        writer.putInt(11);
        writer.putInt(13);
        writer.putInt(21);
      }
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
          ForwardIndexReader<?> reader =
              ForwardIndexReaderFactory.getInstance().createRawIndexReader(buffer, DataType.INT, true)) {
        assertTrue(reader instanceof FixedBytePower2ChunkSVForwardIndexReader,
            "legacy version 7 was routed to " + reader.getClass().getSimpleName());
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  private static IndexCreationContext newContext(File indexDir, boolean hasDictionary) {
    return newContext(indexDir, hasDictionary, 1);
  }

  private static IndexCreationContext newContext(File indexDir, boolean hasDictionary, int totalDocs) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    return newContext(indexDir, hasDictionary, tableConfig, totalDocs);
  }

  private static IndexCreationContext newContext(File indexDir, boolean hasDictionary, TableConfig tableConfig) {
    return newContext(indexDir, hasDictionary, tableConfig, 1);
  }

  private static IndexCreationContext newContext(File indexDir, boolean hasDictionary, TableConfig tableConfig,
      int totalDocs) {
    return newContext(indexDir, hasDictionary, tableConfig, totalDocs, DataType.INT);
  }

  private static IndexCreationContext newContext(File indexDir, boolean hasDictionary, TableConfig tableConfig,
      int totalDocs, DataType dataType) {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, dataType, true);
    DataType storedType = fieldSpec.getDataType().getStoredType();
    int elementSize = storedType.isFixedWidth() ? storedType.size() : 8;
    ColumnMetadataImpl metadata = new ColumnMetadataImpl.Builder()
        .setFieldSpec(fieldSpec)
        .setTotalDocs(totalDocs)
        .setCardinality(2)
        .setHasDictionary(hasDictionary)
        .setLengthOfShortestElement(storedType.isFixedWidth() ? elementSize : 1)
        .setLengthOfLongestElement(elementSize)
        .setTotalNumberOfEntries(totalDocs)
        .setMaxNumberOfMultiValues(fieldSpec.isSingleValueField() ? 0 : 1)
        .build();
    return new IndexCreationContext.Builder(indexDir, tableConfig, metadata).build();
  }
}
