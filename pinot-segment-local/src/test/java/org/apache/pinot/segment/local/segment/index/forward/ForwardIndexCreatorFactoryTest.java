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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.CompressionStatsTrackingForwardIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReaderV7;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBytePower2ChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
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
      long[] values = storedType == DataType.INT ? new long[]{11, 13, 21}
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

  @DataProvider(name = "malformedV7Files")
  public Object[][] malformedV7Files() {
    return new Object[][]{
        {"metadata", IllegalArgumentException.class, "does not match segment metadata"},
        {"chunkSize", IllegalArgumentException.class, "positive power of two"},
        {"specLength", IllegalArgumentException.class, "Invalid specLength"},
        {"offset", IllegalArgumentException.class, "Corrupt chunkOffsets"},
        {"truncated", IllegalArgumentException.class, "Corrupt per-chunk header"},
        {"trailing", IllegalArgumentException.class, "Corrupt V7 data section"},
        {"encodedSize", IllegalArgumentException.class, "Corrupt per-chunk header"},
        {"decodedSize", IllegalStateException.class, "decodedSize"}
    };
  }

  @Test(dataProvider = "malformedV7Files")
  public void testMalformedV7FileIsRejected(String mutation, Class<? extends RuntimeException> failureType,
      String message) throws Exception {
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
        case "truncated":
          bytes = Arrays.copyOf(bytes, bytes.length - 1);
          break;
        case "trailing":
          bytes = Arrays.copyOf(bytes, bytes.length + 1);
          break;
        case "encodedSize":
          header.putInt(firstFrame, -1);
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
      RuntimeException failure = expectThrows(failureType, () -> {
        try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
            ForwardIndexReader<?> reader =
                ForwardIndexReaderFactory.getInstance().createIndexReader(buffer, metadata)) {
          FixedByteChunkSVForwardIndexReaderV7 v7Reader = (FixedByteChunkSVForwardIndexReaderV7) reader;
          try (FixedByteChunkSVForwardIndexReaderV7.Context context = v7Reader.createContext()) {
            v7Reader.getInt(0, context);
          }
        }
      });
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
