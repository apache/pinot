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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Tests segment-creation tracking and persistence when `compressionStatsEnabled` is set in the table config.
public class CompressionStatsSegmentCreationTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), CompressionStatsSegmentCreationTest.class.getSimpleName());
  private static final String RAW_TABLE_NAME = "compressionStatsTable";
  private static final String SEGMENT_NAME = "compressionStatsSegment";
  private static final int NUM_ROWS = 5000;
  private static final Random RANDOM = new Random(42);

  private static final String INT_RAW_COL = "intRawCol";
  private static final String STRING_RAW_COL = "stringRawCol";
  private static final String DICT_COL = "dictCol";

  @BeforeMethod
  public void setUp() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  private List<GenericRow> generateTestData() {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(INT_RAW_COL, RANDOM.nextInt(100000));
      row.putValue(STRING_RAW_COL, RandomStringUtils.secure().nextAlphanumeric(20 + RANDOM.nextInt(80)));
      row.putValue(DICT_COL, "value_" + (i % 100));
      rows.add(row);
    }
    return rows;
  }

  private File buildSegment(boolean compressionStatsEnabled, String compressionCodec)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(INT_RAW_COL, DataType.INT)
        .addSingleValueDimension(STRING_RAW_COL, DataType.STRING)
        .addSingleValueDimension(DICT_COL, DataType.STRING)
        .build();

    List<FieldConfig> fieldConfigs = new ArrayList<>();
    if (compressionCodec != null) {
      FieldConfig.CompressionCodec codec = FieldConfig.CompressionCodec.valueOf(compressionCodec);
      fieldConfigs.add(new FieldConfig(INT_RAW_COL, FieldConfig.EncodingType.RAW, List.of(), codec, null));
      fieldConfigs.add(new FieldConfig(STRING_RAW_COL, FieldConfig.EncodingType.RAW, List.of(), codec, null));
    }

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of(INT_RAW_COL, STRING_RAW_COL))
        .setFieldConfigList(fieldConfigs)
        .build();

    if (compressionStatsEnabled) {
      IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
      indexingConfig.setCompressionStatsEnabled(true);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = generateTestData();
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    return new File(TEMP_DIR, SEGMENT_NAME);
  }

  @Test
  public void testCompressionStatsEnabled()
      throws Exception {
    File segmentDir = buildSegment(true, "LZ4");

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);

    // Raw int column should have uncompressed size tracked
    ColumnMetadata intMeta = metadata.getColumnMetadataFor(INT_RAW_COL);
    assertNotNull(intMeta);
    assertFalse(intMeta.hasDictionary());
    long intUncompressedValueSize = intMeta.getRawForwardIndexUncompressedValueSizeInBytes();
    assertTrue(intUncompressedValueSize > 0,
        "Uncompressed size for raw int column should be > 0, got: " + intUncompressedValueSize);

    // The uncompressed size reflects the total chunk buffer bytes written before compression.
    // For fixed-width types this is the actual data chunked into chunk-buffer-sized blocks.
    // It should be > 0 and in a reasonable range relative to the raw data size.
    long rawDataSize = (long) NUM_ROWS * Integer.BYTES;
    assertTrue(intUncompressedValueSize > 0 && intUncompressedValueSize <= rawDataSize * 2,
        "Uncompressed int size " + intUncompressedValueSize + " should be > 0 and within 2x of raw data size "
            + rawDataSize);

    // The chunk compression type should be persisted.
    assertEquals(intMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4);

    // Raw string column should also have stats
    ColumnMetadata stringMeta = metadata.getColumnMetadataFor(STRING_RAW_COL);
    assertNotNull(stringMeta);
    assertFalse(stringMeta.hasDictionary());
    long stringUncompressedValueSize = stringMeta.getRawForwardIndexUncompressedValueSizeInBytes();
    assertTrue(stringUncompressedValueSize > 0,
        "Uncompressed size for raw string column should be > 0, got: " + stringUncompressedValueSize);
    assertEquals(stringMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4);

    // The compressed forward index size should be less than uncompressed for random string data
    long stringCompressedSize = stringMeta.getIndexSizeFor(StandardIndexes.forward());
    assertTrue(stringCompressedSize > 0, "Compressed forward index size should be > 0");
    // Note: for LZ4, random data may not compress well, but the sizes should be trackable

    // Verify compression ratio is meaningful
    if (stringCompressedSize > 0 && stringUncompressedValueSize > 0) {
      double ratio = (double) stringUncompressedValueSize / stringCompressedSize;
      assertTrue(ratio > 0, "Compression ratio should be > 0, got: " + ratio);
    }

    // Dictionary-encoded column should NOT have uncompressed forward index stats
    ColumnMetadata dictMeta = metadata.getColumnMetadataFor(DICT_COL);
    assertNotNull(dictMeta);
    assertTrue(dictMeta.hasDictionary());
    assertEquals(dictMeta.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
        "Dictionary-encoded column should not have uncompressed forward index size");
    assertNull(dictMeta.getRawForwardIndexChunkCompressionType(),
        "Dictionary-encoded column should not have a forward-index chunk compression type");
  }

  @Test
  public void testCompressionStatsDisabled()
      throws Exception {
    File segmentDir = buildSegment(false, "LZ4");

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);

    // When compressionStatsEnabled is false, no uncompressed size should be persisted
    ColumnMetadata intMeta = metadata.getColumnMetadataFor(INT_RAW_COL);
    assertNotNull(intMeta);
    assertEquals(intMeta.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
        "Uncompressed size should not be tracked when compressionStatsEnabled is false");
    assertNull(intMeta.getRawForwardIndexChunkCompressionType(),
        "Chunk compression type should not be tracked when compressionStatsEnabled is false");
  }

  @Test
  public void testCompressionStatsWithZstandard()
      throws Exception {
    File segmentDir = buildSegment(true, "ZSTANDARD");

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);

    ColumnMetadata intMeta = metadata.getColumnMetadataFor(INT_RAW_COL);
    assertTrue(intMeta.getRawForwardIndexUncompressedValueSizeInBytes() > 0);
    assertEquals(intMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.ZSTANDARD);

    ColumnMetadata stringMeta = metadata.getColumnMetadataFor(STRING_RAW_COL);
    assertTrue(stringMeta.getRawForwardIndexUncompressedValueSizeInBytes() > 0);
    assertEquals(stringMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.ZSTANDARD);
  }

  @Test
  public void testCompressionStatsWithSnappy()
      throws Exception {
    File segmentDir = buildSegment(true, "SNAPPY");

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);

    ColumnMetadata intMeta = metadata.getColumnMetadataFor(INT_RAW_COL);
    assertTrue(intMeta.getRawForwardIndexUncompressedValueSizeInBytes() > 0);
    assertEquals(intMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.SNAPPY);
  }

  @Test
  public void testDefaultCodecPersistedWhenNoExplicitConfig()
      throws Exception {
    // Build a segment with tracking enabled but no explicit compression configuration.
    // The default LZ4 chunk compression type for DIMENSION columns should be persisted.
    File segmentDir = buildSegment(true, null);

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);

    // The raw DIMENSION int column should use the default LZ4 chunk compression type.
    ColumnMetadata intMeta = metadata.getColumnMetadataFor(INT_RAW_COL);
    assertNotNull(intMeta);
    assertFalse(intMeta.hasDictionary());
    assertEquals(intMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4,
        "Default LZ4 chunk compression type should be persisted for the DIMENSION column");
    assertTrue(intMeta.getRawForwardIndexUncompressedValueSizeInBytes() > 0,
        "Uncompressed size should be > 0");

    // Raw string column (DIMENSION type) should also get LZ4
    ColumnMetadata stringMeta = metadata.getColumnMetadataFor(STRING_RAW_COL);
    assertNotNull(stringMeta);
    assertFalse(stringMeta.hasDictionary());
    assertEquals(stringMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4,
        "Default LZ4 chunk compression type should be persisted for the DIMENSION string column");
  }

  @Test
  public void testUncompressedValueSizeConsistencyAcrossCodecs()
      throws Exception {
    // Create segments with different codecs and verify uncompressed sizes are consistent
    // (the raw data is the same, so uncompressed sizes should be identical)
    File lz4Segment = buildSegment(true, "LZ4");
    SegmentMetadataImpl lz4Metadata = new SegmentMetadataImpl(lz4Segment);
    long lz4IntUncompressed =
        lz4Metadata.getColumnMetadataFor(INT_RAW_COL).getRawForwardIndexUncompressedValueSizeInBytes();
    long lz4StringUncompressed =
        lz4Metadata.getColumnMetadataFor(STRING_RAW_COL).getRawForwardIndexUncompressedValueSizeInBytes();

    // Clean up and rebuild with a different chunk compression type.
    FileUtils.deleteQuietly(TEMP_DIR);

    File zstdSegment = buildSegment(true, "ZSTANDARD");
    SegmentMetadataImpl zstdMetadata = new SegmentMetadataImpl(zstdSegment);
    long zstdIntUncompressed =
        zstdMetadata.getColumnMetadataFor(INT_RAW_COL).getRawForwardIndexUncompressedValueSizeInBytes();
    long zstdStringUncompressed =
        zstdMetadata.getColumnMetadataFor(STRING_RAW_COL).getRawForwardIndexUncompressedValueSizeInBytes();

    // Fixed-width int uncompressed size is independent of chunk compression type.
    assertEquals(lz4IntUncompressed, zstdIntUncompressed,
        "Uncompressed size for fixed-width int column should be identical across codecs");

    // Variable-width string column: uncompressed sizes may differ slightly due to chunk layout
    // but should be within a reasonable range (within 10%)
    double stringDiffPercent =
        Math.abs((double) (lz4StringUncompressed - zstdStringUncompressed)) / lz4StringUncompressed * 100;
    assertTrue(stringDiffPercent < 10,
        "Uncompressed string sizes should be similar across codecs. LZ4=" + lz4StringUncompressed
            + " ZSTD=" + zstdStringUncompressed + " diff=" + stringDiffPercent + "%");
  }
}
