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
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Exercises compression-statistics corner cases, including dictionary-only segments, disabled tracking, old segments,
/// and configuration serialization.
public class CompressionStatsCornerCaseTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), CompressionStatsCornerCaseTest.class.getSimpleName());
  private static final String RAW_TABLE_NAME = "compressionCornerCase";
  private static final String SEGMENT_NAME = "cornerCaseSegment";
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

  /// Builds a segment with the requested compression-statistics and chunk-compression configuration.
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
      tableConfig.getIndexingConfig().setCompressionStatsEnabled(true);
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

  @Test
  public void testAllDictionaryColumnsNoCrash()
      throws Exception {
    // When ALL columns are dictionary-encoded, compression stats should gracefully produce no stats.
    // This tests division-by-zero safety: forward-index storage size zero produces ratio zero.
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(DICT_COL, DataType.STRING)
        .addSingleValueDimension("dictCol2", DataType.INT)
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .build();
    tableConfig.getIndexingConfig().setCompressionStatsEnabled(true);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(DICT_COL, "value_" + (i % 50));
      row.putValue("dictCol2", i % 100);
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    File segmentDir = new File(TEMP_DIR, SEGMENT_NAME);
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);

    for (String colName : schema.getColumnNames()) {
      ColumnMetadata colMeta = metadata.getColumnMetadataFor(colName);
      assertTrue(colMeta.hasDictionary(), colName + " should have dictionary");
      assertEquals(colMeta.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
          colName + " should not have uncompressed forward index size");
      assertNull(colMeta.getRawForwardIndexChunkCompressionType(),
          colName + " should not have a forward-index chunk compression type");
    }
  }

  @Test
  public void testFlagOffThenOnProducesStats()
      throws Exception {
    // Flag OFF: no stats persisted
    File segmentDirOff = buildSegment(false, "LZ4");
    SegmentMetadataImpl metadataOff = new SegmentMetadataImpl(segmentDirOff);

    ColumnMetadata rawMetaOff = metadataOff.getColumnMetadataFor(INT_RAW_COL);
    assertFalse(rawMetaOff.hasDictionary());
    assertEquals(rawMetaOff.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
        "Flag OFF should not track uncompressed size");
    assertNull(rawMetaOff.getRawForwardIndexChunkCompressionType(),
        "Flag OFF should not track the chunk compression type");

    // Clean up and rebuild with flag ON
    FileUtils.deleteQuietly(TEMP_DIR);

    // Flag ON: stats should be persisted
    File segmentDirOn = buildSegment(true, "LZ4");
    SegmentMetadataImpl metadataOn = new SegmentMetadataImpl(segmentDirOn);

    ColumnMetadata rawMetaOn = metadataOn.getColumnMetadataFor(INT_RAW_COL);
    assertFalse(rawMetaOn.hasDictionary());
    assertTrue(rawMetaOn.getRawForwardIndexUncompressedValueSizeInBytes() > 0,
        "Flag ON should track uncompressed size");
    assertEquals(rawMetaOn.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4,
        "Flag ON should track the chunk compression type");
  }

  @Test
  public void testOldSegmentWithoutStatsIsBackwardCompatible()
      throws Exception {
    // Simulate an "old" segment: raw columns but no compressionStatsEnabled, no field configs
    File segmentDir = buildSegment(false, null);
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);

    ColumnMetadata rawMeta = metadata.getColumnMetadataFor(INT_RAW_COL);
    assertNotNull(rawMeta);
    assertFalse(rawMeta.hasDictionary());
    assertEquals(rawMeta.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
        "Old segment should return INDEX_NOT_FOUND for uncompressed size");
    assertNull(rawMeta.getRawForwardIndexChunkCompressionType(),
        "Old segment should return null for the chunk compression type");

    ColumnMetadata dictMeta = metadata.getColumnMetadataFor(DICT_COL);
    assertNotNull(dictMeta);
    assertTrue(dictMeta.hasDictionary());
    assertEquals(dictMeta.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE);
    assertNull(dictMeta.getRawForwardIndexChunkCompressionType());
    assertEquals(dictMeta.getDictionaryEncodedUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
        "Dictionary uncompressed-value size must remain unavailable when tracking is disabled");
  }

  @Test
  public void testTrackedZeroByteRawColumnRemainsAvailable()
      throws Exception {
    String column = "emptyRaw";
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(column, DataType.STRING)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of(column))
        .build();
    tableConfig.getIndexingConfig().setCompressionStatsEnabled(true);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName("zeroByteSegment");
    GenericRow row = new GenericRow();
    row.putValue(column, "");
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(row)));
    driver.build();

    ColumnMetadata metadata = new SegmentMetadataImpl(new File(TEMP_DIR, "zeroByteSegment"))
        .getColumnMetadataFor(column);
    assertEquals(metadata.getRawForwardIndexUncompressedValueSizeInBytes(), 0L,
        "A tracked empty value is available and must not be confused with an untracked column");
    assertEquals(metadata.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4);
  }

  @Test
  public void testIndexingConfigJsonRoundTrip()
      throws Exception {
    IndexingConfig original = new IndexingConfig();
    original.setCompressionStatsEnabled(true);

    String json = JsonUtils.objectToString(original);
    assertTrue(json.contains("compressionStatsEnabled"),
        "JSON should contain compressionStatsEnabled field");

    IndexingConfig deserialized = JsonUtils.stringToObject(json, IndexingConfig.class);
    assertTrue(deserialized.isCompressionStatsEnabled(),
        "Deserialized config should have compressionStatsEnabled=true");

    original.setCompressionStatsEnabled(false);
    json = JsonUtils.objectToString(original);
    deserialized = JsonUtils.stringToObject(json, IndexingConfig.class);
    assertFalse(deserialized.isCompressionStatsEnabled(),
        "Deserialized config should have compressionStatsEnabled=false");
  }

  @Test
  public void testTableConfigJsonRoundTripWithCompressionStats()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setNoDictionaryColumns(List.of("col1", "col2"))
        .build();
    tableConfig.getIndexingConfig().setCompressionStatsEnabled(true);

    String json = JsonUtils.objectToString(tableConfig);
    TableConfig deserialized = JsonUtils.stringToObject(json, TableConfig.class);

    assertTrue(deserialized.getIndexingConfig().isCompressionStatsEnabled(),
        "Table config round-trip should preserve compressionStatsEnabled");
    assertEquals(deserialized.getIndexingConfig().getNoDictionaryColumns(), List.of("col1", "col2"),
        "Table config round-trip should preserve noDictionaryColumns");
  }

  @Test
  public void testOldIndexingConfigJsonWithoutFieldDeserializes()
      throws Exception {
    String oldJson = "{\"noDictionaryColumns\":[\"col1\"]}";
    IndexingConfig deserialized = JsonUtils.stringToObject(oldJson, IndexingConfig.class);
    assertFalse(deserialized.isCompressionStatsEnabled(),
        "Missing compressionStatsEnabled in IndexingConfig should default to false");
  }
}
