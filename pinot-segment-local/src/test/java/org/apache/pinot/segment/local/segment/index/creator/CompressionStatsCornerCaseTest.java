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


/**
 * Corner case tests for the compression stats feature:
 * <ul>
 *   <li>All dictionary-encoded columns (no raw columns) — division by zero safe</li>
 *   <li>Feature flag toggle OFF→ON produces stats</li>
 *   <li>Old segments without stats (backward compatibility)</li>
 *   <li>IndexingConfig JSON serialization round-trip</li>
 *   <li>TableConfig JSON round-trip</li>
 * </ul>
 *
 * <p>Uses the same segment building pattern as {@link CompressionStatsSegmentCreationTest}.
 */
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

  /**
   * Builds a segment with the given config. Uses the same proven pattern as
   * {@link CompressionStatsSegmentCreationTest}.
   */
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
      row.putValue(STRING_RAW_COL, RandomStringUtils.randomAlphanumeric(20 + RANDOM.nextInt(80)));
      row.putValue(DICT_COL, "value_" + (i % 100));
      rows.add(row);
    }
    return rows;
  }

  @Test
  public void testAllDictionaryColumnsNoCrash()
      throws Exception {
    // When ALL columns are dictionary-encoded, compression stats should gracefully produce no stats.
    // This tests the division-by-zero safety (compressed = 0 → ratio = 0).
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
      assertEquals(colMeta.getUncompressedForwardIndexSizeBytes(), ColumnMetadata.INDEX_NOT_FOUND,
          colName + " should not have uncompressed forward index size");
      assertNull(colMeta.getCompressionCodec(),
          colName + " should not have compression codec");
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
    assertEquals(rawMetaOff.getUncompressedForwardIndexSizeBytes(), ColumnMetadata.INDEX_NOT_FOUND,
        "Flag OFF should not track uncompressed size");
    assertNull(rawMetaOff.getCompressionCodec(),
        "Flag OFF should not track compression codec");

    // Clean up and rebuild with flag ON
    FileUtils.deleteQuietly(TEMP_DIR);

    // Flag ON: stats should be persisted
    File segmentDirOn = buildSegment(true, "LZ4");
    SegmentMetadataImpl metadataOn = new SegmentMetadataImpl(segmentDirOn);

    ColumnMetadata rawMetaOn = metadataOn.getColumnMetadataFor(INT_RAW_COL);
    assertFalse(rawMetaOn.hasDictionary());
    assertTrue(rawMetaOn.getUncompressedForwardIndexSizeBytes() > 0,
        "Flag ON should track uncompressed size");
    assertEquals(rawMetaOn.getCompressionCodec(), "LZ4",
        "Flag ON should track compression codec");
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
    assertEquals(rawMeta.getUncompressedForwardIndexSizeBytes(), ColumnMetadata.INDEX_NOT_FOUND,
        "Old segment should return INDEX_NOT_FOUND for uncompressed size");
    assertNull(rawMeta.getCompressionCodec(),
        "Old segment should return null for compression codec");

    ColumnMetadata dictMeta = metadata.getColumnMetadataFor(DICT_COL);
    assertNotNull(dictMeta);
    assertTrue(dictMeta.hasDictionary());
    assertEquals(dictMeta.getUncompressedForwardIndexSizeBytes(), ColumnMetadata.INDEX_NOT_FOUND);
    assertNull(dictMeta.getCompressionCodec());
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
