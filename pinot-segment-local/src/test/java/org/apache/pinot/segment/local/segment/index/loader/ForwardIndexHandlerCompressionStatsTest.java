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
package org.apache.pinot.segment.local.segment.index.loader;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests that compression stats metadata fields ({@code forwardIndex.compressionCodec} and
 * {@code forwardIndex.uncompressedSizeBytes}) are correctly persisted during ForwardIndexHandler
 * reload operations:
 * <ul>
 *   <li>Compression codec change (e.g., SNAPPY → LZ4) persists the new codec in metadata</li>
 *   <li>Dictionary-to-raw conversion persists both codec and uncompressed size</li>
 *   <li>Compression codec metadata survives multiple consecutive codec changes</li>
 * </ul>
 */
public class ForwardIndexHandlerCompressionStatsTest {
  private static final String RAW_TABLE_NAME = "compressionStatsReloadTest";
  private static final String SEGMENT_NAME = "compressionStatsReloadSegment";
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), ForwardIndexHandlerCompressionStatsTest.class.getSimpleName());
  private static final File INDEX_DIR = new File(TEMP_DIR, SEGMENT_NAME);

  private static final String RAW_INT_COL = "rawIntCol";
  private static final String RAW_STRING_COL = "rawStringCol";
  private static final String DICT_INT_COL = "dictIntCol";
  private static final String DICT_STRING_COL = "dictStringCol";

  // Use > 1024 rows to ensure multiple full chunks are flushed before close().
  // V4 writer normalizes numDocsPerChunk=1000 to 1024 (next power-of-2).
  // With only 1000 rows, all data fits in one partial chunk (flushed only at close),
  // so getUncompressedSize() called before close() would return 0.
  private static final int NUM_ROWS = 5000;
  private static final Random RANDOM = new Random(42);

  //@formatter:off
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(RAW_INT_COL, DataType.INT)
      .addSingleValueDimension(RAW_STRING_COL, DataType.STRING)
      .addSingleValueDimension(DICT_INT_COL, DataType.INT)
      .addSingleValueDimension(DICT_STRING_COL, DataType.STRING)
      .build();
  //@formatter:on

  private static final List<GenericRow> TEST_DATA;

  static {
    TEST_DATA = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(RAW_INT_COL, RANDOM.nextInt(100000));
      row.putValue(RAW_STRING_COL, "str_" + i + "_" + RANDOM.nextInt(10000));
      row.putValue(DICT_INT_COL, i % 100);
      row.putValue(DICT_STRING_COL, "dict_" + (i % 50));
      TEST_DATA.add(row);
    }
  }

  private Set<String> _noDictionaryColumns;
  private Map<String, FieldConfig> _fieldConfigMap;

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    _noDictionaryColumns = new HashSet<>(List.of(RAW_INT_COL, RAW_STRING_COL));
    _fieldConfigMap = new HashMap<>();
    _fieldConfigMap.put(RAW_INT_COL,
        new FieldConfig(RAW_INT_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.SNAPPY, null));
    _fieldConfigMap.put(RAW_STRING_COL,
        new FieldConfig(RAW_STRING_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.SNAPPY, null));
    buildSegment();
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  private void buildSegment()
      throws Exception {
    TableConfig tableConfig = createTableConfig();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(TEMP_DIR.getPath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(TEST_DATA));
    driver.build();
  }

  private TableConfig createTableConfig() {
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(new ArrayList<>(_noDictionaryColumns))
        .setFieldConfigList(new ArrayList<>(_fieldConfigMap.values()))
        .build();
    config.getIndexingConfig().setCompressionStatsEnabled(true);
    return config;
  }

  private IndexLoadingConfig createIndexLoadingConfig() {
    return new IndexLoadingConfig(createTableConfig(), SCHEMA);
  }

  @Test
  public void testCompressionCodecPersistedOnCodecChange()
      throws Exception {
    // Change compression from SNAPPY to LZ4 for the raw int column
    _fieldConfigMap.put(RAW_INT_COL,
        new FieldConfig(RAW_INT_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.LZ4, null));

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertTrue(handler.needUpdateIndices(writer), "Handler should detect compression change");
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    // Validate that the new codec is persisted in metadata
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata colMeta = metadata.getColumnMetadataFor(RAW_INT_COL);
    assertFalse(colMeta.hasDictionary());
    assertEquals(colMeta.getCompressionCodec(), "LZ4",
        "Compression codec should be LZ4 after codec change");
  }

  @Test
  public void testCompressionCodecPersistedOnMultipleCodecChanges()
      throws Exception {
    // First change: SNAPPY → LZ4
    _fieldConfigMap.put(RAW_INT_COL,
        new FieldConfig(RAW_INT_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.LZ4, null));

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    SegmentMetadataImpl metadata1 = new SegmentMetadataImpl(INDEX_DIR);
    assertEquals(metadata1.getColumnMetadataFor(RAW_INT_COL).getCompressionCodec(), "LZ4");

    // Second change: LZ4 → ZSTANDARD
    _fieldConfigMap.put(RAW_INT_COL,
        new FieldConfig(RAW_INT_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.ZSTANDARD, null));

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertTrue(handler.needUpdateIndices(writer), "Handler should detect LZ4 → ZSTANDARD change");
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    SegmentMetadataImpl metadata2 = new SegmentMetadataImpl(INDEX_DIR);
    assertEquals(metadata2.getColumnMetadataFor(RAW_INT_COL).getCompressionCodec(), "ZSTANDARD",
        "Compression codec should be ZSTANDARD after second codec change");
  }

  @Test
  public void testDictToRawPersistsCodecAndUncompressedSize()
      throws Exception {
    // Convert DICT_INT_COL from dictionary to raw with LZ4 compression
    _noDictionaryColumns.add(DICT_INT_COL);
    _fieldConfigMap.put(DICT_INT_COL,
        new FieldConfig(DICT_INT_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.LZ4, null));

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertTrue(handler.needUpdateIndices(writer), "Handler should detect dict-to-raw change");
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    // Validate metadata
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata colMeta = metadata.getColumnMetadataFor(DICT_INT_COL);
    assertFalse(colMeta.hasDictionary(), "Column should no longer have dictionary");
    assertEquals(colMeta.getCompressionCodec(), "LZ4",
        "Compression codec should be LZ4 after dict-to-raw conversion");
    assertTrue(colMeta.getUncompressedForwardIndexSizeBytes() > 0,
        "Uncompressed size should be > 0 after dict-to-raw conversion, got: "
            + colMeta.getUncompressedForwardIndexSizeBytes());
  }

  @Test
  public void testDictToRawStringColumnPersistsCodecAndUncompressedSize()
      throws Exception {
    // Convert DICT_STRING_COL from dictionary to raw with ZSTANDARD compression
    _noDictionaryColumns.add(DICT_STRING_COL);
    _fieldConfigMap.put(DICT_STRING_COL,
        new FieldConfig(DICT_STRING_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.ZSTANDARD, null));

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertTrue(handler.needUpdateIndices(writer), "Handler should detect dict-to-raw change for string column");
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata colMeta = metadata.getColumnMetadataFor(DICT_STRING_COL);
    assertFalse(colMeta.hasDictionary(), "String column should no longer have dictionary");
    assertEquals(colMeta.getCompressionCodec(), "ZSTANDARD",
        "Compression codec should be ZSTANDARD after dict-to-raw conversion");
    assertTrue(colMeta.getUncompressedForwardIndexSizeBytes() > 0,
        "Uncompressed size should be > 0 for string column after dict-to-raw, got: "
            + colMeta.getUncompressedForwardIndexSizeBytes());
  }

  @Test
  public void testCompressionCodecNotPersistedForDictColumns()
      throws Exception {
    // Dictionary columns should NOT have compression codec in metadata
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(INDEX_DIR);

    ColumnMetadata dictIntMeta = metadata.getColumnMetadataFor(DICT_INT_COL);
    assertTrue(dictIntMeta.hasDictionary());
    assertNull(dictIntMeta.getCompressionCodec(),
        "Dictionary column should not have compression codec in metadata");
    assertEquals(dictIntMeta.getUncompressedForwardIndexSizeBytes(), ColumnMetadata.INDEX_NOT_FOUND,
        "Dictionary column should not have uncompressed forward index size");

    ColumnMetadata dictStringMeta = metadata.getColumnMetadataFor(DICT_STRING_COL);
    assertTrue(dictStringMeta.hasDictionary());
    assertNull(dictStringMeta.getCompressionCodec(),
        "Dictionary string column should not have compression codec");
    assertEquals(dictStringMeta.getUncompressedForwardIndexSizeBytes(), ColumnMetadata.INDEX_NOT_FOUND,
        "Dictionary string column should not have uncompressed forward index size");
  }

  @Test
  public void testCodecChangeForStringColumn()
      throws Exception {
    // Change compression from SNAPPY to ZSTANDARD for the raw string column
    _fieldConfigMap.put(RAW_STRING_COL,
        new FieldConfig(RAW_STRING_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.ZSTANDARD, null));

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertTrue(handler.needUpdateIndices(writer), "Handler should detect SNAPPY → ZSTANDARD change");
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata colMeta = metadata.getColumnMetadataFor(RAW_STRING_COL);
    assertFalse(colMeta.hasDictionary());
    assertEquals(colMeta.getCompressionCodec(), "ZSTANDARD",
        "String column compression codec should be ZSTANDARD after change");
  }

  @Test
  public void testUnchangedColumnsRetainOriginalMetadata()
      throws Exception {
    // Change compression for RAW_INT_COL only, RAW_STRING_COL should be unaffected
    _fieldConfigMap.put(RAW_INT_COL,
        new FieldConfig(RAW_INT_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.LZ4, null));

    SegmentMetadataImpl metadataBefore = new SegmentMetadataImpl(INDEX_DIR);
    int totalDocsBefore = metadataBefore.getColumnMetadataFor(RAW_STRING_COL).getTotalDocs();

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    SegmentMetadataImpl metadataAfter = new SegmentMetadataImpl(INDEX_DIR);

    // The unchanged column should still have the same metadata
    ColumnMetadata unchangedMeta = metadataAfter.getColumnMetadataFor(RAW_STRING_COL);
    assertFalse(unchangedMeta.hasDictionary());
    assertEquals(unchangedMeta.getTotalDocs(), totalDocsBefore,
        "Unchanged column should retain original totalDocs");

    // The changed column should have the new codec
    ColumnMetadata changedMeta = metadataAfter.getColumnMetadataFor(RAW_INT_COL);
    assertEquals(changedMeta.getCompressionCodec(), "LZ4",
        "Changed column should have LZ4 codec");
  }

  @Test
  public void testDefaultCodecPersistedOnDictToRaw()
      throws Exception {
    // Convert DICT_INT_COL from dictionary to raw WITHOUT specifying a compression codec.
    // The handler should resolve and persist the default codec (LZ4 for DIMENSION columns).
    _noDictionaryColumns.add(DICT_INT_COL);
    // Use EncodingType.RAW but no explicit compression codec (null → default)
    _fieldConfigMap.put(DICT_INT_COL,
        new FieldConfig(DICT_INT_COL, FieldConfig.EncodingType.RAW, List.of(), null, null));

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertTrue(handler.needUpdateIndices(writer), "Handler should detect dict-to-raw change");
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata colMeta = metadata.getColumnMetadataFor(DICT_INT_COL);
    assertFalse(colMeta.hasDictionary(), "Column should no longer have dictionary");
    assertEquals(colMeta.getCompressionCodec(), "LZ4",
        "Default codec LZ4 should be persisted for DIMENSION column even when no explicit codec configured");
    assertTrue(colMeta.getUncompressedForwardIndexSizeBytes() > 0,
        "Uncompressed size should be > 0 after dict-to-raw conversion");
  }

  @Test
  public void testRawToDictClearsCompressionStats()
      throws Exception {
    // First, verify that the raw column has compression stats persisted
    SegmentMetadataImpl metadataBefore = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata rawMeta = metadataBefore.getColumnMetadataFor(RAW_INT_COL);
    assertFalse(rawMeta.hasDictionary(), "Column should start as raw");

    // Convert RAW_INT_COL from raw to dictionary-encoded
    _noDictionaryColumns.remove(RAW_INT_COL);
    _fieldConfigMap.remove(RAW_INT_COL);

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertTrue(handler.needUpdateIndices(writer), "Handler should detect raw-to-dict change");
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    // Validate that compression stats metadata has been cleared
    SegmentMetadataImpl metadataAfter = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata dictMeta = metadataAfter.getColumnMetadataFor(RAW_INT_COL);
    assertTrue(dictMeta.hasDictionary(), "Column should now have dictionary");
    assertNull(dictMeta.getCompressionCodec(),
        "Compression codec should be cleared after raw-to-dict conversion");
    assertEquals(dictMeta.getUncompressedForwardIndexSizeBytes(), ColumnMetadata.INDEX_NOT_FOUND,
        "Uncompressed forward index size should be cleared after raw-to-dict conversion");
  }
}
