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

import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Tests compression metadata persistence across [ForwardIndexHandler] reload operations.
///
/// Coverage includes chunk-compression changes, dictionary/raw conversions, forward-index disable/re-enable, and
/// stale metadata cleanup.
public class ForwardIndexHandlerCompressionStatsTest {
  private static final String RAW_TABLE_NAME = "compressionStatsReloadTest";
  private static final String SEGMENT_NAME = "compressionStatsReloadSegment";
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), ForwardIndexHandlerCompressionStatsTest.class.getSimpleName());
  private static final File INDEX_DIR = new File(TEMP_DIR, SEGMENT_NAME);

  private static final String RAW_INT_COL = "rawIntCol";
  private static final String RAW_INT_MV_COL = "rawIntMvCol";
  private static final String RAW_STRING_COL = "rawStringCol";
  private static final String DICT_INT_COL = "dictIntCol";
  private static final String DICT_INT_MV_COL = "dictIntMvCol";
  private static final String DICT_STRING_COL = "dictStringCol";

  private static final int NUM_ROWS = 5000;
  private static final Random RANDOM = new Random(42);

  //@formatter:off
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(RAW_INT_COL, DataType.INT)
      .addMultiValueDimension(RAW_INT_MV_COL, DataType.INT)
      .addSingleValueDimension(RAW_STRING_COL, DataType.STRING)
      .addSingleValueDimension(DICT_INT_COL, DataType.INT)
      .addMultiValueDimension(DICT_INT_MV_COL, DataType.INT)
      .addSingleValueDimension(DICT_STRING_COL, DataType.STRING)
      .build();
  //@formatter:on

  private static final List<GenericRow> TEST_DATA;

  static {
    TEST_DATA = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(RAW_INT_COL, RANDOM.nextInt(100000));
      row.putValue(RAW_INT_MV_COL, new Integer[]{i, i + 1, i + 2});
      row.putValue(RAW_STRING_COL, "str_" + i + "_" + RANDOM.nextInt(10000));
      row.putValue(DICT_INT_COL, i % 100);
      row.putValue(DICT_INT_MV_COL, new Integer[]{i % 100, (i + 1) % 100, (i + 2) % 100});
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
    _noDictionaryColumns = new HashSet<>(List.of(RAW_INT_COL, RAW_INT_MV_COL, RAW_STRING_COL));
    _fieldConfigMap = new HashMap<>();
    _fieldConfigMap.put(RAW_INT_COL,
        new FieldConfig(RAW_INT_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.SNAPPY, null));
    _fieldConfigMap.put(RAW_STRING_COL,
        new FieldConfig(RAW_STRING_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.SNAPPY, null));
    _fieldConfigMap.put(RAW_INT_MV_COL,
        new FieldConfig(RAW_INT_MV_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.SNAPPY, null));
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
  public void testChunkCompressionTypePersistedOnCompressionChange()
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

    // Validate that the new chunk compression type is persisted in metadata.
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata colMeta = metadata.getColumnMetadataFor(RAW_INT_COL);
    assertFalse(colMeta.hasDictionary());
    assertEquals(colMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4,
        "Chunk compression type should be LZ4 after the compression change");
  }

  @Test
  public void testChunkCompressionTypePersistedOnMultipleCompressionChanges()
      throws Exception {
    // First change: SNAPPY to LZ4.
    _fieldConfigMap.put(RAW_INT_COL,
        new FieldConfig(RAW_INT_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.LZ4, null));

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    SegmentMetadataImpl metadata1 = new SegmentMetadataImpl(INDEX_DIR);
    assertEquals(metadata1.getColumnMetadataFor(RAW_INT_COL).getRawForwardIndexChunkCompressionType(),
        ChunkCompressionType.LZ4);

    // Second change: LZ4 to ZSTANDARD.
    _fieldConfigMap.put(RAW_INT_COL,
        new FieldConfig(RAW_INT_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.ZSTANDARD, null));

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertTrue(handler.needUpdateIndices(writer), "Handler should detect the LZ4 to ZSTANDARD change");
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    SegmentMetadataImpl metadata2 = new SegmentMetadataImpl(INDEX_DIR);
    assertEquals(metadata2.getColumnMetadataFor(RAW_INT_COL).getRawForwardIndexChunkCompressionType(),
        ChunkCompressionType.ZSTANDARD,
        "Chunk compression type should be ZSTANDARD after the second change");
  }

  @Test
  public void testDictToRawPersistsChunkCompressionTypeAndUncompressedValueSize()
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
    assertEquals(colMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4,
        "Chunk compression type should be LZ4 after dict-to-raw conversion");
    assertTrue(colMeta.getRawForwardIndexUncompressedValueSizeInBytes() > 0,
        "Uncompressed size should be > 0 after dict-to-raw conversion, got: "
            + colMeta.getRawForwardIndexUncompressedValueSizeInBytes());
  }

  @Test
  public void testDictToRawKeepingDictionaryPersistsRawCompressionStats()
      throws Exception {
    ObjectNode dictIndexes = JsonUtils.newObjectNode();
    dictIndexes.set("dictionary", JsonUtils.newObjectNode());
    _fieldConfigMap.put(DICT_INT_COL,
        new FieldConfig(DICT_INT_COL, FieldConfig.EncodingType.RAW, null, null, CompressionCodec.LZ4, null,
            dictIndexes, null, null));

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertEquals(handler.computeOperations(writer),
          Map.of(DICT_INT_COL, List.of(ForwardIndexHandler.Operation.ENABLE_RAW_FORWARD_INDEX)));
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata colMeta = metadata.getColumnMetadataFor(DICT_INT_COL);
    assertTrue(colMeta.hasDictionary(), "Column should keep the dictionary for secondary-index compatibility");
    assertEquals(colMeta.getForwardIndexEncoding(), FieldConfig.EncodingType.RAW);
    assertEquals(colMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4,
        "Raw forward-index chunk compression type should persist when the dictionary is retained");
    assertTrue(colMeta.getRawForwardIndexUncompressedValueSizeInBytes() > 0,
        "Uncompressed size should be persisted for raw forward index with retained dictionary");
    assertEquals(colMeta.getDictionaryEncodedUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
        "Dictionary uncompressed value stats should be cleared once the forward index is raw");
  }

  @Test
  public void testDictToRawStringColumnPersistsChunkCompressionTypeAndUncompressedValueSize()
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
    assertEquals(colMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.ZSTANDARD,
        "Chunk compression type should be ZSTANDARD after dict-to-raw conversion");
    assertTrue(colMeta.getRawForwardIndexUncompressedValueSizeInBytes() > 0,
        "Uncompressed size should be > 0 for string column after dict-to-raw, got: "
            + colMeta.getRawForwardIndexUncompressedValueSizeInBytes());
  }

  @Test
  public void testChunkCompressionTypeNotPersistedForDictionaryColumns()
      throws Exception {
    // Dictionary columns do not have a forward-index chunk compression type.
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(INDEX_DIR);

    ColumnMetadata dictIntMeta = metadata.getColumnMetadataFor(DICT_INT_COL);
    assertTrue(dictIntMeta.hasDictionary());
    assertNull(dictIntMeta.getRawForwardIndexChunkCompressionType(),
        "Dictionary column should not have a chunk compression type in metadata");
    assertEquals(dictIntMeta.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
        "Dictionary column should not have uncompressed forward index size");

    ColumnMetadata dictStringMeta = metadata.getColumnMetadataFor(DICT_STRING_COL);
    assertTrue(dictStringMeta.hasDictionary());
    assertNull(dictStringMeta.getRawForwardIndexChunkCompressionType(),
        "Dictionary string column should not have a chunk compression type");
    assertEquals(dictStringMeta.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
        "Dictionary string column should not have uncompressed forward index size");
  }

  @Test
  public void testCompressionChangeForStringColumn()
      throws Exception {
    // Change compression from SNAPPY to ZSTANDARD for the raw string column
    _fieldConfigMap.put(RAW_STRING_COL,
        new FieldConfig(RAW_STRING_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.ZSTANDARD, null));

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertTrue(handler.needUpdateIndices(writer), "Handler should detect the SNAPPY to ZSTANDARD change");
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata colMeta = metadata.getColumnMetadataFor(RAW_STRING_COL);
    assertFalse(colMeta.hasDictionary());
    assertEquals(colMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.ZSTANDARD,
        "String column chunk compression type should be ZSTANDARD after the change");
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

    // The changed column should have the new chunk compression type.
    ColumnMetadata changedMeta = metadataAfter.getColumnMetadataFor(RAW_INT_COL);
    assertEquals(changedMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4,
        "Changed column should have LZ4 chunk compression");
  }

  @Test
  public void testDefaultChunkCompressionTypePersistedOnDictToRaw()
      throws Exception {
    // Convert DICT_INT_COL from dictionary to raw without specifying compression.
    // The handler should persist the default LZ4 chunk compression type for DIMENSION columns.
    _noDictionaryColumns.add(DICT_INT_COL);
    // Use RAW encoding without an explicit compression type so the default is selected.
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
    assertEquals(colMeta.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4,
        "Default LZ4 chunk compression type should be persisted for the DIMENSION column");
    assertTrue(colMeta.getRawForwardIndexUncompressedValueSizeInBytes() > 0,
        "Uncompressed size should be > 0 after dict-to-raw conversion");
  }

  @Test
  public void testChunkCompressionTypeClearedWhenCompressionStatsDisabled()
      throws Exception {
    // The initial segment was built with compressionStatsEnabled=true and SNAPPY compression,
    // so RAW_INT_COL already has "SNAPPY" in metadata.
    _fieldConfigMap.put(RAW_INT_COL,
        new FieldConfig(RAW_INT_COL, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.LZ4, null));

    TableConfig configWithStatsDisabled = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(new ArrayList<>(_noDictionaryColumns))
        .setFieldConfigList(new ArrayList<>(_fieldConfigMap.values()))
        .build();
    // compressionStatsEnabled defaults to false; do not set it.
    IndexLoadingConfig loadingConfig = new IndexLoadingConfig(configWithStatsDisabled, SCHEMA);

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, loadingConfig);
      assertTrue(handler.needUpdateIndices(writer), "Handler should detect compression change");
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    // The rewritten index is intentionally untracked, so both old fields must be removed rather than left stale.
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata colMeta = metadata.getColumnMetadataFor(RAW_INT_COL);
    assertFalse(colMeta.hasDictionary());
    assertNull(colMeta.getRawForwardIndexChunkCompressionType());
    assertEquals(colMeta.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE);
  }

  @Test
  public void testDictIdCompressionRewriteBackfillsDictionaryStats()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      Map<String, String> metadataUpdates = new HashMap<>();
      metadataUpdates.put(V1Constants.MetadataKeys.Column.getKeyFor(DICT_INT_MV_COL,
          V1Constants.MetadataKeys.Column.FORWARD_INDEX_DICTIONARY_ENCODED_UNCOMPRESSED_VALUE_SIZE_IN_BYTES), null);
      SegmentMetadataUtils.updateMetadataProperties(segmentDirectory, metadataUpdates);
    }
    _fieldConfigMap.put(DICT_INT_MV_COL, new FieldConfig(DICT_INT_MV_COL, FieldConfig.EncodingType.DICTIONARY,
        List.of(), CompressionCodec.MV_ENTRY_DICT, null));

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertEquals(handler.computeOperations(writer),
          Map.of(DICT_INT_MV_COL, List.of(ForwardIndexHandler.Operation.CHANGE_INDEX_COMPRESSION_TYPE)));
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    ColumnMetadata metadata = new SegmentMetadataImpl(INDEX_DIR).getColumnMetadataFor(DICT_INT_MV_COL);
    assertEquals(metadata.getForwardIndexEncoding(), FieldConfig.EncodingType.DICTIONARY);
    assertNull(metadata.getRawForwardIndexChunkCompressionType());
    assertEquals(metadata.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE);
    assertEquals(metadata.getDictionaryEncodedUncompressedValueSizeInBytes(), (long) NUM_ROWS * 3 * Integer.BYTES);
  }

  @Test
  public void testDictIdCompressionRewriteClearsStatsWhenDisabled()
      throws Exception {
    _fieldConfigMap.put(DICT_INT_MV_COL, new FieldConfig(DICT_INT_MV_COL, FieldConfig.EncodingType.DICTIONARY,
        List.of(), CompressionCodec.MV_ENTRY_DICT, null));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(new ArrayList<>(_noDictionaryColumns))
        .setFieldConfigList(new ArrayList<>(_fieldConfigMap.values()))
        .build();

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory,
          new IndexLoadingConfig(tableConfig, SCHEMA));
      assertTrue(handler.needUpdateIndices(writer));
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    ColumnMetadata metadata = new SegmentMetadataImpl(INDEX_DIR).getColumnMetadataFor(DICT_INT_MV_COL);
    assertNull(metadata.getRawForwardIndexChunkCompressionType());
    assertEquals(metadata.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE);
    assertEquals(metadata.getDictionaryEncodedUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE);
  }

  @Test
  public void testDisableAndReenableForwardIndexReplacesStaleStats()
      throws Exception {
    ObjectNode retainedIndexes = JsonUtils.newObjectNode();
    retainedIndexes.set("dictionary", JsonUtils.newObjectNode());
    retainedIndexes.set("inverted", JsonUtils.newObjectNode());
    _fieldConfigMap.put(DICT_INT_COL,
        new FieldConfig(DICT_INT_COL, FieldConfig.EncodingType.DICTIONARY, null, null, null, null, retainedIndexes,
            null, null));
    FileUtils.deleteQuietly(TEMP_DIR);
    buildSegment();

    _fieldConfigMap.put(DICT_INT_COL,
        new FieldConfig(DICT_INT_COL, FieldConfig.EncodingType.RAW, null, null, CompressionCodec.LZ4, null,
            retainedIndexes, null, null));
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    ColumnMetadata initialMetadata = new SegmentMetadataImpl(INDEX_DIR).getColumnMetadataFor(DICT_INT_COL);
    assertTrue(initialMetadata.hasDictionary());
    assertEquals(initialMetadata.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4);
    assertTrue(initialMetadata.getRawForwardIndexUncompressedValueSizeInBytes() > 0);

    _fieldConfigMap.put(DICT_INT_COL,
        new FieldConfig(DICT_INT_COL, FieldConfig.EncodingType.RAW, null, null, CompressionCodec.LZ4, null,
            retainedIndexes, Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true"), null));
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertTrue(handler.needUpdateIndices(writer));
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    ColumnMetadata disabledMetadata = new SegmentMetadataImpl(INDEX_DIR).getColumnMetadataFor(DICT_INT_COL);
    assertTrue(disabledMetadata.hasDictionary());
    assertNull(disabledMetadata.getRawForwardIndexChunkCompressionType(),
        "Disabling the forward index must remove the previous raw chunk type");
    assertEquals(disabledMetadata.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
        "Disabling the forward index must remove the previous raw byte count");

    _fieldConfigMap.put(DICT_INT_COL,
        new FieldConfig(DICT_INT_COL, FieldConfig.EncodingType.RAW, null, null, CompressionCodec.ZSTANDARD, null,
            retainedIndexes, null, null));
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      assertTrue(handler.needUpdateIndices(writer));
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    ColumnMetadata reenabledMetadata = new SegmentMetadataImpl(INDEX_DIR).getColumnMetadataFor(DICT_INT_COL);
    assertTrue(reenabledMetadata.hasDictionary());
    assertEquals(reenabledMetadata.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.ZSTANDARD,
        "The regenerated raw index should report its new chunk type");
    assertTrue(reenabledMetadata.getRawForwardIndexUncompressedValueSizeInBytes() > 0,
        "The regenerated raw index should report newly measured bytes");
    assertEquals(reenabledMetadata.getDictionaryEncodedUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
        "Raw forward-index accounting must not be mixed with dictionary ingest accounting");
  }

  @Test
  public void testRawToDictDoesNotUseUnavailableLegacyEntryCount()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      Map<String, String> metadataUpdates = new HashMap<>();
      metadataUpdates.put(V1Constants.MetadataKeys.Column.getKeyFor(RAW_INT_MV_COL,
          V1Constants.MetadataKeys.Column.TOTAL_NUMBER_OF_ENTRIES), null);
      SegmentMetadataUtils.updateMetadataProperties(segmentDirectory, metadataUpdates);
    }
    assertEquals(new SegmentMetadataImpl(INDEX_DIR).getColumnMetadataFor(RAW_INT_MV_COL).getTotalNumberOfEntries(),
        ColumnMetadata.UNAVAILABLE);

    _noDictionaryColumns.remove(RAW_INT_MV_COL);
    _fieldConfigMap.remove(RAW_INT_MV_COL);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      ForwardIndexHandler handler = new ForwardIndexHandler(segmentDirectory, createIndexLoadingConfig());
      handler.updateIndices(writer);
      handler.postUpdateIndicesCleanup(writer);
    }

    ColumnMetadata dictionaryMetadata = new SegmentMetadataImpl(INDEX_DIR).getColumnMetadataFor(RAW_INT_MV_COL);
    assertEquals(dictionaryMetadata.getDictionaryEncodedUncompressedValueSizeInBytes(),
        (long) NUM_ROWS * 3 * Integer.BYTES,
        "Dictionary accounting must use the values observed during rewrite, not unavailable legacy metadata");
  }

  @Test
  public void testRawToDictReplacesRawStatsWithDictionaryStats()
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

    // Validate that raw metadata is cleared and dictionary uncompressed value bytes are persisted.
    SegmentMetadataImpl metadataAfter = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata dictMeta = metadataAfter.getColumnMetadataFor(RAW_INT_COL);
    assertTrue(dictMeta.hasDictionary(), "Column should now have dictionary");
    assertNull(dictMeta.getRawForwardIndexChunkCompressionType(),
        "Chunk compression type should be cleared after raw-to-dict conversion");
    assertEquals(dictMeta.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
        "Uncompressed forward index size should be cleared after raw-to-dict conversion");
    assertTrue(dictMeta.getDictionaryEncodedUncompressedValueSizeInBytes() > 0,
        "Dictionary uncompressed value size should be collected during raw-to-dictionary conversion");
  }
}
