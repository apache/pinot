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
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.local.utils.SegmentAllIndexPreprocessThrottler;
import org.apache.pinot.segment.local.utils.SegmentPreprocessThrottler;
import org.apache.pinot.segment.local.utils.SegmentStarTreePreprocessThrottler;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SegmentPreProcessorTest implements PinotBuffersAfterClassCheckRule {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), SegmentPreProcessorTest.class.getSimpleName());
  private static final File INDEX_DIR = new File(TEMP_DIR, SEGMENT_NAME);
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final String SCHEMA = "data/testDataMVSchema.json";

  // For create inverted indices tests.
  private static final String COLUMN1_NAME = "column1";
  private static final String COLUMN7_NAME = "column7";
  private static final String COLUMN10_NAME = "column10";
  private static final String COLUMN13_NAME = "column13";
  private static final String NO_SUCH_COLUMN_NAME = "noSuchColumn";
  private static final String NEW_COLUMN_INVERTED_INDEX = "newStringMVDimension";

  // For create text index tests
  private static final String EXISTING_STRING_COL_RAW = "column4";
  private static final String EXISTING_STRING_COL_DICT = "column5";
  private static final String NEWLY_ADDED_STRING_COL_RAW = "newTextColRaw";
  private static final String NEWLY_ADDED_STRING_COL_DICT = "newTextColDict";
  private static final String NEWLY_ADDED_STRING_MV_COL_RAW = "newTextMVColRaw";
  private static final String NEWLY_ADDED_STRING_MV_COL_DICT = "newTextMVColDict";

  // For int RAW column
  private static final String EXISTING_INT_COL_RAW = "column2";

  // For raw MV column.
  private static final String EXISTING_INT_COL_RAW_MV = "column6";

  // For create fst index tests
  private static final String NEWLY_ADDED_FST_COL_DICT = "newFSTColDict";

  // For create no forward index column tests
  private static final String NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV = "newForwardIndexDisabledColumnSV";
  private static final String NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV = "newForwardIndexDisabledColumnMV";

  // For update default value tests.
  private static final String NEW_COLUMNS_SCHEMA1 = "data/newColumnsSchema1.json";
  private static final String NEW_COLUMNS_SCHEMA2 = "data/newColumnsSchema2.json";
  private static final String NEW_COLUMNS_SCHEMA3 = "data/newColumnsSchema3.json";
  private static final String NEW_COLUMNS_SCHEMA_WITH_FST = "data/newColumnsSchemaWithFST.json";
  private static final String NEW_COLUMNS_SCHEMA_WITH_TEXT = "data/newColumnsSchemaWithText.json";
  private static final String NEW_COLUMNS_SCHEMA_WITH_H3_JSON = "data/newColumnsSchemaWithH3Json.json";
  private static final String NEW_COLUMNS_SCHEMA_WITH_NO_FORWARD_INDEX =
      "data/newColumnsSchemaWithForwardIndexDisabled.json";
  private static final String NEW_INT_METRIC_COLUMN_NAME = "newIntMetric";
  private static final String NEW_LONG_METRIC_COLUMN_NAME = "newLongMetric";
  private static final String NEW_FLOAT_METRIC_COLUMN_NAME = "newFloatMetric";
  private static final String NEW_DOUBLE_METRIC_COLUMN_NAME = "newDoubleMetric";
  private static final String NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME = "newBooleanSVDimension";
  private static final String NEW_INT_SV_DIMENSION_COLUMN_NAME = "newIntSVDimension";
  private static final String NEW_STRING_MV_DIMENSION_COLUMN_NAME = "newStringMVDimension";
  private static final String NEW_RAW_STRING_SV_DIMENSION_COLUMN_NAME = "newRawStringSVDimension";
  private static final String NEW_NULL_RETURN_STRING_SV_DIMENSION_COLUMN_NAME = "newNullReturnStringSVDimension";
  private static final String NEW_WRONG_ARG_DATE_TRUNC_DERIVED_COLUMN_NAME = "newWrongArgDateTruncDerivedColumn";
  private static final String NEW_HLL_BYTE_METRIC_COLUMN_NAME = "newHLLByteMetric";
  private static final String NEW_TDIGEST_BYTE_METRIC_COLUMN_NAME = "newTDigestByteMetric";

  private static final SegmentPreprocessThrottler SEGMENT_PREPROCESS_THROTTLER =
      new SegmentPreprocessThrottler(new SegmentAllIndexPreprocessThrottler(2, 4, true),
          new SegmentStarTreePreprocessThrottler(1));

  private final File _avroFile;
  private final Schema _schema;
  private final Schema _newColumnsSchema1;
  private final Schema _newColumnsSchema2;
  private final Schema _newColumnsSchema3;
  private final Schema _newColumnsSchemaWithFST;
  private final Schema _newColumnsSchemaWithText;
  private final Schema _newColumnsSchemaWithH3Json;
  private final Schema _newColumnsSchemaWithForwardIndexDisabled;

  private Set<String> _noDictionaryColumns;
  private Set<String> _invertedIndexColumns;
  private Set<String> _rangeIndexColumns;
  private Map<String, FieldConfig> _fieldConfigMap;
  private IngestionConfig _ingestionConfig;
  private ColumnMinMaxValueGeneratorMode _columnMinMaxValueGeneratorMode;
  private Map<String, BloomFilterConfig> _bloomFilterConfigs;
  private Map<String, JsonIndexConfig> _jsonIndexConfigs;
  private List<StarTreeIndexConfig> _starTreeIndexConfigs;
  private boolean _enableDefaultStarTree;

  public SegmentPreProcessorTest()
      throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    URL resourceUrl = classLoader.getResource(AVRO_DATA);
    assertNotNull(resourceUrl);
    _avroFile = new File(resourceUrl.getFile());

    // For newColumnsSchema, we add 4 different data type metric columns with one user-defined default null value, and
    // 3 different data type dimension columns with one user-defined default null value and one multi-value column.
    resourceUrl = classLoader.getResource(SCHEMA);
    assertNotNull(resourceUrl);
    _schema = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA1);
    assertNotNull(resourceUrl);
    _newColumnsSchema1 = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA2);
    assertNotNull(resourceUrl);
    _newColumnsSchema2 = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA3);
    assertNotNull(resourceUrl);
    _newColumnsSchema3 = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA_WITH_FST);
    assertNotNull(resourceUrl);
    _newColumnsSchemaWithFST = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA_WITH_TEXT);
    assertNotNull(resourceUrl);
    _newColumnsSchemaWithText = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA_WITH_H3_JSON);
    assertNotNull(resourceUrl);
    _newColumnsSchemaWithH3Json = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA_WITH_NO_FORWARD_INDEX);
    assertNotNull(resourceUrl);
    _newColumnsSchemaWithForwardIndexDisabled = Schema.fromFile(new File(resourceUrl.getFile()));
  }

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    resetIndexConfigs();
  }

  private void resetIndexConfigs() {
    _noDictionaryColumns = new HashSet<>();
    _noDictionaryColumns.add(EXISTING_STRING_COL_RAW);
    _noDictionaryColumns.add(EXISTING_INT_COL_RAW_MV);
    _noDictionaryColumns.add(EXISTING_INT_COL_RAW);

    _invertedIndexColumns = new HashSet<>();
    _invertedIndexColumns.add(COLUMN7_NAME);

    _rangeIndexColumns = new HashSet<>();

    _fieldConfigMap = new HashMap<>();

    // The segment generation code in SegmentColumnarIndexCreator will throw exception if start and end time in time
    // column are not in acceptable range. For this test, we first need to fix the input avro data to have the time
    // column values in allowed range. Until then, the check is explicitly disabled.
    _ingestionConfig = new IngestionConfig();
    _ingestionConfig.setRowTimeValueCheck(false);
    _ingestionConfig.setSegmentTimeValueCheck(false);

    _columnMinMaxValueGeneratorMode = null;
    _bloomFilterConfigs = null;
    _jsonIndexConfigs = null;
    _starTreeIndexConfigs = null;
    _enableDefaultStarTree = false;
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  private void buildSegment(SegmentVersion segmentVersion)
      throws Exception {
    buildV1Segment();
    if (segmentVersion == SegmentVersion.v3) {
      convertV1SegmentToV3();
    }
  }

  private void buildV1Segment()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);

    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfigWithSchema(_avroFile, TEMP_DIR, RAW_TABLE_NAME, createTableConfig(),
            _schema);
    config.setOutDir(TEMP_DIR.getPath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
  }

  private void convertV1SegmentToV3()
      throws Exception {
    new SegmentV1V2ToV3FormatConverter().convert(INDEX_DIR);
  }

  private void buildV3Segment()
      throws Exception {
    buildV1Segment();
    convertV1SegmentToV3();
  }

  private TableConfig createTableConfig() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName("daysSinceEpoch")
            .setNoDictionaryColumns(new ArrayList<>(_noDictionaryColumns))
            .setInvertedIndexColumns(new ArrayList<>(_invertedIndexColumns))
            .setCreateInvertedIndexDuringSegmentGeneration(true)
            .setRangeIndexColumns(new ArrayList<>(_rangeIndexColumns))
            .setFieldConfigList(new ArrayList<>(_fieldConfigMap.values())).setNullHandlingEnabled(true)
            .setIngestionConfig(_ingestionConfig).build();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (_columnMinMaxValueGeneratorMode != null) {
      indexingConfig.setColumnMinMaxValueGeneratorMode(_columnMinMaxValueGeneratorMode.name());
    }
    indexingConfig.setBloomFilterConfigs(_bloomFilterConfigs);
    indexingConfig.setJsonIndexConfigs(_jsonIndexConfigs);
    if (_starTreeIndexConfigs != null || _enableDefaultStarTree) {
      indexingConfig.setEnableDynamicStarTreeCreation(true);
      indexingConfig.setStarTreeIndexConfigs(_starTreeIndexConfigs);
      indexingConfig.setEnableDefaultStarTree(_enableDefaultStarTree);
    }
    return tableConfig;
  }

  private IndexLoadingConfig createIndexLoadingConfig(Schema schema) {
    return new IndexLoadingConfig(createTableConfig(), schema);
  }

  private void runPreProcessor()
      throws Exception {
    runPreProcessor(_schema);
  }

  private void runPreProcessor(Schema schema)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, createIndexLoadingConfig(schema),
            schema)) {
      processor.process(SEGMENT_PREPROCESS_THROTTLER);
    }
  }

  @DataProvider(name = "bothV1AndV3")
  public SegmentVersion[][] bothV1AndV3() {
    return new SegmentVersion[][]{{SegmentVersion.v1}, {SegmentVersion.v3}};
  }

  /**
   * Test to check for default column handling and text index creation during
   * segment load after a new raw column is added to the schema with text index
   * creation enabled.
   * This will exercise both code paths in SegmentPreprocessor (segment load):
   * (1) Default column handler to add forward index and dictionary
   * (2) Text index handler to add text index
   */
  @Test(dataProvider = "bothV1AndV3")
  public void testEnableTextIndexOnNewColumnRaw(SegmentVersion segmentVersion)
      throws Exception {
    buildSegment(segmentVersion);
    _fieldConfigMap.put(NEWLY_ADDED_STRING_COL_RAW,
        new FieldConfig(NEWLY_ADDED_STRING_COL_RAW, FieldConfig.EncodingType.RAW, List.of(FieldConfig.IndexType.TEXT),
            null, null));
    _fieldConfigMap.put(NEWLY_ADDED_STRING_MV_COL_RAW,
        new FieldConfig(NEWLY_ADDED_STRING_MV_COL_RAW, FieldConfig.EncodingType.RAW,
            List.of(FieldConfig.IndexType.TEXT), null, null));
    checkTextIndexCreation(NEWLY_ADDED_STRING_COL_RAW, 1, 1, _newColumnsSchemaWithText, true, true, true, 4);
    checkTextIndexCreation(NEWLY_ADDED_STRING_MV_COL_RAW, 1, 1, _newColumnsSchemaWithText, true, true, false, 4, false,
        1);
  }

  @Test(dataProvider = "bothV1AndV3", expectedExceptions = UnsupportedOperationException.class,
      expectedExceptionsMessageRegExp = "FST index is currently only supported on dictionary encoded columns: column4")
  public void testEnableFSTIndexOnExistingColumnRaw(SegmentVersion segmentVersion)
      throws Exception {
    buildSegment(segmentVersion);
    _fieldConfigMap.put(EXISTING_STRING_COL_RAW,
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.RAW, List.of(FieldConfig.IndexType.FST), null,
            null));
    runPreProcessor();
  }

  @Test(dataProvider = "bothV1AndV3")
  public void testEnableFSTIndexOnNewColumnDictEncoded(SegmentVersion segmentVersion)
      throws Exception {
    buildSegment(segmentVersion);
    _fieldConfigMap.put(NEWLY_ADDED_FST_COL_DICT,
        new FieldConfig(NEWLY_ADDED_FST_COL_DICT, FieldConfig.EncodingType.DICTIONARY,
            List.of(FieldConfig.IndexType.FST), null, null));
    checkFSTIndexCreation(NEWLY_ADDED_FST_COL_DICT, 1, 1, _newColumnsSchemaWithFST, true, true, 4);
  }

  @Test(dataProvider = "bothV1AndV3")
  public void testEnableFSTIndexOnExistingColumnDictEncoded(SegmentVersion segmentVersion)
      throws Exception {
    buildSegment(segmentVersion);
    _fieldConfigMap.put(EXISTING_STRING_COL_DICT,
        new FieldConfig(EXISTING_STRING_COL_DICT, FieldConfig.EncodingType.DICTIONARY,
            List.of(FieldConfig.IndexType.FST), null, null));
    checkFSTIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _newColumnsSchemaWithFST, false, false, 26);
  }

  @Test
  public void testSimpleEnableDictionarySV()
      throws Exception {
    // TEST 1. Check running forwardIndexHandler on a V1 segment. No-op for all existing raw columns.
    buildV1Segment();
    checkForwardIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0, ChunkCompressionType.LZ4,
        true, 0, DataType.STRING, 100000);
    validateIndex(StandardIndexes.forward(), EXISTING_INT_COL_RAW, 42242, 16, false, false, false, 0, true, 0,
        ChunkCompressionType.LZ4, false, DataType.INT, 100000);

    // Convert the segment to V3.
    convertV1SegmentToV3();

    // TEST 2: Enable dictionary on EXISTING_STRING_COL_RAW
    _noDictionaryColumns.remove(EXISTING_STRING_COL_RAW);
    checkForwardIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, true, false, 4, null, true, 0,
        DataType.STRING, 100000);

    // TEST 3: Enable dictionary on EXISTING_INT_COL_RAW
    _noDictionaryColumns.remove(EXISTING_INT_COL_RAW);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW, 42242, 16, _schema, false, true, false, 0, null, true, 0,
        DataType.INT, 100000);
  }

  @Test
  public void testSimpleEnableDictionaryMV()
      throws Exception {
    // TEST 1. Check running forwardIndexHandler on a V1 segment. No-op for all existing raw columns.
    buildV1Segment();
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, false, false, 0,
        ChunkCompressionType.LZ4, false, 13, DataType.INT, 106688);

    // Convert the segment to V3.
    convertV1SegmentToV3();

    // TEST 2: Enable dictionary on EXISTING_STRING_COL_RAW
    _noDictionaryColumns.remove(EXISTING_INT_COL_RAW_MV);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, true, false, 0, null, false, 13,
        DataType.INT, 106688);
  }

  @Test
  public void testEnableDictAndOtherIndexesSV()
      throws Exception {
    // TEST 1: EXISTING_STRING_COL_RAW. Enable dictionary. Also add inverted index and text index. Reload code path
    // will create dictionary, inverted index and text index.
    buildV3Segment();
    _noDictionaryColumns.remove(EXISTING_STRING_COL_RAW);
    _invertedIndexColumns.add(EXISTING_STRING_COL_RAW);
    _fieldConfigMap.put(EXISTING_STRING_COL_RAW,
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.DICTIONARY,
            List.of(FieldConfig.IndexType.INVERTED, FieldConfig.IndexType.TEXT), null, null));
    checkForwardIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, true, false, 4, null, true, 0,
        DataType.STRING, 100000);
    validateIndex(StandardIndexes.inverted(), EXISTING_STRING_COL_RAW, 5, 3, false, true, false, 4, true, 0, null,
        false, DataType.STRING, 100000);
    validateIndex(StandardIndexes.text(), EXISTING_STRING_COL_RAW, 5, 3, false, true, false, 4, true, 0, null, false,
        DataType.STRING, 100000);

    // TEST 2: EXISTING_STRING_COL_RAW. Enable dictionary on a raw column that already has text index.
    resetIndexConfigs();
    _fieldConfigMap.put(EXISTING_STRING_COL_RAW,
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.RAW, List.of(FieldConfig.IndexType.TEXT),
            null, null));
    buildV3Segment();
    validateIndex(StandardIndexes.text(), EXISTING_STRING_COL_RAW, 5, 3, false, false, false, 0, true, 0, null, false,
        DataType.STRING, 100000);

    // At this point, the segment has text index. Now, the reload path should create a dictionary.
    _noDictionaryColumns.remove(EXISTING_STRING_COL_RAW);
    _fieldConfigMap.put(EXISTING_STRING_COL_RAW,
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.DICTIONARY,
            List.of(FieldConfig.IndexType.TEXT), null, null));
    checkForwardIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, true, false, 4, null, true, 0,
        DataType.STRING, 100000);
    validateIndex(StandardIndexes.text(), EXISTING_STRING_COL_RAW, 5, 3, false, true, false, 4, true, 0, null, false,
        DataType.STRING, 100000);

    // TEST 3: EXISTING_INT_COL_RAW. Enable dictionary on a column that already has range index.
    resetIndexConfigs();
    _rangeIndexColumns.add(EXISTING_INT_COL_RAW);
    buildV3Segment();
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW, 42242, 16, false, false, false, 0, true, 0,
        ChunkCompressionType.LZ4, false, DataType.INT, 100000);
    long oldRangeIndexSize =
        new SegmentMetadataImpl(INDEX_DIR).getColumnMetadataFor(EXISTING_INT_COL_RAW).getIndexSizeMap()
            .get(StandardIndexes.range());
    // At this point, the segment has range index. Now the reload path should create a dictionary and rewrite the
    // range index.
    _noDictionaryColumns.remove(EXISTING_INT_COL_RAW);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW, 42242, 16, _schema, false, true, false, 0, null, true, 0,
        DataType.INT, 100000);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW, 42242, 16, false, true, false, 0, true, 0, null, false,
        DataType.INT, 100000);
    long newRangeIndexSize =
        new SegmentMetadataImpl(INDEX_DIR).getColumnMetadataFor(EXISTING_INT_COL_RAW).getIndexSizeMap()
            .get(StandardIndexes.range());
    assertNotEquals(oldRangeIndexSize, newRangeIndexSize);
  }

  @Test
  public void testEnableDictAndOtherIndexesMV()
      throws Exception {
    // TEST 1: EXISTING_INT_COL_RAW_MV. Enable dictionary for an MV column. Also enable inverted index and range index.
    buildV3Segment();
    _noDictionaryColumns.remove(EXISTING_INT_COL_RAW_MV);
    _invertedIndexColumns.add(EXISTING_INT_COL_RAW_MV);
    _rangeIndexColumns.add(EXISTING_INT_COL_RAW_MV);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, true, false, 0, null, false, 13,
        DataType.INT, 106688);
    validateIndex(StandardIndexes.inverted(), EXISTING_INT_COL_RAW_MV, 18499, 15, false, true, false, 0, false, 13,
        null, false, DataType.INT, 106688);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW_MV, 18499, 15, false, true, false, 0, false, 13, null,
        false, DataType.INT, 106688);

    // TEST 2: EXISTING_INT_COL_RAW_MV. Enable dictionary for an MV column that already has range index.
    resetIndexConfigs();
    _rangeIndexColumns.add(EXISTING_INT_COL_RAW_MV);
    buildV3Segment();
    validateIndex(StandardIndexes.forward(), EXISTING_INT_COL_RAW_MV, 18499, 15, false, false, false, 0, false, 13,
        ChunkCompressionType.LZ4, false, DataType.INT, 106688);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW_MV, 18499, 15, false, false, false, 0, false, 13,
        ChunkCompressionType.LZ4, false, DataType.INT, 106688);

    // Enable dictionary.
    _noDictionaryColumns.remove(EXISTING_INT_COL_RAW_MV);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, true, false, 0, null, false, 13,
        DataType.INT, 106688);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW_MV, 18499, 15, false, true, false, 0, false, 13, null,
        false, DataType.INT, 106688);
  }

  @Test
  public void testSimpleDisableDictionary()
      throws Exception {
    // TEST 1. Check running forwardIndexHandler on a V1 segment. No-op for all existing dict columns.
    buildV1Segment();
    checkForwardIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _schema, false, true, false, 26, null, true, 0,
        DataType.STRING, 100000);
    validateIndex(StandardIndexes.forward(), COLUMN10_NAME, 3960, 12, false, true, false, 0, true, 0, null, false,
        DataType.INT, 100000);

    // Convert the segment to V3.
    convertV1SegmentToV3();

    // TEST 2: Disable dictionary for EXISTING_STRING_COL_DICT.
    _noDictionaryColumns.add(EXISTING_STRING_COL_DICT);
    checkForwardIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _schema, false, false, false, 0, ChunkCompressionType.LZ4,
        true, 0, DataType.STRING, 100000);

    // TEST 3: Disable dictionary for COLUMN10_NAME
    _noDictionaryColumns.add(COLUMN10_NAME);
    checkForwardIndexCreation(COLUMN10_NAME, 3960, 12, _schema, false, false, false, 0, ChunkCompressionType.LZ4, true,
        0, DataType.INT, 100000);
  }

  @Test
  public void testDisableDictAndOtherIndexesSV()
      throws Exception {
    // TEST 1: Disable dictionary on a column that has inverted index. Should be a no-op and column should still have
    // a dictionary.
    _invertedIndexColumns.add(COLUMN1_NAME);
    buildV3Segment();
    _noDictionaryColumns.add(COLUMN1_NAME);
    checkForwardIndexCreation(COLUMN1_NAME, 51594, 16, _schema, false, true, false, 0, null, true, 0, DataType.INT,
        100000);

    // TEST 2: Disable dictionary. Also remove inverted index on column1.
    _invertedIndexColumns.remove(COLUMN1_NAME);
    checkForwardIndexCreation(COLUMN1_NAME, 51594, 16, _schema, false, false, false, 0, null, true, 0, DataType.INT,
        100000);

    // TEST 3: Disable dictionary for a column (Column10) that has range index.
    _rangeIndexColumns.add(COLUMN10_NAME);
    buildV3Segment();
    validateIndex(StandardIndexes.forward(), COLUMN10_NAME, 3960, 12, false, true, false, 0, true, 0, null, false,
        DataType.INT, 100000);
    validateIndex(StandardIndexes.range(), COLUMN10_NAME, 3960, 12, false, true, false, 0, true, 0, null, false,
        DataType.INT, 100000);
    long oldRangeIndexSize = new SegmentMetadataImpl(INDEX_DIR).getColumnMetadataFor(COLUMN10_NAME).getIndexSizeMap()
        .get(StandardIndexes.range());
    _noDictionaryColumns.add(COLUMN10_NAME);
    checkForwardIndexCreation(COLUMN10_NAME, 3960, 12, _schema, false, false, false, 0, ChunkCompressionType.LZ4, true,
        0, DataType.INT, 100000);
    validateIndex(StandardIndexes.range(), COLUMN10_NAME, 3960, 12, false, false, false, 0, true, 0,
        ChunkCompressionType.LZ4, false, DataType.INT, 100000);
    long newRangeIndexSize = new SegmentMetadataImpl(INDEX_DIR).getColumnMetadataFor(COLUMN10_NAME).getIndexSizeMap()
        .get(StandardIndexes.range());
    assertNotEquals(oldRangeIndexSize, newRangeIndexSize);

    // TEST4: Disable dictionary but add text index.
    validateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4, false, true, false, 26, true, 0, null,
        false, DataType.STRING, 100000);
    _noDictionaryColumns.add(EXISTING_STRING_COL_DICT);
    _fieldConfigMap.put(EXISTING_STRING_COL_DICT,
        new FieldConfig(EXISTING_STRING_COL_DICT, FieldConfig.EncodingType.RAW, List.of(FieldConfig.IndexType.TEXT),
            null, null));
    checkForwardIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _schema, false, false, false, 0, ChunkCompressionType.LZ4,
        true, 0, DataType.STRING, 100000);
    validateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4, false, false, false, 0, true, 0, null,
        false, DataType.STRING, 100000);
  }

  @Test
  public void testDisableDictAndOtherIndexesMV()
      throws Exception {
    // Set up: Enable dictionary on MV column6 and validate.
    buildV3Segment();
    _noDictionaryColumns.remove(EXISTING_INT_COL_RAW_MV);
    _rangeIndexColumns.add(EXISTING_INT_COL_RAW_MV);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, true, false, 0, null, false, 13,
        DataType.INT, 106688);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW_MV, 18499, 15, false, true, false, 0, false, 13, null,
        false, DataType.INT, 106688);

    // TEST 1: Disable dictionary on a column where range index is already enabled.
    _noDictionaryColumns.add(EXISTING_INT_COL_RAW_MV);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, false, false, 0, null, false, 13,
        DataType.INT, 106688);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW_MV, 18499, 15, false, false, false, 0, false, 13, null,
        false, DataType.INT, 106688);

    // TEST 2. Disable dictionary on a column where inverted index is enabled. Should be a no-op.
    validateIndex(StandardIndexes.forward(), COLUMN7_NAME, 359, 9, false, true, false, 0, false, 24, null, false,
        DataType.INT, 134090);
    validateIndex(StandardIndexes.inverted(), COLUMN7_NAME, 359, 9, false, true, false, 0, false, 24, null, false,
        DataType.INT, 134090);
    _noDictionaryColumns.add(COLUMN7_NAME);
    checkForwardIndexCreation(COLUMN7_NAME, 359, 9, _schema, false, true, false, 0, null, false, 24, DataType.INT,
        134090);
    validateIndex(StandardIndexes.inverted(), COLUMN7_NAME, 359, 9, false, true, false, 0, false, 24, null, false,
        DataType.INT, 134090);

    // TEST 3: Disable dictionary and disable inverted index on column7.
    _invertedIndexColumns.remove(COLUMN7_NAME);
    checkForwardIndexCreation(COLUMN7_NAME, 359, 9, _schema, false, false, false, 0, null, false, 24, DataType.INT,
        134090);
  }

  @Test
  public void testForwardIndexHandlerChangeCompression()
      throws Exception {
    // Test1: Rewriting forward index will be a no-op for v1 segments. Default LZ4 compressionType will be retained.
    buildV1Segment();
    _fieldConfigMap.put(EXISTING_STRING_COL_RAW,
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.ZSTANDARD,
            null));
    checkForwardIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0, ChunkCompressionType.LZ4,
        true, 0, DataType.STRING, 100000);

    // Convert the segment to V3.
    convertV1SegmentToV3();

    // Test2: Now forward index will be rewritten with ZSTANDARD compressionType.
    checkForwardIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0,
        ChunkCompressionType.ZSTANDARD, true, 0, DataType.STRING, 100000);

    // Test3: Change compression on existing raw index column. Also add text index on same column. Check correctness.
    _fieldConfigMap.put(EXISTING_STRING_COL_RAW,
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.RAW, List.of(FieldConfig.IndexType.TEXT),
            CompressionCodec.SNAPPY, null));
    checkTextIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0);
    validateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_RAW, 5, 3, false, false, false, 0, true, 0,
        ChunkCompressionType.SNAPPY, false, DataType.STRING, 100000);

    // Test4: Change compression on RAW index column. Change another index on another column. Check correctness.
    resetIndexConfigs();
    buildV3Segment();
    _fieldConfigMap.put(EXISTING_STRING_COL_RAW,
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.ZSTANDARD,
            null));
    _fieldConfigMap.put(EXISTING_STRING_COL_DICT,
        new FieldConfig(EXISTING_STRING_COL_DICT, FieldConfig.EncodingType.DICTIONARY,
            List.of(FieldConfig.IndexType.FST), null, null));
    // Check FST index
    checkFSTIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _newColumnsSchemaWithFST, false, false, 26);
    // Check forward index.
    validateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_RAW, 5, 3, false, false, false, 0, true, 0,
        ChunkCompressionType.ZSTANDARD, false, DataType.STRING, 100000);

    // Test5: Change compressionType for an MV column
    _fieldConfigMap.put(EXISTING_INT_COL_RAW_MV,
        new FieldConfig(EXISTING_INT_COL_RAW_MV, FieldConfig.EncodingType.RAW, List.of(), CompressionCodec.ZSTANDARD,
            null));
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, false, false, 0,
        ChunkCompressionType.ZSTANDARD, false, 13, DataType.INT, 106688);
  }

  /**
   * Test to check for default column handling and text index creation during
   * segment load after a new dictionary encoded column is added to the schema
   * with text index creation enabled.
   * This will exercise both code paths in SegmentPreprocessor (segment load):
   * (1) Default column handler to add forward index and dictionary
   * (2) Text index handler to add text index
   */
  @Test(dataProvider = "bothV1AndV3")
  public void testEnableTextIndexOnNewColumnDictEncoded(SegmentVersion segmentVersion)
      throws Exception {
    buildSegment(segmentVersion);
    _fieldConfigMap.put(NEWLY_ADDED_STRING_COL_DICT,
        new FieldConfig(NEWLY_ADDED_STRING_COL_DICT, FieldConfig.EncodingType.DICTIONARY,
            List.of(FieldConfig.IndexType.TEXT), null, null));
    _fieldConfigMap.put(NEWLY_ADDED_STRING_MV_COL_DICT,
        new FieldConfig(NEWLY_ADDED_STRING_MV_COL_DICT, FieldConfig.EncodingType.DICTIONARY,
            List.of(FieldConfig.IndexType.TEXT), null, null));
    checkTextIndexCreation(NEWLY_ADDED_STRING_COL_DICT, 1, 1, _newColumnsSchemaWithText, true, true, true, 4);
    validateIndex(StandardIndexes.text(), NEWLY_ADDED_STRING_MV_COL_DICT, 1, 1, true, true, false, 4, false, 1, null,
        false, DataType.STRING, 100000);
  }

  /**
   * Test to check text index creation during segment load after text index
   * creation is enabled on an existing raw column.
   * This will exercise the SegmentPreprocessor code path during segment load
   */
  @Test(dataProvider = "bothV1AndV3")
  public void testEnableTextIndexOnExistingRawColumn(SegmentVersion segmentVersion)
      throws Exception {
    buildSegment(segmentVersion);
    _fieldConfigMap.put(EXISTING_STRING_COL_RAW,
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.RAW, List.of(FieldConfig.IndexType.TEXT),
            null, null));
    checkTextIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0);
  }

  /**
   * Test to check text index creation during segment load after text index
   * creation is enabled on an existing dictionary encoded column.
   * This will exercise the SegmentPreprocessor code path during segment load
   */
  @Test(dataProvider = "bothV1AndV3")
  public void testEnableTextIndexOnExistingDictEncodedColumn(SegmentVersion segmentVersion)
      throws Exception {
    buildSegment(segmentVersion);
    _fieldConfigMap.put(EXISTING_STRING_COL_DICT,
        new FieldConfig(EXISTING_STRING_COL_DICT, FieldConfig.EncodingType.DICTIONARY,
            List.of(FieldConfig.IndexType.TEXT), null, null));
    checkTextIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _schema, false, true, false, 26);
  }

  private void checkFSTIndexCreation(String column, int cardinality, int bits, Schema schema, boolean isAutoGenerated,
      boolean isSorted, int dictionaryElementSize)
      throws Exception {
    createAndValidateIndex(StandardIndexes.fst(), column, cardinality, bits, schema, isAutoGenerated, true, isSorted,
        dictionaryElementSize, true, 0, null, false, DataType.STRING, 100000);
  }

  private void checkTextIndexCreation(String column, int cardinality, int bits, Schema schema, boolean isAutoGenerated,
      boolean hasDictionary, boolean isSorted, int dictionaryElementSize)
      throws Exception {
    createAndValidateIndex(StandardIndexes.text(), column, cardinality, bits, schema, isAutoGenerated, hasDictionary,
        isSorted, dictionaryElementSize, true, 0, null, false, DataType.STRING, 100000);
  }

  private void checkTextIndexCreation(String column, int cardinality, int bits, Schema schema, boolean isAutoGenerated,
      boolean hasDictionary, boolean isSorted, int dictionaryElementSize, boolean isSingleValue,
      int maxNumberOfMultiValues)
      throws Exception {
    createAndValidateIndex(StandardIndexes.text(), column, cardinality, bits, schema, isAutoGenerated, hasDictionary,
        isSorted, dictionaryElementSize, isSingleValue, maxNumberOfMultiValues, null, false, DataType.STRING, 100000);
  }

  private void checkForwardIndexCreation(String column, int cardinality, int bits, Schema schema,
      boolean isAutoGenerated, boolean hasDictionary, boolean isSorted, int dictionaryElementSize,
      ChunkCompressionType expectedCompressionType, boolean isSingleValue, int maxNumberOfMultiValues,
      DataType dataType, int totalNumberOfEntries)
      throws Exception {
    createAndValidateIndex(StandardIndexes.forward(), column, cardinality, bits, schema, isAutoGenerated, hasDictionary,
        isSorted, dictionaryElementSize, isSingleValue, maxNumberOfMultiValues, expectedCompressionType, false,
        dataType, totalNumberOfEntries);
  }

  private void createAndValidateIndex(IndexType<?, ?, ?> indexType, String column, int cardinality, int bits,
      Schema schema, boolean isAutoGenerated, boolean hasDictionary, boolean isSorted, int dictionaryElementSize,
      boolean isSingleValued, int maxNumberOfMultiValues, ChunkCompressionType expectedCompressionType,
      boolean forwardIndexDisabled, DataType dataType, int totalNumberOfEntries)
      throws Exception {
    runPreProcessor(schema);
    validateIndex(indexType, column, cardinality, bits, isAutoGenerated, hasDictionary, isSorted, dictionaryElementSize,
        isSingleValued, maxNumberOfMultiValues, expectedCompressionType, forwardIndexDisabled, dataType,
        totalNumberOfEntries);
  }

  private void validateIndex(IndexType<?, ?, ?> indexType, String column, int cardinality, int bits,
      boolean isAutoGenerated, boolean hasDictionary, boolean isSorted, int dictionaryElementSize,
      boolean isSingleValued, int maxNumberOfMultiValues, ChunkCompressionType expectedCompressionType,
      boolean forwardIndexDisabled, DataType dataType, int totalNumberOfEntries)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
    assertEquals(columnMetadata.hasDictionary(), hasDictionary);
    assertEquals(columnMetadata.getFieldSpec(), new DimensionFieldSpec(column, dataType, isSingleValued));
    assertEquals(columnMetadata.getCardinality(), cardinality);
    assertEquals(columnMetadata.getTotalDocs(), 100000);
    assertEquals(columnMetadata.getBitsPerElement(), bits);
    assertEquals(columnMetadata.getColumnMaxLength(), dictionaryElementSize);
    assertEquals(columnMetadata.isSorted(), isSorted);
    assertEquals(columnMetadata.getMaxNumberOfMultiValues(), maxNumberOfMultiValues);
    assertEquals(columnMetadata.getTotalNumberOfEntries(), totalNumberOfEntries);
    assertEquals(columnMetadata.isAutoGenerated(), isAutoGenerated);

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      if (indexType != StandardIndexes.forward() || !forwardIndexDisabled) {
        assertTrue(reader.hasIndexFor(column, indexType));
      } else {
        // Verify forward index when it is configured to be disabled
        if (isSorted) {
          // Disabling the forward index for sorted columns should be a no-op
          assertTrue(reader.hasIndexFor(column, StandardIndexes.forward()));
          assertFalse(reader.hasIndexFor(column, StandardIndexes.inverted()));
        } else {
          // Updating dictionary or forward index for existing columns not supported for v1 segments yet
          if (segmentMetadata.getVersion() == SegmentVersion.v3 || isAutoGenerated) {
            assertFalse(reader.hasIndexFor(column, StandardIndexes.forward()));
          } else {
            assertTrue(reader.hasIndexFor(column, StandardIndexes.forward()));
          }
        }
      }

      // Check if the raw forward index compressionType is correct.
      if (expectedCompressionType != null) {
        assertFalse(hasDictionary);

        try (ForwardIndexReader<?> fwdIndexReader = ForwardIndexType.read(reader, columnMetadata)) {
          ChunkCompressionType compressionType = fwdIndexReader.getCompressionType();
          assertEquals(compressionType, expectedCompressionType);
        }

        File inProgressFile = new File(INDEX_DIR, column + ".fwd.inprogress");
        assertFalse(inProgressFile.exists());

        String fwdIndexFileExtension;
        if (isSingleValued) {
          fwdIndexFileExtension = V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION;
        } else {
          fwdIndexFileExtension = V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION;
        }
        File v1FwdIndexFile = new File(INDEX_DIR, column + fwdIndexFileExtension);
        if (segmentMetadata.getVersion() == SegmentVersion.v3) {
          assertFalse(v1FwdIndexFile.exists());
        } else {
          assertTrue(v1FwdIndexFile.exists());
        }
      }

      // if the text index is enabled on a new column with dictionary,
      // then dictionary should be created by the default column handler
      if (hasDictionary) {
        assertTrue(reader.hasIndexFor(column, StandardIndexes.dictionary()));
      } else {
        assertFalse(reader.hasIndexFor(column, StandardIndexes.dictionary()));
      }
    }
  }

  private void validateIndexDoesNotExist(String column, IndexType<?, ?, ?> indexType)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertFalse(reader.hasIndexFor(column, indexType));
    }
  }

  private void validateIndexExists(String column, IndexType<?, ?, ?> indexType)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertTrue(reader.hasIndexFor(column, indexType));
    }
  }

  @Test
  public void testV1CreateInvertedIndices()
      throws Exception {
    buildV1Segment();

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v1);

    String col1FileName = COLUMN1_NAME + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;
    String col7FileName = COLUMN7_NAME + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;
    String col13FileName = COLUMN13_NAME + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;
    String badColFileName = NO_SUCH_COLUMN_NAME + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;

    File col1File = new File(INDEX_DIR, col1FileName);
    File col7File = new File(INDEX_DIR, col7FileName);
    File col13File = new File(INDEX_DIR, col13FileName);
    File badColFile = new File(INDEX_DIR, badColFileName);
    assertFalse(col1File.exists());
    assertTrue(col7File.exists());
    assertFalse(col13File.exists());
    assertFalse(badColFile.exists());
    FileTime col7LastModifiedTime = Files.getLastModifiedTime(col7File.toPath());

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the first time.
    checkInvertedIndexCreation(false);
    assertTrue(col1File.exists());
    assertTrue(col7File.exists());
    assertTrue(col13File.exists());
    assertFalse(badColFile.exists());
    assertEquals(Files.getLastModifiedTime(col7File.toPath()), col7LastModifiedTime);

    // Update inverted index file last modified time.
    FileTime col1LastModifiedTime = Files.getLastModifiedTime(col1File.toPath());
    FileTime col13LastModifiedTime = Files.getLastModifiedTime(col13File.toPath());

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the second time.
    checkInvertedIndexCreation(true);
    assertTrue(col1File.exists());
    assertTrue(col7File.exists());
    assertTrue(col13File.exists());
    assertFalse(badColFile.exists());
    assertEquals(Files.getLastModifiedTime(col1File.toPath()), col1LastModifiedTime);
    assertEquals(Files.getLastModifiedTime(col7File.toPath()), col7LastModifiedTime);
    assertEquals(Files.getLastModifiedTime(col13File.toPath()), col13LastModifiedTime);
  }

  @Test
  public void testV3CreateInvertedIndices()
      throws Exception {
    buildV3Segment();

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);

    File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(INDEX_DIR, SegmentVersion.v3);
    File singleFileIndex = new File(segmentDirectoryPath, "columns.psf");
    FileTime lastModifiedTime = Files.getLastModifiedTime(singleFileIndex.toPath());
    long fileSize = singleFileIndex.length();

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the first time.
    checkInvertedIndexCreation(false);
    long addedLength = 0L;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      // 8 bytes overhead is for checking integrity of the segment.
      addedLength += reader.getIndexFor(COLUMN1_NAME, StandardIndexes.inverted()).size() + 8;
      addedLength += reader.getIndexFor(COLUMN13_NAME, StandardIndexes.inverted()).size() + 8;
    }
    FileTime newLastModifiedTime = Files.getLastModifiedTime(singleFileIndex.toPath());
    assertTrue(newLastModifiedTime.compareTo(lastModifiedTime) > 0);
    long newFileSize = singleFileIndex.length();
    assertEquals(fileSize + addedLength, newFileSize);

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the second time.
    checkInvertedIndexCreation(true);
    assertEquals(Files.getLastModifiedTime(singleFileIndex.toPath()), newLastModifiedTime);
    assertEquals(singleFileIndex.length(), newFileSize);
  }

  private void checkInvertedIndexCreation(boolean reCreate)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      if (reCreate) {
        assertTrue(reader.hasIndexFor(COLUMN1_NAME, StandardIndexes.inverted()));
        assertTrue(reader.hasIndexFor(COLUMN7_NAME, StandardIndexes.inverted()));
        assertTrue(reader.hasIndexFor(COLUMN13_NAME, StandardIndexes.inverted()));
        assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, StandardIndexes.inverted()));
      } else {
        assertFalse(reader.hasIndexFor(COLUMN1_NAME, StandardIndexes.inverted()));
        assertTrue(reader.hasIndexFor(COLUMN7_NAME, StandardIndexes.inverted()));
        assertFalse(reader.hasIndexFor(COLUMN13_NAME, StandardIndexes.inverted()));
        assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, StandardIndexes.inverted()));
      }
    }

    _invertedIndexColumns.add(COLUMN1_NAME);
    _invertedIndexColumns.add(COLUMN13_NAME);
    runPreProcessor();

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertTrue(reader.hasIndexFor(COLUMN1_NAME, StandardIndexes.inverted()));
      assertTrue(reader.hasIndexFor(COLUMN7_NAME, StandardIndexes.inverted()));
      assertTrue(reader.hasIndexFor(COLUMN13_NAME, StandardIndexes.inverted()));
      assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, StandardIndexes.inverted()));
    }
  }

  @Test(dataProvider = "bothV1AndV3")
  public void testUpdateDefaultColumns(SegmentVersion segmentVersion)
      throws Exception {
    buildSegment(segmentVersion);

    _noDictionaryColumns.add(NEW_RAW_STRING_SV_DIMENSION_COLUMN_NAME);
    _invertedIndexColumns.add(NEW_COLUMN_INVERTED_INDEX);
    _ingestionConfig.setTransformConfigs(
        List.of(new TransformConfig(NEW_INT_SV_DIMENSION_COLUMN_NAME, "plus(column1, 1)"),
            new TransformConfig(NEW_RAW_STRING_SV_DIMENSION_COLUMN_NAME, "reverse(column3)"),
            // Ensure that null values returned by transform functions for derived columns are handled appropriately
            // during segment reload
            new TransformConfig(NEW_NULL_RETURN_STRING_SV_DIMENSION_COLUMN_NAME,
                "json_path_string(column21, 'non-existent-path', null)"),
            // Ensure that any transform function failures result in a null value if error on failure is false
            new TransformConfig(NEW_WRONG_ARG_DATE_TRUNC_DERIVED_COLUMN_NAME, "dateTrunc('abcd', column1)")));
    checkUpdateDefaultColumns();

    // Try to use the third schema and update default value again.
    // For the third schema, we changed the default value for column 'newStringMVDimension' to 'notSameLength',
    // which is not the same length as before. This should be fine for segment format v1.
    // We added two new columns and also removed the NEW_INT_SV_DIMENSION_COLUMN_NAME from schema.
    // NEW_INT_SV_DIMENSION_COLUMN_NAME exists before processing but removed afterwards.
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    assertNotNull(segmentMetadata.getColumnMetadataFor(NEW_INT_SV_DIMENSION_COLUMN_NAME));

    runPreProcessor(_newColumnsSchema3);

    segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    assertNull(segmentMetadata.getColumnMetadataFor(NEW_INT_SV_DIMENSION_COLUMN_NAME));

    ColumnMetadata hllMetricMetadata = segmentMetadata.getColumnMetadataFor(NEW_HLL_BYTE_METRIC_COLUMN_NAME);
    FieldSpec expectedHllMetricFieldSpec = _newColumnsSchema3.getFieldSpecFor(NEW_HLL_BYTE_METRIC_COLUMN_NAME);
    assertEquals(hllMetricMetadata.getFieldSpec(), expectedHllMetricFieldSpec);
    ByteArray expectedDefaultValue = new ByteArray((byte[]) expectedHllMetricFieldSpec.getDefaultNullValue());
    assertEquals(hllMetricMetadata.getMinValue(), expectedDefaultValue);
    assertEquals(hllMetricMetadata.getMaxValue(), expectedDefaultValue);

    ColumnMetadata tDigestMetricMetadata = segmentMetadata.getColumnMetadataFor(NEW_TDIGEST_BYTE_METRIC_COLUMN_NAME);
    FieldSpec expectedTDigestMetricFieldSpec = _newColumnsSchema3.getFieldSpecFor(NEW_TDIGEST_BYTE_METRIC_COLUMN_NAME);
    assertEquals(tDigestMetricMetadata.getFieldSpec(), expectedTDigestMetricFieldSpec);
    expectedDefaultValue = new ByteArray((byte[]) expectedTDigestMetricFieldSpec.getDefaultNullValue());
    assertEquals(tDigestMetricMetadata.getMinValue(), expectedDefaultValue);
    assertEquals(tDigestMetricMetadata.getMaxValue(), expectedDefaultValue);
  }

  private void checkUpdateDefaultColumns()
      throws Exception {
    // Update default value.
    runPreProcessor(_newColumnsSchema1);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);

    // Check column metadata.
    // Check all field for one column, and do necessary checks for other columns.
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_INT_METRIC_COLUMN_NAME);
    assertEquals(columnMetadata.getFieldSpec(), _newColumnsSchema1.getFieldSpecFor(NEW_INT_METRIC_COLUMN_NAME));
    assertEquals(columnMetadata.getCardinality(), 1);
    assertEquals(columnMetadata.getTotalDocs(), 100000);
    assertEquals(columnMetadata.getBitsPerElement(), 1);
    assertEquals(columnMetadata.getColumnMaxLength(), 0);
    assertTrue(columnMetadata.isSorted());
    assertTrue(columnMetadata.hasDictionary());
    assertEquals(columnMetadata.getMaxNumberOfMultiValues(), 0);
    assertEquals(columnMetadata.getTotalNumberOfEntries(), 100000);
    assertTrue(columnMetadata.isAutoGenerated());
    assertEquals(columnMetadata.getMinValue(), 1);
    assertEquals(columnMetadata.getMaxValue(), 1);

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_LONG_METRIC_COLUMN_NAME);
    assertEquals(columnMetadata.getFieldSpec(), _newColumnsSchema1.getFieldSpecFor(NEW_LONG_METRIC_COLUMN_NAME));
    assertEquals(columnMetadata.getMinValue(), 0L);
    assertEquals(columnMetadata.getMaxValue(), 0L);

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_FLOAT_METRIC_COLUMN_NAME);
    assertEquals(columnMetadata.getFieldSpec(), _newColumnsSchema1.getFieldSpecFor(NEW_FLOAT_METRIC_COLUMN_NAME));
    assertEquals(columnMetadata.getMinValue(), 0f);
    assertEquals(columnMetadata.getMaxValue(), 0f);

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_DOUBLE_METRIC_COLUMN_NAME);
    assertEquals(columnMetadata.getFieldSpec(), _newColumnsSchema1.getFieldSpecFor(NEW_DOUBLE_METRIC_COLUMN_NAME));
    assertEquals(columnMetadata.getMinValue(), 0.0);
    assertEquals(columnMetadata.getMaxValue(), 0.0);

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME);
    assertEquals(columnMetadata.getFieldSpec(),
        _newColumnsSchema1.getFieldSpecFor(NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME));
    assertEquals(columnMetadata.getColumnMaxLength(), 0);
    assertEquals(columnMetadata.getMinValue(), 0);
    assertEquals(columnMetadata.getMaxValue(), 0);

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME);
    assertEquals(columnMetadata.getFieldSpec(),
        _newColumnsSchema1.getFieldSpecFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME));
    assertEquals(columnMetadata.getColumnMaxLength(), 4);
    assertFalse(columnMetadata.isSorted());
    assertEquals(columnMetadata.getMaxNumberOfMultiValues(), 1);
    assertEquals(columnMetadata.getTotalNumberOfEntries(), 100000);
    assertEquals(columnMetadata.getMinValue(), "null");
    assertEquals(columnMetadata.getMaxValue(), "null");

    // Derived column
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_INT_SV_DIMENSION_COLUMN_NAME);
    assertEquals(columnMetadata.getFieldSpec(), _newColumnsSchema1.getFieldSpecFor(NEW_INT_SV_DIMENSION_COLUMN_NAME));
    assertTrue(columnMetadata.isAutoGenerated());
    ColumnMetadata originalColumnMetadata = segmentMetadata.getColumnMetadataFor(COLUMN1_NAME);
    assertEquals(columnMetadata.getCardinality(), originalColumnMetadata.getCardinality());
    assertEquals(columnMetadata.getBitsPerElement(), originalColumnMetadata.getBitsPerElement());
    assertEquals(columnMetadata.isSorted(), originalColumnMetadata.isSorted());
    assertEquals(columnMetadata.getMinValue(), (int) originalColumnMetadata.getMinValue() + 1);
    assertEquals(columnMetadata.getMaxValue(), (int) originalColumnMetadata.getMaxValue() + 1);

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_RAW_STRING_SV_DIMENSION_COLUMN_NAME);
    assertEquals(columnMetadata.getFieldSpec(),
        _newColumnsSchema1.getFieldSpecFor(NEW_RAW_STRING_SV_DIMENSION_COLUMN_NAME));
    assertTrue(columnMetadata.isAutoGenerated());
    originalColumnMetadata = segmentMetadata.getColumnMetadataFor("column3");
    assertEquals(columnMetadata.getCardinality(), originalColumnMetadata.getCardinality());
    assertEquals(columnMetadata.getBitsPerElement(), originalColumnMetadata.getBitsPerElement());
    assertEquals(columnMetadata.getTotalNumberOfEntries(), originalColumnMetadata.getTotalNumberOfEntries());

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_NULL_RETURN_STRING_SV_DIMENSION_COLUMN_NAME);
    // All the values should be the default null value
    assertEquals(columnMetadata.getCardinality(), 1);
    assertTrue(columnMetadata.isAutoGenerated());
    assertEquals(columnMetadata.getMinValue(), "nil");
    assertEquals(columnMetadata.getMaxValue(), "nil");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_WRONG_ARG_DATE_TRUNC_DERIVED_COLUMN_NAME);
    // All the values should be the default null value
    assertEquals(columnMetadata.getCardinality(), 1);
    assertTrue(columnMetadata.isAutoGenerated());
    assertEquals(columnMetadata.getMinValue(), Long.MIN_VALUE);
    assertEquals(columnMetadata.getMaxValue(), Long.MIN_VALUE);

    // Check dictionary and forward index exist.
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertTrue(reader.hasIndexFor(NEW_INT_METRIC_COLUMN_NAME, StandardIndexes.dictionary()));
      assertTrue(reader.hasIndexFor(NEW_INT_METRIC_COLUMN_NAME, StandardIndexes.forward()));
      assertTrue(reader.hasIndexFor(NEW_LONG_METRIC_COLUMN_NAME, StandardIndexes.dictionary()));
      assertTrue(reader.hasIndexFor(NEW_LONG_METRIC_COLUMN_NAME, StandardIndexes.forward()));
      assertTrue(reader.hasIndexFor(NEW_FLOAT_METRIC_COLUMN_NAME, StandardIndexes.dictionary()));
      assertTrue(reader.hasIndexFor(NEW_FLOAT_METRIC_COLUMN_NAME, StandardIndexes.forward()));
      assertTrue(reader.hasIndexFor(NEW_DOUBLE_METRIC_COLUMN_NAME, StandardIndexes.dictionary()));
      assertTrue(reader.hasIndexFor(NEW_DOUBLE_METRIC_COLUMN_NAME, StandardIndexes.forward()));
      assertTrue(reader.hasIndexFor(NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME, StandardIndexes.dictionary()));
      assertTrue(reader.hasIndexFor(NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME, StandardIndexes.forward()));
      assertTrue(reader.hasIndexFor(NEW_INT_SV_DIMENSION_COLUMN_NAME, StandardIndexes.dictionary()));
      assertTrue(reader.hasIndexFor(NEW_INT_SV_DIMENSION_COLUMN_NAME, StandardIndexes.forward()));
      assertTrue(reader.hasIndexFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME, StandardIndexes.dictionary()));
      assertTrue(reader.hasIndexFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME, StandardIndexes.forward()));
      // Dictionary shouldn't be created for raw derived column
      assertFalse(reader.hasIndexFor(NEW_RAW_STRING_SV_DIMENSION_COLUMN_NAME, StandardIndexes.dictionary()));
      assertTrue(reader.hasIndexFor(NEW_RAW_STRING_SV_DIMENSION_COLUMN_NAME, StandardIndexes.forward()));

      // Null vector index should be created for derived column with null values
      assertTrue(
          reader.hasIndexFor(NEW_NULL_RETURN_STRING_SV_DIMENSION_COLUMN_NAME, StandardIndexes.nullValueVector()));
      assertTrue(reader.hasIndexFor(NEW_NULL_RETURN_STRING_SV_DIMENSION_COLUMN_NAME, StandardIndexes.forward()));
      assertTrue(reader.hasIndexFor(NEW_NULL_RETURN_STRING_SV_DIMENSION_COLUMN_NAME, StandardIndexes.dictionary()));
      assertTrue(reader.hasIndexFor(NEW_WRONG_ARG_DATE_TRUNC_DERIVED_COLUMN_NAME, StandardIndexes.nullValueVector()));
      assertTrue(reader.hasIndexFor(NEW_WRONG_ARG_DATE_TRUNC_DERIVED_COLUMN_NAME, StandardIndexes.forward()));
      assertTrue(reader.hasIndexFor(NEW_WRONG_ARG_DATE_TRUNC_DERIVED_COLUMN_NAME, StandardIndexes.dictionary()));

      assertTrue(reader.hasIndexFor(NEW_INT_METRIC_COLUMN_NAME, StandardIndexes.nullValueVector()));
      assertTrue(reader.hasIndexFor(NEW_LONG_METRIC_COLUMN_NAME, StandardIndexes.nullValueVector()));
      assertTrue(reader.hasIndexFor(NEW_FLOAT_METRIC_COLUMN_NAME, StandardIndexes.nullValueVector()));
      assertTrue(reader.hasIndexFor(NEW_DOUBLE_METRIC_COLUMN_NAME, StandardIndexes.nullValueVector()));
      assertTrue(reader.hasIndexFor(NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME, StandardIndexes.nullValueVector()));
      assertTrue(reader.hasIndexFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME, StandardIndexes.nullValueVector()));
    }

    // Use the second schema and update default value again.
    // For the second schema, we changed the default value for column 'newIntMetric' to 2, and added default value
    // 'abcd' (keep the same length as 'null') to column 'newStringMVDimension'.
    runPreProcessor(_newColumnsSchema2);
    segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);

    // Check column metadata.
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_INT_METRIC_COLUMN_NAME);
    assertEquals(columnMetadata.getMinValue(), 2);
    assertEquals(columnMetadata.getMaxValue(), 2);
    assertEquals(columnMetadata.getFieldSpec().getDefaultNullValue(), 2);

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME);
    assertEquals(columnMetadata.getMinValue(), "abcd");
    assertEquals(columnMetadata.getMaxValue(), "abcd");
    assertEquals(columnMetadata.getFieldSpec().getDefaultNullValue(), "abcd");
  }

  @Test(dataProvider = "bothV1AndV3")
  public void testColumnMinMaxValue(SegmentVersion segmentVersion)
      throws Exception {
    buildSegment(segmentVersion);

    // Remove min/max value from the metadata
    removeMinMaxValuesFromMetadataFile();

    _columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.NONE;
    runPreProcessor();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    ColumnMetadata timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    ColumnMetadata dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column1");
    ColumnMetadata metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    assertNull(timeColumnMetadata.getMinValue());
    assertNull(timeColumnMetadata.getMaxValue());
    assertNull(dimensionColumnMetadata.getMinValue());
    assertNull(dimensionColumnMetadata.getMaxValue());
    assertNull(metricColumnMetadata.getMinValue());
    assertNull(metricColumnMetadata.getMaxValue());

    _columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.TIME;
    runPreProcessor();
    segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column5");
    metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    assertEquals(timeColumnMetadata.getMinValue(), 1756015683);
    assertEquals(timeColumnMetadata.getMaxValue(), 1756015683);
    assertNull(dimensionColumnMetadata.getMinValue());
    assertNull(dimensionColumnMetadata.getMaxValue());
    assertNull(metricColumnMetadata.getMinValue());
    assertNull(metricColumnMetadata.getMaxValue());

    _columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.NON_METRIC;
    runPreProcessor();
    segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column5");
    metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    assertEquals(timeColumnMetadata.getMinValue(), 1756015683);
    assertEquals(timeColumnMetadata.getMaxValue(), 1756015683);
    assertEquals(dimensionColumnMetadata.getMinValue(), "AKXcXcIqsqOJFsdwxZ");
    assertEquals(dimensionColumnMetadata.getMaxValue(), "yQkJTLOQoOqqhkAClgC");
    assertNull(metricColumnMetadata.getMinValue());
    assertNull(metricColumnMetadata.getMaxValue());

    _columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.ALL;
    runPreProcessor();
    segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column5");
    metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    assertEquals(timeColumnMetadata.getMinValue(), 1756015683);
    assertEquals(timeColumnMetadata.getMaxValue(), 1756015683);
    assertEquals(dimensionColumnMetadata.getMinValue(), "AKXcXcIqsqOJFsdwxZ");
    assertEquals(dimensionColumnMetadata.getMaxValue(), "yQkJTLOQoOqqhkAClgC");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column14");
    assertEquals(dimensionColumnMetadata.getMaxValue(), -9223372036854775808L);
    assertEquals(dimensionColumnMetadata.getMinValue(), -9223372036854775808L);
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column15");
    assertEquals(dimensionColumnMetadata.getMaxValue(), Float.NEGATIVE_INFINITY);
    assertEquals(dimensionColumnMetadata.getMinValue(), Float.NEGATIVE_INFINITY);
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column16");
    assertEquals(dimensionColumnMetadata.getMaxValue(), Double.NEGATIVE_INFINITY);
    assertEquals(dimensionColumnMetadata.getMinValue(), Double.NEGATIVE_INFINITY);
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column17");
    assertEquals(dimensionColumnMetadata.getMaxValue(), new BigDecimal("0"));
    assertEquals(dimensionColumnMetadata.getMinValue(), new BigDecimal("0"));
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column18");
    assertEquals(dimensionColumnMetadata.getMaxValue(), 0);
    assertEquals(dimensionColumnMetadata.getMinValue(), 0);
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column19");
    assertEquals(dimensionColumnMetadata.getMaxValue().toString(), "0");
    assertEquals(dimensionColumnMetadata.getMinValue().toString(), "0");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column20");
    assertEquals(dimensionColumnMetadata.getMaxValue(), "null");
    assertEquals(dimensionColumnMetadata.getMinValue(), "null");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column21");
    assertEquals(dimensionColumnMetadata.getMaxValue(), "null");
    assertEquals(dimensionColumnMetadata.getMinValue(), "null");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column22");
    assertEquals(dimensionColumnMetadata.getMaxValue().toString(), "");
    assertEquals(dimensionColumnMetadata.getMinValue().toString(), "");
    assertEquals(metricColumnMetadata.getMinValue(), 890662862);
    assertEquals(metricColumnMetadata.getMaxValue(), 890662862);
  }

  @Test
  public void testV1CleanupIndices()
      throws Exception {
    buildV1Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v1);

    // Need to create two default columns with Bytes and JSON string for H3 and JSON index.
    // Other kinds of indices can all be put on column3 with String values.
    String strColumn = "column3";
    _invertedIndexColumns.add(strColumn);
    _rangeIndexColumns.add(strColumn);
    _fieldConfigMap.put(strColumn, new FieldConfig(strColumn, FieldConfig.EncodingType.DICTIONARY,
        List.of(FieldConfig.IndexType.INVERTED, FieldConfig.IndexType.RANGE, FieldConfig.IndexType.TEXT,
            FieldConfig.IndexType.FST), null, null));
    _bloomFilterConfigs = Map.of(strColumn, new BloomFilterConfig(0.1, 1024, true));

    // V1 use separate file for each column index.
    File iiFile = new File(INDEX_DIR, strColumn + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    File rgFile = new File(INDEX_DIR, strColumn + V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION);
    File txtFile = new File(INDEX_DIR, strColumn + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    File fstFile = new File(INDEX_DIR, strColumn + V1Constants.Indexes.LUCENE_V912_FST_INDEX_FILE_EXTENSION);
    File bfFile = new File(INDEX_DIR, strColumn + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);

    assertFalse(iiFile.exists());
    assertFalse(rgFile.exists());
    assertFalse(txtFile.exists());
    assertFalse(fstFile.exists());
    assertFalse(bfFile.exists());

    // Create all kinds of indices.
    runPreProcessor();
    assertTrue(iiFile.exists());
    assertTrue(rgFile.exists());
    assertTrue(txtFile.exists());
    assertTrue(fstFile.exists());
    assertTrue(bfFile.exists());

    // Remove all kinds of indices.
    resetIndexConfigs();
    runPreProcessor();
    assertFalse(iiFile.exists());
    assertFalse(rgFile.exists());
    assertFalse(txtFile.exists());
    assertFalse(fstFile.exists());
    assertFalse(bfFile.exists());
  }

  @Test
  public void testV3CleanupIndices()
      throws Exception {
    buildV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);

    // V3 use single file for all column indices.
    File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(INDEX_DIR, SegmentVersion.v3);
    File singleFileIndex = new File(segmentDirectoryPath, "columns.psf");
    long initFileSize = singleFileIndex.length();

    // Need to create two default columns with Bytes and JSON string for H3 and JSON index.
    // Other kinds of indices can all be put on column3 with String values.
    String strColumn = "column3";
    _invertedIndexColumns.add(strColumn);
    _rangeIndexColumns.add(strColumn);
    _fieldConfigMap.put(strColumn, new FieldConfig(strColumn, FieldConfig.EncodingType.DICTIONARY,
        List.of(FieldConfig.IndexType.INVERTED, FieldConfig.IndexType.RANGE, FieldConfig.IndexType.TEXT,
            FieldConfig.IndexType.FST), null, null));
    _bloomFilterConfigs = Map.of(strColumn, new BloomFilterConfig(0.1, 1024, true));

    // Create all kinds of indices.
    runPreProcessor();
    long addedLength = 0;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      addedLength += reader.getIndexFor(strColumn, StandardIndexes.inverted()).size() + 8;
      addedLength += reader.getIndexFor(strColumn, StandardIndexes.range()).size() + 8;
      addedLength += reader.getIndexFor(strColumn, StandardIndexes.fst()).size() + 8;
      addedLength += reader.getIndexFor(strColumn, StandardIndexes.bloomFilter()).size() + 8;
      assertTrue(reader.hasIndexFor(strColumn, StandardIndexes.text()));
    }
    assertEquals(singleFileIndex.length(), initFileSize + addedLength);

    // Remove all kinds of indices, and size gets back initial size.
    resetIndexConfigs();
    runPreProcessor();
    assertEquals(singleFileIndex.length(), initFileSize);
  }

  @Test
  public void testV1CleanupH3AndTextIndices()
      throws Exception {
    buildV1Segment();

    // Add the two derived columns for H3 and Json index.
    runPreProcessor(_newColumnsSchemaWithH3Json);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    assertNotNull(segmentMetadata.getColumnMetadataFor("newH3Col"));
    assertNotNull(segmentMetadata.getColumnMetadataFor("newJsonCol"));

    // V1 use separate file for each column index.
    File h3File = new File(INDEX_DIR, "newH3Col" + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);
    File jsFile = new File(INDEX_DIR, "newJsonCol" + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    assertFalse(h3File.exists());
    assertFalse(jsFile.exists());

    // Create H3 and Json indices.
    _fieldConfigMap.put("newH3Col",
        new FieldConfig("newH3Col", FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.H3), null,
            Map.of("resolutions", "5")));
    _jsonIndexConfigs = Map.of("newJsonCol", new JsonIndexConfig());
    runPreProcessor(_newColumnsSchemaWithH3Json);
    assertTrue(h3File.exists());
    assertTrue(jsFile.exists());

    // Remove H3 and Json indices.
    resetIndexConfigs();
    runPreProcessor(_newColumnsSchemaWithH3Json);
    assertFalse(h3File.exists());
    assertFalse(jsFile.exists());
  }

  @Test
  public void testV3CleanupH3AndTextIndices()
      throws Exception {
    buildV3Segment();

    // V3 use single file for all column indices.
    File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(INDEX_DIR, SegmentVersion.v3);
    File singleFileIndex = new File(segmentDirectoryPath, "columns.psf");

    // Add the two derived columns for H3 and Json index.
    runPreProcessor(_newColumnsSchemaWithH3Json);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    assertNotNull(segmentMetadata.getColumnMetadataFor("newH3Col"));
    assertNotNull(segmentMetadata.getColumnMetadataFor("newJsonCol"));
    long initFileSize = singleFileIndex.length();

    // Create H3 and Json indices.
    _fieldConfigMap.put("newH3Col",
        new FieldConfig("newH3Col", FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.H3), null,
            Map.of("resolutions", "5")));
    _jsonIndexConfigs = Map.of("newJsonCol", new JsonIndexConfig());
    runPreProcessor(_newColumnsSchemaWithH3Json);
    long addedLength = 0;
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      addedLength += reader.getIndexFor("newH3Col", StandardIndexes.h3()).size() + 8;
      addedLength += reader.getIndexFor("newJsonCol", StandardIndexes.json()).size() + 8;
    }
    assertEquals(singleFileIndex.length(), initFileSize + addedLength);

    // Remove H3 and Json indices, and size gets back to initial.
    resetIndexConfigs();
    runPreProcessor(_newColumnsSchemaWithH3Json);
    assertEquals(singleFileIndex.length(), initFileSize);
  }

  @Test(dataProvider = "bothV1AndV3")
  public void testIfNeedProcess(SegmentVersion segmentVersion)
      throws Exception {
    buildSegment(segmentVersion);

    // Require to add some default columns with new schema.
    verifyProcessNeeded();

    // No preprocessing needed if required to add certain index on non-existing, sorted or raw column.
    // Add inverted index to sorted column
    _invertedIndexColumns.add("daysSinceEpoch");
    verifyProcessNotNeeded();
    _invertedIndexColumns.remove("daysSinceEpoch");
    // Add range index to sorted column
    _rangeIndexColumns.add("daysSinceEpoch");
    verifyProcessNotNeeded();
    _rangeIndexColumns.remove("daysSinceEpoch");
    // Add inverted index to raw column
    _invertedIndexColumns.add(EXISTING_STRING_COL_RAW);
    verifyProcessNotNeeded();
    _invertedIndexColumns.remove(EXISTING_STRING_COL_RAW);
    // Add inverted index to non-existing column
    _invertedIndexColumns.add("newColumnX");
    verifyProcessNotNeeded();
    _invertedIndexColumns.remove("newColumnX");
    // Add range index to non-existing column
    _rangeIndexColumns.add("newColumnX");
    verifyProcessNotNeeded();
    _rangeIndexColumns.remove("newColumnX");
    // Add text/FST/H3 index to non-existing column
    _fieldConfigMap.put("newColumnX", new FieldConfig("newColumnX", FieldConfig.EncodingType.DICTIONARY,
        List.of(FieldConfig.IndexType.TEXT, FieldConfig.IndexType.FST, FieldConfig.IndexType.H3), null,
        Map.of("resolutions", "5")));
    verifyProcessNotNeeded();
    _fieldConfigMap.remove("newColumnX");
    // Add bloom filter to non-existing column
    _bloomFilterConfigs = Map.of("newColumnX", new BloomFilterConfig(0.1, 1024, true));
    verifyProcessNotNeeded();
    _bloomFilterConfigs = null;
    // Add json index to non-existing column
    _jsonIndexConfigs = Map.of("newColumnX", new JsonIndexConfig());
    verifyProcessNotNeeded();
    _jsonIndexConfigs = null;

    // Require to add different types of indices. Add one new index a time to test the index handlers separately.
    _invertedIndexColumns.add("column3");
    verifyProcessNeeded();
    _rangeIndexColumns.add("column3");
    verifyProcessNeeded();
    _fieldConfigMap.put("column3",
        new FieldConfig("column3", FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.TEXT), null,
            null));
    verifyProcessNeeded();
    _fieldConfigMap.put("column3", new FieldConfig("column3", FieldConfig.EncodingType.DICTIONARY,
        List.of(FieldConfig.IndexType.TEXT, FieldConfig.IndexType.FST), null, null));
    verifyProcessNeeded();
    _fieldConfigMap.put("newH3Col",
        new FieldConfig("newH3Col", FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.H3), null,
            Map.of("resolutions", "5")));
    verifyProcessNeeded();
    _bloomFilterConfigs = Map.of("column3", new BloomFilterConfig(0.1, 1024, true));
    verifyProcessNeeded();
    _jsonIndexConfigs = Map.of("newJsonCol", new JsonIndexConfig());
    verifyProcessNeeded();

    // Require to add startree index.
    _enableDefaultStarTree = true;
    verifyProcessNeeded();

    // Require to update min and max values.
    removeMinMaxValuesFromMetadataFile();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    segmentMetadata.getColumnMetadataMap().forEach((k, v) -> {
      assertNull(v.getMinValue(), "checking column: " + k);
      assertNull(v.getMaxValue(), "checking column: " + k);
    });
    _columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.ALL;
    verifyProcessNeeded();
    segmentMetadata = new SegmentMetadataImpl(INDEX_DIR);
    segmentMetadata.getColumnMetadataMap().forEach((k, v) -> {
      assertNotNull(v.getMinValue(), "checking column: " + k);
      assertNotNull(v.getMaxValue(), "checking column: " + k);
    });
  }

  private void verifyProcessNeeded()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            createIndexLoadingConfig(_newColumnsSchemaWithH3Json), _newColumnsSchemaWithH3Json)) {
      assertTrue(processor.needProcess());
      processor.process(SEGMENT_PREPROCESS_THROTTLER);
    }
    verifyProcessNotNeeded();
  }

  private void verifyProcessNotNeeded()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            createIndexLoadingConfig(_newColumnsSchemaWithH3Json), _newColumnsSchemaWithH3Json)) {
      assertFalse(processor.needProcess());
    }
  }

  @Test
  public void testNeedAddMinMaxValue()
      throws Exception {
    String[] stringValuesInvalid = {"A,", "B,", "C,", "D,", "E"};
    String[] stringValuesValid = {"A", "B", "C", "D", "E"};
    long[] longValues = {1588316400000L, 1588489200000L, 1588662000000L, 1588834800000L, 1589007600000L};
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .addMetric("longCol", FieldSpec.DataType.LONG).build();

    // build good segment, no needPreprocess
    buildTestSegment(tableConfig, schema, stringValuesValid, longValues);
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.ALL.name());
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema), schema)) {
      assertFalse(processor.needProcess());
    }

    // build bad segment, still no needPreprocess, since minMaxInvalid flag should be set
    buildTestSegment(tableConfig, schema, stringValuesInvalid, longValues);
    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.NONE.name());
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema), schema)) {
      assertFalse(processor.needProcess());
    }
    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.ALL.name());
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema), schema)) {
      assertFalse(processor.needProcess());
    }

    // modify metadata, to remove min/max, now needPreprocess
    removeMinMaxValuesFromMetadataFile();
    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.NONE.name());
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema), schema)) {
      assertFalse(processor.needProcess());
    }
    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.ALL.name());
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema), schema)) {
      assertTrue(processor.needProcess());
    }
  }

  @Test
  public void testNeedAddMinMaxValueOnLongString()
      throws Exception {
    String longString = RandomStringUtils.randomAlphanumeric(1000);
    String[] stringValuesValid = {"B", "C", "D", "E", longString};
    long[] longValues = {1588316400000L, 1588489200000L, 1588662000000L, 1588834800000L, 1589007600000L};
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .addMetric("longCol", FieldSpec.DataType.LONG).build();

    // build good segment, no needPreprocess
    buildTestSegment(tableConfig, schema, stringValuesValid, longValues);
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.ALL.name());
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema), schema)) {
      assertFalse(processor.needProcess());
    }
  }

  @Test
  public void testStarTreeCreationWithDictionaryChanges()
      throws Exception {
    // Build the sample segment
    String[] stringValues = {"A", "C", "B", "C", "D", "E", "E", "E"};
    long[] longValues = {2, 1, 2, 3, 4, 5, 3, 2};
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .addMetric("longCol", DataType.LONG).build();

    // Build good segment, no need for preprocess
    buildTestSegment(tableConfig, schema, stringValues, longValues);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema), schema)) {
      assertFalse(processor.needProcess());
    }

    // Update table config to convert dict to noDict for longCol
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setNoDictionaryColumns(List.of("longCol"));
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema), schema)) {
      assertTrue(processor.needProcess());
      processor.process(SEGMENT_PREPROCESS_THROTTLER);
    }

    // Update table config to convert noDict to dict for longCol
    indexingConfig.setNoDictionaryColumns(null);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema), schema)) {
      assertTrue(processor.needProcess());
      processor.process(SEGMENT_PREPROCESS_THROTTLER);
    }

    // Update table config to convert dict to noDict for longCol and add the Startree index config
    indexingConfig.setNoDictionaryColumns(List.of("longCol"));
    indexingConfig.setEnableDynamicStarTreeCreation(true);
    StarTreeIndexConfig starTreeIndexConfig =
        new StarTreeIndexConfig(List.of("stringCol"), null, List.of("SUM__longCol"), null, 1000);
    indexingConfig.setStarTreeIndexConfigs(List.of(starTreeIndexConfig));
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema), schema)) {
      assertTrue(processor.needProcess());
      processor.process(SEGMENT_PREPROCESS_THROTTLER);
    }

    // Remove Startree index but keep the no dictionary for longCol
    indexingConfig.setStarTreeIndexConfigs(null);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema), schema)) {
      assertTrue(processor.needProcess());
      processor.process(SEGMENT_PREPROCESS_THROTTLER);
    }

    // Update table config to convert noDict to dict for longCol and also add the Startree index
    indexingConfig.setNoDictionaryColumns(null);
    indexingConfig.setStarTreeIndexConfigs(List.of(starTreeIndexConfig));
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema), schema)) {
      assertTrue(processor.needProcess());
      processor.process(SEGMENT_PREPROCESS_THROTTLER);
    }
  }

  private void buildTestSegment(TableConfig tableConfig, Schema schema, String[] stringValues, long[] longValues)
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>(3);
    for (int i = 0; i < 5; i++) {
      GenericRow row = new GenericRow();
      row.putValue("stringCol", stringValues[i]);
      row.putValue("longCol", longValues[i]);
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
  }

  private static void removeMinMaxValuesFromMetadataFile()
      throws ConfigurationException {
    PropertiesConfiguration configuration = SegmentMetadataUtils.getPropertiesConfiguration(INDEX_DIR);
    Iterator<String> keys = configuration.getKeys();
    List<String> keysToClear = new ArrayList<>();
    while (keys.hasNext()) {
      String key = keys.next();
      if (key.endsWith(V1Constants.MetadataKeys.Column.MIN_VALUE) || key.endsWith(
          V1Constants.MetadataKeys.Column.MAX_VALUE) || key.endsWith(
          V1Constants.MetadataKeys.Column.MIN_MAX_VALUE_INVALID)) {
        keysToClear.add(key);
      }
    }
    for (String key : keysToClear) {
      configuration.clearProperty(key);
    }
    SegmentMetadataUtils.savePropertiesConfiguration(configuration, INDEX_DIR);
  }

  /**
   * Test to check the behavior of the forward index disabled feature when enabled on a new SV column
   */
  @Test(dataProvider = "bothV1AndV3")
  public void testForwardIndexDisabledOnNewColumnsSV(SegmentVersion segmentVersion)
      throws Exception {
    // Add a new column with no forward index, but with inverted index
    buildSegment(segmentVersion);
    _invertedIndexColumns.add(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    _fieldConfigMap.put(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV,
        new FieldConfig(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, FieldConfig.EncodingType.DICTIONARY, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    // Forward index is always going to be present for default SV columns with forward index disabled. This is because
    // such default columns are going to be sorted and the forwardIndexDisabled flag is a no-op for sorted columns
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, true, 4, true, 0, null, true, DataType.STRING, 100000);

    // Add a new raw column with no forward index
    resetIndexConfigs();
    buildSegment(segmentVersion);
    _noDictionaryColumns.add(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    _fieldConfigMap.put(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV,
        new FieldConfig(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, FieldConfig.EncodingType.RAW, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    // Forward index is always going to be present for default SV columns with forward index disabled. This is because
    // such default columns are going to be sorted and the forwardIndexDisabled flag is a no-op for sorted columns
    // Even raw columns are created with dictionary and forward index
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, true, 4, true, 0, null, true, DataType.STRING, 100000);

    // Add a new column with no forward index or inverted index
    resetIndexConfigs();
    buildSegment(segmentVersion);
    _fieldConfigMap.put(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV,
        new FieldConfig(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, FieldConfig.EncodingType.DICTIONARY, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    // Disabling inverted index should be fine for disabling the forward index
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, true, 4, true, 0, null, true, DataType.STRING, 100000);
  }

  /**
   * Test to check the behavior of the forward index disabled feature when enabled on a new MV column
   */
  @Test(dataProvider = "bothV1AndV3")
  public void testForwardIndexDisabledOnNewColumnsMV(SegmentVersion segmentVersion)
      throws Exception {
    // Add a new column with no forward index, but with inverted index
    buildSegment(segmentVersion);
    _invertedIndexColumns.add(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    _fieldConfigMap.put(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV,
        new FieldConfig(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, FieldConfig.EncodingType.DICTIONARY, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, false, 4, false, 1, null, true, DataType.STRING, 100000);
    validateIndex(StandardIndexes.inverted(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1, true, true, false, 4,
        false, 1, null, true, DataType.STRING, 100000);

    // Add a new raw column with no forward index
    resetIndexConfigs();
    buildSegment(segmentVersion);
    _noDictionaryColumns.add(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    _fieldConfigMap.put(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV,
        new FieldConfig(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, FieldConfig.EncodingType.RAW, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    if (segmentVersion == SegmentVersion.v1) {
      // For V1 segments reload doesn't support modifying the forward index yet. Since for MV columns we create a
      // dictionary in the default column handler, the forward index should indeed be disabled but we should still have
      // the dictionary.
      createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1,
          _newColumnsSchemaWithForwardIndexDisabled, true, true, false, 4, false, 1, null, true, DataType.STRING,
          100000);
      validateIndexExists(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.dictionary());
    } else {
      createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1,
          _newColumnsSchemaWithForwardIndexDisabled, true, false, false, 0, false, 1, null, true, DataType.STRING,
          100000);
      validateIndexDoesNotExist(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.dictionary());
    }
    validateIndexDoesNotExist(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.inverted());

    // Add a new column with no forward index or inverted index
    resetIndexConfigs();
    buildSegment(segmentVersion);
    _fieldConfigMap.put(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV,
        new FieldConfig(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, FieldConfig.EncodingType.DICTIONARY, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, false, 4, false, 1, null, true, DataType.STRING, 100000);
    validateIndexDoesNotExist(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.inverted());
    validateIndexExists(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.dictionary());
  }
}
