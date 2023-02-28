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
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SegmentPreProcessorTest {
  private static final File INDEX_DIR = new File(SegmentPreProcessorTest.class.toString());
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
  private static final String EXISTING_FORWARD_INDEX_DISABLED_COL_SV = "column10";
  private static final String EXISTING_FORWARD_INDEX_DISABLED_COL_MV = "column7";

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
  private static final String NEW_HLL_BYTE_METRIC_COLUMN_NAME = "newHLLByteMetric";
  private static final String NEW_TDIGEST_BYTE_METRIC_COLUMN_NAME = "newTDigestByteMetric";

  private File _indexDir;
  private PinotConfiguration _configuration;
  private File _avroFile;
  private Schema _schema;
  private Schema _newColumnsSchema1;
  private Schema _newColumnsSchema2;
  private Schema _newColumnsSchema3;
  private Schema _newColumnsSchemaWithFST;
  private Schema _newColumnsSchemaWithText;
  private Schema _newColumnsSchemaWithH3Json;
  private Schema _newColumnsSchemaWithForwardIndexDisabled;

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    Map<String, Object> props = new HashMap<>();
    props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap.toString());
    _configuration = new PinotConfiguration(props);

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

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private TableConfig getDefaultTableConfig() {
    // The segment generation code in SegmentColumnarIndexCreator will throw exception if start and end time in time
    // column are not in acceptable range. For this test, we first need to fix the input avro data to have the time
    // column values in allowed range. Until then, the check is explicitly disabled
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(false);
    ingestionConfig.setSegmentTimeValueCheck(false);
    // Make the list modifiable
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("daysSinceEpoch")
        .setInvertedIndexColumns(
            new ArrayList<>(Arrays.asList(COLUMN1_NAME, COLUMN7_NAME, COLUMN13_NAME, NO_SUCH_COLUMN_NAME)))
        .setNoDictionaryColumns(
            new ArrayList<>(Arrays.asList(EXISTING_INT_COL_RAW, EXISTING_INT_COL_RAW_MV, EXISTING_STRING_COL_RAW)))
        .setIngestionConfig(ingestionConfig).setNullHandlingEnabled(true).build();
  }

  private TableConfig getTableConfigNoIndex() {
    TableConfig tableConfig = getDefaultTableConfig();
    tableConfig.getIndexingConfig().setInvertedIndexColumns(null);
    return tableConfig;
  }

  private void constructV1Segment(TableConfig tableConfig)
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Create inverted index for 'column7' when constructing the segment.
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithSchema(_avroFile, INDEX_DIR, "testTable", tableConfig, _schema);
    segmentGeneratorConfig.setInvertedIndexCreationColumns(Collections.singletonList(COLUMN7_NAME));
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(segmentGeneratorConfig);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
  }

  private void constructV1Segment()
      throws Exception {
    constructV1Segment(getDefaultTableConfig());
  }

  private void constructV3Segment(TableConfig tableConfig)
      throws Exception {
    constructV1Segment(tableConfig);
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
  }

  private void constructV3Segment()
      throws Exception {
    constructV3Segment(getDefaultTableConfig());
  }

  /**
   * Test to check for default column handling and text index creation during segment load after new columns are added
   * to the schema with text index creation enabled.
   * This will exercise both code paths in SegmentPreprocessor (segment load):
   * (1) Default column handler to add forward index and dictionary
   * (2) Text index handler to add text index
   */
  @Test
  public void testEnableTextIndexOnNewColumn()
      throws Exception {
    TableConfig tableConfig = getDefaultTableConfig();
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    for (String column : Arrays.asList(NEWLY_ADDED_STRING_COL_DICT, NEWLY_ADDED_STRING_MV_COL_DICT)) {
      fieldConfigs.add(new FieldConfig(column, FieldConfig.EncodingType.DICTIONARY,
          Collections.singletonList(FieldConfig.IndexType.TEXT), null, null));
    }
    List<String> noDictionaryColumns = tableConfig.getIndexingConfig().getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    for (String column : Arrays.asList(NEWLY_ADDED_STRING_COL_RAW, NEWLY_ADDED_STRING_MV_COL_RAW)) {
      noDictionaryColumns.add(column);
      fieldConfigs.add(
          new FieldConfig(column, FieldConfig.EncodingType.RAW, Collections.singletonList(FieldConfig.IndexType.TEXT),
              FieldConfig.CompressionCodec.LZ4, null));
    }
    tableConfig.setFieldConfigList(fieldConfigs);

    // All new columns (default column) should have dictionary regardless of the config
    constructV1Segment();
    createAndValidateIndex(tableConfig, _newColumnsSchemaWithText, NEWLY_ADDED_STRING_COL_DICT,
        Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 1, 1, true, true, true, 4, true, 0, null,
        DataType.STRING, 100000);
    validateIndex(NEWLY_ADDED_STRING_MV_COL_DICT, Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 1, 1,
        true, true, false, 4, false, 1, null, DataType.STRING, 100000);
    validateIndex(NEWLY_ADDED_STRING_COL_RAW, Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 1, 1, true,
        true, true, 4, true, 0, null, DataType.STRING, 100000);
    validateIndex(NEWLY_ADDED_STRING_MV_COL_RAW, Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 1, 1,
        true, true, false, 4, false, 1, null, DataType.STRING, 100000);

    constructV3Segment();
    createAndValidateIndex(tableConfig, _newColumnsSchemaWithText, NEWLY_ADDED_STRING_COL_DICT,
        Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 1, 1, true, true, true, 4, true, 0, null,
        DataType.STRING, 100000);
    validateIndex(NEWLY_ADDED_STRING_MV_COL_DICT, Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 1, 1,
        true, true, false, 4, false, 1, null, DataType.STRING, 100000);
    validateIndex(NEWLY_ADDED_STRING_COL_RAW, Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 1, 1, true,
        true, true, 4, true, 0, null, DataType.STRING, 100000);
    validateIndex(NEWLY_ADDED_STRING_MV_COL_RAW, Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 1, 1,
        true, true, false, 4, false, 1, null, DataType.STRING, 100000);
  }

  /**
   * Test to check text index creation during segment load after text index creation is enabled on existing columns.
   */
  @Test
  public void testEnableTextIndexOnExistingColumn()
      throws Exception {
    TableConfig tableConfig = getDefaultTableConfig();
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    fieldConfigs.add(new FieldConfig(EXISTING_STRING_COL_DICT, FieldConfig.EncodingType.DICTIONARY,
        Collections.singletonList(FieldConfig.IndexType.TEXT), null, null));
    fieldConfigs.add(new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.RAW,
        Collections.singletonList(FieldConfig.IndexType.TEXT), null, null));
    tableConfig.setFieldConfigList(fieldConfigs);

    constructV1Segment();
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_DICT,
        Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 9, 4, false, true, false, 26, true, 0, null,
        DataType.STRING, 100000);
    validateIndex(EXISTING_STRING_COL_RAW, Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 5, 3, false,
        false, false, 0, true, 0, ChunkCompressionType.LZ4, DataType.STRING, 100000);

    constructV3Segment();
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_DICT,
        Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 9, 4, false, true, false, 26, true, 0, null,
        DataType.STRING, 100000);
    validateIndex(EXISTING_STRING_COL_RAW, Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 5, 3, false,
        false, false, 0, true, 0, ChunkCompressionType.LZ4, DataType.STRING, 100000);
  }

  @Test
  public void testEnableFSTIndexOnNewColumn()
      throws Exception {
    TableConfig tableConfig = getDefaultTableConfig();
    tableConfig.setFieldConfigList(Collections.singletonList(
        new FieldConfig(NEWLY_ADDED_FST_COL_DICT, FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.FST), null, null)));

    constructV1Segment();
    createAndValidateIndex(tableConfig, _newColumnsSchemaWithFST, NEWLY_ADDED_FST_COL_DICT,
        Collections.singletonList(ColumnIndexType.FST_INDEX), false, 1, 1, true, true, true, 4, true, 0, null,
        DataType.STRING, 100000);

    constructV3Segment();
    createAndValidateIndex(tableConfig, _newColumnsSchemaWithFST, NEWLY_ADDED_FST_COL_DICT,
        Collections.singletonList(ColumnIndexType.FST_INDEX), false, 1, 1, true, true, true, 4, true, 0, null,
        DataType.STRING, 100000);
  }

  @Test
  public void testEnableFSTIndexOnExistingColumnDictEncoded()
      throws Exception {
    TableConfig tableConfig = getDefaultTableConfig();
    tableConfig.setFieldConfigList(Collections.singletonList(
        new FieldConfig(EXISTING_STRING_COL_DICT, FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.FST), null, null)));

    constructV1Segment();
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_DICT,
        Collections.singletonList(ColumnIndexType.FST_INDEX), false, 9, 4, false, true, false, 26, true, 0, null,
        DataType.STRING, 100000);

    constructV3Segment();
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_DICT,
        Collections.singletonList(ColumnIndexType.FST_INDEX), false, 9, 4, false, true, false, 26, true, 0, null,
        DataType.STRING, 100000);
  }

  @Test
  public void testEnableFSTIndexOnExistingColumnRaw()
      throws Exception {
    TableConfig tableConfig = getDefaultTableConfig();
    tableConfig.setFieldConfigList(Collections.singletonList(
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.RAW,
            Collections.singletonList(FieldConfig.IndexType.FST), FieldConfig.CompressionCodec.LZ4, null)));

    constructV1Segment();
    SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
    SegmentPreProcessor v3Processor =
        new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(tableConfig, _schema));
    expectThrows(UnsupportedOperationException.class, v3Processor::process);

    constructV3Segment();
    segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader().load(_indexDir.toURI(),
        new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
    SegmentPreProcessor v1Processor =
        new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(tableConfig, _schema));
    expectThrows(UnsupportedOperationException.class, v1Processor::process);
  }

  @Test
  public void testEnableDictionary()
      throws Exception {
    // Enable dictionary on EXISTING_INT_COL_RAW, EXISTING_STRING_COL_RAW and EXISTING_INT_COL_RAW_MV
    constructV3Segment();
    TableConfig tableConfig = getDefaultTableConfig();
    List<String> noDictionaryColumns = tableConfig.getIndexingConfig().getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertTrue(noDictionaryColumns.remove(EXISTING_INT_COL_RAW));
    assertTrue(noDictionaryColumns.remove(EXISTING_STRING_COL_RAW));
    assertTrue(noDictionaryColumns.remove(EXISTING_INT_COL_RAW_MV));

    createAndValidateIndex(tableConfig, _schema, EXISTING_INT_COL_RAW, Collections.emptyList(), false, 42242, 16, false,
        true, false, 0, true, 0, null, DataType.INT, 100000);
    validateIndex(EXISTING_STRING_COL_RAW, Collections.emptyList(), false, 5, 3, false, true, false, 4, true, 0, null,
        DataType.STRING, 100000);
    validateIndex(EXISTING_INT_COL_RAW_MV, Collections.emptyList(), false, 18499, 15, false, true, false, 0, false, 13,
        null, DataType.INT, 106688);
  }

  @Test
  public void testEnableDictAndOtherIndexesSV()
      throws Exception {
    // TEST 1: EXISTING_STRING_COL_RAW. Enable dictionary. Also add inverted index and text index. Reload code path
    // will create dictionary, inverted index and text index.
    constructV3Segment();
    TableConfig tableConfig = getDefaultTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertTrue(noDictionaryColumns.remove(EXISTING_STRING_COL_RAW));
    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    assertNotNull(invertedIndexColumns);
    assertFalse(invertedIndexColumns.contains(EXISTING_STRING_COL_RAW));
    invertedIndexColumns.add(EXISTING_STRING_COL_RAW);
    tableConfig.setFieldConfigList(Collections.singletonList(
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.TEXT), null, null)));
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_RAW,
        Arrays.asList(ColumnIndexType.INVERTED_INDEX, ColumnIndexType.TEXT_INDEX), false, 5, 3, false, true, false, 4,
        true, 0, null, DataType.STRING, 100000);

    // TEST 2: EXISTING_STRING_COL_RAW. Enable dictionary on a raw column that already has text index.
    tableConfig = getDefaultTableConfig();
    tableConfig.setFieldConfigList(Collections.singletonList(
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.RAW,
            Collections.singletonList(FieldConfig.IndexType.TEXT), null, null)));
    constructV3Segment(tableConfig);
    validateIndex(EXISTING_STRING_COL_RAW, Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 5, 3, false,
        false, false, 0, true, 0, ChunkCompressionType.LZ4, DataType.STRING, 100000);
    noDictionaryColumns = tableConfig.getIndexingConfig().getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertTrue(noDictionaryColumns.remove(EXISTING_STRING_COL_RAW));
    tableConfig.setFieldConfigList(Collections.singletonList(
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.TEXT), null, null)));
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_RAW,
        Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 5, 3, false, true, false, 4, true, 0, null,
        DataType.STRING, 100000);

    // TEST 3: EXISTING_INT_COL_RAW. Enable dictionary on a column that already has range index.
    tableConfig = getDefaultTableConfig();
    indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setRangeIndexColumns(Collections.singletonList(EXISTING_INT_COL_RAW));
    constructV3Segment(tableConfig);
    validateIndex(EXISTING_INT_COL_RAW, Collections.singletonList(ColumnIndexType.RANGE_INDEX), false, 42242, 16, false,
        false, false, 0, true, 0, ChunkCompressionType.LZ4, DataType.INT, 100000);
    long oldRangeIndexSize =
        new SegmentMetadataImpl(_indexDir).getColumnMetadataFor(EXISTING_INT_COL_RAW).getIndexSizeMap()
            .get(ColumnIndexType.RANGE_INDEX);
    // At this point, the segment has range index. Now the reload path should create a dictionary and rewrite the
    // range index.
    noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertTrue(noDictionaryColumns.remove(EXISTING_INT_COL_RAW));
    createAndValidateIndex(tableConfig, _schema, EXISTING_INT_COL_RAW,
        Collections.singletonList(ColumnIndexType.RANGE_INDEX), false, 42242, 16, false, true, false, 0, true, 0, null,
        DataType.INT, 100000);
    long newRangeIndexSize =
        new SegmentMetadataImpl(_indexDir).getColumnMetadataFor(EXISTING_INT_COL_RAW).getIndexSizeMap()
            .get(ColumnIndexType.RANGE_INDEX);
    assertNotEquals(newRangeIndexSize, oldRangeIndexSize);
  }

  @Test
  public void testEnableDictAndOtherIndexesMV()
      throws Exception {
    // TEST 1: EXISTING_INT_COL_RAW_MV. Enable dictionary for an MV column. Also enable inverted index and range index.
    constructV3Segment();
    TableConfig tableConfig = getDefaultTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertTrue(noDictionaryColumns.remove(EXISTING_INT_COL_RAW_MV));
    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    assertNotNull(invertedIndexColumns);
    assertFalse(invertedIndexColumns.contains(EXISTING_INT_COL_RAW_MV));
    invertedIndexColumns.add(EXISTING_INT_COL_RAW_MV);
    indexingConfig.setRangeIndexColumns(Collections.singletonList(EXISTING_INT_COL_RAW_MV));
    createAndValidateIndex(tableConfig, _schema, EXISTING_INT_COL_RAW_MV,
        Arrays.asList(ColumnIndexType.INVERTED_INDEX, ColumnIndexType.RANGE_INDEX), false, 18499, 15, false, true,
        false, 0, false, 13, null, DataType.INT, 106688);

    // TEST 2: EXISTING_INT_COL_RAW_MV. Enable dictionary for an MV column that already has range index.
    tableConfig = getDefaultTableConfig();
    indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setRangeIndexColumns(Collections.singletonList(EXISTING_INT_COL_RAW_MV));
    constructV3Segment(tableConfig);
    validateIndex(EXISTING_INT_COL_RAW_MV, Collections.singletonList(ColumnIndexType.RANGE_INDEX), false, 18499, 15,
        false, false, false, 0, false, 13, ChunkCompressionType.LZ4, DataType.INT, 106688);
    // Enable dictionary.
    noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertTrue(noDictionaryColumns.remove(EXISTING_INT_COL_RAW_MV));
    createAndValidateIndex(tableConfig, _schema, EXISTING_INT_COL_RAW_MV,
        Collections.singletonList(ColumnIndexType.RANGE_INDEX), false, 18499, 15, false, true, false, 0, false, 13,
        null, DataType.INT, 106688);
  }

  @Test
  public void testDisableDictionary()
      throws Exception {
    // Disable dictionary for EXISTING_STRING_COL_DICT
    TableConfig tableConfig = getDefaultTableConfig();
    List<String> noDictionaryColumns = tableConfig.getIndexingConfig().getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertFalse(noDictionaryColumns.contains(EXISTING_STRING_COL_DICT));
    noDictionaryColumns.add(EXISTING_STRING_COL_DICT);

    // No-op on V1 segment
    constructV1Segment();
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_DICT, Collections.emptyList(), false, 9, 4, false,
        true, false, 26, true, 0, null, DataType.STRING, 100000);

    constructV3Segment();
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_DICT, Collections.emptyList(), false, 9, 4, false,
        false, false, 0, true, 0, ChunkCompressionType.LZ4, DataType.STRING, 100000);

    // Disable dictionary for COLUMN10_NAME
    constructV3Segment();
    tableConfig = getDefaultTableConfig();
    noDictionaryColumns = tableConfig.getIndexingConfig().getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertFalse(noDictionaryColumns.contains(COLUMN10_NAME));
    noDictionaryColumns.add(COLUMN10_NAME);
    createAndValidateIndex(tableConfig, _schema, COLUMN10_NAME, Collections.emptyList(), false, 3960, 12, false, false,
        false, 0, true, 0, ChunkCompressionType.LZ4, DataType.INT, 100000);
  }

  @Test
  public void testDisableDictAndOtherIndexesSV()
      throws Exception {
    // TEST 1: Disable dictionary on a column that has inverted index. Should be a no-op and column should still have
    // a dictionary.
    constructV3Segment();
    TableConfig tableConfig = getDefaultTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertFalse(noDictionaryColumns.contains(COLUMN1_NAME));
    noDictionaryColumns.add(COLUMN1_NAME);
    createAndValidateIndex(tableConfig, _schema, COLUMN1_NAME,
        Collections.singletonList(ColumnIndexType.INVERTED_INDEX), false, 51594, 16, false, true, false, 0, true, 0,
        null, DataType.INT, 100000);

    // TEST 2: Disable dictionary. Also remove inverted index on column1.
    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    assertNotNull(invertedIndexColumns);
    assertTrue(invertedIndexColumns.remove(COLUMN1_NAME));
    createAndValidateIndex(tableConfig, _schema, COLUMN1_NAME, Collections.emptyList(), false, 51594, 16, false, false,
        false, 0, true, 0, ChunkCompressionType.LZ4, DataType.INT, 100000);
    validateIndexDoesNotExist(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX);

    // TEST 3: Disable dictionary for a column (Column10) that has range index.
    tableConfig = getDefaultTableConfig();
    indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setRangeIndexColumns(Collections.singletonList(COLUMN10_NAME));
    constructV3Segment(tableConfig);
    validateIndex(COLUMN10_NAME, Collections.singletonList(ColumnIndexType.RANGE_INDEX), false, 3960, 12, false, true,
        false, 0, true, 0, null, DataType.INT, 100000);
    long oldRangeIndexSize = new SegmentMetadataImpl(_indexDir).getColumnMetadataFor(COLUMN10_NAME).getIndexSizeMap()
        .get(ColumnIndexType.RANGE_INDEX);
    noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertFalse(noDictionaryColumns.contains(COLUMN10_NAME));
    noDictionaryColumns.add(COLUMN10_NAME);
    createAndValidateIndex(tableConfig, _schema, COLUMN10_NAME, Collections.singletonList(ColumnIndexType.RANGE_INDEX),
        false, 3960, 12, false, false, false, 0, true, 0, ChunkCompressionType.LZ4, DataType.INT, 100000);
    long newRangeIndexSize = new SegmentMetadataImpl(_indexDir).getColumnMetadataFor(COLUMN10_NAME).getIndexSizeMap()
        .get(ColumnIndexType.RANGE_INDEX);
    assertNotEquals(newRangeIndexSize, oldRangeIndexSize);

    // TEST4: Disable dictionary but add text index.
    constructV3Segment();
    tableConfig = getDefaultTableConfig();
    noDictionaryColumns = tableConfig.getIndexingConfig().getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertFalse(noDictionaryColumns.contains(EXISTING_STRING_COL_DICT));
    noDictionaryColumns.add(EXISTING_STRING_COL_DICT);
    tableConfig.setFieldConfigList(Collections.singletonList(
        new FieldConfig(EXISTING_STRING_COL_DICT, FieldConfig.EncodingType.RAW,
            Collections.singletonList(FieldConfig.IndexType.TEXT), FieldConfig.CompressionCodec.SNAPPY, null)));
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_DICT,
        Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 9, 4, false, false, false, 0, true, 0,
        ChunkCompressionType.SNAPPY, DataType.STRING, 100000);
  }

  @Test
  public void testDisableDictAndOtherIndexesMV()
      throws Exception {
    // TEST 1: Disable dictionary on a column where range index is already enabled.
    TableConfig tableConfig = getDefaultTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertTrue(noDictionaryColumns.remove(EXISTING_INT_COL_RAW_MV));
    constructV3Segment(tableConfig);
    validateIndex(EXISTING_INT_COL_RAW_MV, Collections.emptyList(), false, 18499, 15, false, true, false, 0, false, 13,
        null, DataType.INT, 106688);
    noDictionaryColumns.add(EXISTING_INT_COL_RAW_MV);
    indexingConfig.setRangeIndexColumns(Collections.singletonList(EXISTING_INT_COL_RAW_MV));
    createAndValidateIndex(tableConfig, _schema, EXISTING_INT_COL_RAW_MV,
        Collections.singletonList(ColumnIndexType.RANGE_INDEX), false, 18499, 15, false, false, false, 0, false, 13,
        ChunkCompressionType.LZ4, DataType.INT, 106688);

    // TEST 2. Disable dictionary on a column where inverted index is enabled. Should be a no-op.
    noDictionaryColumns.add(COLUMN7_NAME);
    createAndValidateIndex(tableConfig, _schema, COLUMN7_NAME,
        Collections.singletonList(ColumnIndexType.INVERTED_INDEX), false, 359, 9, false, true, false, 0, false, 24,
        null, DataType.INT, 134090);

    // TEST 3: Disable dictionary and disable inverted index on column7.
    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    assertNotNull(invertedIndexColumns);
    assertTrue(invertedIndexColumns.remove(COLUMN7_NAME));
    createAndValidateIndex(tableConfig, _schema, COLUMN7_NAME, Collections.emptyList(), false, 359, 9, false, false,
        false, 0, false, 24, ChunkCompressionType.LZ4, DataType.INT, 134090);
    validateIndexDoesNotExist(COLUMN7_NAME, ColumnIndexType.INVERTED_INDEX);
  }

  @Test
  public void testForwardIndexHandlerChangeCompression()
      throws Exception {
    TableConfig tableConfig = getDefaultTableConfig();
    tableConfig.setFieldConfigList(Collections.singletonList(
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.RAW, Collections.emptyList(),
            FieldConfig.CompressionCodec.ZSTANDARD, null)));

    // Test1: Rewriting forward index will be a no-op for v1 segments. Default LZ4 compressionType will be retained.
    constructV1Segment();
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_RAW, Collections.emptyList(), false, 5, 3, false,
        false, false, 0, true, 0, ChunkCompressionType.LZ4, DataType.STRING, 100000);

    // Test2: For v3 segments, forward index will be rewritten with ZSTANDARD compressionType.
    constructV3Segment();
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_RAW, Collections.emptyList(), false, 5, 3, false,
        false, false, 0, true, 0, ChunkCompressionType.ZSTANDARD, DataType.STRING, 100000);

    // Test3: Change compression on existing raw index column. Also add text index on same column. Check correctness.
    constructV3Segment();
    tableConfig.setFieldConfigList(Collections.singletonList(
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.RAW,
            Collections.singletonList(FieldConfig.IndexType.TEXT), FieldConfig.CompressionCodec.ZSTANDARD, null)));
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_RAW,
        Collections.singletonList(ColumnIndexType.TEXT_INDEX), false, 5, 3, false, false, false, 0, true, 0,
        ChunkCompressionType.ZSTANDARD, DataType.STRING, 100000);

    // Test4: Change compression on RAW index column. Change another index on another column. Check correctness.
    constructV3Segment();
    tableConfig.setFieldConfigList(Arrays.asList(
        new FieldConfig(EXISTING_STRING_COL_RAW, FieldConfig.EncodingType.RAW, Collections.emptyList(),
            FieldConfig.CompressionCodec.ZSTANDARD, null),
        new FieldConfig(EXISTING_STRING_COL_DICT, FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.FST), null, null)));
    createAndValidateIndex(tableConfig, _schema, EXISTING_STRING_COL_RAW, Collections.emptyList(), false, 5, 3, false,
        false, false, 0, true, 0, ChunkCompressionType.ZSTANDARD, DataType.STRING, 100000);
    validateIndex(EXISTING_STRING_COL_DICT, Collections.singletonList(ColumnIndexType.FST_INDEX), false, 9, 4, false,
        true, false, 26, true, 0, null, DataType.STRING, 100000);

    // Test5: Change compressionType for an MV column
    constructV3Segment();
    tableConfig.setFieldConfigList(Collections.singletonList(
        new FieldConfig(EXISTING_INT_COL_RAW_MV, FieldConfig.EncodingType.RAW, Collections.emptyList(),
            FieldConfig.CompressionCodec.ZSTANDARD, null)));
    createAndValidateIndex(tableConfig, _schema, EXISTING_INT_COL_RAW_MV, Collections.emptyList(), false, 18499, 15,
        false, false, false, 0, false, 13, ChunkCompressionType.ZSTANDARD, DataType.INT, 106688);
  }

  private void createAndValidateIndex(TableConfig tableConfig, Schema schema, String column,
      List<ColumnIndexType> indexTypes, boolean forwardIndexDisabled, int cardinality, int bits,
      boolean isAutoGenerated, boolean hasDictionary, boolean isSorted, int dictionaryElementSize,
      boolean isSingleValued, int maxNumberOfMultiValues, ChunkCompressionType expectedCompressionType,
      DataType dataType, int totalNumberOfEntries)
      throws Exception {
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema))) {
      processor.process();
      validateIndex(column, indexTypes, forwardIndexDisabled, cardinality, bits, isAutoGenerated, hasDictionary,
          isSorted, dictionaryElementSize, isSingleValued, maxNumberOfMultiValues, expectedCompressionType, dataType,
          totalNumberOfEntries);
    }
  }

  private void validateIndex(String column, List<ColumnIndexType> indexTypes, boolean forwardIndexDisabled,
      int cardinality, int bits, boolean isAutoGenerated, boolean hasDictionary, boolean isSorted,
      int dictionaryElementSize, boolean isSingleValued, int maxNumberOfMultiValues,
      ChunkCompressionType expectedCompressionType, DataType dataType, int totalNumberOfEntries)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
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

    try (SegmentDirectory segmentDirectory1 = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory1.createReader()) {
      for (ColumnIndexType indexType : indexTypes) {
        assertTrue(reader.hasIndexFor(column, indexType));
      }
      assertEquals(reader.hasIndexFor(column, ColumnIndexType.DICTIONARY), hasDictionary);
      if (isSingleValued && isSorted && forwardIndexDisabled) {
        // Disabling the forward index for sorted columns should be a no-op
        assertTrue(reader.hasIndexFor(column, ColumnIndexType.FORWARD_INDEX));
        assertFalse(reader.hasIndexFor(column, ColumnIndexType.INVERTED_INDEX));
      } else if (forwardIndexDisabled) {
        if (segmentMetadata.getVersion() == SegmentVersion.v3 || isAutoGenerated) {
          assertFalse(reader.hasIndexFor(column, ColumnIndexType.FORWARD_INDEX));
          assertTrue(reader.hasIndexFor(column, ColumnIndexType.INVERTED_INDEX));
          assertTrue(reader.hasIndexFor(column, ColumnIndexType.DICTIONARY));
        } else {
          // Updating dictionary or forward index for existing columns not supported for v1 segments yet
          assertTrue(reader.hasIndexFor(column, ColumnIndexType.FORWARD_INDEX));
        }
      }

      // Check if the raw forward index compressionType is correct.
      if (!hasDictionary) {
        assertNotNull(expectedCompressionType);
        try (ForwardIndexReader fwdIndexReader = LoaderUtils.getForwardIndexReader(reader, columnMetadata)) {
          ChunkCompressionType compressionType = fwdIndexReader.getCompressionType();
          assertEquals(compressionType, expectedCompressionType);
        }
        File inProgressFile = new File(_indexDir, column + ".fwd.inprogress");
        assertFalse(inProgressFile.exists());
      } else {
        assertNull(expectedCompressionType);
      }
    }
  }

  private void validateIndexDoesNotExist(String column, ColumnIndexType indexType)
      throws Exception {
    try (SegmentDirectory segmentDirectory1 = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory1.createReader()) {
      assertFalse(reader.hasIndexFor(column, indexType));
    }
  }

  @Test
  public void testV1CreateInvertedIndices()
      throws Exception {
    constructV1Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v1);

    String col1FileName = COLUMN1_NAME + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;
    String col7FileName = COLUMN7_NAME + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;
    String col13FileName = COLUMN13_NAME + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;
    String badColFileName = NO_SUCH_COLUMN_NAME + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;

    File col1File = new File(_indexDir, col1FileName);
    File col7File = new File(_indexDir, col7FileName);
    File col13File = new File(_indexDir, col13FileName);
    File badColFile = new File(_indexDir, badColFileName);
    assertFalse(col1File.exists());
    assertTrue(col7File.exists());
    assertFalse(col13File.exists());
    assertFalse(badColFile.exists());
    FileTime col7LastModifiedTime = Files.getLastModifiedTime(col7File.toPath());

    // Sleep 10ms to prevent the same last modified time when modifying the file.
    Thread.sleep(10);

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

    // Sleep 10ms to prevent the same last modified time when modifying the file.
    Thread.sleep(10);

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
    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);

    File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3);
    File singleFileIndex = new File(segmentDirectoryPath, "columns.psf");
    FileTime lastModifiedTime = Files.getLastModifiedTime(singleFileIndex.toPath());
    long fileSize = singleFileIndex.length();

    // Sleep 10ms to prevent the same last modified time when modifying the file.
    Thread.sleep(10);

    // Create inverted index the first time.
    checkInvertedIndexCreation(false);
    long addedLength = 0L;
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      // 8 bytes overhead is for checking integrity of the segment.
      addedLength += reader.getIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX).size() + 8;
      addedLength += reader.getIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX).size() + 8;
    }
    FileTime newLastModifiedTime = Files.getLastModifiedTime(singleFileIndex.toPath());
    assertTrue(newLastModifiedTime.compareTo(lastModifiedTime) > 0);
    long newFileSize = singleFileIndex.length();
    assertEquals(fileSize + addedLength, newFileSize);

    // Sleep 10ms to prevent the same last modified time when modifying the file.
    Thread.sleep(10);

    // Create inverted index the second time.
    checkInvertedIndexCreation(true);
    assertEquals(Files.getLastModifiedTime(singleFileIndex.toPath()), newLastModifiedTime);
    assertEquals(singleFileIndex.length(), newFileSize);
  }

  private void checkInvertedIndexCreation(boolean reCreate)
      throws Exception {
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      if (reCreate) {
        assertTrue(reader.hasIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX));
        assertTrue(reader.hasIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX));
        assertTrue(reader.hasIndexFor(COLUMN7_NAME, ColumnIndexType.INVERTED_INDEX));
        assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, ColumnIndexType.INVERTED_INDEX));
      } else {
        assertFalse(reader.hasIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX));
        assertTrue(reader.hasIndexFor(COLUMN7_NAME, ColumnIndexType.INVERTED_INDEX));
        assertFalse(reader.hasIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX));
        assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, ColumnIndexType.INVERTED_INDEX));
      }
    }

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(getDefaultTableConfig()))) {
      processor.process();
    }

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertTrue(reader.hasIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX));
      assertTrue(reader.hasIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX));
      assertTrue(reader.hasIndexFor(COLUMN7_NAME, ColumnIndexType.INVERTED_INDEX));
      assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, ColumnIndexType.INVERTED_INDEX));
    }
  }

  @Test
  public void testV1UpdateDefaultColumns()
      throws Exception {
    constructV1Segment();
    checkUpdateDefaultColumns();
  }

  @Test
  public void testV3UpdateDefaultColumns()
      throws Exception {
    constructV3Segment();
    checkUpdateDefaultColumns();
  }

  private void checkUpdateDefaultColumns()
      throws Exception {
    TableConfig tableConfig = getDefaultTableConfig();
    List<String> invertedIndexColumns = tableConfig.getIndexingConfig().getInvertedIndexColumns();
    assertNotNull(invertedIndexColumns);
    assertFalse(invertedIndexColumns.contains(NEW_COLUMN_INVERTED_INDEX));
    invertedIndexColumns.add(NEW_COLUMN_INVERTED_INDEX);
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(
        Collections.singletonList(new TransformConfig(NEW_INT_SV_DIMENSION_COLUMN_NAME, "plus(column1, 1)")));
    tableConfig.setIngestionConfig(ingestionConfig);

    // Update default value.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchema1))) {
      processor.process();
    }
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);

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

    // Check dictionary and forward index exist.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertTrue(reader.hasIndexFor(NEW_INT_METRIC_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      assertTrue(reader.hasIndexFor(NEW_INT_METRIC_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
      assertTrue(reader.hasIndexFor(NEW_LONG_METRIC_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      assertTrue(reader.hasIndexFor(NEW_LONG_METRIC_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
      assertTrue(reader.hasIndexFor(NEW_FLOAT_METRIC_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      assertTrue(reader.hasIndexFor(NEW_FLOAT_METRIC_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
      assertTrue(reader.hasIndexFor(NEW_DOUBLE_METRIC_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      assertTrue(reader.hasIndexFor(NEW_DOUBLE_METRIC_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
      assertTrue(reader.hasIndexFor(NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      assertTrue(reader.hasIndexFor(NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
      assertTrue(reader.hasIndexFor(NEW_INT_SV_DIMENSION_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      assertTrue(reader.hasIndexFor(NEW_INT_SV_DIMENSION_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
      assertTrue(reader.hasIndexFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      assertTrue(reader.hasIndexFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));

      assertTrue(reader.hasIndexFor(NEW_INT_METRIC_COLUMN_NAME, ColumnIndexType.NULLVALUE_VECTOR));
      assertTrue(reader.hasIndexFor(NEW_LONG_METRIC_COLUMN_NAME, ColumnIndexType.NULLVALUE_VECTOR));
      assertTrue(reader.hasIndexFor(NEW_FLOAT_METRIC_COLUMN_NAME, ColumnIndexType.NULLVALUE_VECTOR));
      assertTrue(reader.hasIndexFor(NEW_DOUBLE_METRIC_COLUMN_NAME, ColumnIndexType.NULLVALUE_VECTOR));
      assertTrue(reader.hasIndexFor(NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME, ColumnIndexType.NULLVALUE_VECTOR));
      assertTrue(reader.hasIndexFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME, ColumnIndexType.NULLVALUE_VECTOR));
    }

    // Use the second schema and update default value again.
    // For the second schema, we changed the default value for column 'newIntMetric' to 2, and added default value
    // 'abcd' (keep the same length as 'null') to column 'newStringMVDimension'.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchema2))) {
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);

    // Check column metadata.
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_INT_METRIC_COLUMN_NAME);
    assertEquals(columnMetadata.getMinValue(), 2);
    assertEquals(columnMetadata.getMaxValue(), 2);
    assertEquals(columnMetadata.getFieldSpec().getDefaultNullValue(), 2);

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME);
    assertEquals(columnMetadata.getMinValue(), "abcd");
    assertEquals(columnMetadata.getMaxValue(), "abcd");
    assertEquals(columnMetadata.getFieldSpec().getDefaultNullValue(), "abcd");

    // Use the third schema and update default value again.
    // For the third schema, we changed the default value for column 'newStringMVDimension' to 'notSameLength', which is
    // not the same length as before.
    // We added two new columns and also removed the NEW_INT_SV_DIMENSION_COLUMN_NAME from schema.
    // NEW_INT_SV_DIMENSION_COLUMN_NAME exists before processing but removed afterwards.
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertNotNull(segmentMetadata.getColumnMetadataFor(NEW_INT_SV_DIMENSION_COLUMN_NAME));
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchema3))) {
      processor.process();
    }

    segmentMetadata = new SegmentMetadataImpl(_indexDir);
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

  @Test
  public void testColumnMinMaxValue()
      throws Exception {
    constructV1Segment();

    // Remove min/max value from the metadata
    removeMinMaxValuesFromMetadataFile(_indexDir);

    TableConfig tableConfig = getDefaultTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.NONE.name());
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig))) {
      processor.process();
    }
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    ColumnMetadata dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column1");
    ColumnMetadata metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    assertNull(timeColumnMetadata.getMinValue());
    assertNull(timeColumnMetadata.getMaxValue());
    assertNull(dimensionColumnMetadata.getMinValue());
    assertNull(dimensionColumnMetadata.getMaxValue());
    assertNull(metricColumnMetadata.getMinValue());
    assertNull(metricColumnMetadata.getMaxValue());

    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.TIME.name());
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig))) {
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column5");
    metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    assertEquals(timeColumnMetadata.getMinValue(), 1756015683);
    assertEquals(timeColumnMetadata.getMaxValue(), 1756015683);
    assertNull(dimensionColumnMetadata.getMinValue());
    assertNull(dimensionColumnMetadata.getMaxValue());
    assertNull(metricColumnMetadata.getMinValue());
    assertNull(metricColumnMetadata.getMaxValue());

    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.NON_METRIC.name());
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig))) {
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column5");
    metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    assertEquals(timeColumnMetadata.getMinValue(), 1756015683);
    assertEquals(timeColumnMetadata.getMaxValue(), 1756015683);
    assertEquals(dimensionColumnMetadata.getMinValue(), "AKXcXcIqsqOJFsdwxZ");
    assertEquals(dimensionColumnMetadata.getMaxValue(), "yQkJTLOQoOqqhkAClgC");
    assertNull(metricColumnMetadata.getMinValue());
    assertNull(metricColumnMetadata.getMaxValue());

    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.ALL.name());
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig))) {
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column5");
    metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    assertEquals(timeColumnMetadata.getMinValue(), 1756015683);
    assertEquals(timeColumnMetadata.getMaxValue(), 1756015683);
    assertEquals(dimensionColumnMetadata.getMinValue(), "AKXcXcIqsqOJFsdwxZ");
    assertEquals(dimensionColumnMetadata.getMaxValue(), "yQkJTLOQoOqqhkAClgC");
    assertEquals(metricColumnMetadata.getMinValue(), 890662862);
    assertEquals(metricColumnMetadata.getMaxValue(), 890662862);
  }

  @Test
  public void testV1CleanupIndices()
      throws Exception {
    constructV1Segment();

    // Need to create two default columns with Bytes and JSON string for H3 and JSON index.
    // Other kinds of indices can all be put on column3 with String values.
    String strColumn = "column3";
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInvertedIndexColumns(Collections.singletonList(strColumn))
        .setRangeIndexColumns(Collections.singletonList(strColumn))
        .setBloomFilterConfigs(Collections.singletonMap(strColumn, new BloomFilterConfig(0.1, 1024, true)))
        .setFieldConfigList(Collections.singletonList(new FieldConfig(strColumn, FieldConfig.EncodingType.DICTIONARY,
            Arrays.asList(FieldConfig.IndexType.FST, FieldConfig.IndexType.TEXT), null, null))).build();

    // V1 use separate file for each column index.
    File iiFile = new File(_indexDir, strColumn + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    File rgFile = new File(_indexDir, strColumn + V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION);
    File txtFile = new File(_indexDir, strColumn + V1Constants.Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION);
    File fstFile = new File(_indexDir, strColumn + V1Constants.Indexes.FST_INDEX_FILE_EXTENSION);
    File bfFile = new File(_indexDir, strColumn + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);

    assertFalse(iiFile.exists());
    assertFalse(rgFile.exists());
    assertFalse(txtFile.exists());
    assertFalse(fstFile.exists());
    assertFalse(bfFile.exists());

    // Create all kinds of indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig))) {
      processor.process();
    }
    assertTrue(iiFile.exists());
    assertTrue(rgFile.exists());
    assertTrue(txtFile.exists());
    assertTrue(fstFile.exists());
    assertTrue(bfFile.exists());

    // Remove all kinds of indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(getDefaultTableConfig()))) {
      processor.process();
    }
    assertFalse(iiFile.exists());
    assertFalse(rgFile.exists());
    assertFalse(txtFile.exists());
    assertFalse(fstFile.exists());
    assertFalse(bfFile.exists());
  }

  @Test
  public void testV3CleanupIndices()
      throws Exception {
    constructV3Segment();

    // V3 use single file for all column indices.
    File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3);
    File singleFileIndex = new File(segmentDirectoryPath, "columns.psf");

    // There are a few indices initially. Remove them to prepare an initial state.
    TableConfig tableConfigNoIndex = getTableConfigNoIndex();
    long initFileSize = singleFileIndex.length();
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfigNoIndex))) {
      processor.process();
    }
    assertTrue(singleFileIndex.length() < initFileSize);
    initFileSize = singleFileIndex.length();

    // Need to create two default columns with Bytes and JSON string for H3 and JSON index.
    // Other kinds of indices can all be put on column3 with String values.
    String strColumn = "column3";
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setInvertedIndexColumns(Collections.singletonList(strColumn))
        .setRangeIndexColumns(Collections.singletonList(strColumn))
        .setBloomFilterConfigs(Collections.singletonMap(strColumn, new BloomFilterConfig(0.1, 1024, true)))
        .setFieldConfigList(Collections.singletonList(new FieldConfig(strColumn, FieldConfig.EncodingType.DICTIONARY,
            Arrays.asList(FieldConfig.IndexType.FST, FieldConfig.IndexType.TEXT), null, null))).build();

    // Create all kinds of indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig))) {
      processor.process();
    }

    long addedLength = 0;
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      addedLength += reader.getIndexFor(strColumn, ColumnIndexType.INVERTED_INDEX).size() + 8;
      addedLength += reader.getIndexFor(strColumn, ColumnIndexType.RANGE_INDEX).size() + 8;
      addedLength += reader.getIndexFor(strColumn, ColumnIndexType.FST_INDEX).size() + 8;
      addedLength += reader.getIndexFor(strColumn, ColumnIndexType.BLOOM_FILTER).size() + 8;
      assertTrue(reader.hasIndexFor(strColumn, ColumnIndexType.TEXT_INDEX));
    }
    assertEquals(singleFileIndex.length(), initFileSize + addedLength);

    // Remove all kinds of indices, and size gets back initial size.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfigNoIndex))) {
      processor.process();
    }
    assertEquals(singleFileIndex.length(), initFileSize);
  }

  @Test
  public void testV1CleanupH3AndJsonIndices()
      throws Exception {
    constructV1Segment();

    // V1 use separate file for each column index.
    File h3File = new File(_indexDir, "newH3Col" + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);
    File jsFile = new File(_indexDir, "newJsonCol" + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    assertFalse(h3File.exists());
    assertFalse(jsFile.exists());

    // Create H3 and Json indices.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setJsonIndexColumns(Collections.singletonList("newJsonCol")).setFieldConfigList(Collections.singletonList(
            new FieldConfig("newH3Col", FieldConfig.EncodingType.DICTIONARY,
                Collections.singletonList(FieldConfig.IndexType.H3), null,
                Collections.singletonMap("resolutions", "5")))).build();
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithH3Json))) {
      processor.process();
    }
    assertTrue(h3File.exists());
    assertTrue(jsFile.exists());

    // Remove H3 and Json indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(getDefaultTableConfig(), _newColumnsSchemaWithH3Json))) {
      processor.process();
    }
    assertFalse(h3File.exists());
    assertFalse(jsFile.exists());
  }

  @Test
  public void testV3CleanupH3AndTextIndices()
      throws Exception {
    constructV3Segment();

    // V3 use single file for all column indices.
    File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3);
    File singleFileIndex = new File(segmentDirectoryPath, "columns.psf");

    // There are a few indices initially. Remove them to prepare an initial state.
    // Also use the schema with columns for H3 and Json index to add those columns.
    TableConfig tableConfigNoIndex = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfigNoIndex, _newColumnsSchemaWithH3Json))) {
      processor.process();
    }
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertNotNull(segmentMetadata.getColumnMetadataFor("newH3Col"));
    assertNotNull(segmentMetadata.getColumnMetadataFor("newJsonCol"));
    long initFileSize = singleFileIndex.length();

    // Create H3 and Json indices.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setJsonIndexColumns(Collections.singletonList("newJsonCol")).setFieldConfigList(Collections.singletonList(
            new FieldConfig("newH3Col", FieldConfig.EncodingType.DICTIONARY,
                Collections.singletonList(FieldConfig.IndexType.H3), null,
                Collections.singletonMap("resolutions", "5")))).build();
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithH3Json))) {
      processor.process();
    }

    long addedLength = 0;
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      addedLength += reader.getIndexFor("newH3Col", ColumnIndexType.H3_INDEX).size() + 8;
      addedLength += reader.getIndexFor("newJsonCol", ColumnIndexType.JSON_INDEX).size() + 8;
    }
    assertEquals(singleFileIndex.length(), initFileSize + addedLength);

    // Remove H3 and Json indices, and size gets back to initial.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfigNoIndex, _newColumnsSchemaWithH3Json))) {
      processor.process();
    }
    assertEquals(singleFileIndex.length(), initFileSize);
  }

  @Test
  public void testV1IfNeedProcess()
      throws Exception {
    constructV1Segment();
    testIfNeedProcess();
  }

  @Test
  public void testV3IfNeedProcess()
      throws Exception {
    constructV3Segment();
    testIfNeedProcess();
  }

  private void testIfNeedProcess()
      throws Exception {
    // There are a few indices initially. Remove them to prepare an initial state.
    TableConfig tableConfigNoIndex = getTableConfigNoIndex();
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfigNoIndex))) {
      assertTrue(processor.needProcess());
      processor.process();
      assertFalse(processor.needProcess());
    }

    // Require to add some default columns with new schema.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfigNoIndex, _newColumnsSchemaWithH3Json))) {
      assertTrue(processor.needProcess());
      processor.process();
      assertFalse(processor.needProcess());
    }

    // No preprocessing needed if required to add certain index on non-existing, sorted or non-dictionary column.
    for (Map.Entry<String, Consumer<TableConfig>> entry : createConfigPrepFunctionNeedNoops().entrySet()) {
      String testCase = entry.getKey();
      TableConfig tableConfig = getTableConfigNoIndex();
      entry.getValue().accept(tableConfig);
      try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
          .load(_indexDir.toURI(),
              new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
          SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
              new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithH3Json))) {
        assertFalse(processor.needProcess(), testCase);
      }
    }

    // Require to add different types of indices. Add one new index a time
    // to test the index handlers separately.
    TableConfig tableConfig = getTableConfigNoIndex();
    for (Map.Entry<String, Consumer<TableConfig>> entry : createConfigPrepFunctions().entrySet()) {
      String testCase = entry.getKey();
      entry.getValue().accept(tableConfig);
      try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
          .load(_indexDir.toURI(),
              new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
          SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
              new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithH3Json))) {
        assertTrue(processor.needProcess(), testCase);
        processor.process();
        assertFalse(processor.needProcess(), testCase);
      }
    }

    // Require to add startree index.
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setEnableDynamicStarTreeCreation(true);
    indexingConfig.setEnableDefaultStarTree(true);
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithH3Json))) {
      assertTrue(processor.needProcess());
      processor.process();
      assertFalse(processor.needProcess());
    }

    // Require to update min and max values.
    removeMinMaxValuesFromMetadataFile(_indexDir);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    segmentMetadata.getColumnMetadataMap().forEach((k, v) -> {
      assertNull(v.getMinValue(), "checking column: " + k);
      assertNull(v.getMaxValue(), "checking column: " + k);
    });
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithH3Json))) {
      assertTrue(processor.needProcess());
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    segmentMetadata.getColumnMetadataMap().forEach((k, v) -> {
      if (v.hasDictionary()) {
        assertNotNull(v.getMinValue(), "checking column: " + k);
        assertNotNull(v.getMaxValue(), "checking column: " + k);
      } else {
        assertNull(v.getMinValue(), "checking column: " + k);
        assertNull(v.getMaxValue(), "checking column: " + k);
      }
    });
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
    File segment = buildTestSegmentForMinMax(tableConfig, schema, "validSegment", stringValuesValid, longValues);
    SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(segment.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.ALL.name());
    SegmentPreProcessor processor =
        new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(tableConfig, schema));
    assertFalse(processor.needProcess());
    FileUtils.deleteQuietly(INDEX_DIR);

    // build bad segment, still no needPreprocess, since minMaxInvalid flag should be set
    segment = buildTestSegmentForMinMax(tableConfig, schema, "invalidSegment", stringValuesInvalid, longValues);
    segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader().load(segment.toURI(),
        new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.NONE.name());
    processor = new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(tableConfig, schema));
    assertFalse(processor.needProcess());

    indexingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.ALL.name());
    processor = new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(tableConfig, schema));
    assertFalse(processor.needProcess());

    // modify metadata, to remove min/max, now needPreprocess
    removeMinMaxValuesFromMetadataFile(segment);
    segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader().load(segment.toURI(),
        new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
    processor = new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(tableConfig, schema));
    assertTrue(processor.needProcess());
  }

  private File buildTestSegmentForMinMax(TableConfig tableConfig, Schema schema, String segmentName,
      String[] stringValues, long[] longValues)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getAbsolutePath());
    config.setSegmentName(segmentName);

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
    driver.getOutputDirectory().deleteOnExit();
    return driver.getOutputDirectory();
  }

  private static void removeMinMaxValuesFromMetadataFile(File indexDir) {
    PropertiesConfiguration configuration = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    Iterator<String> keys = configuration.getKeys();
    while (keys.hasNext()) {
      String key = keys.next();
      if (key.endsWith(V1Constants.MetadataKeys.Column.MIN_VALUE) || key.endsWith(
          V1Constants.MetadataKeys.Column.MAX_VALUE) || key.endsWith(
          V1Constants.MetadataKeys.Column.MIN_MAX_VALUE_INVALID)) {
        configuration.clearProperty(key);
      }
    }
    SegmentMetadataUtils.savePropertiesConfiguration(configuration);
  }

  private static Map<String, Consumer<TableConfig>> createConfigPrepFunctions() {
    Map<String, Consumer<TableConfig>> testCases = new HashMap<>();
    testCases.put("addInvertedIndex", (TableConfig config) -> config.getIndexingConfig()
        .setInvertedIndexColumns(Collections.singletonList("column3")));
    testCases.put("addRangeIndex",
        (TableConfig config) -> config.getIndexingConfig().setRangeIndexColumns(Collections.singletonList("column3")));
    testCases.put("addTextIndex", (TableConfig config) -> config.setFieldConfigList(Collections.singletonList(
        new FieldConfig("column3", FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.TEXT), null, null))));
    testCases.put("addFSTIndex", (TableConfig config) -> config.setFieldConfigList(Collections.singletonList(
        new FieldConfig("column3", FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.FST), null, null))));
    testCases.put("addBloomFilter", (TableConfig config) -> config.getIndexingConfig()
        .setBloomFilterConfigs(Collections.singletonMap("column3", new BloomFilterConfig(0.1, 1024, true))));
    testCases.put("addH3Index", (TableConfig config) -> config.setFieldConfigList(Collections.singletonList(
        new FieldConfig("newH3Col", FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.H3), null, Collections.singletonMap("resolutions", "5")))));
    testCases.put("addJsonIndex", (TableConfig config) -> config.getIndexingConfig()
        .setJsonIndexColumns(Collections.singletonList("newJsonCol")));
    return testCases;
  }

  private static Map<String, Consumer<TableConfig>> createConfigPrepFunctionNeedNoops() {
    Map<String, Consumer<TableConfig>> testCases = new HashMap<>();
    // daysSinceEpoch is a sorted column, thus inverted index and range index skip it.
    testCases.put("addInvertedIndexOnSortedColumn", (TableConfig config) -> config.getIndexingConfig()
        .setInvertedIndexColumns(Collections.singletonList("daysSinceEpoch")));
    testCases.put("addRangeIndexOnSortedColumn", (TableConfig config) -> config.getIndexingConfig()
        .setRangeIndexColumns(Collections.singletonList("daysSinceEpoch")));
    // column4 is unsorted non-dictionary encoded column, so inverted index and bloom filter skip it.
    // In fact, the validation logic when updating index configs already blocks this to happen.
    testCases.put("addInvertedIndexOnNonDictColumn", (TableConfig config) -> config.getIndexingConfig()
        .setInvertedIndexColumns(Collections.singletonList("column4")));
    // No index is added on non-existing columns.
    // The validation logic when updating index configs already blocks this to happen.
    testCases.put("addInvertedIndexOnAbsentColumn", (TableConfig config) -> config.getIndexingConfig()
        .setInvertedIndexColumns(Collections.singletonList("newColumnX")));
    testCases.put("addRangeIndexOnAbsentColumn", (TableConfig config) -> config.getIndexingConfig()
        .setRangeIndexColumns(Collections.singletonList("newColumnX")));
    testCases.put("addTextIndexOnAbsentColumn", (TableConfig config) -> config.setFieldConfigList(
        Collections.singletonList(new FieldConfig("newColumnX", FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.TEXT), null, null))));
    testCases.put("addFSTIndexOnAbsentColumn", (TableConfig config) -> config.setFieldConfigList(
        Collections.singletonList(new FieldConfig("newColumnX", FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.FST), null, null))));
    testCases.put("addBloomFilterOnAbsentColumn", (TableConfig config) -> config.getIndexingConfig()
        .setBloomFilterConfigs(Collections.singletonMap("newColumnX", new BloomFilterConfig(0.1, 1024, true))));
    testCases.put("addH3IndexOnAbsentColumn", (TableConfig config) -> config.setFieldConfigList(
        Collections.singletonList(new FieldConfig("newColumnX", FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.H3), null, Collections.singletonMap("resolutions", "5")))));
    testCases.put("addJsonIndexOnAbsentColumn", (TableConfig config) -> config.getIndexingConfig()
        .setJsonIndexColumns(Collections.singletonList("newColumnX")));
    return testCases;
  }

  /**
   * Test to check the behavior of the forward index disabled feature when enabled on a new SV column
   */
  @Test
  public void testForwardIndexDisabledOnNewColumnsSV()
      throws Exception {
    TableConfig tableConfig = getDefaultTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    assertNotNull(invertedIndexColumns);
    assertFalse(invertedIndexColumns.contains(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV));
    invertedIndexColumns.add(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    tableConfig.setFieldConfigList(Collections.singletonList(
        new FieldConfig(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.INVERTED), null,
            Collections.singletonMap(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.toString(true)))));

    // Create a segment in V1, add a new column with no forward index enabled
    constructV1Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Forward index is always going to be present for default SV columns with forward index disabled. This is because
    // such default columns are going to be sorted and the forwardIndexDisabled flag is a no-op for sorted columns
    createAndValidateIndex(tableConfig, _newColumnsSchemaWithForwardIndexDisabled,
        NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, Collections.emptyList(), false, 1, 1, true, true, true, 4, true, 0,
        null, DataType.STRING, 100000);

    // Create a segment in V3, add a column with no forward index enabled
    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Forward index is always going to be present for default SV columns with forward index disabled. This is because
    // such default columns are going to be sorted and the forwardIndexDisabled flag is a no-op for sorted columns
    createAndValidateIndex(tableConfig, _newColumnsSchemaWithForwardIndexDisabled,
        NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, Collections.emptyList(), false, 1, 1, true, true, true, 4, true, 0,
        null, DataType.STRING, 100000);

    // Add the column to the no dictionary column list
    List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertFalse(noDictionaryColumns.contains(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV));
    noDictionaryColumns.add(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);

    // Create a segment in v1, add a new column with no forward index enabled
    constructV1Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithForwardIndexDisabled))) {
      processor.process();
      fail("Forward index cannot be disabled for dictionary disabled columns!");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Dictionary disabled column: newForwardIndexDisabledColumnSV cannot disable the forward index");
    }

    // Create a segment in V3, add a new column with no forward index enabled
    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithForwardIndexDisabled))) {
      processor.process();
      fail("Forward index cannot be disabled for dictionary disabled columns!");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Dictionary disabled column: newForwardIndexDisabledColumnSV cannot disable the forward index");
    }

    // Reset the no dictionary columns
    assertTrue(noDictionaryColumns.remove(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV));

    // Add the column to the sorted list
    indexingConfig.setSortedColumn(Collections.singletonList(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV));

    // Create a segment in V1, add a new column with no forward index enabled
    constructV1Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Column should be sorted and should be created successfully since for SV columns the forwardIndexDisabled flag
    // is a no-op
    createAndValidateIndex(tableConfig, _newColumnsSchemaWithForwardIndexDisabled,
        NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, Collections.emptyList(), false, 1, 1, true, true, true, 4, true, 0,
        null, DataType.STRING, 100000);

    // Create a segment in V3, add a new column with no forward index enabled
    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Column should be sorted and should be created successfully since for SV columns the forwardIndexDisabled flag
    // is a no-op
    createAndValidateIndex(tableConfig, _newColumnsSchemaWithForwardIndexDisabled,
        NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, Collections.emptyList(), false, 1, 1, true, true, true, 4, true, 0,
        null, DataType.STRING, 100000);

    // Reset the sorted column list
    indexingConfig.setSortedColumn(null);

    // Remove the column from the inverted index column list and validate that it fails
    assertTrue(invertedIndexColumns.remove(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV));

    constructV1Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithForwardIndexDisabled))) {
      processor.process();
      fail("Should not be able to disable forward index without inverted index for column");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Inverted index must be enabled for forward index disabled column: newForwardIndexDisabledColumnSV");
    }

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithForwardIndexDisabled))) {
      processor.process();
      fail("Forward index cannot be disabled without enabling the inverted index!");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Inverted index must be enabled for forward index disabled column: newForwardIndexDisabledColumnSV");
    }
  }

  /**
   * Test to check the behavior of the forward index disabled feature when enabled on a new MV column
   */
  @Test
  public void testForwardIndexDisabledOnNewColumnsMV()
      throws Exception {
    TableConfig tableConfig = getDefaultTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    assertNotNull(invertedIndexColumns);
    assertFalse(invertedIndexColumns.contains(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV));
    invertedIndexColumns.add(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    tableConfig.setFieldConfigList(Collections.singletonList(
        new FieldConfig(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, FieldConfig.EncodingType.DICTIONARY,
            Collections.singletonList(FieldConfig.IndexType.INVERTED), null,
            Collections.singletonMap(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.toString(true)))));

    // Create a segment in V1, add a new column with no forward index enabled
    constructV1Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Forward index is always going to be present for default SV columns with forward index disabled. This is because
    // such default columns are going to be sorted and the forwardIndexDisabled flag is a no-op for sorted columns
    createAndValidateIndex(tableConfig, _newColumnsSchemaWithForwardIndexDisabled,
        NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, Collections.singletonList(ColumnIndexType.INVERTED_INDEX), false, 1,
        1, true, true, false, 4, false, 1, null, DataType.STRING, 100000);

    // Create a segment in V3, add a column with no forward index enabled
    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Forward index is always going to be present for default SV columns with forward index disabled. This is because
    // such default columns are going to be sorted and the forwardIndexDisabled flag is a no-op for sorted columns
    createAndValidateIndex(tableConfig, _newColumnsSchemaWithForwardIndexDisabled,
        NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, Collections.singletonList(ColumnIndexType.INVERTED_INDEX), false, 1,
        1, true, true, false, 4, false, 1, null, DataType.STRING, 100000);

    // Add the column to the no dictionary column list
    List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    assertFalse(noDictionaryColumns.contains(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV));
    noDictionaryColumns.add(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);

    // Create a segment in v1, add a new column with no forward index enabled
    constructV1Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithForwardIndexDisabled))) {
      processor.process();
      fail("Forward index cannot be disabled for dictionary disabled columns!");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Dictionary disabled column: newForwardIndexDisabledColumnMV cannot disable the forward index");
    }

    // Create a segment in V3, add a new column with no forward index enabled
    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithForwardIndexDisabled))) {
      processor.process();
      fail("Forward index cannot be disabled for dictionary disabled columns!");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Dictionary disabled column: newForwardIndexDisabledColumnMV cannot disable the forward index");
    }

    // Reset the no dictionary columns
    assertTrue(noDictionaryColumns.remove(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV));

    // Remove the column from the inverted index column list and validate that it fails
    assertTrue(invertedIndexColumns.remove(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV));

    constructV1Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithForwardIndexDisabled))) {
      processor.process();
      fail("Should not be able to disable forward index without inverted index for column");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Inverted index must be enabled for forward index disabled column: newForwardIndexDisabledColumnMV");
    }

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, _newColumnsSchemaWithForwardIndexDisabled))) {
      processor.process();
      fail("Forward index cannot be disabled without enabling the inverted index!");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(),
          "Inverted index must be enabled for forward index disabled column: newForwardIndexDisabledColumnMV");
    }
  }
}
