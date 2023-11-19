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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
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
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
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
  private IndexLoadingConfig _indexLoadingConfig;
  private File _avroFile;
  private Schema _schema;
  private TableConfig _tableConfig;
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

    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(false);
    ingestionConfig.setSegmentTimeValueCheck(false);
    _tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("daysSinceEpoch")
            .setIngestionConfig(ingestionConfig).setNullHandlingEnabled(true).build();
    _indexLoadingConfig = getDefaultIndexLoadingConfig();

    // We specify two columns without inverted index ('column1', 'column13'), one non-existing column ('noSuchColumn')
    // and one column with existed inverted index ('column7').
    _indexLoadingConfig.setInvertedIndexColumns(
        Sets.newHashSet(COLUMN1_NAME, COLUMN7_NAME, COLUMN13_NAME, NO_SUCH_COLUMN_NAME));

    _indexLoadingConfig.setTableConfig(_tableConfig);

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

  private IndexLoadingConfig getDefaultIndexLoadingConfig() {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();

    // Set RAW columns. Otherwise, they will end up being converted to dict columns (default) during segment reload.
    indexLoadingConfig.setNoDictionaryColumns(
        Sets.newHashSet(EXISTING_STRING_COL_RAW, EXISTING_INT_COL_RAW_MV, EXISTING_INT_COL_RAW));

    indexLoadingConfig.setTableConfig(_tableConfig);
    return indexLoadingConfig;
  }

  private void constructV1Segment(List<String> invertedIndexCols, List<String> textIndexCols,
      List<String> rangeIndexCols, List<String> forwardIndexDisabledCols)
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    List<String> rawCols = new ArrayList<>();
    rawCols.add(EXISTING_STRING_COL_RAW);
    rawCols.add(EXISTING_INT_COL_RAW_MV);
    rawCols.add(EXISTING_INT_COL_RAW);

    // Create inverted index for 'column7' when constructing the segment.
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithSchema(_avroFile, INDEX_DIR, "testTable", _tableConfig, _schema);
    segmentGeneratorConfig.setRawIndexCreationColumns(rawCols);
    segmentGeneratorConfig.setIndexOn(StandardIndexes.inverted(), IndexConfig.ENABLED, COLUMN7_NAME);
    if (invertedIndexCols.size() > 0) {
      segmentGeneratorConfig.addInvertedIndexCreationColumns(invertedIndexCols);
    }
    if (textIndexCols.size() > 0) {
      TextIndexConfig textIndexConfig = new TextIndexConfigBuilder(segmentGeneratorConfig.getFSTIndexType()).build();
      segmentGeneratorConfig.setIndexOn(StandardIndexes.text(), textIndexConfig, textIndexCols);
    }
    if (rangeIndexCols.size() > 0) {
      segmentGeneratorConfig.setRangeIndexCreationColumns(rangeIndexCols);
    }
    if (forwardIndexDisabledCols.size() > 0) {
      segmentGeneratorConfig.setForwardIndexDisabledColumns(forwardIndexDisabledCols);
      segmentGeneratorConfig.addInvertedIndexCreationColumns(forwardIndexDisabledCols);
    }

    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(segmentGeneratorConfig);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
  }

  private void constructV3Segment()
      throws Exception {
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
  }

  /**
   * Test to check for default column handling and text index creation during
   * segment load after a new raw column is added to the schema with text index
   * creation enabled.
   * This will exercise both code paths in SegmentPreprocessor (segment load):
   * (1) Default column handler to add forward index and dictionary
   * (2) Text index handler to add text index
   */
  @Test
  public void testEnableTextIndexOnNewColumnRaw()
      throws Exception {
    Set<String> textIndexColumns = new HashSet<>();
    textIndexColumns.add(NEWLY_ADDED_STRING_COL_RAW);
    textIndexColumns.add(NEWLY_ADDED_STRING_MV_COL_RAW);
    _indexLoadingConfig.setTextIndexColumns(textIndexColumns);

    // Create a segment in V3, add a new raw column with text index enabled
    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_STRING_COL_RAW);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);
    checkTextIndexCreation(NEWLY_ADDED_STRING_COL_RAW, 1, 1, _newColumnsSchemaWithText, true, true, true, 4);
    checkTextIndexCreation(NEWLY_ADDED_STRING_MV_COL_RAW, 1, 1, _newColumnsSchemaWithText, true, true, false, 4, false,
        1);

    // Create a segment in V1, add a new raw column with text index enabled
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_STRING_COL_RAW);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);
    checkTextIndexCreation(NEWLY_ADDED_STRING_COL_RAW, 1, 1, _newColumnsSchemaWithText, true, true, true, 4);
  }

  @Test
  public void testEnableFSTIndexOnExistingColumnRaw()
      throws Exception {
    Set<String> fstColumns = new HashSet<>();
    fstColumns.add(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.setFSTIndexColumns(fstColumns);
    constructV3Segment();
    SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
    SegmentPreProcessor v3Processor =
        new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, _newColumnsSchemaWithFST);
    expectThrows(UnsupportedOperationException.class, () -> v3Processor.process());

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader().load(_indexDir.toURI(),
        new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
    SegmentPreProcessor v1Processor =
        new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, _newColumnsSchemaWithFST);
    expectThrows(UnsupportedOperationException.class, () -> v1Processor.process());
  }

  @Test
  public void testEnableFSTIndexOnNewColumnDictEncoded()
      throws Exception {
    Set<String> fstColumns = new HashSet<>();
    fstColumns.add(NEWLY_ADDED_FST_COL_DICT);
    _indexLoadingConfig.setFSTIndexColumns(fstColumns);

    constructV3Segment();
    checkFSTIndexCreation(NEWLY_ADDED_FST_COL_DICT, 1, 1, _newColumnsSchemaWithFST, true, true, 4);

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    checkFSTIndexCreation(NEWLY_ADDED_FST_COL_DICT, 1, 1, _newColumnsSchemaWithFST, true, true, 4);
  }

  @Test
  public void testEnableFSTIndexOnExistingColumnDictEncoded()
      throws Exception {
    Set<String> fstColumns = new HashSet<>();
    fstColumns.add(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.setFSTIndexColumns(fstColumns);

    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    assertNotNull(columnMetadata);
    checkFSTIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _newColumnsSchemaWithFST, false, false, 26);

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    assertNotNull(columnMetadata);
    checkFSTIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _newColumnsSchemaWithFST, false, false, 26);
  }

  @Test
  public void testSimpleEnableDictionarySV()
      throws Exception {
    // TEST 1. Check running forwardIndexHandler on a V1 segment. No-op for all existing raw columns.
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    checkForwardIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0, ChunkCompressionType.LZ4,
        true, 0, DataType.STRING, 100000);
    validateIndex(StandardIndexes.forward(), EXISTING_INT_COL_RAW, 42242, 16, _schema, false, false, false, 0, true,
        0, ChunkCompressionType.LZ4, false, DataType.INT, 100000);

    // Convert the segment to V3.
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);

    // TEST 2: Enable dictionary on EXISTING_STRING_COL_RAW
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_STRING_COL_RAW);
    checkForwardIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, true, false, 4, null, true, 0,
        DataType.STRING, 100000);

    // TEST 3: Enable dictionary on EXISTING_INT_COL_RAW
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_INT_COL_RAW);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW, 42242, 16, _schema, false, true, false, 0, null, true, 0,
        DataType.INT, 100000);
  }

  @Test
  public void testSimpleEnableDictionaryMV()
      throws Exception {
    // Add raw columns in indexingConfig so that the ForwardIndexHandler doesn't end up converting them to dictionary
    // enabled columns
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_INT_COL_RAW_MV);

    // TEST 1. Check running forwardIndexHandler on a V1 segment. No-op for all existing raw columns.
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, false, false, 0,
        ChunkCompressionType.LZ4, false, 13, DataType.INT, 106688);

    // Convert the segment to V3.
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);

    // TEST 2: Enable dictionary on EXISTING_STRING_COL_RAW
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_INT_COL_RAW_MV);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, true, false, 0, null, false, 13,
        DataType.INT, 106688);
  }

  @Test
  public void testEnableDictAndOtherIndexesSV()
      throws Exception {
    // Add raw columns in indexingConfig so that the ForwardIndexHandler doesn't end up converting them to dictionary
    // enabled columns
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_INT_COL_RAW);
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_STRING_COL_RAW);
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);

    // TEST 1: EXISTING_STRING_COL_RAW. Enable dictionary. Also add inverted index and text index. Reload code path
    // will create dictionary, inverted index and text index.
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.addInvertedIndexColumns(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.addTextIndexColumns(EXISTING_STRING_COL_RAW);
    checkForwardIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, true, false, 4, null, true, 0,
        DataType.STRING, 100000);
    validateIndex(StandardIndexes.inverted(), EXISTING_STRING_COL_RAW, 5, 3, _schema, false, true, false, 4, true,
        0, null, false, DataType.STRING, 100000);
    validateIndex(StandardIndexes.text(), EXISTING_STRING_COL_RAW, 5, 3, _schema, false, true, false, 4, true, 0,
        null, false, DataType.STRING, 100000);

    // TEST 2: EXISTING_STRING_COL_RAW. Enable dictionary on a raw column that already has text index.
    List<String> textIndexCols = new ArrayList<>();
    textIndexCols.add(EXISTING_STRING_COL_RAW);
    constructV1Segment(Collections.emptyList(), textIndexCols, Collections.emptyList(),
        Collections.emptyList());
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    validateIndex(StandardIndexes.text(), EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0, true, 0,
        null, false, DataType.STRING, 100000);

    // At this point, the segment has text index. Now, the reload path should create a dictionary.
    checkForwardIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, true, false, 4, null, true, 0,
        DataType.STRING, 100000);
    validateIndex(StandardIndexes.text(), EXISTING_STRING_COL_RAW, 5, 3, _schema, false, true, false, 4, true, 0,
        null, false, DataType.STRING, 100000);
    // Add it back so that this column is not rewritten for the other tests below.
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_STRING_COL_RAW);

    // TEST 3: EXISTING_INT_COL_RAW. Enable dictionary on a column that already has range index.
    List<String> rangeIndexCols = new ArrayList<>();
    rangeIndexCols.add(EXISTING_INT_COL_RAW);
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), rangeIndexCols, Collections.emptyList());
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW, 42242, 16, _schema, false, false, false, 0, true,
        0, ChunkCompressionType.LZ4, false, DataType.INT, 100000);
    long oldRangeIndexSize =
        new SegmentMetadataImpl(_indexDir).getColumnMetadataFor(EXISTING_INT_COL_RAW).getIndexSizeMap()
            .get(StandardIndexes.range());
    // At this point, the segment has range index. Now the reload path should create a dictionary and rewrite the
    // range index.
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_INT_COL_RAW);
    _indexLoadingConfig.addRangeIndexColumns(EXISTING_INT_COL_RAW);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW, 42242, 16, _schema, false, true, false, 0, null, true, 0,
        DataType.INT, 100000);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW, 42242, 16, _schema, false, true, false, 0, true, 0,
        null, false, DataType.INT, 100000);
    long newRangeIndexSize =
        new SegmentMetadataImpl(_indexDir).getColumnMetadataFor(EXISTING_INT_COL_RAW).getIndexSizeMap()
            .get(StandardIndexes.range());
    assertNotEquals(oldRangeIndexSize, newRangeIndexSize);
    // Add it back so that this column is not rewritten for the other tests below.
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_INT_COL_RAW);
  }

  @Test
  public void testEnableDictAndOtherIndexesMV()
      throws Exception {
    // Add raw columns in indexingConfig so that the ForwardIndexHandler doesn't end up converting them to dictionary
    // enabled columns
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_INT_COL_RAW_MV);
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);

    // TEST 1: EXISTING_INT_COL_RAW_MV. Enable dictionary for an MV column. Also enable inverted index and range index.
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_INT_COL_RAW_MV);
    _indexLoadingConfig.addInvertedIndexColumns(EXISTING_INT_COL_RAW_MV);
    _indexLoadingConfig.addRangeIndexColumns(EXISTING_INT_COL_RAW_MV);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, true, false, 0, null, false, 13,
        DataType.INT, 106688);
    validateIndex(StandardIndexes.inverted(), EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, true, false, 0,
        false, 13, null, false, DataType.INT, 106688);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, true, false, 0,
        false, 13, null, false, DataType.INT, 106688);

    // TEST 2: EXISTING_INT_COL_RAW_MV. Enable dictionary for an MV column that already has range index.
    List<String> rangeIndexCols = new ArrayList<>();
    rangeIndexCols.add(EXISTING_INT_COL_RAW_MV);
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), rangeIndexCols, Collections.emptyList());
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    validateIndex(StandardIndexes.forward(), EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, false, false, 0,
        false, 13, ChunkCompressionType.LZ4, false, DataType.INT, 106688);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, false, false, 0,
        false, 13, ChunkCompressionType.LZ4, false, DataType.INT, 106688);

    // Enable dictionary.
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_INT_COL_RAW_MV);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, true, false, 0, null, false, 13,
        DataType.INT, 106688);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, true, false, 0,
        false, 13, null, false, DataType.INT, 106688);
  }

  @Test
  public void testSimpleDisableDictionary()
      throws Exception {
    // TEST 1. Check running forwardIndexHandler on a V1 segment. No-op for all existing dict columns.
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    checkForwardIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _schema, false, true, false, 26, null, true, 0,
        DataType.STRING, 100000);
    validateIndex(StandardIndexes.forward(), COLUMN10_NAME, 3960, 12, _schema, false, true, false, 0, true, 0, null,
        false, DataType.INT, 100000);

    // Convert the segment to V3.
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);

    // TEST 2: Disable dictionary for EXISTING_STRING_COL_DICT.
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_STRING_COL_DICT);
    checkForwardIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _schema, false, false, false, 0, ChunkCompressionType.LZ4,
        true, 0, DataType.STRING, 100000);

    // TEST 3: Disable dictionary for COLUMN10_NAME
    _indexLoadingConfig.addNoDictionaryColumns(COLUMN10_NAME);
    checkForwardIndexCreation(COLUMN10_NAME, 3960, 12, _schema, false, false, false, 0, ChunkCompressionType.LZ4, true,
        0, DataType.INT, 100000);
  }

  @Test
  public void testDisableDictAndOtherIndexesSV()
      throws Exception {
    // Validate No-op.
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);

    // TEST 1: Disable dictionary on a column that has inverted index. Should be a no-op and column should still have
    // a dictionary.
    _indexLoadingConfig.addNoDictionaryColumns(COLUMN1_NAME);
    checkForwardIndexCreation(COLUMN1_NAME, 51594, 16, _schema, false, true, false, 0, null, true, 0, DataType.INT,
        100000);

    // TEST 2: Disable dictionary. Also remove inverted index on column1.
    _indexLoadingConfig.addNoDictionaryColumns(COLUMN1_NAME);
    _indexLoadingConfig.removeInvertedIndexColumns(COLUMN1_NAME);
    checkForwardIndexCreation(COLUMN1_NAME, 51594, 16, _schema, false, false, false, 0, null, true, 0, DataType.INT,
        100000);

    // TEST 3: Disable dictionary for a column (Column10) that has range index.
    List<String> rangeIndexCols = new ArrayList<>();
    rangeIndexCols.add(COLUMN10_NAME);
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), rangeIndexCols, Collections.emptyList());
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    validateIndex(StandardIndexes.forward(), COLUMN10_NAME, 3960, 12, _schema, false, true, false, 0, true, 0, null,
        false, DataType.INT, 100000);
    validateIndex(StandardIndexes.range(), COLUMN10_NAME, 3960, 12, _schema, false, true, false, 0, true, 0, null,
        false, DataType.INT, 100000);
    long oldRangeIndexSize = new SegmentMetadataImpl(_indexDir).getColumnMetadataFor(COLUMN10_NAME).getIndexSizeMap()
        .get(StandardIndexes.range());


    _indexLoadingConfig.addNoDictionaryColumns(COLUMN10_NAME);
    _indexLoadingConfig.addRangeIndexColumns(COLUMN10_NAME);
    checkForwardIndexCreation(COLUMN10_NAME, 3960, 12, _schema, false, false, false, 0, ChunkCompressionType.LZ4, true,
        0, DataType.INT, 100000);
    validateIndex(StandardIndexes.range(), COLUMN10_NAME, 3960, 12, _schema, false, false, false, 0, true, 0,
        ChunkCompressionType.LZ4, false, DataType.INT, 100000);
    long newRangeIndexSize = new SegmentMetadataImpl(_indexDir).getColumnMetadataFor(COLUMN10_NAME).getIndexSizeMap()
        .get(StandardIndexes.range());
    assertNotEquals(oldRangeIndexSize, newRangeIndexSize);

    // TEST4: Disable dictionary but add text index.
    validateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4, _schema, false, true, false, 26, true,
        0, null, false, DataType.STRING, 100000);

    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.addTextIndexColumns(EXISTING_STRING_COL_DICT);
    checkForwardIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _schema, false, false, false, 0, ChunkCompressionType.LZ4,
        true, 0, DataType.STRING, 100000);
    validateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4, _schema, false, false, false, 0, true,
        0, null, false, DataType.STRING, 100000);
  }

  @Test
  public void testDisableDictAndOtherIndexesMV()
      throws Exception {
    // Set up: Enable dictionary on MV column6 and validate.
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());

    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_INT_COL_RAW_MV);
    _indexLoadingConfig.addRangeIndexColumns(EXISTING_INT_COL_RAW_MV);
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, true, false, 0, null, false, 13,
        DataType.INT, 106688);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, true, false, 0,
        false, 13, null, false, DataType.INT, 106688);

    // TEST 1: Disable dictionary on a column where range index is already enabled.
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_INT_COL_RAW_MV);
    checkForwardIndexCreation(EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, false, false, 0, null, false, 13,
        DataType.INT, 106688);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW_MV, 18499, 15, _schema, false, false, false, 0,
        false, 13, null, false, DataType.INT, 106688);

    // TEST 2. Disable dictionary on a column where inverted index is enabled. Should be a no-op.
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    validateIndex(StandardIndexes.forward(), COLUMN7_NAME, 359, 9, _newColumnsSchemaWithForwardIndexDisabled, false,
        true, false, 0, false, 24, null, false, DataType.INT, 134090);
    validateIndex(StandardIndexes.inverted(), COLUMN7_NAME, 359, 9, _newColumnsSchemaWithForwardIndexDisabled,
        false, true, false, 0, false, 24, null, false, DataType.INT, 134090);

    _indexLoadingConfig.addNoDictionaryColumns(COLUMN7_NAME);
    checkForwardIndexCreation(COLUMN7_NAME, 359, 9, _schema, false, true, false, 0, null, false, 24, DataType.INT,
        134090);
    validateIndex(StandardIndexes.inverted(), COLUMN7_NAME, 359, 9, _newColumnsSchemaWithForwardIndexDisabled,
        false, true, false, 0, false, 24, null, false, DataType.INT, 134090);

    // TEST 3: Disable dictionary and disable inverted index on column7.
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);

    _indexLoadingConfig.addNoDictionaryColumns(COLUMN7_NAME);
    _indexLoadingConfig.removeInvertedIndexColumns(COLUMN7_NAME);
    checkForwardIndexCreation(COLUMN7_NAME, 359, 9, _schema, false, false, false, 0, null, false, 24, DataType.INT,
        134090);
  }

  @Test
  public void testForwardIndexHandlerChangeCompression()
      throws Exception {
    Map<String, CompressionCodec> compressionConfigs = new HashMap<>();
    compressionConfigs.put(EXISTING_STRING_COL_RAW, CompressionCodec.ZSTANDARD);
    _indexLoadingConfig.setCompressionConfigs(compressionConfigs);
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_STRING_COL_RAW);

    // Test1: Rewriting forward index will be a no-op for v1 segments. Default LZ4 compressionType will be retained.
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    checkForwardIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0, ChunkCompressionType.LZ4,
        true, 0, DataType.STRING, 100000);

    // Convert the segment to V3.
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);

    // Test2: Now forward index will be rewritten with ZSTANDARD compressionType.
    checkForwardIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0,
        ChunkCompressionType.ZSTANDARD, true, 0, DataType.STRING, 100000);

    // Test3: Change compression on existing raw index column. Also add text index on same column. Check correctness.
    compressionConfigs.put(EXISTING_STRING_COL_RAW, CompressionCodec.SNAPPY);
    _indexLoadingConfig.setCompressionConfigs(compressionConfigs);
    Set<String> textIndexColumns = new HashSet<>();
    textIndexColumns.add(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.setTextIndexColumns(textIndexColumns);

    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    assertNotNull(columnMetadata);
    checkTextIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0);
    validateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0, true, 0,
        ChunkCompressionType.SNAPPY, false, DataType.STRING, 100000);

    // Test4: Change compression on RAW index column. Change another index on another column. Check correctness.
    compressionConfigs.put(EXISTING_STRING_COL_RAW, CompressionCodec.ZSTANDARD);
    _indexLoadingConfig.setCompressionConfigs(compressionConfigs);
    Set<String> fstColumns = new HashSet<>();
    fstColumns.add(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.setFSTIndexColumns(fstColumns);

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    assertNotNull(columnMetadata);
    // Check FST index
    checkFSTIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _newColumnsSchemaWithFST, false, false, 26);
    // Check forward index.
    validateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0, true, 0,
        ChunkCompressionType.ZSTANDARD, false, DataType.STRING, 100000);

    // Test5: Change compressionType for an MV column
    compressionConfigs.put(EXISTING_INT_COL_RAW_MV, CompressionCodec.ZSTANDARD);
    _indexLoadingConfig.setCompressionConfigs(compressionConfigs);
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_INT_COL_RAW_MV);

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
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
  @Test
  public void testEnableTextIndexOnNewColumnDictEncoded()
      throws Exception {
    Set<String> textIndexColumns = new HashSet<>();
    textIndexColumns.add(NEWLY_ADDED_STRING_COL_DICT);
    textIndexColumns.add(NEWLY_ADDED_STRING_MV_COL_DICT);
    _indexLoadingConfig.setTextIndexColumns(textIndexColumns);

    // Create a segment in V3, add a new dict encoded column with text index enabled
    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_STRING_COL_RAW);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);
    checkTextIndexCreation(NEWLY_ADDED_STRING_COL_DICT, 1, 1, _newColumnsSchemaWithText, true, true, true, 4);

    checkTextIndexCreation(NEWLY_ADDED_STRING_MV_COL_DICT, 1, 1, _newColumnsSchemaWithText, true, true, false, 4, false,
        1);

    // Create a segment in V1, add a new dict encoded column with text index enabled
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_STRING_COL_RAW);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);
    checkTextIndexCreation(NEWLY_ADDED_STRING_COL_DICT, 1, 1, _newColumnsSchemaWithText, true, true, true, 4);
  }

  /**
   * Test to check text index creation during segment load after text index
   * creation is enabled on an existing raw column.
   * This will exercise the SegmentPreprocessor code path during segment load
   * @throws Exception
   */
  @Test
  public void testEnableTextIndexOnExistingRawColumn()
      throws Exception {
    Set<String> textIndexColumns = new HashSet<>();
    textIndexColumns.add(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.setTextIndexColumns(textIndexColumns);

    // Create a segment in V3, enable text index on existing column
    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    assertNotNull(columnMetadata);
    checkTextIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0);

    // Create a segment in V1, add a new column with text index enabled
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    assertNotNull(columnMetadata);
    checkTextIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0);
  }

  /**
   * Test to check text index creation during segment load after text index
   * creation is enabled on an existing dictionary encoded column.
   * This will exercise the SegmentPreprocessor code path during segment load
   * @throws Exception
   */
  @Test
  public void testEnableTextIndexOnExistingDictEncodedColumn()
      throws Exception {
    constructV3Segment();
    Set<String> textIndexColumns = new HashSet<>();
    textIndexColumns.add(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.setTextIndexColumns(textIndexColumns);

    // Create a segment in V3, enable text index on existing column
    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    assertNotNull(columnMetadata);
    // SegmentPreprocessor should have created the text index using TextIndexHandler
    checkTextIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _schema, false, true, false, 26);

    // Create a segment in V1, add a new column with text index enabled
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    assertNotNull(columnMetadata);
    // SegmentPreprocessor should have created the text index using TextIndexHandler
    checkTextIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _schema, false, true, false, 26);
  }

  private void checkFSTIndexCreation(String column, int cardinality, int bits, Schema schema, boolean isAutoGenerated,
      boolean isSorted, int dictionaryElementSize)
      throws Exception {
    createAndValidateIndex(StandardIndexes.fst(), column, cardinality, bits, schema, isAutoGenerated, true,
        isSorted, dictionaryElementSize, true, 0, null, false, DataType.STRING, 100000);
  }

  private void checkTextIndexCreation(String column, int cardinality, int bits, Schema schema, boolean isAutoGenerated,
      boolean hasDictionary, boolean isSorted, int dictionaryElementSize)
      throws Exception {
    createAndValidateIndex(StandardIndexes.text(), column, cardinality, bits, schema, isAutoGenerated,
        hasDictionary, isSorted, dictionaryElementSize, true, 0, null, false, DataType.STRING, 100000);
  }

  private void checkTextIndexCreation(String column, int cardinality, int bits, Schema schema, boolean isAutoGenerated,
      boolean hasDictionary, boolean isSorted, int dictionaryElementSize, boolean isSingleValue,
      int maxNumberOfMultiValues)
      throws Exception {
    createAndValidateIndex(StandardIndexes.text(), column, cardinality, bits, schema, isAutoGenerated,
        hasDictionary, isSorted, dictionaryElementSize, isSingleValue, maxNumberOfMultiValues, null, false,
        DataType.STRING, 100000);
  }

  private void checkForwardIndexCreation(String column, int cardinality, int bits, Schema schema,
      boolean isAutoGenerated, boolean hasDictionary, boolean isSorted, int dictionaryElementSize,
      ChunkCompressionType expectedCompressionType, boolean isSingleValue, int maxNumberOfMultiValues,
      DataType dataType, int totalNumberOfEntries)
      throws Exception {
    createAndValidateIndex(StandardIndexes.forward(), column, cardinality, bits, schema, isAutoGenerated,
        hasDictionary, isSorted, dictionaryElementSize, isSingleValue, maxNumberOfMultiValues, expectedCompressionType,
        false, dataType, totalNumberOfEntries);
  }

  private void createAndValidateIndex(IndexType<?, ?, ?> indexType, String column, int cardinality, int bits,
      Schema schema, boolean isAutoGenerated, boolean hasDictionary, boolean isSorted, int dictionaryElementSize,
      boolean isSingleValued, int maxNumberOfMultiValues, ChunkCompressionType expectedCompressionType,
      boolean forwardIndexDisabled, DataType dataType, int totalNumberOfEntries)
      throws Exception {

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, schema)) {
      processor.process();
      validateIndex(indexType, column, cardinality, bits, schema, isAutoGenerated, hasDictionary, isSorted,
          dictionaryElementSize, isSingleValued, maxNumberOfMultiValues, expectedCompressionType, forwardIndexDisabled,
          dataType, totalNumberOfEntries);
    }
  }

  private void validateIndex(IndexType<?, ?, ?> indexType, String column, int cardinality, int bits, Schema schema,
      boolean isAutoGenerated, boolean hasDictionary, boolean isSorted, int dictionaryElementSize,
      boolean isSingleValued, int maxNumberOfMultiValues, ChunkCompressionType expectedCompressionType,
      boolean forwardIndexDisabled, DataType dataType, int totalNumberOfEntries)
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
      if (indexType != StandardIndexes.forward() || !forwardIndexDisabled) {
        assertTrue(reader.hasIndexFor(column, indexType));
      }
      if (isSingleValued && isSorted && forwardIndexDisabled) {
        // Disabling the forward index for sorted columns should be a no-op
        assertTrue(reader.hasIndexFor(column, StandardIndexes.forward()));
        assertFalse(reader.hasIndexFor(column, StandardIndexes.inverted()));
      } else if (forwardIndexDisabled) {
        if (segmentMetadata.getVersion() == SegmentVersion.v3 || isAutoGenerated) {
          assertFalse(reader.hasIndexFor(column, StandardIndexes.forward()));
        } else {
          // Updating dictionary or forward index for existing columns not supported for v1 segments yet
          assertTrue(reader.hasIndexFor(column, StandardIndexes.forward()));
        }
      }

      // Check if the raw forward index compressionType is correct.
      if (expectedCompressionType != null) {
        assertFalse(hasDictionary);

        try (ForwardIndexReader fwdIndexReader = ForwardIndexType.read(reader, columnMetadata)) {
          ChunkCompressionType compressionType = fwdIndexReader.getCompressionType();
          assertEquals(compressionType, expectedCompressionType);
        }

        File inProgressFile = new File(_indexDir, column + ".fwd.inprogress");
        assertFalse(inProgressFile.exists());

        String fwdIndexFileExtension;
        if (isSingleValued) {
          fwdIndexFileExtension = V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION;
        } else {
          fwdIndexFileExtension = V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION;
        }
        File v1FwdIndexFile = new File(_indexDir, column + fwdIndexFileExtension);
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
    try (SegmentDirectory segmentDirectory1 = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory1.createReader()) {
      assertFalse(reader.hasIndexFor(column, indexType));
    }
  }

  private void validateIndexExists(String column, IndexType<?, ?, ?> indexType)
      throws Exception {
    try (SegmentDirectory segmentDirectory1 = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory1.createReader()) {
      assertTrue(reader.hasIndexFor(column, indexType));
    }
  }

  @Test
  public void testV1CreateInvertedIndices()
      throws Exception {
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());

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
    constructV3Segment();

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);

    File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3);
    File singleFileIndex = new File(segmentDirectoryPath, "columns.psf");
    FileTime lastModifiedTime = Files.getLastModifiedTime(singleFileIndex.toPath());
    long fileSize = singleFileIndex.length();

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the first time.
    checkInvertedIndexCreation(false);
    long addedLength = 0L;
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
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
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      if (reCreate) {
        assertTrue(reader.hasIndexFor(COLUMN1_NAME, StandardIndexes.inverted()));
        assertTrue(reader.hasIndexFor(COLUMN13_NAME, StandardIndexes.inverted()));
        assertTrue(reader.hasIndexFor(COLUMN7_NAME, StandardIndexes.inverted()));
        assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, StandardIndexes.inverted()));
      } else {
        assertFalse(reader.hasIndexFor(COLUMN1_NAME, StandardIndexes.inverted()));
        assertTrue(reader.hasIndexFor(COLUMN7_NAME, StandardIndexes.inverted()));
        assertFalse(reader.hasIndexFor(COLUMN13_NAME, StandardIndexes.inverted()));
        assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, StandardIndexes.inverted()));
      }
    }

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, null)) {
      processor.process();
    }

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertTrue(reader.hasIndexFor(COLUMN1_NAME, StandardIndexes.inverted()));
      assertTrue(reader.hasIndexFor(COLUMN13_NAME, StandardIndexes.inverted()));
      assertTrue(reader.hasIndexFor(COLUMN7_NAME, StandardIndexes.inverted()));
      assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, StandardIndexes.inverted()));
    }
  }

  @Test
  public void testV1UpdateDefaultColumns()
      throws Exception {
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(
        Collections.singletonList(new TransformConfig(NEW_INT_SV_DIMENSION_COLUMN_NAME, "plus(column1, 1)")));
    _tableConfig.setIngestionConfig(ingestionConfig);
    _indexLoadingConfig.addInvertedIndexColumns(NEW_COLUMN_INVERTED_INDEX);
    checkUpdateDefaultColumns();

    // Try to use the third schema and update default value again.
    // For the third schema, we changed the default value for column 'newStringMVDimension' to 'notSameLength',
    // which is not the same length as before. This should be fine for segment format v1.
    // We added two new columns and also removed the NEW_INT_SV_DIMENSION_COLUMN_NAME from schema.
    // NEW_INT_SV_DIMENSION_COLUMN_NAME exists before processing but removed afterwards.
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertNotNull(segmentMetadata.getColumnMetadataFor(NEW_INT_SV_DIMENSION_COLUMN_NAME));
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig,
            _newColumnsSchema3)) {
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
  public void testV3UpdateDefaultColumns()
      throws Exception {
    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(
        Collections.singletonList(new TransformConfig(NEW_INT_SV_DIMENSION_COLUMN_NAME, "plus(column1, 1)")));
    _tableConfig.setIngestionConfig(ingestionConfig);
    _indexLoadingConfig.addInvertedIndexColumns(NEW_COLUMN_INVERTED_INDEX);
    checkUpdateDefaultColumns();

    // Try to use the third schema and update default value again.
    // For the third schema, we changed the default value for column 'newStringMVDimension' to 'notSameLength', which
    // is not the same length as before. This should be fine for segment format v3 as well.
    // We added two new columns and also removed the NEW_INT_SV_DIMENSION_COLUMN_NAME from schema.
    // NEW_INT_SV_DIMENSION_COLUMN_NAME exists before processing but removed afterwards.
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertNotNull(segmentMetadata.getColumnMetadataFor(NEW_INT_SV_DIMENSION_COLUMN_NAME));
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig,
            _newColumnsSchema3)) {
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

  private void checkUpdateDefaultColumns()
      throws Exception {
    // Update default value.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig,
            _newColumnsSchema1)) {
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
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig,
            _newColumnsSchema2)) {
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
  }

  @Test
  public void testColumnMinMaxValue()
      throws Exception {
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());

    // Remove min/max value from the metadata
    removeMinMaxValuesFromMetadataFile(_indexDir);

    IndexLoadingConfig indexLoadingConfig = getDefaultIndexLoadingConfig();
    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.NONE);
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, null)) {
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

    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.TIME);
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, null)) {
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

    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.NON_METRIC);
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, null)) {
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

    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.ALL);
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, null)) {
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
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v1);

    // Need to create two default columns with Bytes and JSON string for H3 and JSON index.
    // Other kinds of indices can all be put on column3 with String values.
    String strColumn = "column3";
    IndexLoadingConfig indexLoadingConfig = getDefaultIndexLoadingConfig();
    indexLoadingConfig.setInvertedIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    indexLoadingConfig.setRangeIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    indexLoadingConfig.setTextIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    indexLoadingConfig.setFSTIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    indexLoadingConfig.setBloomFilterConfigs(ImmutableMap.of(strColumn, new BloomFilterConfig(0.1, 1024, true)));

    // V1 use separate file for each column index.
    File iiFile = new File(_indexDir, strColumn + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    File rgFile = new File(_indexDir, strColumn + V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION);
    File txtFile = new File(_indexDir, strColumn + V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION);
    File fstFile = new File(_indexDir, strColumn + V1Constants.Indexes.LUCENE_V9_FST_INDEX_FILE_EXTENSION);
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
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, null)) {
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
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, getDefaultIndexLoadingConfig(),
            null)) {
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
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);

    // V3 use single file for all column indices.
    File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3);
    File singleFileIndex = new File(segmentDirectoryPath, "columns.psf");

    // There are a few indices initially. Remove them to prepare an initial state.
    long initFileSize = singleFileIndex.length();
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());

        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, getDefaultIndexLoadingConfig(),
            null)) {
      processor.process();
    }
    assertTrue(singleFileIndex.length() < initFileSize);
    initFileSize = singleFileIndex.length();

    // Need to create two default columns with Bytes and JSON string for H3 and JSON index.
    // Other kinds of indices can all be put on column3 with String values.
    String strColumn = "column3";
    IndexLoadingConfig indexLoadingConfig = getDefaultIndexLoadingConfig();
    indexLoadingConfig.setInvertedIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    indexLoadingConfig.setRangeIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    indexLoadingConfig.setTextIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    indexLoadingConfig.setFSTIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    indexLoadingConfig.setBloomFilterConfigs(ImmutableMap.of(strColumn, new BloomFilterConfig(0.1, 1024, true)));

    // Create all kinds of indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, null)) {
      processor.process();
    }

    long addedLength = 0;
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      addedLength += reader.getIndexFor(strColumn, StandardIndexes.inverted()).size() + 8;
      addedLength += reader.getIndexFor(strColumn, StandardIndexes.range()).size() + 8;
      addedLength += reader.getIndexFor(strColumn, StandardIndexes.fst()).size() + 8;
      addedLength += reader.getIndexFor(strColumn, StandardIndexes.bloomFilter()).size() + 8;
      assertTrue(reader.hasIndexFor(strColumn, StandardIndexes.text()));
    }
    assertEquals(singleFileIndex.length(), initFileSize + addedLength);

    // Remove all kinds of indices, and size gets back initial size.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, getDefaultIndexLoadingConfig(),
            null)) {
      processor.process();
    }
    assertEquals(singleFileIndex.length(), initFileSize);
  }

  @Test
  public void testV1CleanupH3AndTextIndices()
      throws Exception {
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());

    // Remove all indices and add the two derived columns for H3 and Json index.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, getDefaultIndexLoadingConfig(),
            _newColumnsSchemaWithH3Json)) {
      processor.process();
    }

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertNotNull(segmentMetadata.getColumnMetadataFor("newH3Col"));
    assertNotNull(segmentMetadata.getColumnMetadataFor("newJsonCol"));

    _indexLoadingConfig = getDefaultIndexLoadingConfig();
    _indexLoadingConfig.setH3IndexConfigs(
        ImmutableMap.of("newH3Col", new H3IndexConfig(ImmutableMap.of("resolutions", "5"))));
    _indexLoadingConfig.setJsonIndexColumns(new HashSet<>(Collections.singletonList("newJsonCol")));

    // V1 use separate file for each column index.
    File h3File = new File(_indexDir, "newH3Col" + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);
    File jsFile = new File(_indexDir, "newJsonCol" + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);

    assertFalse(h3File.exists());
    assertFalse(jsFile.exists());

    // Create H3 and Json indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, null)) {
      processor.process();
    }
    assertTrue(h3File.exists());
    assertTrue(jsFile.exists());

    // Remove H3 and Json indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, getDefaultIndexLoadingConfig(),
            null)) {
      processor.process();
    }
    assertFalse(h3File.exists());
    assertFalse(jsFile.exists());
  }

  @Test
  public void testV3CleanupH3AndTextIndices()
      throws Exception {
    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);

    // V3 use single file for all column indices.
    File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3);
    File singleFileIndex = new File(segmentDirectoryPath, "columns.psf");

    // There are a few indices initially. Remove them to prepare an initial state.
    // Also use the schema with columns for H3 and Json index to add those columns.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, getDefaultIndexLoadingConfig(),
            _newColumnsSchemaWithH3Json)) {
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertNotNull(segmentMetadata.getColumnMetadataFor("newH3Col"));
    assertNotNull(segmentMetadata.getColumnMetadataFor("newJsonCol"));
    long initFileSize = singleFileIndex.length();

    IndexLoadingConfig indexLoadingConfig = getDefaultIndexLoadingConfig();
    indexLoadingConfig.setH3IndexConfigs(
        ImmutableMap.of("newH3Col", new H3IndexConfig(ImmutableMap.of("resolutions", "5"))));
    indexLoadingConfig.setJsonIndexColumns(new HashSet<>(Collections.singletonList("newJsonCol")));

    // Create H3 and Json indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, null)) {
      processor.process();
    }

    long addedLength = 0;
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      addedLength += reader.getIndexFor("newH3Col", StandardIndexes.h3()).size() + 8;
      addedLength += reader.getIndexFor("newJsonCol", StandardIndexes.json()).size() + 8;
    }
    assertEquals(singleFileIndex.length(), initFileSize + addedLength);

    // Remove H3 and Json indices, and size gets back to initial.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, getDefaultIndexLoadingConfig(),
            null)) {
      processor.process();
    }
    assertEquals(singleFileIndex.length(), initFileSize);
  }

  @Test
  public void testV1IfNeedProcess()
      throws Exception {
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v1);

    testIfNeedProcess();
  }

  @Test
  public void testV3IfNeedProcess()
      throws Exception {
    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);

    testIfNeedProcess();
  }

  @Test
  public void testV3IfNeedProcessWithForwardIndexDisabledColumn()
      throws Exception {
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.singletonList(EXISTING_FORWARD_INDEX_DISABLED_COL_SV));
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);

    testIfNeedProcess();
  }

  private void testIfNeedProcess()
      throws Exception {
    // There are a few indices initially. Require to remove them with an empty IndexLoadingConfig.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, getDefaultIndexLoadingConfig(),
            null)) {
      assertTrue(processor.needProcess());
      processor.process();
      assertFalse(processor.needProcess());
    }

    // Require to add some default columns with new schema.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, getDefaultIndexLoadingConfig(),
            _newColumnsSchemaWithH3Json)) {
      assertTrue(processor.needProcess());
      processor.process();
      assertFalse(processor.needProcess());
    }

    // No preprocessing needed if required to add certain index on non-existing, sorted or non-dictionary column.
    for (Map.Entry<String, Consumer<IndexLoadingConfig>> entry : createConfigPrepFunctionNeedNoops().entrySet()) {
      String testCase = entry.getKey();
      IndexLoadingConfig config = getDefaultIndexLoadingConfig();
      entry.getValue().accept(config);
      try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
          .load(_indexDir.toURI(),
              new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
          SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, config,
              _newColumnsSchemaWithH3Json)) {
        assertFalse(processor.needProcess(), testCase);
      }
    }

    // Require to add different types of indices. Add one new index a time
    // to test the index handlers separately.
    IndexLoadingConfig config = getDefaultIndexLoadingConfig();
    for (Map.Entry<String, Consumer<IndexLoadingConfig>> entry : createConfigPrepFunctions().entrySet()) {
      String testCase = entry.getKey();
      entry.getValue().accept(config);
      try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
          .load(_indexDir.toURI(),
              new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
          SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, config,
              _newColumnsSchemaWithH3Json)) {
        assertTrue(processor.needProcess(), testCase);
        processor.process();
        assertFalse(processor.needProcess(), testCase);
      }
    }

    // Require to add startree index.
    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setEnableDynamicStarTreeCreation(true);
    indexingConfig.setEnableDefaultStarTree(true);
    _tableConfig.setIndexingConfig(indexingConfig);
    IndexLoadingConfig configWithStarTreeIndex = new IndexLoadingConfig(null, _tableConfig);
    // Set RAW columns. Otherwise, they will end up being converted to dict columns (default) during segment reload.
    configWithStarTreeIndex.addNoDictionaryColumns(EXISTING_STRING_COL_RAW, EXISTING_INT_COL_RAW_MV,
        EXISTING_INT_COL_RAW);
    createConfigPrepFunctions().forEach((k, v) -> v.accept(configWithStarTreeIndex));
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(_indexDir.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, configWithStarTreeIndex,
            _newColumnsSchemaWithH3Json)) {
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
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, configWithStarTreeIndex,
            _newColumnsSchemaWithH3Json)) {
      assertTrue(processor.needProcess());
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    segmentMetadata.getColumnMetadataMap().forEach((k, v) -> {
      assertNotNull(v.getMinValue(), "checking column: " + k);
      assertNotNull(v.getMaxValue(), "checking column: " + k);
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

    FileUtils.deleteQuietly(INDEX_DIR);

    // build good segment, no needPreprocess
    File segment = buildTestSegment(tableConfig, schema, "validSegment", stringValuesValid, longValues);
    SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(segment.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
    IndexLoadingConfig indexLoadingConfig = getDefaultIndexLoadingConfig();
    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.ALL);
    SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, schema);
    assertFalse(processor.needProcess());

    // build bad segment, still no needPreprocess, since minMaxInvalid flag should be set
    FileUtils.deleteQuietly(INDEX_DIR);
    segment = buildTestSegment(tableConfig, schema, "invalidSegment", stringValuesInvalid, longValues);
    segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader().load(segment.toURI(),
        new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
    indexLoadingConfig = getDefaultIndexLoadingConfig();
    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.NONE);
    processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, schema);
    assertFalse(processor.needProcess());

    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.ALL);
    processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, schema);
    assertFalse(processor.needProcess());

    // modify metadata, to remove min/max, now needPreprocess
    removeMinMaxValuesFromMetadataFile(segment);
    segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader().load(segment.toURI(),
        new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());
    processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, schema);
    assertTrue(processor.needProcess());

    FileUtils.deleteQuietly(INDEX_DIR);
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
    FileUtils.deleteQuietly(INDEX_DIR);

    // Build good segment, no need for preprocess
    File segment = buildTestSegment(tableConfig, schema, "validSegment", stringValues, longValues);
    SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(segment.toURI(),
            new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_configuration).build());

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    try (SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, schema)) {
      assertFalse(processor.needProcess());
    }

    // Update table config to convert dict to noDict for longCol
    indexLoadingConfig.setNoDictionaryColumns(Set.of("longCol"));
    try (SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, schema)) {
      assertTrue(processor.needProcess());
      processor.process();
    }

    // Update table config to convert noDict to dict for longCol
    indexLoadingConfig.setNoDictionaryColumns(Set.of());
    try (SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, schema)) {
      assertTrue(processor.needProcess());
      processor.process();
    }

    // Update table config to convert dict to noDict for longCol and add the Startree index config
    StarTreeIndexConfig starTreeIndexConfig =
        new StarTreeIndexConfig(List.of("stringCol"), null, List.of("SUM__longCol"), null, 1000);
    tableConfig.getIndexingConfig().setStarTreeIndexConfigs(List.of(starTreeIndexConfig));
    tableConfig.getIndexingConfig().setEnableDynamicStarTreeCreation(true);
    tableConfig.getIndexingConfig().setNoDictionaryColumns(List.of("longCol"));
    indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    try (SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, schema)) {
      assertTrue(processor.needProcess());
      processor.process();
    }

    // Remove Startree index but keep the no dictionary for longCol
    tableConfig.getIndexingConfig().setStarTreeIndexConfigs(List.of());
    tableConfig.getIndexingConfig().setEnableDynamicStarTreeCreation(true);
    indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    try (SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, schema)) {
      assertTrue(processor.needProcess());
      processor.process();
    }

    // Update table config to convert noDict to dict for longCol and also add the Startree index
    tableConfig.getIndexingConfig().setStarTreeIndexConfigs(List.of(starTreeIndexConfig));
    tableConfig.getIndexingConfig().setEnableDynamicStarTreeCreation(true);
    tableConfig.getIndexingConfig().setNoDictionaryColumns(List.of());
    indexLoadingConfig = new IndexLoadingConfig(null, tableConfig);
    try (SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, schema)) {
      assertTrue(processor.needProcess());
      processor.process();
    }
  }

  private File buildTestSegment(final TableConfig tableConfig, final Schema schema, String segmentName,
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
    SegmentMetadataUtils.savePropertiesConfiguration(configuration, indexDir);
  }

  private static Map<String, Consumer<IndexLoadingConfig>> createConfigPrepFunctions() {
    Map<String, Consumer<IndexLoadingConfig>> testCases = new HashMap<>();
    testCases.put("addInvertedIndex", (IndexLoadingConfig config) -> config.setInvertedIndexColumns(
        new HashSet<>(Collections.singletonList("column3"))));
    testCases.put("addRangeIndex", (IndexLoadingConfig config) -> config.setRangeIndexColumns(
        new HashSet<>(Collections.singletonList("column3"))));
    testCases.put("addTextIndex",
        (IndexLoadingConfig config) -> config.setTextIndexColumns(new HashSet<>(Collections.singletonList("column3"))));
    testCases.put("addFSTIndex",
        (IndexLoadingConfig config) -> config.setFSTIndexColumns(new HashSet<>(Collections.singletonList("column3"))));
    testCases.put("addBloomFilter", (IndexLoadingConfig config) -> config.setBloomFilterConfigs(
        ImmutableMap.of("column3", new BloomFilterConfig(0.1, 1024, true))));
    testCases.put("addH3Index", (IndexLoadingConfig config) -> config.setH3IndexConfigs(
        ImmutableMap.of("newH3Col", new H3IndexConfig(ImmutableMap.of("resolutions", "5")))));
    testCases.put("addJsonIndex", (IndexLoadingConfig config) -> config.setJsonIndexColumns(
        new HashSet<>(Collections.singletonList("newJsonCol"))));
    return testCases;
  }

  private static Map<String, Consumer<IndexLoadingConfig>> createConfigPrepFunctionNeedNoops() {
    Map<String, Consumer<IndexLoadingConfig>> testCases = new HashMap<>();
    // daysSinceEpoch is a sorted column, thus inverted index and range index skip it.
    testCases.put("addInvertedIndexOnSortedColumn", (IndexLoadingConfig config) -> config.setInvertedIndexColumns(
        new HashSet<>(Collections.singletonList("daysSinceEpoch"))));
    testCases.put("addRangeIndexOnSortedColumn", (IndexLoadingConfig config) -> config.setRangeIndexColumns(
        new HashSet<>(Collections.singletonList("daysSinceEpoch"))));
    // column4 is unsorted non-dictionary encoded column, so inverted index and bloom filter skip it.
    // In fact, the validation logic when updating index configs already blocks this to happen.
    testCases.put("addInvertedIndexOnNonDictColumn", (IndexLoadingConfig config) -> config.setInvertedIndexColumns(
        new HashSet<>(Collections.singletonList("column4"))));
    // No index is added on non-existing columns.
    // The validation logic when updating index configs already blocks this to happen.
    testCases.put("addInvertedIndexOnAbsentColumn", (IndexLoadingConfig config) -> config.setInvertedIndexColumns(
        new HashSet<>(Collections.singletonList("newColumnX"))));
    testCases.put("addRangeIndexOnAbsentColumn", (IndexLoadingConfig config) -> config.setRangeIndexColumns(
        new HashSet<>(Collections.singletonList("newColumnX"))));
    testCases.put("addTextIndexOnAbsentColumn", (IndexLoadingConfig config) -> config.setTextIndexColumns(
        new HashSet<>(Collections.singletonList("newColumnX"))));
    testCases.put("addFSTIndexOnAbsentColumn", (IndexLoadingConfig config) -> config.setFSTIndexColumns(
        new HashSet<>(Collections.singletonList("newColumnX"))));
    testCases.put("addBloomFilterOnAbsentColumn", (IndexLoadingConfig config) -> config.setBloomFilterConfigs(
        ImmutableMap.of("newColumnX", new BloomFilterConfig(0.1, 1024, true))));
    testCases.put("addH3IndexOnAbsentColumn", (IndexLoadingConfig config) -> config.setH3IndexConfigs(
        ImmutableMap.of("newColumnX", new H3IndexConfig(ImmutableMap.of("resolutions", "5")))));
    testCases.put("addJsonIndexOnAbsentColumn", (IndexLoadingConfig config) -> config.setJsonIndexColumns(
        new HashSet<>(Collections.singletonList("newColumnX"))));
    return testCases;
  }

  /**
   * Test to check the behavior of the forward index disabled feature when enabled on a new SV column
   */
  @Test
  public void testForwardIndexDisabledOnNewColumnsSV()
      throws Exception {
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV));
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    // Create a segment in V3, add a new column with no forward index enabled
    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Forward index is always going to be present for default SV columns with forward index disabled. This is because
    // such default columns are going to be sorted and the forwardIndexDisabled flag is a no-op for sorted columns
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, true, 4, true, 0, null, true, DataType.STRING, 100000);

    // Create a segment in V1, add a column with no forward index enabled
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Forward index is always going to be present for default SV columns with forward index disabled. This is because
    // such default columns are going to be sorted and the forwardIndexDisabled flag is a no-op for sorted columns
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, true, 4, true, 0, null, true, DataType.STRING, 100000);

    // Add the column to the no dictionary column list
    Set<String> existingNoDictionaryColumns = _indexLoadingConfig.getNoDictionaryColumns();
    _indexLoadingConfig.setNoDictionaryColumns(_indexLoadingConfig.getForwardIndexDisabledColumns());

    // Create a segment in V3, add a new column with no forward index enabled
    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Forward index is always going to be present for default SV columns with forward index disabled. This is because
    // such default columns are going to be sorted and the forwardIndexDisabled flag is a no-op for sorted columns
    // Even raw columns are created with dictionary and forward index
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, true, 4, true, 0, null, false, DataType.STRING, 100000);

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Forward index is always going to be present for default SV columns with forward index disabled. This is because
    // such default columns are going to be sorted and the forwardIndexDisabled flag is a no-op for sorted columns
    // Even raw columns are created with dictionary and forward index
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, true, 4, true, 0, null, false, DataType.STRING, 100000);

    // Reset the no dictionary columns
    _indexLoadingConfig.setNoDictionaryColumns(existingNoDictionaryColumns);

    // Add the column to the sorted list
    List<String> existingSortedColumns = _indexLoadingConfig.getSortedColumns();
    _indexLoadingConfig.setSortedColumn(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);

    // Create a segment in V3, add a new column with no forward index enabled
    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Column should be sorted and should be created successfully since for SV columns the forwardIndexDisabled flag
    // is a no-op
    createAndValidateIndex(StandardIndexes.forward(),
        NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, 1, 1, _newColumnsSchemaWithForwardIndexDisabled, true, true, true,
        4, true, 0, null, false, DataType.STRING, 100000);

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Column should be sorted and should be created successfully since for SV columns the forwardIndexDisabled flag
    // is a no-op
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, true, 4, true, 0, null, false, DataType.STRING, 100000);

    // Reset the sorted column list
    _indexLoadingConfig.setSortedColumn(existingSortedColumns.isEmpty() ? null : existingSortedColumns.get(0));

    // Remove the column from the inverted index column list and validate that it fails
    _indexLoadingConfig.setInvertedIndexColumns(new HashSet<>());

    // Create a segment in V3, add a new column with no forward index enabled
    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Disabling inverted index should be fine for disabling the forward index
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, true, 4, true, 0, null, false, DataType.STRING, 100000);

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Disabling inverted index should be fine for disabling the forward index
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_SV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, true, 4, true, 0, null, false, DataType.STRING,
        100000);

    _indexLoadingConfig.setForwardIndexDisabledColumns(new HashSet<>());
  }

  /**
   * Test to check the behavior of the forward index disabled feature when enabled on a new MV column
   */
  @Test
  public void testForwardIndexDisabledOnNewColumnsMV()
      throws Exception {
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV));
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    // Create a segment in V3, add a new column with no forward index enabled
    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Validate that the forward index doesn't exist and that inverted index does exist
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, false, 4, false, 1, null, true, DataType.STRING, 100000);
    createAndValidateIndex(StandardIndexes.inverted(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, false, 4, false, 1, null, true, DataType.STRING, 100000);

    // Create a segment in V1, add a column with no forward index enabled
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // Validate that the forward index doesn't exist and that inverted index does exist
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, false, 4, false, 1, null, true, DataType.STRING, 100000);
    createAndValidateIndex(StandardIndexes.inverted(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, false, 4, false, 1, null, true, DataType.STRING, 100000);

    // Add the column to the no dictionary column list
    Set<String> existingNoDictionaryColumns = new HashSet<>(_indexLoadingConfig.getNoDictionaryColumns());
    _indexLoadingConfig.addNoDictionaryColumns(_indexLoadingConfig.getForwardIndexDisabledColumns());
    // Disable the inverted index and validate that disabling the forward index goes through
    _indexLoadingConfig.removeInvertedIndexColumns(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);

    // Create a segment in V3, add a new column with no forward index enabled
    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, false, false, 0, false, 1, null, true, DataType.STRING,
        100000);
    validateIndexDoesNotExist(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.inverted());
    validateIndexDoesNotExist(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.dictionary());

    // Re-enable the inverted index
    _indexLoadingConfig.addInvertedIndexColumns(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    // For V1 segments reload doesn't support modifying the forward index yet. Since for MV columns we create a
    // dictionary and inverted index in the default column handler, the forward index should indeed be disabled
    // but we should still have the dictionary and inverted index.
    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, false, 4, false, 1, null, true, DataType.STRING,
        100000);
    validateIndexExists(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.inverted());
    validateIndexExists(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.dictionary());

    // Reset the no dictionary columns
    _indexLoadingConfig.setNoDictionaryColumns(existingNoDictionaryColumns);
    _indexLoadingConfig.removeNoDictionaryColumns(_indexLoadingConfig.getForwardIndexDisabledColumns());

    // Remove the column from the inverted index column list and validate that it fails
    _indexLoadingConfig.setInvertedIndexColumns(new HashSet<>());

    // Create a segment in V3, add a new column with no forward index enabled
    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, false, 4, false, 1, null, true, DataType.STRING,
        100000);
    validateIndexDoesNotExist(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.inverted());
    validateIndexExists(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.dictionary());

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV);
    // should be null since column does not exist in the schema
    assertNull(columnMetadata);

    createAndValidateIndex(StandardIndexes.forward(), NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, 1, 1,
        _newColumnsSchemaWithForwardIndexDisabled, true, true, false, 4, false, 1, null, true, DataType.STRING,
        100000);
    validateIndexDoesNotExist(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.inverted());
    validateIndexExists(NEWLY_ADDED_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.dictionary());

    _indexLoadingConfig.setForwardIndexDisabledColumns(new HashSet<>());
  }

  /**
   * Test to check the behavior of the forward index disabled feature
   * when enabled on an existing dictionary encoded column
   */
  @Test
  public void testForwardIndexDisabledOnExistingColumnDictEncoded()
      throws Exception {
    _indexLoadingConfig.addForwardIndexDisabledColumns(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    assertNotNull(columnMetadata);

    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, true, DataType.STRING,
        100000);

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());

    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, true, DataType.STRING,
        100000);

    // Add the column to the noDictionaryColumns list
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_STRING_COL_DICT);
    // Also remove the column from the inverted index list and try again
    _indexLoadingConfig.removeInvertedIndexColumns(EXISTING_STRING_COL_DICT);

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    assertNotNull(columnMetadata);

    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, true, 0, null, true, DataType.STRING,
        100000);
    validateIndexDoesNotExist(EXISTING_STRING_COL_DICT, StandardIndexes.inverted());
    validateIndexDoesNotExist(EXISTING_STRING_COL_DICT, StandardIndexes.dictionary());

    // Reset the noDictionaryColumns list
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_STRING_COL_DICT);

    // Remove te column from the inverted index list
    _indexLoadingConfig.removeInvertedIndexColumns(EXISTING_STRING_COL_DICT);

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    assertNotNull(columnMetadata);

    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, true, DataType.STRING,
        100000);
    validateIndexDoesNotExist(EXISTING_STRING_COL_DICT, StandardIndexes.inverted());
    validateIndexExists(EXISTING_STRING_COL_DICT, StandardIndexes.dictionary());

    // Reset the inverted index list
    _indexLoadingConfig.addInvertedIndexColumns(EXISTING_STRING_COL_DICT);

    // Test updating two columns to be forward index disabled, one being a MV column
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_STRING_COL_DICT, COLUMN7_NAME));
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    assertNotNull(columnMetadata);
    ColumnMetadata columnMetadata2 = segmentMetadata.getColumnMetadataFor(COLUMN7_NAME);
    assertNotNull(columnMetadata2);

    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, true, DataType.STRING,
        100000);
    validateIndexExists(EXISTING_STRING_COL_DICT, StandardIndexes.inverted());
    validateIndexExists(EXISTING_STRING_COL_DICT, StandardIndexes.dictionary());
    createAndValidateIndex(StandardIndexes.forward(), COLUMN7_NAME, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, true, DataType.INT,
        134090);
    validateIndexExists(COLUMN7_NAME, StandardIndexes.inverted());
    validateIndexExists(COLUMN7_NAME, StandardIndexes.dictionary());

    // Add another index on the column which has forward index disabled
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_STRING_COL_DICT, COLUMN1_NAME));
    _indexLoadingConfig.addTextIndexColumns(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.addFSTIndexColumns(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.addRangeIndexColumn(COLUMN1_NAME);
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    assertNotNull(columnMetadata);
    columnMetadata2 = segmentMetadata.getColumnMetadataFor(COLUMN1_NAME);
    assertNotNull(columnMetadata2);

    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, true, DataType.STRING,
        100000);
    createAndValidateIndex(StandardIndexes.text(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, true, DataType.STRING,
        100000);
    createAndValidateIndex(StandardIndexes.fst(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, true, DataType.STRING,
        100000);
    validateIndexExists(EXISTING_STRING_COL_DICT, StandardIndexes.inverted());
    validateIndexExists(EXISTING_STRING_COL_DICT, StandardIndexes.dictionary());
    createAndValidateIndex(StandardIndexes.forward(), COLUMN1_NAME, 51594, 16,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);
    createAndValidateIndex(StandardIndexes.range(), COLUMN1_NAME, 51594, 16,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);
    validateIndexExists(COLUMN1_NAME, StandardIndexes.inverted());
    validateIndexExists(COLUMN1_NAME, StandardIndexes.dictionary());

    // Reset indexLoadingConfig to remove the EXISTING_STRING_COL_DICT column from additional index lists
    _indexLoadingConfig.removeTextIndexColumns(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.removeFSTIndexColumns(EXISTING_STRING_COL_DICT);

    // Remove COLUMN1_NAME from the dictionary list and inverted index column list while leaving it on the range list
    // Test reload fails since dictionary is being disabled for a forward index disabled column and range index has
    // to be changed.
    _indexLoadingConfig.addNoDictionaryColumns(COLUMN1_NAME);
    _indexLoadingConfig.removeInvertedIndexColumns(COLUMN1_NAME);

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(COLUMN1_NAME);
    assertNotNull(columnMetadata);

    try {
      createAndValidateIndex(StandardIndexes.forward(), COLUMN1_NAME, 51594, 16,
          _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, true, 0, null, true, DataType.INT,
          100000);
      Assert.fail("Should fail since we are disabling dictionary for forward index disabled column with range index");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Must disable range (enabled) index to disable the dictionary and forward "
          + "index for column: column1 or refresh / back-fill the forward index");
    }

    // Reset indexLoadingConfig to remove the column from additional index lists
    // TODO: This is wrong! If this test fail, the state will be incorrect
    //  and it may produce false errors in other tests!
    _indexLoadingConfig.removeRangeIndexColumns(COLUMN1_NAME);
    _indexLoadingConfig.removeNoDictionaryColumns(COLUMN1_NAME);
    _indexLoadingConfig.addInvertedIndexColumns(COLUMN1_NAME);
  }

  /**
   * Test to check the behavior of the forward index disabled feature
   * when enabled on an existing raw column
   */
  @Test
  public void testForwardIndexDisabledOnExistingColumnRaw()
      throws Exception {
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_STRING_COL_RAW));
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    assertNotNull(columnMetadata);

    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_RAW, 5, 3,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 4, true, 0, null, true, DataType.STRING,
        100000);

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());

    // No dictionary is created nor is the forward index disabled since this is a v1 type segment
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_RAW, 5, 3,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, true, 0, null, true, DataType.STRING,
        100000);

    // Add the column back to the noDictionaryColumns list
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_STRING_COL_RAW);

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    assertNotNull(columnMetadata);

    try {
      createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_RAW, 5, 3,
          _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, true, 0, null, true, DataType.STRING,
          100000);
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Must disable inverted index (enabled) and FST (disabled) index to disable "
          + "the dictionary and forward index for column: column4");
    }

    // Also remove the column from the inverted index list and validate that now it works
    _indexLoadingConfig.removeInvertedIndexColumns(EXISTING_STRING_COL_RAW);

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    assertNotNull(columnMetadata);

    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_RAW, 5, 3,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, true, 0, null, true, DataType.STRING,
        100000);
    validateIndexDoesNotExist(EXISTING_STRING_COL_RAW, StandardIndexes.inverted());
    validateIndexDoesNotExist(EXISTING_STRING_COL_RAW, StandardIndexes.dictionary());

    // Reset just the noDictionaryColumns list and keep the column off the inverted index list
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_STRING_COL_RAW);

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    assertNotNull(columnMetadata);

    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_RAW, 5, 3,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 4, true, 0, null, true, DataType.STRING,
        100000);
    validateIndexDoesNotExist(EXISTING_STRING_COL_RAW, StandardIndexes.inverted());
    validateIndexExists(EXISTING_STRING_COL_RAW, StandardIndexes.dictionary());

    // Reset the inverted index list
    _indexLoadingConfig.addInvertedIndexColumns(EXISTING_STRING_COL_RAW);

    // Test updating two columns to be forward index disabled, one being a MV column
    _indexLoadingConfig.setForwardIndexDisabledColumns(
        Sets.newHashSet(EXISTING_STRING_COL_RAW, EXISTING_INT_COL_RAW_MV));
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_STRING_COL_RAW, EXISTING_INT_COL_RAW_MV);
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    assertNotNull(columnMetadata);
    ColumnMetadata columnMetadata2 = segmentMetadata.getColumnMetadataFor(EXISTING_INT_COL_RAW_MV);
    assertNotNull(columnMetadata2);

    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_RAW, 5, 3,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 4, true, 0, null, true, DataType.STRING,
        100000);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_INT_COL_RAW_MV, 18499, 15,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 13, null, true, DataType.INT,
        106688);

    // Add another index on the column which has forward index disabled
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_STRING_COL_RAW, EXISTING_INT_COL_RAW));
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_INT_COL_RAW);
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.addTextIndexColumns(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.addFSTIndexColumns(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.addRangeIndexColumn(EXISTING_INT_COL_RAW);
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV3Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    assertNotNull(columnMetadata);
    columnMetadata2 = segmentMetadata.getColumnMetadataFor(EXISTING_INT_COL_RAW);
    assertNotNull(columnMetadata2);

    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_RAW, 5, 3,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 4, true, 0, null, true, DataType.STRING,
        100000);
    createAndValidateIndex(StandardIndexes.text(), EXISTING_STRING_COL_RAW, 5, 3,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 4, true, 0, null, true, DataType.STRING,
        100000);
    createAndValidateIndex(StandardIndexes.fst(), EXISTING_STRING_COL_RAW, 5, 3,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 4, true, 0, null, true, DataType.STRING,
        100000);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_INT_COL_RAW, 42242, 16,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);
    createAndValidateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW, 42242, 16,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);

    // Reset indexLoadingConfig to remove the column from additional index lists
    _indexLoadingConfig.removeTextIndexColumns(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.removeFSTIndexColumns(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.removeRangeIndexColumns(EXISTING_INT_COL_RAW);

    // Add back columns to noDictionaryColumns list
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_INT_COL_RAW_MV);
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_INT_COL_RAW);

    // EXISTING_INT_COL_RAW: Disable forward index and enable dictionary and inverted index on a column that already
    // has range index.
    List<String> rangeIndexCols = new ArrayList<>();
    rangeIndexCols.add(EXISTING_INT_COL_RAW);
    constructV1Segment(Collections.emptyList(), Collections.emptyList(), rangeIndexCols, Collections.emptyList());
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW, 42242, 16, _schema, false, false, false, 0, true,
        0, ChunkCompressionType.LZ4, false, DataType.INT, 100000);

    // At this point, the segment has range index. Now the reload path should disable the forward index create a
    // dictionary and inverted index and rewrite the range index.
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_INT_COL_RAW);
    _indexLoadingConfig.addRangeIndexColumn(EXISTING_INT_COL_RAW);
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_INT_COL_RAW));

    createAndValidateIndex(StandardIndexes.forward(), EXISTING_INT_COL_RAW, 42242, 16,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);
    validateIndex(StandardIndexes.range(), EXISTING_INT_COL_RAW, 42242, 16,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);

    // Add it back so that this column is not rewritten for the other tests below.
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_INT_COL_RAW);
  }

  /**
   * Test to check the behavior of enabling a dictionary based forward
   * index on a column which has forward index disabled
   */
  @Test
  public void testForwardIndexEnabledWithDictOnExistingForwardIndexDisabledColumn()
      throws Exception {
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_FORWARD_INDEX_DISABLED_COL_SV));
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.singletonList(EXISTING_FORWARD_INDEX_DISABLED_COL_SV));
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);
    assertNotNull(columnMetadata);

    // Forward index should be disabled for column10
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);

    // At this point, the segment has forward index disabled for column10. Enable the forward index and invoke reload
    _indexLoadingConfig.setForwardIndexDisabledColumns(new HashSet<>());

    // Forward index should be enabled for column10 along with inverted index and dictionary
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, false, DataType.INT,
        100000);
    validateIndex(StandardIndexes.dictionary(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, false, DataType.INT,
        100000);
    validateIndex(StandardIndexes.inverted(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, false, DataType.INT,
        100000);

    // Enable the forward index but disable the inverted index
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_FORWARD_INDEX_DISABLED_COL_SV));
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.singletonList(EXISTING_FORWARD_INDEX_DISABLED_COL_SV));
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);
    assertNotNull(columnMetadata);

    // Forward index should be disabled for column10
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);

    // At this point, the segment has forward index disabled for column10. Enable the forward index and invoke reload.
    // Disable the inverted index and ensure it gets removed.
    _indexLoadingConfig.setForwardIndexDisabledColumns(new HashSet<>());
    _indexLoadingConfig.removeInvertedIndexColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);

    // Forward index should be enabled for column10 dictionary. Inverted index should be disabled.
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, false, DataType.INT,
        100000);
    validateIndex(StandardIndexes.dictionary(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, false, DataType.INT,
        100000);
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, StandardIndexes.inverted());

    // Enable the forward index for two columns at once, one being a MV column
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV));
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Arrays.asList(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, EXISTING_FORWARD_INDEX_DISABLED_COL_MV));
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);
    assertNotNull(columnMetadata);
    ColumnMetadata columnMetadata2 = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_MV);
    assertNotNull(columnMetadata2);

    // Forward index should be disabled for column10 and column7
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, true, DataType.INT,
        134090);

    // At this point, the segment has forward index disabled for column10 and column7. Enable the forward index and
    // invoke reload.
    _indexLoadingConfig.setForwardIndexDisabledColumns(new HashSet<>());

    // Forward index should be enabled for column10 and column7 along with inverted index and dictionary
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, false, DataType.INT,
        100000);
    validateIndex(StandardIndexes.dictionary(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, false, DataType.INT,
        100000);
    validateIndex(StandardIndexes.inverted(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, false, DataType.INT,
        100000);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, false, DataType.INT,
        134090);
    validateIndex(StandardIndexes.dictionary(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, false, DataType.INT,
        134090);
    validateIndex(StandardIndexes.inverted(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, false, DataType.INT,
        134090);

    //  Enable the forward index for multiple columns at once and enable other indexes
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV, EXISTING_STRING_COL_DICT));
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Arrays.asList(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, EXISTING_FORWARD_INDEX_DISABLED_COL_MV,
            EXISTING_STRING_COL_DICT));
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);
    assertNotNull(columnMetadata);
    columnMetadata2 = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_MV);
    assertNotNull(columnMetadata2);
    ColumnMetadata columnMetadata3 = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    assertNotNull(columnMetadata3);

    // Forward index should be disabled for column10 and column7
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, true, DataType.INT,
        134090);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, true, DataType.STRING,
        100000);

    // At this point, the segment has forward index disabled for column10, column5 and column7. Enable the forward
    // index and invoke reload.
    _indexLoadingConfig.setForwardIndexDisabledColumns(new HashSet<>());
    _indexLoadingConfig.addRangeIndexColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV);
    _indexLoadingConfig.addTextIndexColumns(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.addFSTIndexColumns(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.removeInvertedIndexColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_MV);

    // Forward index should be enabled for column10 and column7 along with inverted index and dictionary
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, false, DataType.INT,
        100000);
    validateIndex(StandardIndexes.dictionary(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, false, DataType.INT,
        100000);
    validateIndex(StandardIndexes.inverted(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, false, DataType.INT,
        100000);
    validateIndex(StandardIndexes.range(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, false, DataType.INT,
        100000);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, false, DataType.INT,
        134090);
    validateIndex(StandardIndexes.dictionary(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, false, DataType.INT,
        134090);
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.inverted());
    validateIndex(StandardIndexes.range(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, false, DataType.INT,
        134090);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, false, DataType.STRING,
        100000);
    validateIndex(StandardIndexes.dictionary(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, false, DataType.STRING,
        100000);
    validateIndex(StandardIndexes.inverted(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, false, DataType.STRING,
        100000);
    validateIndex(StandardIndexes.text(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, false, DataType.STRING,
        100000);
    validateIndex(StandardIndexes.fst(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, false, DataType.STRING,
        100000);

    // Reset the indexLoadingConfig
    _indexLoadingConfig.removeRangeIndexColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV);
    _indexLoadingConfig.removeTextIndexColumns(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.removeFSTIndexColumns(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.removeInvertedIndexColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_STRING_COL_DICT);
  }

  /**
   * Test to check the behavior of enabling a no dictionary based forward
   * index on a column which has forward index disabled
   */
  @Test
  public void testForwardIndexEnabledWithRawOnExistingForwardIndexDisabledColumn()
      throws Exception {
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_FORWARD_INDEX_DISABLED_COL_SV));
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.singletonList(EXISTING_FORWARD_INDEX_DISABLED_COL_SV));
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);
    assertNotNull(columnMetadata);

    // Forward index should be disabled for column10
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);

    // At this point, the segment has forward index disabled for column10. Enable the forward index, disable the
    // dictionary and inverted index
    _indexLoadingConfig.setForwardIndexDisabledColumns(new HashSet<>());
    _indexLoadingConfig.removeInvertedIndexColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);

    // Forward index should be enabled for column10. Dictionary and inverted index shouldn't be present.
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, true, 0, ChunkCompressionType.LZ4, false,
        DataType.INT, 100000);
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, StandardIndexes.dictionary());
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, StandardIndexes.inverted());

    // Reset indexLoadingConfig
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);

    // Enable the forward index for two columns at once, one being a MV column
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV));
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Arrays.asList(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, EXISTING_FORWARD_INDEX_DISABLED_COL_MV));
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);
    assertNotNull(columnMetadata);
    ColumnMetadata columnMetadata2 = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_MV);
    assertNotNull(columnMetadata2);

    // Forward index should be disabled for column10 and column7
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, true, DataType.INT,
        134090);

    // At this point, the segment has forward index disabled for column10 and column7. Enable the forward index and
    // invoke reload.
    _indexLoadingConfig.setForwardIndexDisabledColumns(new HashSet<>());
    _indexLoadingConfig.removeInvertedIndexColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV);
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV);

    // Forward index should be enabled for column10 and column7. No dictionary or inverted index should exist.
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, true, 0, ChunkCompressionType.LZ4, false,
        DataType.INT, 100000);
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, StandardIndexes.dictionary());
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, StandardIndexes.inverted());
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, false, 24, ChunkCompressionType.LZ4, false,
        DataType.INT, 134090);
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.dictionary());
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.inverted());

    // Reset indexLoadingConfig
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV);

    // Enable the forward index for two columns at once, one as raw another as dictionary enabled
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV));
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Arrays.asList(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, EXISTING_FORWARD_INDEX_DISABLED_COL_MV));
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);
    assertNotNull(columnMetadata);
    columnMetadata2 = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_MV);
    assertNotNull(columnMetadata2);

    // Forward index should be disabled for column10 and column7
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, true, DataType.INT,
        134090);

    // At this point, the segment has forward index disabled for column10 and column7. Enable the forward index and
    // invoke reload.
    _indexLoadingConfig.setForwardIndexDisabledColumns(new HashSet<>());
    _indexLoadingConfig.removeInvertedIndexColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);

    // Forward index should be enabled for column10 and column7. No dictionary or inverted index should exist.
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, true, 0, ChunkCompressionType.LZ4, false,
        DataType.INT, 100000);
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, StandardIndexes.dictionary());
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, StandardIndexes.inverted());
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, false, DataType.INT,
        134090);
    validateIndex(StandardIndexes.dictionary(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, false, DataType.INT,
        134090);
    validateIndex(StandardIndexes.inverted(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, false, DataType.INT,
        134090);

    // Reset indexLoadingConfig
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);
    _indexLoadingConfig.removeInvertedIndexColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_MV);

    //  Enable the forward index for multiple columns at once and enable other indexes
    _indexLoadingConfig.setForwardIndexDisabledColumns(Sets.newHashSet(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV, EXISTING_STRING_COL_DICT));
    _indexLoadingConfig.getForwardIndexDisabledColumns().forEach(c -> _indexLoadingConfig.addInvertedIndexColumns(c));

    constructV1Segment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Arrays.asList(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, EXISTING_FORWARD_INDEX_DISABLED_COL_MV,
            EXISTING_STRING_COL_DICT));
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_SV);
    assertNotNull(columnMetadata);
    columnMetadata2 = segmentMetadata.getColumnMetadataFor(EXISTING_FORWARD_INDEX_DISABLED_COL_MV);
    assertNotNull(columnMetadata2);
    ColumnMetadata columnMetadata3 = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    assertNotNull(columnMetadata3);

    // Forward index should be disabled for column10 and column7
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, true, 0, null, true, DataType.INT,
        100000);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 0, false, 24, null, true, DataType.INT,
        134090);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, true, false, 26, true, 0, null, true, DataType.STRING,
        100000);

    // At this point, the segment has forward index disabled for column10, column5 and column7. Enable the forward
    // index and invoke reload.
    _indexLoadingConfig.setForwardIndexDisabledColumns(new HashSet<>());
    _indexLoadingConfig.addRangeIndexColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV);
    _indexLoadingConfig.addTextIndexColumns(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.removeInvertedIndexColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV, EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.addNoDictionaryColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV, EXISTING_STRING_COL_DICT);

    // Forward index should be enabled for column10 and column7. Dictionary and inverted index should not exist
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, true, 0, ChunkCompressionType.LZ4, false,
        DataType.INT, 100000);
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, StandardIndexes.dictionary());
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_SV, StandardIndexes.inverted());
    validateIndex(StandardIndexes.range(), EXISTING_FORWARD_INDEX_DISABLED_COL_SV, 3960, 12,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, true, 0, ChunkCompressionType.LZ4, false,
        DataType.INT, 100000);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, false, 24, ChunkCompressionType.LZ4, false,
        DataType.INT, 134090);
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.dictionary());
    validateIndexDoesNotExist(EXISTING_FORWARD_INDEX_DISABLED_COL_MV, StandardIndexes.inverted());
    validateIndex(StandardIndexes.range(), EXISTING_FORWARD_INDEX_DISABLED_COL_MV, 359, 9,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, false, 24, ChunkCompressionType.LZ4, false,
        DataType.INT, 134090);
    createAndValidateIndex(StandardIndexes.forward(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, true, 0, ChunkCompressionType.LZ4, false,
        DataType.STRING, 100000);
    validateIndexDoesNotExist(EXISTING_STRING_COL_DICT, StandardIndexes.dictionary());
    validateIndexDoesNotExist(EXISTING_STRING_COL_DICT, StandardIndexes.inverted());
    validateIndex(StandardIndexes.text(), EXISTING_STRING_COL_DICT, 9, 4,
        _newColumnsSchemaWithForwardIndexDisabled, false, false, false, 0, true, 0, ChunkCompressionType.LZ4, false,
        DataType.STRING, 100000);

    // Reset the indexLoadingConfig
    _indexLoadingConfig.removeRangeIndexColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV);
    _indexLoadingConfig.removeTextIndexColumns(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.removeNoDictionaryColumns(EXISTING_FORWARD_INDEX_DISABLED_COL_SV,
        EXISTING_FORWARD_INDEX_DISABLED_COL_MV, EXISTING_STRING_COL_DICT);
  }
}
