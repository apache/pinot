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
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.loader.LocalSegmentDirectoryLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
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

  // For create fst index tests
  private static final String NEWLY_ADDED_FST_COL_DICT = "newFSTColDict";

  // For update default value tests.
  private static final String NEW_COLUMNS_SCHEMA1 = "data/newColumnsSchema1.json";
  private static final String NEW_COLUMNS_SCHEMA2 = "data/newColumnsSchema2.json";
  private static final String NEW_COLUMNS_SCHEMA3 = "data/newColumnsSchema3.json";
  private static final String NEW_COLUMNS_SCHEMA_WITH_FST = "data/newColumnsSchemaWithFST.json";
  private static final String NEW_COLUMNS_SCHEMA_WITH_TEXT = "data/newColumnsSchemaWithText.json";
  private static final String NEW_COLUMNS_SCHEMA_WITH_H3_JSON = "data/newColumnsSchemaWithH3Json.json";
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

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    Map<String, Object> props = new HashMap<>();
    props.put(LocalSegmentDirectoryLoader.READ_MODE_KEY, ReadMode.mmap.toString());
    _configuration = new PinotConfiguration(props);

    // We specify two columns without inverted index ('column1', 'column13'), one non-existing column ('noSuchColumn')
    // and one column with existed inverted index ('column7').
    _indexLoadingConfig = new IndexLoadingConfig();
    _indexLoadingConfig.setInvertedIndexColumns(
        new HashSet<>(Arrays.asList(COLUMN1_NAME, COLUMN7_NAME, COLUMN13_NAME, NO_SUCH_COLUMN_NAME)));
    _tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("daysSinceEpoch").build();
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
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void constructV1Segment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Create inverted index for 'column7' when constructing the segment.
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithSchema(_avroFile, INDEX_DIR, "testTable", _tableConfig, _schema);
    segmentGeneratorConfig.setInvertedIndexCreationColumns(Collections.singletonList(COLUMN7_NAME));
    segmentGeneratorConfig.setRawIndexCreationColumns(Collections.singletonList(EXISTING_STRING_COL_RAW));
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    segmentGeneratorConfig.setSkipTimeValueCheck(true);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(segmentGeneratorConfig);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
  }

  private void constructV3Segment()
      throws Exception {
    constructV1Segment();
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
    _indexLoadingConfig.getNoDictionaryColumns().add(NEWLY_ADDED_STRING_COL_RAW);
    _indexLoadingConfig.getNoDictionaryColumns().add(NEWLY_ADDED_STRING_MV_COL_RAW);

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
    constructV1Segment();
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
    _indexLoadingConfig.setNativeFSTIndexColumns(fstColumns);
    _indexLoadingConfig.getNoDictionaryColumns().add(EXISTING_STRING_COL_RAW);
    constructV3Segment();
    SegmentDirectory segmentDirectory =
        SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader().load(_indexDir.toURI(), _configuration);
    SegmentPreProcessor v3Processor =
        new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, _newColumnsSchemaWithFST);
    expectThrows(UnsupportedOperationException.class, () -> v3Processor.process());

    constructV1Segment();
    segmentDirectory =
        SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader().load(_indexDir.toURI(), _configuration);
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
    _indexLoadingConfig.setNativeFSTIndexColumns(fstColumns);

    constructV3Segment();
    checkFSTIndexCreation(NEWLY_ADDED_FST_COL_DICT, 1, 1, _newColumnsSchemaWithFST, true, true, 4);

    constructV1Segment();
    checkFSTIndexCreation(NEWLY_ADDED_FST_COL_DICT, 1, 1, _newColumnsSchemaWithFST, true, true, 4);
  }

  @Test
  public void testEnableFSTIndexOnExistingColumnDictEncoded()
      throws Exception {
    Set<String> fstColumns = new HashSet<>();
    fstColumns.add(EXISTING_STRING_COL_DICT);
    _indexLoadingConfig.setFSTIndexColumns(fstColumns);
    _indexLoadingConfig.setNativeFSTIndexColumns(fstColumns);

    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    assertNotNull(columnMetadata);
    checkFSTIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _newColumnsSchemaWithFST, false, false, 26);

    constructV1Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    assertNotNull(columnMetadata);
    checkFSTIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _newColumnsSchemaWithFST, false, false, 26);
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
    constructV1Segment();
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
    constructV1Segment();
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
    constructV1Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    assertNotNull(columnMetadata);
    // SegmentPreprocessor should have created the text index using TextIndexHandler
    checkTextIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _schema, false, true, false, 26);
  }

  private void checkFSTIndexCreation(String column, int cardinality, int bits, Schema schema, boolean isAutoGenerated,
      boolean isSorted, int dictionaryElementSize)
      throws Exception {
    checkIndexCreation(ColumnIndexType.FST_INDEX, column, cardinality, bits, schema, isAutoGenerated, true, isSorted,
        dictionaryElementSize, true, 0);
  }

  private void checkTextIndexCreation(String column, int cardinality, int bits, Schema schema, boolean isAutoGenerated,
      boolean hasDictionary, boolean isSorted, int dictionaryElementSize)
      throws Exception {
    checkIndexCreation(ColumnIndexType.TEXT_INDEX, column, cardinality, bits, schema, isAutoGenerated, hasDictionary,
        isSorted, dictionaryElementSize, true, 0);
  }

  private void checkTextIndexCreation(String column, int cardinality, int bits, Schema schema, boolean isAutoGenerated,
      boolean hasDictionary, boolean isSorted, int dictionaryElementSize, boolean isSingleValue,
      int maxNumberOfMultiValues)
      throws Exception {
    checkIndexCreation(ColumnIndexType.TEXT_INDEX, column, cardinality, bits, schema, isAutoGenerated, hasDictionary,
        isSorted, dictionaryElementSize, isSingleValue, maxNumberOfMultiValues);
  }

  private void checkIndexCreation(ColumnIndexType indexType, String column, int cardinality, int bits, Schema schema,
      boolean isAutoGenerated, boolean hasDictionary, boolean isSorted, int dictionaryElementSize,
      boolean isSingleValued, int maxNumberOfMultiValues)
      throws Exception {

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, schema)) {
      processor.process();
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      assertEquals(columnMetadata.getFieldSpec(), new DimensionFieldSpec(column, DataType.STRING, isSingleValued));
      assertEquals(columnMetadata.getCardinality(), cardinality);
      assertEquals(columnMetadata.getTotalDocs(), 100000);
      assertEquals(columnMetadata.getBitsPerElement(), bits);
      assertEquals(columnMetadata.getColumnMaxLength(), dictionaryElementSize);
      assertEquals(columnMetadata.isSorted(), isSorted);
      assertEquals(columnMetadata.hasDictionary(), hasDictionary);
      assertEquals(columnMetadata.getMaxNumberOfMultiValues(), maxNumberOfMultiValues);
      assertEquals(columnMetadata.getTotalNumberOfEntries(), 100000);
      assertEquals(columnMetadata.isAutoGenerated(), isAutoGenerated);

      try (SegmentDirectory segmentDirectory1 = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
          .load(_indexDir.toURI(), _configuration); SegmentDirectory.Reader reader = segmentDirectory1.createReader()) {
        assertTrue(reader.hasIndexFor(column, indexType));
        assertTrue(reader.hasIndexFor(column, ColumnIndexType.FORWARD_INDEX));
        // if the text index is enabled on a new column with dictionary,
        // then dictionary should be created by the default column handler
        if (hasDictionary) {
          assertTrue(reader.hasIndexFor(column, ColumnIndexType.DICTIONARY));
        } else {
          assertFalse(reader.hasIndexFor(column, ColumnIndexType.DICTIONARY));
        }
      }
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
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration); SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      // 8 bytes overhead is for checking integrity of the segment.
      addedLength += reader.getIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX).size() + 8;
      addedLength += reader.getIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX).size() + 8;
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
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration); SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
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

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, null)) {
      processor.process();
    }

    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration); SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
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
    _tableConfig.setIngestionConfig(new IngestionConfig(null, null, null,
        Collections.singletonList(new TransformConfig(NEW_INT_SV_DIMENSION_COLUMN_NAME, "plus(column1, 1)")), null));
    _indexLoadingConfig.getInvertedIndexColumns().add(NEW_COLUMN_INVERTED_INDEX);
    checkUpdateDefaultColumns();

    // Try to use the third schema and update default value again.
    // For the third schema, we changed the default value for column 'newStringMVDimension' to 'notSameLength',
    // which is not the same length as before. This should be fine for segment format v1.
    // We added two new columns and also removed the NEW_INT_SV_DIMENSION_COLUMN_NAME from schema.
    // NEW_INT_SV_DIMENSION_COLUMN_NAME exists before processing but removed afterwards.
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertNotNull(segmentMetadata.getColumnMetadataFor(NEW_INT_SV_DIMENSION_COLUMN_NAME));
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
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

    _tableConfig.setIngestionConfig(new IngestionConfig(null, null, null,
        Collections.singletonList(new TransformConfig(NEW_INT_SV_DIMENSION_COLUMN_NAME, "plus(column1, 1)")), null));
    _indexLoadingConfig.getInvertedIndexColumns().add(NEW_COLUMN_INVERTED_INDEX);
    checkUpdateDefaultColumns();

    // Try to use the third schema and update default value again.
    // For the third schema, we changed the default value for column 'newStringMVDimension' to 'notSameLength', which
    // is not the same length as before. This should be fine for segment format v3 as well.
    // We added two new columns and also removed the NEW_INT_SV_DIMENSION_COLUMN_NAME from schema.
    // NEW_INT_SV_DIMENSION_COLUMN_NAME exists before processing but removed afterwards.
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertNotNull(segmentMetadata.getColumnMetadataFor(NEW_INT_SV_DIMENSION_COLUMN_NAME));
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
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
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
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
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration); SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
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
    }

    // Use the second schema and update default value again.
    // For the second schema, we changed the default value for column 'newIntMetric' to 2, and added default value
    // 'abcd' (keep the same length as 'null') to column 'newStringMVDimension'.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
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
    constructV1Segment();

    // Remove min/max value from the metadata
    PropertiesConfiguration configuration = SegmentMetadataImpl.getPropertiesConfiguration(_indexDir);
    Iterator<String> keys = configuration.getKeys();
    while (keys.hasNext()) {
      String key = keys.next();
      if (key.endsWith(V1Constants.MetadataKeys.Column.MIN_VALUE) || key.endsWith(
          V1Constants.MetadataKeys.Column.MAX_VALUE)) {
        configuration.clearProperty(key);
      }
    }
    configuration.save();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.NONE);
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
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
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
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
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
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
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
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
    constructV1Segment();

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v1);

    // Need to create two default columns with Bytes and JSON string for H3 and JSON index.
    // Other kinds of indices can all be put on column3 with String values.
    String strColumn = "column3";
    _indexLoadingConfig = new IndexLoadingConfig();
    _indexLoadingConfig.setInvertedIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    _indexLoadingConfig.setRangeIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    _indexLoadingConfig.setTextIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    _indexLoadingConfig.setFSTIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    _indexLoadingConfig.setNativeFSTIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    _indexLoadingConfig.setBloomFilterConfigs(ImmutableMap.of(strColumn, new BloomFilterConfig(0.1, 1024, true)));

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
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, null)) {
      processor.process();
    }
    assertTrue(iiFile.exists());
    assertTrue(rgFile.exists());
    assertTrue(txtFile.exists());
    assertTrue(fstFile.exists());
    assertTrue(bfFile.exists());

    // Remove all kinds of indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(), null)) {
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
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(), null)) {
      processor.process();
    }
    assertTrue(singleFileIndex.length() < initFileSize);
    initFileSize = singleFileIndex.length();

    // Need to create two default columns with Bytes and JSON string for H3 and JSON index.
    // Other kinds of indices can all be put on column3 with String values.
    String strColumn = "column3";
    _indexLoadingConfig = new IndexLoadingConfig();
    _indexLoadingConfig.setInvertedIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    _indexLoadingConfig.setRangeIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    _indexLoadingConfig.setTextIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    _indexLoadingConfig.setFSTIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    _indexLoadingConfig.setNativeFSTIndexColumns(new HashSet<>(Collections.singletonList(strColumn)));
    _indexLoadingConfig.setBloomFilterConfigs(ImmutableMap.of(strColumn, new BloomFilterConfig(0.1, 1024, true)));

    // Create all kinds of indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, null)) {
      processor.process();
    }

    long addedLength = 0;
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration); SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      addedLength += reader.getIndexFor(strColumn, ColumnIndexType.INVERTED_INDEX).size() + 8;
      addedLength += reader.getIndexFor(strColumn, ColumnIndexType.RANGE_INDEX).size() + 8;
      addedLength += reader.getIndexFor(strColumn, ColumnIndexType.FST_INDEX).size() + 8;
      addedLength += reader.getIndexFor(strColumn, ColumnIndexType.NATIVE_FST_INDEX).size() + 8;
      addedLength += reader.getIndexFor(strColumn, ColumnIndexType.BLOOM_FILTER).size() + 8;
      assertTrue(reader.hasIndexFor(strColumn, ColumnIndexType.TEXT_INDEX));
    }
    assertEquals(singleFileIndex.length(), initFileSize + addedLength);

    // Remove all kinds of indices, and size gets back initial size.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(), null)) {
      processor.process();
    }
    assertEquals(singleFileIndex.length(), initFileSize);
  }

  @Test
  public void testV1CleanupH3AndTextIndices()
      throws Exception {
    constructV1Segment();

    // Remove all indices and add the two derived columns for H3 and Json index.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(),
            _newColumnsSchemaWithH3Json)) {
      processor.process();
    }

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertNotNull(segmentMetadata.getColumnMetadataFor("newH3Col"));
    assertNotNull(segmentMetadata.getColumnMetadataFor("newJsonCol"));

    _indexLoadingConfig = new IndexLoadingConfig();
    _indexLoadingConfig.setH3IndexConfigs(
        ImmutableMap.of("newH3Col", new H3IndexConfig(ImmutableMap.of("resolutions", "5"))));
    _indexLoadingConfig.setJsonIndexColumns(new HashSet<>(Collections.singletonList("newJsonCol")));

    // V1 use separate file for each column index.
    File h3File = new File(_indexDir, "newH3Col" + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);
    File jsFile = new File(_indexDir, "newJsonCol" + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);

    assertFalse(h3File.exists());
    assertFalse(jsFile.exists());

    // Create H3 and Json indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, null)) {
      processor.process();
    }
    assertTrue(h3File.exists());
    assertTrue(jsFile.exists());

    // Remove H3 and Json indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(), null)) {
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
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(),
            _newColumnsSchemaWithH3Json)) {
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    assertNotNull(segmentMetadata.getColumnMetadataFor("newH3Col"));
    assertNotNull(segmentMetadata.getColumnMetadataFor("newJsonCol"));
    long initFileSize = singleFileIndex.length();

    _indexLoadingConfig = new IndexLoadingConfig();
    _indexLoadingConfig.setH3IndexConfigs(
        ImmutableMap.of("newH3Col", new H3IndexConfig(ImmutableMap.of("resolutions", "5"))));
    _indexLoadingConfig.setJsonIndexColumns(new HashSet<>(Collections.singletonList("newJsonCol")));

    // Create H3 and Json indices.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, null)) {
      processor.process();
    }

    long addedLength = 0;
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration); SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      addedLength += reader.getIndexFor("newH3Col", ColumnIndexType.H3_INDEX).size() + 8;
      addedLength += reader.getIndexFor("newJsonCol", ColumnIndexType.JSON_INDEX).size() + 8;
    }
    assertEquals(singleFileIndex.length(), initFileSize + addedLength);

    // Remove H3 and Json indices, and size gets back to initial.
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(), null)) {
      processor.process();
    }
    assertEquals(singleFileIndex.length(), initFileSize);
  }
}
