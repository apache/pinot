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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.loader.LocalSegmentDirectoryLoader;
import org.apache.pinot.segment.spi.index.creator.TextIndexType;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


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

  // For create fst index tests
  private static final String NEWLY_ADDED_FST_COL_DICT = "newFSTColDict";

  // For update default value tests.
  private static final String NEW_COLUMNS_SCHEMA1 = "data/newColumnsSchema1.json";
  private static final String NEW_COLUMNS_SCHEMA2 = "data/newColumnsSchema2.json";
  private static final String NEW_COLUMNS_SCHEMA3 = "data/newColumnsSchema3.json";
  private static final String NEW_COLUMNS_SCHEMA4 = "data/newColumnsSchema4.json";
  private static final String NEW_COLUMNS_SCHEMA_WITH_TEXT = "data/newColumnsWithTextSchema.json";
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
  private Schema _newColumnsSchema4;
  private Schema _newColumnsSchemaWithText;

  @BeforeClass
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
    Assert.assertNotNull(resourceUrl);
    _avroFile = new File(resourceUrl.getFile());

    // For newColumnsSchema, we add 4 different data type metric columns with one user-defined default null value, and
    // 3 different data type dimension columns with one user-defined default null value and one multi-value column.
    resourceUrl = classLoader.getResource(SCHEMA);
    Assert.assertNotNull(resourceUrl);
    _schema = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA1);
    Assert.assertNotNull(resourceUrl);
    _newColumnsSchema1 = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA2);
    Assert.assertNotNull(resourceUrl);
    _newColumnsSchema2 = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA3);
    Assert.assertNotNull(resourceUrl);
    _newColumnsSchema3 = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA_WITH_TEXT);
    Assert.assertNotNull(resourceUrl);
    _newColumnsSchemaWithText = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA4);
    Assert.assertNotNull(resourceUrl);
    _newColumnsSchema4 = Schema.fromFile(new File(resourceUrl.getFile()));
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
    _indexLoadingConfig.setTextIndexColumns(textIndexColumns);
    _indexLoadingConfig.getNoDictionaryColumns().add(NEWLY_ADDED_STRING_COL_RAW);

    // Create a segment in V3, add a new raw column with text index enabled
    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_STRING_COL_RAW);
    // should be null since column does not exist in the schema
    Assert.assertNull(columnMetadata);
    checkTextIndexCreation(NEWLY_ADDED_STRING_COL_RAW, 1, 1, _newColumnsSchemaWithText, true, true, true, 4);

    // Create a segment in V1, add a new raw column with text index enabled
    constructV1Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_STRING_COL_RAW);
    // should be null since column does not exist in the schema
    Assert.assertNull(columnMetadata);
    checkTextIndexCreation(NEWLY_ADDED_STRING_COL_RAW, 1, 1, _newColumnsSchemaWithText, true, true, true, 4);
  }

  @Test
  public void testEnableFSTIndexOnExistingColumnRaw()
      throws Exception {
    Set<String> fstColumns = new HashSet<>();
    fstColumns.add(EXISTING_STRING_COL_RAW);
    _indexLoadingConfig.setFSTIndexColumns(fstColumns);
    _indexLoadingConfig.getNoDictionaryColumns().add(EXISTING_STRING_COL_RAW);
    constructV3Segment();
    SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
    SegmentPreProcessor v3Processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, _newColumnsSchema4);
    Assert.expectThrows(UnsupportedOperationException.class, () -> v3Processor.process());

    constructV1Segment();
    segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(_indexDir.toURI(), _configuration);
    SegmentPreProcessor v1Processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, _newColumnsSchema4);
    Assert.expectThrows(UnsupportedOperationException.class, () -> v1Processor.process());
  }

  @Test
  public void testEnableFSTIndexOnNewColumnDictEncoded()
      throws Exception {
    Set<String> fstColumns = new HashSet<>();
    fstColumns.add(NEWLY_ADDED_FST_COL_DICT);
    _indexLoadingConfig.setFSTIndexColumns(fstColumns);

    constructV3Segment();
    checkFSTIndexCreation(NEWLY_ADDED_FST_COL_DICT, 1, 1, _newColumnsSchema4, true, true, 4);

    constructV1Segment();
    checkFSTIndexCreation(NEWLY_ADDED_FST_COL_DICT, 1, 1, _newColumnsSchema4, true, true, 4);
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
    Assert.assertNotNull(columnMetadata);
    checkFSTIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _newColumnsSchema4, false, false, 26);

    constructV1Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_DICT);
    Assert.assertNotNull(columnMetadata);
    checkFSTIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _newColumnsSchema4, false, false, 26);
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
    _indexLoadingConfig.setTextIndexColumns(textIndexColumns);

    // Create a segment in V3, add a new dict encoded column with text index enabled
    constructV3Segment();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_STRING_COL_RAW);
    // should be null since column does not exist in the schema
    Assert.assertNull(columnMetadata);
    checkTextIndexCreation(NEWLY_ADDED_STRING_COL_DICT, 1, 1, _newColumnsSchemaWithText, true, true, true, 4);

    // Create a segment in V1, add a new dict encoded column with text index enabled
    constructV1Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEWLY_ADDED_STRING_COL_RAW);
    // should be null since column does not exist in the schema
    Assert.assertNull(columnMetadata);
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
    // column exists and does not have text index enabled
    Assert.assertNotNull(columnMetadata);
    Assert.assertEquals(columnMetadata.getTextIndexType(), TextIndexType.NONE);
    checkTextIndexCreation(EXISTING_STRING_COL_RAW, 5, 3, _schema, false, false, false, 0);

    // Create a segment in V1, add a new column with text index enabled
    constructV1Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    // column exists and does not have text index enabled
    Assert.assertNotNull(columnMetadata);
    Assert.assertEquals(columnMetadata.getTextIndexType(), TextIndexType.NONE);
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
    // column exists and does not have text index enabled
    Assert.assertNotNull(columnMetadata);
    Assert.assertEquals(columnMetadata.getTextIndexType(), TextIndexType.NONE);
    // SegmentPreprocessor should have created the text index using TextIndexHandler
    checkTextIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _schema, false, true, false, 26);

    // Create a segment in V1, add a new column with text index enabled
    constructV1Segment();
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    columnMetadata = segmentMetadata.getColumnMetadataFor(EXISTING_STRING_COL_RAW);
    // column exists and does not have text index enabled
    Assert.assertNotNull(columnMetadata);
    Assert.assertEquals(columnMetadata.getTextIndexType(), TextIndexType.NONE);
    // SegmentPreprocessor should have created the text index using TextIndexHandler
    checkTextIndexCreation(EXISTING_STRING_COL_DICT, 9, 4, _schema, false, true, false, 26);
  }

  private void checkFSTIndexCreation(String column, int cardinality, int bits, Schema schema, boolean isAutoGenerated,
      boolean isSorted, int dictionaryElementSize)
      throws Exception {
    checkIndexCreation(ColumnIndexType.FST_INDEX, column, cardinality, bits, schema, isAutoGenerated, true, isSorted,
        dictionaryElementSize);
  }

  private void checkTextIndexCreation(String column, int cardinality, int bits, Schema schema, boolean isAutoGenerated,
      boolean hasDictionary, boolean isSorted, int dictionaryElementSize)
      throws Exception {
    ColumnMetadata columnMetadata =
        checkIndexCreation(ColumnIndexType.TEXT_INDEX, column, cardinality, bits, schema, isAutoGenerated,
            hasDictionary, isSorted, dictionaryElementSize);
    Assert.assertEquals(columnMetadata.getTextIndexType(), TextIndexType.LUCENE);
  }

  private ColumnMetadata checkIndexCreation(ColumnIndexType indexType, String column, int cardinality, int bits,
      Schema schema, boolean isAutoGenerated, boolean hasDictionary, boolean isSorted, int dictionaryElementSize)
      throws Exception {

    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, schema)) {
      processor.process();
      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);

      Assert.assertEquals(columnMetadata.getCardinality(), cardinality);
      Assert.assertEquals(columnMetadata.getTotalDocs(), 100000);
      Assert.assertEquals(columnMetadata.getDataType(), DataType.STRING);
      Assert.assertEquals(columnMetadata.getBitsPerElement(), bits);
      Assert.assertEquals(columnMetadata.getColumnMaxLength(), dictionaryElementSize);
      Assert.assertEquals(columnMetadata.getFieldType(), FieldSpec.FieldType.DIMENSION);
      Assert.assertEquals(columnMetadata.isSorted(), isSorted);
      Assert.assertFalse(columnMetadata.hasNulls());
      Assert.assertEquals(columnMetadata.hasDictionary(), hasDictionary);
      Assert.assertTrue(columnMetadata.isSingleValue());
      Assert.assertEquals(columnMetadata.getMaxNumberOfMultiValues(), 0);
      Assert.assertEquals(columnMetadata.getTotalNumberOfEntries(), 100000);
      Assert.assertEquals(columnMetadata.isAutoGenerated(), isAutoGenerated);
      Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "null");

      try (
          SegmentDirectory segmentDirectory1 = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
              .load(_indexDir.toURI(), _configuration);
          SegmentDirectory.Reader reader = segmentDirectory1.createReader()) {
        Assert.assertTrue(reader.hasIndexFor(column, indexType));
        Assert.assertTrue(reader.hasIndexFor(column, ColumnIndexType.FORWARD_INDEX));
        // if the text index is enabled on a new column with dictionary,
        // then dictionary should be created by the default column handler
        if (hasDictionary) {
          Assert.assertTrue(reader.hasIndexFor(column, ColumnIndexType.DICTIONARY));
        } else {
          Assert.assertFalse(reader.hasIndexFor(column, ColumnIndexType.DICTIONARY));
        }
      }
      return columnMetadata;
    }
  }

  @Test
  public void testV1CreateInvertedIndices()
      throws Exception {
    constructV1Segment();

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    Assert.assertEquals(segmentMetadata.getSegmentVersion(), SegmentVersion.v1);

    String col1FileName = segmentMetadata.getBitmapInvertedIndexFileName(COLUMN1_NAME);
    String col7FileName = segmentMetadata.getBitmapInvertedIndexFileName(COLUMN7_NAME);
    String col13FileName = segmentMetadata.getBitmapInvertedIndexFileName(COLUMN13_NAME);
    String badColFileName = segmentMetadata.getBitmapInvertedIndexFileName(NO_SUCH_COLUMN_NAME);
    File col1File = new File(_indexDir, col1FileName);
    File col7File = new File(_indexDir, col7FileName);
    File col13File = new File(_indexDir, col13FileName);
    File badColFile = new File(_indexDir, badColFileName);
    Assert.assertFalse(col1File.exists());
    Assert.assertTrue(col7File.exists());
    Assert.assertFalse(col13File.exists());
    Assert.assertFalse(badColFile.exists());
    FileTime col7LastModifiedTime = Files.getLastModifiedTime(col7File.toPath());

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the first time.
    checkInvertedIndexCreation(false);
    Assert.assertTrue(col1File.exists());
    Assert.assertTrue(col7File.exists());
    Assert.assertTrue(col13File.exists());
    Assert.assertFalse(badColFile.exists());
    Assert.assertEquals(Files.getLastModifiedTime(col7File.toPath()), col7LastModifiedTime);

    // Update inverted index file last modified time.
    FileTime col1LastModifiedTime = Files.getLastModifiedTime(col1File.toPath());
    FileTime col13LastModifiedTime = Files.getLastModifiedTime(col13File.toPath());

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the second time.
    checkInvertedIndexCreation(true);
    Assert.assertTrue(col1File.exists());
    Assert.assertTrue(col7File.exists());
    Assert.assertTrue(col13File.exists());
    Assert.assertFalse(badColFile.exists());
    Assert.assertEquals(Files.getLastModifiedTime(col1File.toPath()), col1LastModifiedTime);
    Assert.assertEquals(Files.getLastModifiedTime(col7File.toPath()), col7LastModifiedTime);
    Assert.assertEquals(Files.getLastModifiedTime(col13File.toPath()), col13LastModifiedTime);
  }

  @Test
  public void testV3CreateInvertedIndices()
      throws Exception {
    constructV3Segment();

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    Assert.assertEquals(segmentMetadata.getSegmentVersion(), SegmentVersion.v3);

    File segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3);
    File singleFileIndex = new File(segmentDirectoryPath, "columns.psf");
    FileTime lastModifiedTime = Files.getLastModifiedTime(singleFileIndex.toPath());
    long fileSize = singleFileIndex.length();

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the first time.
    checkInvertedIndexCreation(false);
    long addedLength = 0L;
    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      // 8 bytes overhead is for checking integrity of the segment.
      addedLength += reader.getIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX).size() + 8;
      addedLength += reader.getIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX).size() + 8;
    }
    FileTime newLastModifiedTime = Files.getLastModifiedTime(singleFileIndex.toPath());
    Assert.assertTrue(newLastModifiedTime.compareTo(lastModifiedTime) > 0);
    long newFileSize = singleFileIndex.length();
    Assert.assertEquals(fileSize + addedLength, newFileSize);

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the second time.
    checkInvertedIndexCreation(true);
    Assert.assertEquals(Files.getLastModifiedTime(singleFileIndex.toPath()), newLastModifiedTime);
    Assert.assertEquals(singleFileIndex.length(), newFileSize);
  }

  private void checkInvertedIndexCreation(boolean reCreate)
      throws Exception {
    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      if (reCreate) {
        Assert.assertTrue(reader.hasIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX));
        Assert.assertTrue(reader.hasIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX));
        Assert.assertTrue(reader.hasIndexFor(COLUMN7_NAME, ColumnIndexType.INVERTED_INDEX));
        Assert.assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, ColumnIndexType.INVERTED_INDEX));
      } else {
        Assert.assertFalse(reader.hasIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX));
        Assert.assertTrue(reader.hasIndexFor(COLUMN7_NAME, ColumnIndexType.INVERTED_INDEX));
        Assert.assertFalse(reader.hasIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX));
        Assert.assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, ColumnIndexType.INVERTED_INDEX));
      }
    }

    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, null)) {
      processor.process();
    }

    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      Assert.assertTrue(reader.hasIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX));
      Assert.assertTrue(reader.hasIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX));
      Assert.assertTrue(reader.hasIndexFor(COLUMN7_NAME, ColumnIndexType.INVERTED_INDEX));
      Assert.assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, ColumnIndexType.INVERTED_INDEX));
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
    // For the third schema, we changed the default value for column 'newStringMVDimension' to 'notSameLength', which
    // is not the same length as before. This should be fine for segment format v1.
    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, _newColumnsSchema3)) {
      processor.process();
    }

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata hllMetricMetadata = segmentMetadata.getColumnMetadataFor(NEW_HLL_BYTE_METRIC_COLUMN_NAME);
    Assert.assertEquals(hllMetricMetadata.getDataType(), DataType.BYTES);
    String expectedDefaultValueString =
        "00000008000000ac00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
    ByteArray expectedDefaultValue = BytesUtils.toByteArray(expectedDefaultValueString);
    Assert.assertEquals(hllMetricMetadata.getMinValue(), expectedDefaultValue);
    Assert.assertEquals(hllMetricMetadata.getMaxValue(), expectedDefaultValue);
    Assert.assertEquals(hllMetricMetadata.getDefaultNullValueString(), expectedDefaultValueString);

    ColumnMetadata tDigestMetricMetadata = segmentMetadata.getColumnMetadataFor(NEW_TDIGEST_BYTE_METRIC_COLUMN_NAME);
    Assert.assertEquals(tDigestMetricMetadata.getDataType(), DataType.BYTES);
    expectedDefaultValueString =
        "0000000141ba085ee15d2f3241ba085ee15d2f324059000000000000000000013ff000000000000041ba085ee15d2f32";
    expectedDefaultValue = BytesUtils.toByteArray(expectedDefaultValueString);
    Assert.assertEquals(tDigestMetricMetadata.getMinValue(), expectedDefaultValue);
    Assert.assertEquals(tDigestMetricMetadata.getMaxValue(), expectedDefaultValue);
    Assert.assertEquals(tDigestMetricMetadata.getDefaultNullValueString(), expectedDefaultValueString);
  }

  private void checkUpdateDefaultColumns()
      throws Exception {
    // Update default value.
    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, _newColumnsSchema1)) {
      processor.process();
    }
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);

    // Check column metadata.
    // Check all field for one column, and do necessary checks for other columns.
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_INT_METRIC_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getCardinality(), 1);
    Assert.assertEquals(columnMetadata.getTotalDocs(), 100000);
    Assert.assertEquals(columnMetadata.getDataType(), DataType.INT);
    Assert.assertEquals(columnMetadata.getBitsPerElement(), 1);
    Assert.assertEquals(columnMetadata.getColumnMaxLength(), 0);
    Assert.assertEquals(columnMetadata.getFieldType(), FieldSpec.FieldType.METRIC);
    Assert.assertTrue(columnMetadata.isSorted());
    Assert.assertFalse(columnMetadata.hasNulls());
    Assert.assertTrue(columnMetadata.hasDictionary());
    Assert.assertTrue(columnMetadata.hasInvertedIndex());
    Assert.assertTrue(columnMetadata.isSingleValue());
    Assert.assertEquals(columnMetadata.getMaxNumberOfMultiValues(), 0);
    Assert.assertEquals(columnMetadata.getTotalNumberOfEntries(), 100000);
    Assert.assertTrue(columnMetadata.isAutoGenerated());
    Assert.assertEquals(columnMetadata.getMinValue(), 1);
    Assert.assertEquals(columnMetadata.getMaxValue(), 1);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "1");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_LONG_METRIC_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDataType(), DataType.LONG);
    Assert.assertEquals(columnMetadata.getMinValue(), 0L);
    Assert.assertEquals(columnMetadata.getMaxValue(), 0L);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "0");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_FLOAT_METRIC_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDataType(), DataType.FLOAT);
    Assert.assertEquals(columnMetadata.getMinValue(), 0f);
    Assert.assertEquals(columnMetadata.getMaxValue(), 0f);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "0.0");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_DOUBLE_METRIC_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDataType(), DataType.DOUBLE);
    Assert.assertEquals(columnMetadata.getMinValue(), 0.0);
    Assert.assertEquals(columnMetadata.getMaxValue(), 0.0);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "0.0");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDataType(), DataType.BOOLEAN);
    Assert.assertEquals(columnMetadata.getColumnMaxLength(), 0);
    Assert.assertEquals(columnMetadata.getFieldType(), FieldSpec.FieldType.DIMENSION);
    Assert.assertEquals(columnMetadata.getMinValue(), 0);
    Assert.assertEquals(columnMetadata.getMaxValue(), 0);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "0");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDataType(), DataType.STRING);
    Assert.assertEquals(columnMetadata.getColumnMaxLength(), 4);
    Assert.assertFalse(columnMetadata.isSorted());
    Assert.assertFalse(columnMetadata.isSingleValue());
    Assert.assertEquals(columnMetadata.getMaxNumberOfMultiValues(), 1);
    Assert.assertEquals(columnMetadata.getTotalNumberOfEntries(), 100000);
    Assert.assertEquals(columnMetadata.getMinValue(), "null");
    Assert.assertEquals(columnMetadata.getMaxValue(), "null");
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "null");

    // Derived column
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_INT_SV_DIMENSION_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDataType(), DataType.INT);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), Integer.toString(Integer.MIN_VALUE));
    Assert.assertTrue(columnMetadata.isAutoGenerated());
    ColumnMetadata originalColumnMetadata = segmentMetadata.getColumnMetadataFor(COLUMN1_NAME);
    Assert.assertEquals(columnMetadata.getCardinality(), originalColumnMetadata.getCardinality());
    Assert.assertEquals(columnMetadata.getBitsPerElement(), originalColumnMetadata.getBitsPerElement());
    Assert.assertEquals(columnMetadata.isSorted(), originalColumnMetadata.isSorted());
    Assert.assertEquals(columnMetadata.getMinValue(), (int) originalColumnMetadata.getMinValue() + 1);
    Assert.assertEquals(columnMetadata.getMaxValue(), (int) originalColumnMetadata.getMaxValue() + 1);

    // Check dictionary and forward index exist.
    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      Assert.assertTrue(reader.hasIndexFor(NEW_INT_METRIC_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      Assert.assertTrue(reader.hasIndexFor(NEW_INT_METRIC_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
      Assert.assertTrue(reader.hasIndexFor(NEW_LONG_METRIC_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      Assert.assertTrue(reader.hasIndexFor(NEW_LONG_METRIC_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
      Assert.assertTrue(reader.hasIndexFor(NEW_FLOAT_METRIC_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      Assert.assertTrue(reader.hasIndexFor(NEW_FLOAT_METRIC_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
      Assert.assertTrue(reader.hasIndexFor(NEW_DOUBLE_METRIC_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      Assert.assertTrue(reader.hasIndexFor(NEW_DOUBLE_METRIC_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
      Assert.assertTrue(reader.hasIndexFor(NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      Assert.assertTrue(reader.hasIndexFor(NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
      Assert.assertTrue(reader.hasIndexFor(NEW_INT_SV_DIMENSION_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      Assert.assertTrue(reader.hasIndexFor(NEW_INT_SV_DIMENSION_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
      Assert.assertTrue(reader.hasIndexFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME, ColumnIndexType.DICTIONARY));
      Assert.assertTrue(reader.hasIndexFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME, ColumnIndexType.FORWARD_INDEX));
    }

    // Use the second schema and update default value again.
    // For the second schema, we changed the default value for column 'newIntMetric' to 2, and added default value
    // 'abcd' (keep the same length as 'null') to column 'newStringMVDimension'.
    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, _indexLoadingConfig, _newColumnsSchema2)) {
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);

    // Check column metadata.
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_INT_METRIC_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getMinValue(), 2);
    Assert.assertEquals(columnMetadata.getMaxValue(), 2);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "2");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getMinValue(), "abcd");
    Assert.assertEquals(columnMetadata.getMaxValue(), "abcd");
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "abcd");
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
      if (key.endsWith(V1Constants.MetadataKeys.Column.MIN_VALUE) || key
          .endsWith(V1Constants.MetadataKeys.Column.MAX_VALUE)) {
        configuration.clearProperty(key);
      }
    }
    configuration.save();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.NONE);
    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, null)) {
      processor.process();
    }
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);
    ColumnMetadata timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    ColumnMetadata dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column1");
    ColumnMetadata metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    Assert.assertNull(timeColumnMetadata.getMinValue());
    Assert.assertNull(timeColumnMetadata.getMaxValue());
    Assert.assertNull(dimensionColumnMetadata.getMinValue());
    Assert.assertNull(dimensionColumnMetadata.getMaxValue());
    Assert.assertNull(metricColumnMetadata.getMinValue());
    Assert.assertNull(metricColumnMetadata.getMaxValue());

    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.TIME);
    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, null)) {
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column5");
    metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    Assert.assertEquals(timeColumnMetadata.getMinValue(), 1756015683);
    Assert.assertEquals(timeColumnMetadata.getMaxValue(), 1756015683);
    Assert.assertNull(dimensionColumnMetadata.getMinValue());
    Assert.assertNull(dimensionColumnMetadata.getMaxValue());
    Assert.assertNull(metricColumnMetadata.getMinValue());
    Assert.assertNull(metricColumnMetadata.getMaxValue());

    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.NON_METRIC);
    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, null)) {
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column5");
    metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    Assert.assertEquals(timeColumnMetadata.getMinValue(), 1756015683);
    Assert.assertEquals(timeColumnMetadata.getMaxValue(), 1756015683);
    Assert.assertEquals(dimensionColumnMetadata.getMinValue(), "AKXcXcIqsqOJFsdwxZ");
    Assert.assertEquals(dimensionColumnMetadata.getMaxValue(), "yQkJTLOQoOqqhkAClgC");
    Assert.assertNull(metricColumnMetadata.getMinValue());
    Assert.assertNull(metricColumnMetadata.getMaxValue());

    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.ALL);
    try (
        SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
            .load(_indexDir.toURI(), _configuration);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig, null)) {
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);
    timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column5");
    metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    Assert.assertEquals(timeColumnMetadata.getMinValue(), 1756015683);
    Assert.assertEquals(timeColumnMetadata.getMaxValue(), 1756015683);
    Assert.assertEquals(dimensionColumnMetadata.getMinValue(), "AKXcXcIqsqOJFsdwxZ");
    Assert.assertEquals(dimensionColumnMetadata.getMaxValue(), "yQkJTLOQoOqqhkAClgC");
    Assert.assertEquals(metricColumnMetadata.getMinValue(), 890662862);
    Assert.assertEquals(metricColumnMetadata.getMaxValue(), 890662862);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
