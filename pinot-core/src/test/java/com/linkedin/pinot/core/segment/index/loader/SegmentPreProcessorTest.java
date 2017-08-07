/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.loader;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import com.linkedin.pinot.core.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.apache.commons.io.FileUtils;
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

  // For update default value tests.
  private static final String NEW_COLUMNS_SCHEMA1 = "data/newColumnsSchema1.json";
  private static final String NEW_COLUMNS_SCHEMA2 = "data/newColumnsSchema2.json";
  private static final String NEW_COLUMNS_SCHEMA3 = "data/newColumnsSchema3.json";
  private static final String NEW_INT_METRIC_COLUMN_NAME = "newIntMetric";
  private static final String NEW_LONG_METRIC_COLUMN_NAME = "newLongMetric";
  private static final String NEW_FLOAT_METRIC_COLUMN_NAME = "newFloatMetric";
  private static final String NEW_DOUBLE_METRIC_COLUMN_NAME = "newDoubleMetric";
  private static final String NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME = "newBooleanSVDimension";
  private static final String NEW_INT_SV_DIMENSION_COLUMN_NAME = "newIntSVDimension";
  private static final String NEW_STRING_MV_DIMENSION_COLUMN_NAME = "newStringMVDimension";

  private File _indexDir;
  private IndexLoadingConfig _indexLoadingConfig;
  private File _avroFile;
  private Schema _schema;
  private Schema _newColumnsSchema1;
  private Schema _newColumnsSchema2;
  private Schema _newColumnsSchema3;

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // We specify two columns without inverted index ('column1', 'column13'), one non-existing column ('noSuchColumn')
    // and one column with existed inverted index ('column7').
    _indexLoadingConfig = new IndexLoadingConfig();
    _indexLoadingConfig.setInvertedIndexColumns(
        new HashSet<>(Arrays.asList(COLUMN1_NAME, COLUMN7_NAME, COLUMN13_NAME, NO_SUCH_COLUMN_NAME)));

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
  }

  private void constructV1Segment() throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Create inverted index for 'column7' when constructing the segment.
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithSchema(_avroFile, INDEX_DIR, "testTable", _schema);
    segmentGeneratorConfig.setInvertedIndexCreationColumns(Collections.singletonList(COLUMN7_NAME));
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(segmentGeneratorConfig);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
  }

  private void constructV3Segment() throws Exception {
    constructV1Segment();
    new SegmentV1V2ToV3FormatConverter().convert(_indexDir);
  }

  @Test
  public void testV1CreateInvertedIndices() throws Exception {
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
  public void testV3CreateInvertedIndices() throws Exception {
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
    try (SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(segmentDirectoryPath, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      // 8 bytes overhead is for checking integrity of the segment.
      try (PinotDataBuffer col1Buffer = reader.getIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX)) {
        addedLength += col1Buffer.size() + 8;
      }
      try (PinotDataBuffer col13Buffer = reader.getIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX)) {
        addedLength += col13Buffer.size() + 8;
      }
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

  private void checkInvertedIndexCreation(boolean reCreate) throws Exception {
    try (SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(_indexDir, ReadMode.mmap);
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

    try (SegmentPreProcessor processor = new SegmentPreProcessor(_indexDir, _indexLoadingConfig, null)) {
      processor.process();
    }

    try (SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(_indexDir, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      Assert.assertTrue(reader.hasIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX));
      Assert.assertTrue(reader.hasIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX));
      Assert.assertTrue(reader.hasIndexFor(COLUMN7_NAME, ColumnIndexType.INVERTED_INDEX));
      Assert.assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, ColumnIndexType.INVERTED_INDEX));
    }
  }

  @Test
  public void testV1UpdateDefaultColumns() throws Exception {
    constructV1Segment();
    checkUpdateDefaultColumns();

    // Try to use the third schema and update default value again.
    // For the third schema, we changed the default value for column 'newStringMVDimension' to 'notSameLength', which
    // is not the same length as before. This should be fine for segment format v1.
    try (SegmentPreProcessor processor = new SegmentPreProcessor(_indexDir, _indexLoadingConfig, _newColumnsSchema3)) {
      processor.process();
    }
  }

  @Test
  public void testV3UpdateDefaultColumns() throws Exception {
    constructV3Segment();
    checkUpdateDefaultColumns();

    // Try to use the third schema and update default value again.
    // For the third schema, we changed the default value for column 'newStringMVDimension' to 'notSameLength', which
    // is not the same length as before. This should throw exception for segment format v3.
    try (SegmentPreProcessor processor = new SegmentPreProcessor(_indexDir, _indexLoadingConfig, _newColumnsSchema3)) {
      processor.process();
      Assert.fail("For segment format v3, should throw exception when trying to update with different length index");
    } catch (Exception e) {
      // PASS.
    }
  }

  private void checkUpdateDefaultColumns() throws Exception {
    // Update default value.
    try (SegmentPreProcessor processor = new SegmentPreProcessor(_indexDir, _indexLoadingConfig, _newColumnsSchema1)) {
      processor.process();
    }
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_indexDir);

    // Check column metadata.
    // Check all field for one column, and do necessary checks for other columns.
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_INT_METRIC_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getCardinality(), 1);
    Assert.assertEquals(columnMetadata.getTotalDocs(), 100000);
    Assert.assertEquals(columnMetadata.getTotalRawDocs(), 100000);
    Assert.assertEquals(columnMetadata.getTotalAggDocs(), 0);
    Assert.assertEquals(columnMetadata.getDataType(), FieldSpec.DataType.INT);
    Assert.assertEquals(columnMetadata.getBitsPerElement(), 1);
    Assert.assertEquals(columnMetadata.getStringColumnMaxLength(), 0);
    Assert.assertEquals(columnMetadata.getFieldType(), FieldSpec.FieldType.METRIC);
    Assert.assertTrue(columnMetadata.isSorted());
    Assert.assertFalse(columnMetadata.hasNulls());
    Assert.assertTrue(columnMetadata.hasDictionary());
    Assert.assertTrue(columnMetadata.hasInvertedIndex());
    Assert.assertTrue(columnMetadata.isSingleValue());
    Assert.assertEquals(columnMetadata.getMaxNumberOfMultiValues(), 0);
    Assert.assertEquals(columnMetadata.getTotalNumberOfEntries(), 100000);
    Assert.assertTrue(columnMetadata.isAutoGenerated());
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "1");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_LONG_METRIC_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDataType(), FieldSpec.DataType.LONG);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "0");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_FLOAT_METRIC_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDataType(), FieldSpec.DataType.FLOAT);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "0.0");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_DOUBLE_METRIC_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDataType(), FieldSpec.DataType.DOUBLE);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "0.0");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_BOOLEAN_SV_DIMENSION_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDataType(), FieldSpec.DataType.STRING);
    Assert.assertEquals(columnMetadata.getStringColumnMaxLength(), 5);
    Assert.assertEquals(columnMetadata.getFieldType(), FieldSpec.FieldType.DIMENSION);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "false");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_INT_SV_DIMENSION_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDataType(), FieldSpec.DataType.INT);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), String.valueOf(Integer.MIN_VALUE));

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDataType(), FieldSpec.DataType.STRING);
    Assert.assertEquals(columnMetadata.getStringColumnMaxLength(), 4);
    Assert.assertFalse(columnMetadata.isSorted());
    Assert.assertFalse(columnMetadata.isSingleValue());
    Assert.assertEquals(columnMetadata.getMaxNumberOfMultiValues(), 1);
    Assert.assertEquals(columnMetadata.getTotalNumberOfEntries(), 100000);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "null");

    // Check dictionary and forward index exist.
    try (SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(_indexDir, ReadMode.mmap);
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
    try (SegmentPreProcessor processor = new SegmentPreProcessor(_indexDir, _indexLoadingConfig, _newColumnsSchema2)) {
      processor.process();
    }
    segmentMetadata = new SegmentMetadataImpl(_indexDir);

    // Check column metadata.
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_INT_METRIC_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "2");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "abcd");
  }

  @Test
  public void testAddColumnMinMaxValue() throws Exception {
    constructV1Segment();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode.NONE);
    try (SegmentPreProcessor processor = new SegmentPreProcessor(_indexDir, indexLoadingConfig, null)) {
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
    try (SegmentPreProcessor processor = new SegmentPreProcessor(_indexDir, indexLoadingConfig, null)) {
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
    try (SegmentPreProcessor processor = new SegmentPreProcessor(_indexDir, indexLoadingConfig, null)) {
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
    try (SegmentPreProcessor processor = new SegmentPreProcessor(_indexDir, indexLoadingConfig, null)) {
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
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
