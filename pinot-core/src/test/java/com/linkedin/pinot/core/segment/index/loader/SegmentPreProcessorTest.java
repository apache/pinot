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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
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
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SegmentPreProcessorTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(SegmentPreProcessor.class.toString());
  private static final String SCHEMA = "data/testDataMVSchema.json";
  private static final String V3_SEGMENT_NAME = "v3";

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

  private File _segmentDirectoryFile;
  private IndexLoadingConfigMetadata _indexLoadingConfigMetadata;
  private Schema _schema;
  private Schema _newColumnsSchema1;
  private Schema _newColumnsSchema2;
  private Schema _newColumnsSchema3;

  @BeforeClass
  public void setUp()
      throws Exception {
    // For indexLoadingConfigMetadata, we specify two columns without inverted index ('column1', 'column13'), one
    // non-existing column ('noSuchColumn') and one column with existed inverted index ('column7').
    _indexLoadingConfigMetadata = new IndexLoadingConfigMetadata(new PropertiesConfiguration());
    _indexLoadingConfigMetadata.initLoadingInvertedIndexColumnSet(
        new String[]{COLUMN1_NAME, COLUMN7_NAME, COLUMN13_NAME, NO_SUCH_COLUMN_NAME});
    _indexLoadingConfigMetadata.setEnableDefaultColumns(true);

    // For newColumnsSchema, we add 4 different data type metric columns with one user-defined default null value, and
    // 3 different data type dimension columns with one user-defined default null value and one multi-value column.
    ClassLoader classLoader = getClass().getClassLoader();
    URL resourceUrl = classLoader.getResource(SCHEMA);
    Preconditions.checkNotNull(resourceUrl);
    _schema = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA1);
    Preconditions.checkNotNull(resourceUrl);
    _newColumnsSchema1 = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA2);
    Preconditions.checkNotNull(resourceUrl);
    _newColumnsSchema2 = Schema.fromFile(new File(resourceUrl.getFile()));
    resourceUrl = classLoader.getResource(NEW_COLUMNS_SCHEMA3);
    Preconditions.checkNotNull(resourceUrl);
    _newColumnsSchema3 = Schema.fromFile(new File(resourceUrl.getFile()));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void constructSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA);
    Assert.assertNotNull(resourceUrl);
    File avroFile = new File(TestUtils.getFileFromResourceUrl(resourceUrl));

    // NOTE: We create inverted index for 'column7' when constructing the segment.
    // Intentionally changed this to TimeUnit.Hours to make it non-default for testing.
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithSchema(avroFile, INDEX_DIR, "testTable", _schema);
    segmentGeneratorConfig.setSegmentNamePostfix("1");
    segmentGeneratorConfig.setInvertedIndexCreationColumns(Collections.singletonList(COLUMN7_NAME));
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(segmentGeneratorConfig);
    driver.build();

    _segmentDirectoryFile = new File(INDEX_DIR, driver.getSegmentName());
  }

  @Test
  public void testV1CreateInvertedIndices()
      throws Exception {
    constructSegment();

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_segmentDirectoryFile);
    String segmentVersion = segmentMetadata.getVersion();
    Assert.assertEquals(SegmentVersion.valueOf(segmentVersion), SegmentVersion.v1);

    String col1FileName = segmentMetadata.getBitmapInvertedIndexFileName(COLUMN1_NAME, segmentVersion);
    String col7FileName = segmentMetadata.getBitmapInvertedIndexFileName(COLUMN7_NAME, segmentVersion);
    String col13FileName = segmentMetadata.getBitmapInvertedIndexFileName(COLUMN13_NAME, segmentVersion);
    String badColFileName = segmentMetadata.getBitmapInvertedIndexFileName(NO_SUCH_COLUMN_NAME, segmentVersion);
    File col1File = new File(_segmentDirectoryFile, col1FileName);
    File col7File = new File(_segmentDirectoryFile, col7FileName);
    File col13File = new File(_segmentDirectoryFile, col13FileName);
    File badColFile = new File(_segmentDirectoryFile, badColFileName);
    Assert.assertFalse(col1File.exists());
    Assert.assertTrue(col7File.exists());
    Assert.assertFalse(col13File.exists());
    Assert.assertFalse(badColFile.exists());
    FileTime col7LastModifiedTime = Files.getLastModifiedTime(col7File.toPath());

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the first time.
    checkInvertedIndexCreation(_segmentDirectoryFile, segmentMetadata, false);
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
    checkInvertedIndexCreation(_segmentDirectoryFile, segmentMetadata, true);
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
    constructSegment();

    // Convert segment format to v3.
    SegmentV1V2ToV3FormatConverter converter = new SegmentV1V2ToV3FormatConverter();
    converter.convert(_segmentDirectoryFile);
    File v3SegmentDirectoryFile = new File(_segmentDirectoryFile, V3_SEGMENT_NAME);

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(v3SegmentDirectoryFile);
    String segmentVersion = segmentMetadata.getVersion();
    Assert.assertEquals(SegmentVersion.valueOf(segmentVersion), SegmentVersion.v3);

    File singleFileIndex = new File(v3SegmentDirectoryFile, "columns.psf");
    FileTime lastModifiedTime = Files.getLastModifiedTime(singleFileIndex.toPath());
    long fileSize = singleFileIndex.length();

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the first time.
    checkInvertedIndexCreation(v3SegmentDirectoryFile, segmentMetadata, false);
    long addedLength = 0L;
    try (
        SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(v3SegmentDirectoryFile, segmentMetadata,
            ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()
    ) {
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
    checkInvertedIndexCreation(v3SegmentDirectoryFile, segmentMetadata, true);
    Assert.assertEquals(Files.getLastModifiedTime(singleFileIndex.toPath()), newLastModifiedTime);
    Assert.assertEquals(singleFileIndex.length(), newFileSize);
  }

  private void checkInvertedIndexCreation(File segmentDirectoryFile, SegmentMetadataImpl segmentMetadata,
      boolean reCreate)
      throws Exception {
    try (
        SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(segmentDirectoryFile, segmentMetadata,
            ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()
    ) {
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

    SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectoryFile, _indexLoadingConfigMetadata, null);
    processor.process();

    try (
        SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(segmentDirectoryFile, segmentMetadata,
            ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()
    ) {
      Assert.assertTrue(reader.hasIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX));
      Assert.assertTrue(reader.hasIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX));
      Assert.assertTrue(reader.hasIndexFor(COLUMN7_NAME, ColumnIndexType.INVERTED_INDEX));
      Assert.assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, ColumnIndexType.INVERTED_INDEX));
    }
  }

  @Test
  public void testV1UpdateDefaultColumns()
      throws Exception {
    constructSegment();

    checkUpdateDefaultColumns(_segmentDirectoryFile);

    // Try to use the third schema and update default value again.
    // For the third schema, we changed the default value for column 'newStringMVDimension' to 'notSameLength', which
    // is not the same length as before. This should be fine for segment format v1.
    SegmentPreProcessor processor =
        new SegmentPreProcessor(_segmentDirectoryFile, _indexLoadingConfigMetadata, _newColumnsSchema3);
    processor.process();
  }

  @Test
  public void testV3UpdateDefaultColumns()
      throws Exception {
    constructSegment();

    // Convert segment format to v3.
    SegmentV1V2ToV3FormatConverter converter = new SegmentV1V2ToV3FormatConverter();
    converter.convert(_segmentDirectoryFile);
    File v3SegmentDirectoryFile = new File(_segmentDirectoryFile, V3_SEGMENT_NAME);

    checkUpdateDefaultColumns(v3SegmentDirectoryFile);

    // Try to use the third schema and update default value again.
    // For the third schema, we changed the default value for column 'newStringMVDimension' to 'notSameLength', which
    // is not the same length as before. This should throw exception for segment format v3.
    try {
      SegmentPreProcessor processor =
          new SegmentPreProcessor(v3SegmentDirectoryFile, _indexLoadingConfigMetadata, _newColumnsSchema3);
      processor.process();
      Assert.fail("For segment format v3, should throw exception when trying to update with different length index");
    } catch (Exception e) {
      // PASS.
    }
  }

  private void checkUpdateDefaultColumns(File segmentDirectoryFile)
      throws Exception {
    // Update default value.
    SegmentPreProcessor processor =
        new SegmentPreProcessor(segmentDirectoryFile, _indexLoadingConfigMetadata, _newColumnsSchema1);
    processor.process();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDirectoryFile);

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
    try (
        SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(segmentDirectoryFile, segmentMetadata,
            ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()
    ) {
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
    processor = new SegmentPreProcessor(segmentDirectoryFile, _indexLoadingConfigMetadata, _newColumnsSchema2);
    processor.process();
    segmentMetadata = new SegmentMetadataImpl(segmentDirectoryFile);

    // Check column metadata.
    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_INT_METRIC_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "2");

    columnMetadata = segmentMetadata.getColumnMetadataFor(NEW_STRING_MV_DIMENSION_COLUMN_NAME);
    Assert.assertEquals(columnMetadata.getDefaultNullValueString(), "abcd");
  }

  @Test
  public void testNullIndexLoadingConfigAndNullSchema()
      throws Exception {
    constructSegment();

    SegmentPreProcessor processor = new SegmentPreProcessor(_segmentDirectoryFile, null, null);
    processor.process();
    // No exception is the validation that we handle null value for indexLoadingConfigMetadata and newColumnsSchema
    // correctly.
  }

  @Test
  public void testAddColumnMinMaxValue()
      throws Exception {
    constructSegment();

    IndexLoadingConfigMetadata indexLoadingConfigMetadata =
        new IndexLoadingConfigMetadata(new PropertiesConfiguration());
    indexLoadingConfigMetadata.setGenerateColumnMinMaxValueMode(ColumnMinMaxValueGeneratorMode.NONE.toString());
    SegmentPreProcessor processor = new SegmentPreProcessor(_segmentDirectoryFile, indexLoadingConfigMetadata, null);
    processor.process();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_segmentDirectoryFile);
    ColumnMetadata timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    ColumnMetadata dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column1");
    ColumnMetadata metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    Assert.assertNull(timeColumnMetadata.getMinValue());
    Assert.assertNull(timeColumnMetadata.getMaxValue());
    Assert.assertNull(dimensionColumnMetadata.getMinValue());
    Assert.assertNull(dimensionColumnMetadata.getMaxValue());
    Assert.assertNull(metricColumnMetadata.getMinValue());
    Assert.assertNull(metricColumnMetadata.getMaxValue());

    indexLoadingConfigMetadata = new IndexLoadingConfigMetadata(new PropertiesConfiguration());
    indexLoadingConfigMetadata.setGenerateColumnMinMaxValueMode(ColumnMinMaxValueGeneratorMode.TIME.toString());
    processor = new SegmentPreProcessor(_segmentDirectoryFile, indexLoadingConfigMetadata, null);
    processor.process();
    segmentMetadata = new SegmentMetadataImpl(_segmentDirectoryFile);
    timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column5");
    metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    Assert.assertEquals(timeColumnMetadata.getMinValue(), 1756015683);
    Assert.assertEquals(timeColumnMetadata.getMaxValue(), 1756015683);
    Assert.assertNull(dimensionColumnMetadata.getMinValue());
    Assert.assertNull(dimensionColumnMetadata.getMaxValue());
    Assert.assertNull(metricColumnMetadata.getMinValue());
    Assert.assertNull(metricColumnMetadata.getMaxValue());

    indexLoadingConfigMetadata.setGenerateColumnMinMaxValueMode(ColumnMinMaxValueGeneratorMode.NON_METRIC.toString());
    processor = new SegmentPreProcessor(_segmentDirectoryFile, indexLoadingConfigMetadata, null);
    processor.process();
    segmentMetadata = new SegmentMetadataImpl(_segmentDirectoryFile);
    timeColumnMetadata = segmentMetadata.getColumnMetadataFor("daysSinceEpoch");
    dimensionColumnMetadata = segmentMetadata.getColumnMetadataFor("column5");
    metricColumnMetadata = segmentMetadata.getColumnMetadataFor("count");
    Assert.assertEquals(timeColumnMetadata.getMinValue(), 1756015683);
    Assert.assertEquals(timeColumnMetadata.getMaxValue(), 1756015683);
    Assert.assertEquals(dimensionColumnMetadata.getMinValue(), "AKXcXcIqsqOJFsdwxZ");
    Assert.assertEquals(dimensionColumnMetadata.getMaxValue(), "yQkJTLOQoOqqhkAClgC");
    Assert.assertNull(metricColumnMetadata.getMinValue());
    Assert.assertNull(metricColumnMetadata.getMaxValue());

    indexLoadingConfigMetadata.setGenerateColumnMinMaxValueMode(ColumnMinMaxValueGeneratorMode.ALL.toString());
    processor = new SegmentPreProcessor(_segmentDirectoryFile, indexLoadingConfigMetadata, null);
    processor.process();
    segmentMetadata = new SegmentMetadataImpl(_segmentDirectoryFile);
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
}
