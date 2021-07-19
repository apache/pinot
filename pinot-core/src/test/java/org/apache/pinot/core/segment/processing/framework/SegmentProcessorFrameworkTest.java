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
package org.apache.pinot.core.segment.processing.framework;

import com.google.common.collect.Lists;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandler;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandlerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * End-to-end tests for SegmentProcessorFramework
 */
public class SegmentProcessorFrameworkTest {

  private File _baseDir;
  private File _emptyInputDir;
  private File _singleSegment;
  private File _multipleSegments;
  private File _multiValueSegments;
  private File _tarredSegments;

  private TableConfig _tableConfig;
  private TableConfig _tableConfigNullValueEnabled;
  private Schema _schema;
  private Schema _schemaMV;

  private final List<Object[]> _rawData = Lists
      .newArrayList(new Object[]{"abc", 1000, 1597719600000L}, new Object[]{null, 2000, 1597773600000L},
          new Object[]{"abc", null, 1597777200000L}, new Object[]{"abc", 4000, 1597795200000L},
          new Object[]{"abc", 3000, 1597802400000L}, new Object[]{null, null, 1597838400000L},
          new Object[]{"xyz", 4000, 1597856400000L}, new Object[]{null, 1000, 1597878000000L},
          new Object[]{"abc", 7000, 1597881600000L}, new Object[]{"xyz", 6000, 1597892400000L});

  private final List<Object[]> _rawDataMultiValue = Lists
      .newArrayList(new Object[]{new String[]{"a", "b"}, 1000, 1597795200000L},
          new Object[]{null, null, 1597795200000L}, new Object[]{null, 1000, 1597795200000L},
          new Object[]{new String[]{"a", "b"}, null, 1597795200000L});

  @BeforeClass
  public void setup()
      throws Exception {
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("time").build();
    _tableConfigNullValueEnabled =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("time")
            .setNullHandlingEnabled(true).build();
    _schema =
        new Schema.SchemaBuilder().setSchemaName("mySchema").addSingleValueDimension("campaign", DataType.STRING, "")
            // NOTE: Intentionally put 1000 as default value to test skipping null values during rollup
            .addMetric("clicks", DataType.INT, 1000)
            .addDateTime("time", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    _schemaMV =
        new Schema.SchemaBuilder().setSchemaName("mySchema").addMultiValueDimension("campaign", DataType.STRING, "")
            // NOTE: Intentionally put 1000 as default value to test skipping null values during rollup
            .addMetric("clicks", DataType.INT, 1000)
            .addDateTime("time", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

    _baseDir = new File(FileUtils.getTempDirectory(), "segment_processor_framework_test_" + System.currentTimeMillis());
    FileUtils.deleteQuietly(_baseDir);
    assertTrue(_baseDir.mkdirs());

    // create segments in many folders
    _emptyInputDir = new File(_baseDir, "empty_input");
    assertTrue(_emptyInputDir.mkdirs());
    _singleSegment = new File(_baseDir, "single_segment");
    createInputSegments(_singleSegment, _rawData, 1, _schema);
    _multipleSegments = new File(_baseDir, "multiple_segments");
    createInputSegments(_multipleSegments, _rawData, 3, _schema);
    _multiValueSegments = new File(_baseDir, "multi_value_segment");
    createInputSegments(_multiValueSegments, _rawDataMultiValue, 1, _schemaMV);
    _tarredSegments = new File(_baseDir, "tarred_segment");
    createInputSegments(_tarredSegments, _rawData, 3, _schema);
    File[] segmentDirs = _tarredSegments.listFiles();
    assertNotNull(segmentDirs);
    for (File segmentDir : segmentDirs) {
      TarGzCompressionUtils.createTarGzFile(segmentDir,
          new File(_tarredSegments, segmentDir.getName() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION));
      FileUtils.deleteQuietly(segmentDir);
    }
  }

  private void createInputSegments(File inputDir, List<Object[]> rawData, int numSegments, Schema schema)
      throws Exception {
    assertTrue(inputDir.mkdirs());

    List<List<GenericRow>> dataLists = new ArrayList<>();
    if (numSegments > 1) {
      List<GenericRow> dataList1 = new ArrayList<>(4);
      IntStream.range(0, 4).forEach(i -> dataList1.add(getGenericRow(rawData.get(i))));
      dataLists.add(dataList1);
      List<GenericRow> dataList2 = new ArrayList<>(4);
      IntStream.range(4, 7).forEach(i -> dataList2.add(getGenericRow(rawData.get(i))));
      dataLists.add(dataList2);
      List<GenericRow> dataList3 = new ArrayList<>(4);
      IntStream.range(7, 10).forEach(i -> dataList3.add(getGenericRow(rawData.get(i))));
      dataLists.add(dataList3);
    } else {
      List<GenericRow> dataList = new ArrayList<>();
      rawData.forEach(r -> dataList.add(getGenericRow(r)));
      dataLists.add(dataList);
    }

    int idx = 0;
    for (List<GenericRow> inputRows : dataLists) {
      RecordReader recordReader = new GenericRowRecordReader(inputRows);
      // NOTE: Generate segments with null value enabled
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfigNullValueEnabled, schema);
      segmentGeneratorConfig.setOutDir(inputDir.getPath());
      segmentGeneratorConfig.setSequenceId(idx++);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(segmentGeneratorConfig, recordReader);
      driver.build();
    }

    File[] files = inputDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, numSegments);
  }

  private GenericRow getGenericRow(Object[] rawRow) {
    GenericRow row = new GenericRow();
    row.putValue("campaign", rawRow[0]);
    row.putValue("clicks", rawRow[1]);
    row.putValue("time", rawRow[2]);
    return row;
  }

  @Test
  public void testBadInputFolders()
      throws Exception {
    SegmentProcessorConfig config;

    try {
      new SegmentProcessorConfig.Builder().setSchema(_schema).build();
      fail("Should fail for missing tableConfig");
    } catch (IllegalStateException e) {
      // expected
    }

    try {
      new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).build();
      fail("Should fail for missing schema");
    } catch (IllegalStateException e) {
      // expected
    }

    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();

    File outputSegmentDir = new File(_baseDir, "output_directory_bad_input_folders");
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());

    // non-existent input dir
    File nonExistent = new File(_baseDir, "non_existent");
    try {
      new SegmentProcessorFramework(nonExistent, config, outputSegmentDir);
      fail("Should fail for non existent input dir");
    } catch (IllegalStateException e) {
      // expected
    }

    // file used as input dir
    File fileInput = new File(_baseDir, "file.txt");
    assertTrue(fileInput.createNewFile());
    try {
      new SegmentProcessorFramework(fileInput, config, outputSegmentDir);
      fail("Should fail for file used as input dir");
    } catch (IllegalStateException e) {
      // expected
    }

    // non existent output dir
    try {
      new SegmentProcessorFramework(_singleSegment, config, nonExistent);
      fail("Should fail for non existent output dir");
    } catch (IllegalStateException e) {
      // expected
    }

    // file used as output dir
    try {
      new SegmentProcessorFramework(_singleSegment, config, fileInput);
      fail("Should fail for file used as output dir");
    } catch (IllegalStateException e) {
      // expected
    }

    // output dir not empty
    try {
      new SegmentProcessorFramework(fileInput, config, _singleSegment);
      fail("Should fail for output dir not empty");
    } catch (IllegalStateException e) {
      // expected
    }

    // empty input dir
    SegmentProcessorFramework framework = new SegmentProcessorFramework(_emptyInputDir, config, outputSegmentDir);
    try {
      framework.processSegments();
      fail("Should fail for empty input");
    } catch (Exception e) {
      framework.cleanup();
    }
  }

  @Test
  public void testSingleSegment()
      throws Exception {
    File outputSegmentDir = new File(_baseDir, "single_segment_output");
    FileUtils.forceMkdir(outputSegmentDir);

    // Default configs
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    SegmentProcessorFramework framework = new SegmentProcessorFramework(_singleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    File[] files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    ImmutableSegment segment = ImmutableSegmentLoader.load(files[0], ReadMode.mmap);
    SegmentMetadata segmentMetadata = segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 10);
    ColumnMetadata campaignMetadata = segmentMetadata.getColumnMetadataFor("campaign");
    assertEquals(campaignMetadata.getCardinality(), 3);
    assertEquals(campaignMetadata.getMinValue(), "");
    assertEquals(campaignMetadata.getMaxValue(), "xyz");
    ColumnMetadata clicksMetadata = segmentMetadata.getColumnMetadataFor("clicks");
    assertEquals(clicksMetadata.getCardinality(), 6);
    assertEquals(clicksMetadata.getMinValue(), 1000);
    assertEquals(clicksMetadata.getMaxValue(), 7000);
    ColumnMetadata timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 10);
    assertEquals(timeMetadata.getMinValue(), 1597719600000L);
    assertEquals(timeMetadata.getMaxValue(), 1597892400000L);
    DataSource campaignDataSource = segment.getDataSource("campaign");
    assertNull(campaignDataSource.getNullValueVector());
    DataSource clicksDataSource = segment.getDataSource("clicks");
    assertNull(clicksDataSource.getNullValueVector());
    DataSource timeDataSource = segment.getDataSource("time");
    assertNull(timeDataSource.getNullValueVector());
    assertEquals(segmentMetadata.getName(), "myTable_1597719600000_1597892400000_0");
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);

    // Default configs - null value enabled
    config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schema).build();
    framework = new SegmentProcessorFramework(_singleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    segment = ImmutableSegmentLoader.load(files[0], ReadMode.mmap);
    segmentMetadata = segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 10);
    campaignMetadata = segmentMetadata.getColumnMetadataFor("campaign");
    assertEquals(campaignMetadata.getCardinality(), 3);
    assertEquals(campaignMetadata.getMinValue(), "");
    assertEquals(campaignMetadata.getMaxValue(), "xyz");
    clicksMetadata = segmentMetadata.getColumnMetadataFor("clicks");
    assertEquals(clicksMetadata.getCardinality(), 6);
    assertEquals(clicksMetadata.getMinValue(), 1000);
    assertEquals(clicksMetadata.getMaxValue(), 7000);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 10);
    assertEquals(timeMetadata.getMinValue(), 1597719600000L);
    assertEquals(timeMetadata.getMaxValue(), 1597892400000L);
    campaignDataSource = segment.getDataSource("campaign");
    NullValueVectorReader campaignNullValueVector = campaignDataSource.getNullValueVector();
    assertNotNull(campaignNullValueVector);
    assertEquals(campaignNullValueVector.getNullBitmap().toArray(), new int[]{1, 5, 7});
    clicksDataSource = segment.getDataSource("clicks");
    NullValueVectorReader clicksNullValueVector = clicksDataSource.getNullValueVector();
    assertNotNull(clicksNullValueVector);
    assertEquals(clicksNullValueVector.getNullBitmap().toArray(), new int[]{2, 5});
    timeDataSource = segment.getDataSource("time");
    NullValueVectorReader timeNullValueVector = timeDataSource.getNullValueVector();
    assertNotNull(timeNullValueVector);
    assertTrue(timeNullValueVector.getNullBitmap().isEmpty());
    assertEquals(segmentMetadata.getName(), "myTable_1597719600000_1597892400000_0");
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);

    // Time filter
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
        new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597795200000L, 1597881600000L).build())
        .build();
    framework = new SegmentProcessorFramework(_singleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    segmentMetadata = new SegmentMetadataImpl(files[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 5);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597878000000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597795200000_1597878000000_0");
    FileUtils.cleanDirectory(outputSegmentDir);

    // Time filter - filtered everything
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
        new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597968000000L, 1598054400000L).build())
        .build();
    framework = new SegmentProcessorFramework(_singleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 0);
    FileUtils.cleanDirectory(outputSegmentDir);

    // Time round
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema)
        .setTimeHandlerConfig(new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setRoundBucketMs(86400000).build())
        .build();
    framework = new SegmentProcessorFramework(_singleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    segmentMetadata = new SegmentMetadataImpl(files[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 10);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 3);
    assertEquals(timeMetadata.getMinValue(), 1597708800000L);
    assertEquals(timeMetadata.getMaxValue(), 1597881600000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597708800000_1597881600000_0");
    FileUtils.cleanDirectory(outputSegmentDir);

    // Time partition
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
        new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setPartitionBucketMs(86400000).build()).build();
    framework = new SegmentProcessorFramework(_singleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 3);
    Arrays.sort(files);
    // segment 0
    segmentMetadata = new SegmentMetadataImpl(files[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 3);
    assertEquals(timeMetadata.getMinValue(), 1597719600000L);
    assertEquals(timeMetadata.getMaxValue(), 1597777200000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597719600000_1597777200000_0");
    // segment 1
    segmentMetadata = new SegmentMetadataImpl(files[1]);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 5);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597878000000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597795200000_1597878000000_1");
    // segment 2
    segmentMetadata = new SegmentMetadataImpl(files[2]);
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 2);
    assertEquals(timeMetadata.getMinValue(), 1597881600000L);
    assertEquals(timeMetadata.getMaxValue(), 1597892400000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597881600000_1597892400000_2");
    FileUtils.cleanDirectory(outputSegmentDir);

    // Time filter, round, partition, rollup - null value enabled
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schema)
        .setTimeHandlerConfig(
            new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597708800000L, 1597881600000L)
                .setRoundBucketMs(86400000).setPartitionBucketMs(86400000).build()).setMergeType(MergeType.ROLLUP)
        .build();
    framework = new SegmentProcessorFramework(_singleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 2);
    Arrays.sort(files);
    // segment 0
    segment = ImmutableSegmentLoader.load(files[0], ReadMode.mmap);
    segmentMetadata = segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    campaignMetadata = segmentMetadata.getColumnMetadataFor("campaign");
    assertEquals(campaignMetadata.getCardinality(), 2);
    assertEquals(campaignMetadata.getMinValue(), "");
    assertEquals(campaignMetadata.getMaxValue(), "abc");
    clicksMetadata = segmentMetadata.getColumnMetadataFor("clicks");
    assertEquals(clicksMetadata.getCardinality(), 2);
    assertEquals(clicksMetadata.getMinValue(), 1000);
    assertEquals(clicksMetadata.getMaxValue(), 2000);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 1);
    assertEquals(timeMetadata.getMinValue(), 1597708800000L);
    assertEquals(timeMetadata.getMaxValue(), 1597708800000L);
    campaignDataSource = segment.getDataSource("campaign");
    campaignNullValueVector = campaignDataSource.getNullValueVector();
    assertNotNull(campaignNullValueVector);
    assertEquals(campaignNullValueVector.getNullBitmap().toArray(), new int[]{0});
    clicksDataSource = segment.getDataSource("clicks");
    clicksNullValueVector = clicksDataSource.getNullValueVector();
    assertNotNull(clicksNullValueVector);
    assertTrue(clicksNullValueVector.getNullBitmap().isEmpty());
    timeDataSource = segment.getDataSource("time");
    timeNullValueVector = timeDataSource.getNullValueVector();
    assertNotNull(timeNullValueVector);
    assertTrue(timeNullValueVector.getNullBitmap().isEmpty());
    assertEquals(segmentMetadata.getName(), "myTable_1597708800000_1597708800000_0");
    segment.destroy();
    // segment 1
    segment = ImmutableSegmentLoader.load(files[1], ReadMode.mmap);
    segmentMetadata = segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    campaignMetadata = segmentMetadata.getColumnMetadataFor("campaign");
    assertEquals(campaignMetadata.getCardinality(), 3);
    assertEquals(campaignMetadata.getMinValue(), "");
    assertEquals(campaignMetadata.getMaxValue(), "xyz");
    clicksMetadata = segmentMetadata.getColumnMetadataFor("clicks");
    assertEquals(clicksMetadata.getCardinality(), 3);
    assertEquals(clicksMetadata.getMinValue(), 1000);
    assertEquals(clicksMetadata.getMaxValue(), 7000);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 1);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597795200000L);
    timeDataSource = segment.getDataSource("campaign");
    campaignNullValueVector = timeDataSource.getNullValueVector();
    assertNotNull(campaignNullValueVector);
    assertEquals(campaignNullValueVector.getNullBitmap().toArray(), new int[]{0});
    clicksDataSource = segment.getDataSource("clicks");
    clicksNullValueVector = clicksDataSource.getNullValueVector();
    assertNotNull(clicksNullValueVector);
    assertTrue(clicksNullValueVector.getNullBitmap().isEmpty());
    timeDataSource = segment.getDataSource("time");
    timeNullValueVector = timeDataSource.getNullValueVector();
    assertNotNull(timeNullValueVector);
    assertTrue(timeNullValueVector.getNullBitmap().isEmpty());
    assertEquals(segmentMetadata.getName(), "myTable_1597795200000_1597795200000_1");
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);

    // Time round, dedup
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema)
        .setTimeHandlerConfig(new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setRoundBucketMs(86400000).build())
        .setMergeType(MergeType.DEDUP).build();
    framework = new SegmentProcessorFramework(_singleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    segmentMetadata = new SegmentMetadataImpl(files[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 8);
    assertEquals(segmentMetadata.getName(), "myTable_1597708800000_1597881600000_0");
    FileUtils.cleanDirectory(outputSegmentDir);

    // Segment config
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setSegmentConfig(
        new SegmentConfig.Builder().setMaxNumRecordsPerSegment(4).setSegmentNamePrefix("myPrefix").build()).build();
    framework = new SegmentProcessorFramework(_singleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 3);
    Arrays.sort(files);
    segmentMetadata = new SegmentMetadataImpl(files[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 4);
    assertEquals(segmentMetadata.getName(), "myPrefix_1597719600000_1597795200000_0");
    segmentMetadata = new SegmentMetadataImpl(files[1]);
    assertEquals(segmentMetadata.getTotalDocs(), 4);
    assertEquals(segmentMetadata.getName(), "myPrefix_1597802400000_1597878000000_1");
    segmentMetadata = new SegmentMetadataImpl(files[2]);
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    assertEquals(segmentMetadata.getName(), "myPrefix_1597881600000_1597892400000_2");
    FileUtils.cleanDirectory(outputSegmentDir);
  }

  @Test
  public void testMultipleSegments()
      throws Exception {
    File outputSegmentDir = new File(_baseDir, "multiple_segments_output");
    FileUtils.forceMkdir(outputSegmentDir);

    // Default configs
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    SegmentProcessorFramework framework = new SegmentProcessorFramework(_multipleSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    File[] files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(files[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 10);
    assertEquals(segmentMetadata.getName(), "myTable_1597719600000_1597892400000_0");
    FileUtils.cleanDirectory(outputSegmentDir);

    // Time round, partition, rollup
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
        new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setRoundBucketMs(86400000).setPartitionBucketMs(86400000)
            .build()).setMergeType(MergeType.ROLLUP).build();
    framework = new SegmentProcessorFramework(_multipleSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 3);
    Arrays.sort(files);
    segmentMetadata = new SegmentMetadataImpl(files[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    assertEquals(segmentMetadata.getName(), "myTable_1597708800000_1597708800000_0");
    segmentMetadata = new SegmentMetadataImpl(files[1]);
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    assertEquals(segmentMetadata.getName(), "myTable_1597795200000_1597795200000_1");
    segmentMetadata = new SegmentMetadataImpl(files[2]);
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    assertEquals(segmentMetadata.getName(), "myTable_1597881600000_1597881600000_2");
    FileUtils.cleanDirectory(outputSegmentDir);
  }

  @Test
  public void testMultiValue()
      throws Exception {
    File outputSegmentDir = new File(_baseDir, "output_directory_multi_value");
    FileUtils.forceMkdir(outputSegmentDir);

    // Rollup - null value enabled
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schemaMV)
            .setMergeType(MergeType.ROLLUP).build();
    SegmentProcessorFramework framework = new SegmentProcessorFramework(_multiValueSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    File[] files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    ImmutableSegment segment = ImmutableSegmentLoader.load(files[0], ReadMode.mmap);
    SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    ColumnMetadata campaignMetadata = segmentMetadata.getColumnMetadataFor("campaign");
    assertEquals(campaignMetadata.getCardinality(), 3);
    assertEquals(campaignMetadata.getMinValue(), "");
    assertEquals(campaignMetadata.getMaxValue(), "b");
    ColumnMetadata clicksMetadata = segmentMetadata.getColumnMetadataFor("clicks");
    assertEquals(clicksMetadata.getCardinality(), 1);
    assertEquals(clicksMetadata.getMinValue(), 1000);
    assertEquals(clicksMetadata.getMaxValue(), 1000);
    ColumnMetadata timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 1);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597795200000L);
    DataSource campaignDataSource = segment.getDataSource("campaign");
    NullValueVectorReader campaignNullValueVector = campaignDataSource.getNullValueVector();
    assertNotNull(campaignNullValueVector);
    assertEquals(campaignNullValueVector.getNullBitmap().toArray(), new int[]{0});
    DataSource clicksDataSource = segment.getDataSource("clicks");
    NullValueVectorReader clicksNullValueVector = clicksDataSource.getNullValueVector();
    assertNotNull(clicksNullValueVector);
    assertTrue(clicksNullValueVector.getNullBitmap().isEmpty());
    DataSource timeDataSource = segment.getDataSource("time");
    NullValueVectorReader timeNullValueVector = timeDataSource.getNullValueVector();
    assertNotNull(timeNullValueVector);
    assertTrue(timeNullValueVector.getNullBitmap().isEmpty());
    assertEquals(segmentMetadata.getName(), "myTable_1597795200000_1597795200000_0");
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);

    // Dedup
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schemaMV)
        .setMergeType(MergeType.DEDUP).build();
    framework = new SegmentProcessorFramework(_multiValueSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    segment = ImmutableSegmentLoader.load(files[0], ReadMode.mmap);
    segmentMetadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    campaignMetadata = segmentMetadata.getColumnMetadataFor("campaign");
    assertEquals(campaignMetadata.getCardinality(), 3);
    assertEquals(campaignMetadata.getMinValue(), "");
    assertEquals(campaignMetadata.getMaxValue(), "b");
    clicksMetadata = segmentMetadata.getColumnMetadataFor("clicks");
    assertEquals(clicksMetadata.getCardinality(), 1);
    assertEquals(clicksMetadata.getMinValue(), 1000);
    assertEquals(clicksMetadata.getMaxValue(), 1000);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 1);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597795200000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597795200000_1597795200000_0");
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);
  }

  @Test
  public void testTarredSegments()
      throws Exception {
    File outputSegmentDir = new File(_baseDir, "output_directory_tarred");
    FileUtils.forceMkdir(outputSegmentDir);

    // Default configs
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    SegmentProcessorFramework framework = new SegmentProcessorFramework(_tarredSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    File[] files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(files[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 10);
    assertEquals(segmentMetadata.getName(), "myTable_1597719600000_1597892400000_0");
    FileUtils.cleanDirectory(outputSegmentDir);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(_baseDir);
  }
}
