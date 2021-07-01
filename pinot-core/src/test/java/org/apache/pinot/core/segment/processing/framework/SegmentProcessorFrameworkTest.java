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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.segment.processing.filter.RecordFilterConfig;
import org.apache.pinot.core.segment.processing.filter.RecordFilterFactory;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadata;
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
  private File _singleDaySingleSegment;
  private File _multipleDaysSingleSegment;
  private File _singleDayMultipleSegments;
  private File _multipleDaysMultipleSegments;
  private File _multiValueSegments;
  private File _tarredSegments;

  private TableConfig _tableConfig;
  private TableConfig _tableConfigNullValueEnabled;
  private Schema _schema;
  private Schema _schemaMV;

  private final List<Object[]> _rawDataSingleDay = Lists
      .newArrayList(new Object[]{"abc", 1000, 1597795200000L}, new Object[]{null, 2000, 1597795200000L},
          new Object[]{"abc", null, 1597795200000L}, new Object[]{"abc", 4000, 1597795200000L},
          new Object[]{"abc", 3000, 1597795200000L}, new Object[]{null, null, 1597795200000L},
          new Object[]{"xyz", 4000, 1597795200000L}, new Object[]{null, 1000, 1597795200000L},
          new Object[]{"abc", 7000, 1597795200000L}, new Object[]{"xyz", 6000, 1597795200000L});

  private final List<Object[]> _rawDataMultipleDays = Lists
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
    // 1. Single segment, single day
    _singleDaySingleSegment = new File(_baseDir, "input_segments_single_day_single_segment");
    createInputSegments(_singleDaySingleSegment, _rawDataSingleDay, 1, _schema);
    // 2. Single segment, multiple days
    _multipleDaysSingleSegment = new File(_baseDir, "input_segments_multiple_day_single_segment");
    createInputSegments(_multipleDaysSingleSegment, _rawDataMultipleDays, 1, _schema);
    // 3. Multiple segments, single day
    _singleDayMultipleSegments = new File(_baseDir, "input_segments_single_day_multiple_segment");
    createInputSegments(_singleDayMultipleSegments, _rawDataSingleDay, 3, _schema);
    // 4. Multiple segments, multiple days
    _multipleDaysMultipleSegments = new File(_baseDir, "input_segments_multiple_day_multiple_segment");
    createInputSegments(_multipleDaysMultipleSegments, _rawDataMultipleDays, 3, _schema);
    // 5. Multi value
    _multiValueSegments = new File(_baseDir, "multi_value_segment");
    createInputSegments(_multiValueSegments, _rawDataMultiValue, 1, _schemaMV);
    // 6. tarred segments
    _tarredSegments = new File(_baseDir, "tarred_segment");
    createInputSegments(_tarredSegments, _rawDataSingleDay, 3, _schema);
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
      new SegmentProcessorFramework(_singleDaySingleSegment, config, nonExistent);
      fail("Should fail for non existent output dir");
    } catch (IllegalStateException e) {
      // expected
    }

    // file used as output dir
    try {
      new SegmentProcessorFramework(_singleDaySingleSegment, config, fileInput);
      fail("Should fail for file used as output dir");
    } catch (IllegalStateException e) {
      // expected
    }

    // output dir not empty
    try {
      new SegmentProcessorFramework(fileInput, config, _singleDaySingleSegment);
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
  public void testSingleDaySingleSegment()
      throws Exception {
    File outputSegmentDir = new File(_baseDir, "output_directory_single_day_single_segment");
    FileUtils.forceMkdir(outputSegmentDir);

    // default configs
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    SegmentProcessorFramework framework =
        new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    File[] files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    ImmutableSegment segment = ImmutableSegmentLoader.load(files[0], ReadMode.mmap);
    SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
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
    assertEquals(timeMetadata.getCardinality(), 1);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597795200000L);
    DataSource campaignDataSource = segment.getDataSource("campaign");
    assertNull(campaignDataSource.getNullValueVector());
    DataSource clicksDataSource = segment.getDataSource("clicks");
    assertNull(clicksDataSource.getNullValueVector());
    DataSource timeDataSource = segment.getDataSource("time");
    assertNull(timeDataSource.getNullValueVector());
    segment.destroy();
    assertEquals(files[0].getName(), "myTable_1597795200000_1597795200000_0");
    FileUtils.cleanDirectory(outputSegmentDir);

    // default configs - null value enabled
    config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schema).build();
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    segment = ImmutableSegmentLoader.load(files[0], ReadMode.mmap);
    segmentMetadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
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
    assertEquals(timeMetadata.getCardinality(), 1);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597795200000L);
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
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);

    // partition - null value enabled
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schema)
        .setPartitionerConfigs(Lists.newArrayList(
            new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.ROUND_ROBIN)
                .setNumPartitions(2).build())).build();
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 2);
    Arrays.sort(files);
    // segment 0
    segment = ImmutableSegmentLoader.load(files[0], ReadMode.mmap);
    assertEquals(segment.getSegmentMetadata().getTotalDocs(), 5);
    campaignDataSource = segment.getDataSource("campaign");
    campaignNullValueVector = campaignDataSource.getNullValueVector();
    assertNotNull(campaignNullValueVector);
    assertTrue(campaignNullValueVector.getNullBitmap().isEmpty());
    clicksDataSource = segment.getDataSource("clicks");
    clicksNullValueVector = clicksDataSource.getNullValueVector();
    assertNotNull(clicksNullValueVector);
    assertEquals(clicksNullValueVector.getNullBitmap().toArray(), new int[]{1});
    segment.destroy();
    // segment 1
    segment = ImmutableSegmentLoader.load(files[1], ReadMode.mmap);
    assertEquals(segment.getSegmentMetadata().getTotalDocs(), 5);
    campaignDataSource = segment.getDataSource("campaign");
    campaignNullValueVector = campaignDataSource.getNullValueVector();
    assertNotNull(campaignNullValueVector);
    assertEquals(campaignNullValueVector.getNullBitmap().toArray(), new int[]{0, 2, 3});
    clicksDataSource = segment.getDataSource("clicks");
    clicksNullValueVector = clicksDataSource.getNullValueVector();
    assertNotNull(clicksNullValueVector);
    assertEquals(clicksNullValueVector.getNullBitmap().toArray(), new int[]{2});
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);

    // record filtering
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setRecordFilterConfig(
        new RecordFilterConfig.Builder().setRecordFilterType(RecordFilterFactory.RecordFilterType.FILTER_FUNCTION)
            .setFilterFunction("Groovy({campaign == \"abc\"}, campaign)").build()).build();
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    assertEquals(new SegmentMetadataImpl(files[0]).getTotalDocs(), 5);
    FileUtils.cleanDirectory(outputSegmentDir);

    // filtered everything
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setRecordFilterConfig(
        new RecordFilterConfig.Builder().setRecordFilterType(RecordFilterFactory.RecordFilterType.FILTER_FUNCTION)
            .setFilterFunction("Groovy({clicks > 0}, clicks)").build()).build();
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 0);
    FileUtils.cleanDirectory(outputSegmentDir);

    // record transformation - null value enabled
    Map<String, String> recordTransformationMap = new HashMap<>();
    recordTransformationMap.put("clicks", "times(clicks, 0)");
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schema)
        .setRecordTransformerConfig(
            new RecordTransformerConfig.Builder().setTransformFunctionsMap(recordTransformationMap).build()).build();
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    segment = ImmutableSegmentLoader.load(files[0], ReadMode.mmap);
    segmentMetadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 10);
    clicksMetadata = segmentMetadata.getColumnMetadataFor("clicks");
    assertEquals(clicksMetadata.getCardinality(), 1);
    assertEquals(clicksMetadata.getMinValue(), 0);
    assertEquals(clicksMetadata.getMaxValue(), 0);
    clicksDataSource = segment.getDataSource("clicks");
    clicksNullValueVector = clicksDataSource.getNullValueVector();
    assertNotNull(clicksNullValueVector);
    assertEquals(clicksNullValueVector.getNullBitmap().toArray(), new int[]{2, 5});
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);

    // rollup
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema)
        .setMergeType(MergeType.ROLLUP).build();
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    segment = ImmutableSegmentLoader.load(files[0], ReadMode.mmap);
    segmentMetadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    campaignMetadata = segmentMetadata.getColumnMetadataFor("campaign");
    assertEquals(campaignMetadata.getCardinality(), 3);
    assertEquals(campaignMetadata.getMinValue(), "");
    assertEquals(campaignMetadata.getMaxValue(), "xyz");
    clicksMetadata = segmentMetadata.getColumnMetadataFor("clicks");
    assertEquals(clicksMetadata.getCardinality(), 3);
    assertEquals(clicksMetadata.getMinValue(), 4000);
    assertEquals(clicksMetadata.getMaxValue(), 16000);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 1);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597795200000L);
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);

    // rollup - null value enabled
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schema)
        .setMergeType(MergeType.ROLLUP).build();
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    segment = ImmutableSegmentLoader.load(files[0], ReadMode.mmap);
    segmentMetadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    campaignMetadata = segmentMetadata.getColumnMetadataFor("campaign");
    assertEquals(campaignMetadata.getCardinality(), 3);
    assertEquals(campaignMetadata.getMinValue(), "");
    assertEquals(campaignMetadata.getMaxValue(), "xyz");
    clicksMetadata = segmentMetadata.getColumnMetadataFor("clicks");
    assertEquals(clicksMetadata.getCardinality(), 3);
    assertEquals(clicksMetadata.getMinValue(), 3000);
    assertEquals(clicksMetadata.getMaxValue(), 15000);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 1);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597795200000L);
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
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);

    // dedup
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema)
        .setMergeType(MergeType.DEDUP).build();
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    assertEquals(new SegmentMetadataImpl(files[0]).getTotalDocs(), 8);
    FileUtils.cleanDirectory(outputSegmentDir);

    // segment config
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setSegmentConfig(
        new SegmentConfig.Builder().setMaxNumRecordsPerSegment(4).setSegmentNamePrefix("myPrefix").build()).build();
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 3);
    Arrays.sort(files);
    assertEquals(new SegmentMetadataImpl(files[0]).getTotalDocs(), 4);
    assertEquals(new SegmentMetadataImpl(files[1]).getTotalDocs(), 4);
    assertEquals(new SegmentMetadataImpl(files[2]).getTotalDocs(), 2);
    assertEquals(files[0].getName(), "myPrefix_1597795200000_1597795200000_0");
    assertEquals(files[1].getName(), "myPrefix_1597795200000_1597795200000_1");
    assertEquals(files[2].getName(), "myPrefix_1597795200000_1597795200000_2");
    FileUtils.cleanDirectory(outputSegmentDir);
  }

  @Test
  public void testMultipleDaysSingleSegment()
      throws Exception {
    File outputSegmentDir = new File(_baseDir, "output_directory_multiple_days_single_segment");
    FileUtils.forceMkdir(outputSegmentDir);

    // default configs
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    SegmentProcessorFramework framework =
        new SegmentProcessorFramework(_multipleDaysSingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    File[] files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    assertEquals(new SegmentMetadataImpl(files[0]).getTotalDocs(), 10);
    FileUtils.cleanDirectory(outputSegmentDir);

    // date partition
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setPartitionerConfigs(
        Lists.newArrayList(
            new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
                .setColumnName("time").build())).build();
    framework = new SegmentProcessorFramework(_multipleDaysSingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 10);
    for (File file : files) {
      assertEquals(new SegmentMetadataImpl(file).getTotalDocs(), 1);
    }
    FileUtils.cleanDirectory(outputSegmentDir);

    // date partition - round
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setPartitionerConfigs(
        Lists.newArrayList(
            new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TRANSFORM_FUNCTION)
                .setTransformFunction("round(\"time\", 86400000)").build())).build();
    framework = new SegmentProcessorFramework(_multipleDaysSingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 3);
    Arrays.sort(files);
    // segment 0
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(files[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    ColumnMetadata timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 3);
    assertEquals(timeMetadata.getMinValue(), 1597719600000L);
    assertEquals(timeMetadata.getMaxValue(), 1597777200000L);
    // segment 1
    segmentMetadata = new SegmentMetadataImpl(files[1]);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 5);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597878000000L);
    // segment 2
    segmentMetadata = new SegmentMetadataImpl(files[2]);
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 2);
    assertEquals(timeMetadata.getMinValue(), 1597881600000L);
    assertEquals(timeMetadata.getMaxValue(), 1597892400000L);
    FileUtils.cleanDirectory(outputSegmentDir);

    // round, date partition, rollup - null value enabled
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schema)
        .setMergeType(MergeType.ROLLUP).setRecordTransformerConfig(new RecordTransformerConfig.Builder()
            .setTransformFunctionsMap(Collections.singletonMap("time", "round(\"time\", 86400000)")).build())
        .setPartitionerConfigs(Lists.newArrayList(
            new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
                .setColumnName("time").build())).build();
    framework = new SegmentProcessorFramework(_multipleDaysSingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 3);
    Arrays.sort(files);
    // segment 0
    ImmutableSegment segment = ImmutableSegmentLoader.load(files[0], ReadMode.mmap);
    segmentMetadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    ColumnMetadata campaignMetadata = segmentMetadata.getColumnMetadataFor("campaign");
    assertEquals(campaignMetadata.getCardinality(), 2);
    assertEquals(campaignMetadata.getMinValue(), "");
    assertEquals(campaignMetadata.getMaxValue(), "abc");
    ColumnMetadata clicksMetadata = segmentMetadata.getColumnMetadataFor("clicks");
    assertEquals(clicksMetadata.getCardinality(), 2);
    assertEquals(clicksMetadata.getMinValue(), 1000);
    assertEquals(clicksMetadata.getMaxValue(), 2000);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 1);
    assertEquals(timeMetadata.getMinValue(), 1597708800000L);
    assertEquals(timeMetadata.getMaxValue(), 1597708800000L);
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
    segment.destroy();
    // segment 1
    segment = ImmutableSegmentLoader.load(files[1], ReadMode.mmap);
    segmentMetadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
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
    segment.destroy();
    // segment 2
    segment = ImmutableSegmentLoader.load(files[2], ReadMode.mmap);
    segmentMetadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    campaignMetadata = segmentMetadata.getColumnMetadataFor("campaign");
    assertEquals(campaignMetadata.getCardinality(), 2);
    assertEquals(campaignMetadata.getMinValue(), "abc");
    assertEquals(campaignMetadata.getMaxValue(), "xyz");
    clicksMetadata = segmentMetadata.getColumnMetadataFor("clicks");
    assertEquals(clicksMetadata.getCardinality(), 2);
    assertEquals(clicksMetadata.getMinValue(), 6000);
    assertEquals(clicksMetadata.getMaxValue(), 7000);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 1);
    assertEquals(timeMetadata.getMinValue(), 1597881600000L);
    assertEquals(timeMetadata.getMaxValue(), 1597881600000L);
    timeDataSource = segment.getDataSource("campaign");
    campaignNullValueVector = timeDataSource.getNullValueVector();
    assertNotNull(campaignNullValueVector);
    assertTrue(campaignNullValueVector.getNullBitmap().isEmpty());
    clicksDataSource = segment.getDataSource("clicks");
    clicksNullValueVector = clicksDataSource.getNullValueVector();
    assertNotNull(clicksNullValueVector);
    assertTrue(clicksNullValueVector.getNullBitmap().isEmpty());
    timeDataSource = segment.getDataSource("time");
    timeNullValueVector = timeDataSource.getNullValueVector();
    assertNotNull(timeNullValueVector);
    assertTrue(timeNullValueVector.getNullBitmap().isEmpty());
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);
  }

  @Test
  public void testSingleDayMultipleSegments()
      throws Exception {
    File outputSegmentDir = new File(_baseDir, "output_directory_single_day_multiple_segments");
    FileUtils.forceMkdir(outputSegmentDir);

    // default configs
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    SegmentProcessorFramework framework =
        new SegmentProcessorFramework(_singleDayMultipleSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    File[] files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    assertEquals(new SegmentMetadataImpl(files[0]).getTotalDocs(), 10);
    FileUtils.cleanDirectory(outputSegmentDir);

    // rollup
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema)
        .setMergeType(MergeType.ROLLUP).build();
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    framework = new SegmentProcessorFramework(_singleDayMultipleSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    assertEquals(new SegmentMetadataImpl(files[0]).getTotalDocs(), 3);
    FileUtils.cleanDirectory(outputSegmentDir);
  }

  @Test
  public void testMultipleDaysMultipleSegments()
      throws Exception {
    File outputSegmentDir = new File(_baseDir, "output_directory_multiple_days_multiple_segments");
    FileUtils.forceMkdir(outputSegmentDir);

    // default configs
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    SegmentProcessorFramework framework =
        new SegmentProcessorFramework(_multipleDaysMultipleSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    File[] files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    assertEquals(new SegmentMetadataImpl(files[0]).getTotalDocs(), 10);
    FileUtils.cleanDirectory(outputSegmentDir);

    // round, date partition, rollup
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema)
        .setMergeType(MergeType.ROLLUP).setRecordTransformerConfig(new RecordTransformerConfig.Builder()
            .setTransformFunctionsMap(Collections.singletonMap("time", "round(\"time\", 86400000)")).build())
        .setPartitionerConfigs(Lists.newArrayList(
            new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
                .setColumnName("time").build())).build();
    framework = new SegmentProcessorFramework(_multipleDaysMultipleSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 3);
    Arrays.sort(files);
    assertEquals(new SegmentMetadataImpl(files[0]).getTotalDocs(), 2);
    assertEquals(new SegmentMetadataImpl(files[1]).getTotalDocs(), 3);
    assertEquals(new SegmentMetadataImpl(files[2]).getTotalDocs(), 2);
    FileUtils.cleanDirectory(outputSegmentDir);
  }

  @Test
  public void testMultiValue()
      throws Exception {
    File outputSegmentDir = new File(_baseDir, "output_directory_multi_value");
    FileUtils.forceMkdir(outputSegmentDir);

    // rollup - null value enabled
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
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);

    // dedup
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
    segment.destroy();
    FileUtils.cleanDirectory(outputSegmentDir);
  }

  @Test
  public void testTarredSegments()
      throws Exception {
    File outputSegmentDir = new File(_baseDir, "output_directory_tarred");
    FileUtils.forceMkdir(outputSegmentDir);

    // default configs
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    SegmentProcessorFramework framework = new SegmentProcessorFramework(_tarredSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    File[] files = outputSegmentDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    assertEquals(new SegmentMetadataImpl(files[0]).getTotalDocs(), 10);
    FileUtils.cleanDirectory(outputSegmentDir);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(_baseDir);
  }
}
