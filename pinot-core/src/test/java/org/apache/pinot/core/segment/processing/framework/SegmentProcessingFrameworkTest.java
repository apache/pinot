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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.processing.collector.CollectorConfig;
import org.apache.pinot.core.segment.processing.collector.CollectorFactory;
import org.apache.pinot.core.segment.processing.filter.RecordFilterConfig;
import org.apache.pinot.core.segment.processing.filter.RecordFilterFactory;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * End-to-end tests for SegmentProcessorFramework
 */
public class SegmentProcessingFrameworkTest {

  private File _baseDir;
  private File _emptyInputDir;
  private File _singleDaySingleSegment;
  private File _multipleDaysSingleSegment;
  private File _singleDayMultipleSegments;
  private File _multipleDaysMultipleSegments;
  private File _multiValueSegments;
  private File _tarredSegments;
  private Schema _pinotSchema;
  private Schema _pinotSchemaMV;
  private TableConfig _tableConfig;
  private final List<Object[]> _rawDataMultipleDays = Lists
      .newArrayList(new Object[]{"abc", 1000, 1597719600000L}, new Object[]{"pqr", 2000, 1597773600000L},
          new Object[]{"abc", 1000, 1597777200000L}, new Object[]{"abc", 4000, 1597795200000L},
          new Object[]{"abc", 3000, 1597802400000L}, new Object[]{"pqr", 1000, 1597838400000L},
          new Object[]{"xyz", 4000, 1597856400000L}, new Object[]{"pqr", 1000, 1597878000000L},
          new Object[]{"abc", 7000, 1597881600000L}, new Object[]{"xyz", 6000, 1597892400000L});

  private final List<Object[]> _rawDataSingleDay = Lists
      .newArrayList(new Object[]{"abc", 1000, 1597795200000L}, new Object[]{"pqr", 2000, 1597795200000L},
          new Object[]{"abc", 1000, 1597795200000L}, new Object[]{"abc", 4000, 1597795200000L},
          new Object[]{"abc", 3000, 1597795200000L}, new Object[]{"pqr", 1000, 1597795200000L},
          new Object[]{"xyz", 4000, 1597795200000L}, new Object[]{"pqr", 1000, 1597795200000L},
          new Object[]{"abc", 7000, 1597795200000L}, new Object[]{"xyz", 6000, 1597795200000L});

  private final List<Object[]> _multiValue = Lists
      .newArrayList(new Object[]{new String[]{"a", "b"}, 1000, 1597795200000L},
          new Object[]{new String[]{"a"}, 1000, 1597795200000L}, new Object[]{new String[]{"a"}, 1000, 1597795200000L},
          new Object[]{new String[]{"a", "b"}, 1000, 1597795200000L});

  @BeforeClass
  public void setup()
      throws Exception {
    _tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("timeValue").build();
    _pinotSchema = new Schema.SchemaBuilder().setSchemaName("mySchema")
        .addSingleValueDimension("campaign", FieldSpec.DataType.STRING).addMetric("clicks", FieldSpec.DataType.INT)
        .addDateTime("timeValue", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    _pinotSchemaMV = new Schema.SchemaBuilder().setSchemaName("mySchema")
        .addMultiValueDimension("campaign", FieldSpec.DataType.STRING).addMetric("clicks", FieldSpec.DataType.INT)
        .addDateTime("timeValue", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

    _baseDir = new File(FileUtils.getTempDirectory(), "segment_processor_framework_test_" + System.currentTimeMillis());
    FileUtils.deleteQuietly(_baseDir);
    assertTrue(_baseDir.mkdirs());

    // create segments in many folders
    _emptyInputDir = new File(_baseDir, "empty_input");
    assertTrue(_emptyInputDir.mkdirs());
    // 1. Single segment, single day
    _singleDaySingleSegment = new File(_baseDir, "input_segments_single_day_single_segment");
    createInputSegments(_singleDaySingleSegment, _rawDataSingleDay, 1, _pinotSchema);
    // 2. Single segment, multiple days
    _multipleDaysSingleSegment = new File(_baseDir, "input_segments_multiple_day_single_segment");
    createInputSegments(_multipleDaysSingleSegment, _rawDataMultipleDays, 1, _pinotSchema);
    // 3. Multiple segments single day
    _singleDayMultipleSegments = new File(_baseDir, "input_segments_single_day_multiple_segment");
    createInputSegments(_singleDayMultipleSegments, _rawDataSingleDay, 3, _pinotSchema);
    // 4. Multiple segments, multiple days
    _multipleDaysMultipleSegments = new File(_baseDir, "input_segments_multiple_day_multiple_segment");
    createInputSegments(_multipleDaysMultipleSegments, _rawDataMultipleDays, 3, _pinotSchema);
    // 5. Multi value
    _multiValueSegments = new File(_baseDir, "multi_value_segment");
    createInputSegments(_multiValueSegments, _multiValue, 1, _pinotSchemaMV);
    // 6. tarred segments
    _tarredSegments = new File(_baseDir, "tarred_segment");
    createInputSegments(_tarredSegments, _rawDataSingleDay, 3, _pinotSchema);
    File[] segmentDirs = _tarredSegments.listFiles();
    for (File segmentDir : segmentDirs) {
      TarGzCompressionUtils.createTarGzFile(segmentDir, new File(_tarredSegments, segmentDir.getName() + ".tar.gz"));
      FileUtils.deleteQuietly(segmentDir);
    }
  }

  private void createInputSegments(File inputDir, List<Object[]> rawData, int numSegments, Schema pinotSchema)
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
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfig, pinotSchema);
      segmentGeneratorConfig.setTableName(_tableConfig.getTableName());
      segmentGeneratorConfig.setOutDir(inputDir.getAbsolutePath());
      segmentGeneratorConfig.setSequenceId(idx++);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(segmentGeneratorConfig, recordReader);
      driver.build();
    }

    assertEquals(inputDir.listFiles().length, numSegments);
  }

  private GenericRow getGenericRow(Object[] rawRow) {
    GenericRow row = new GenericRow();
    row.putValue("campaign", rawRow[0]);
    row.putValue("clicks", rawRow[1]);
    row.putValue("timeValue", rawRow[2]);
    return row;
  }

  @Test
  public void testBadInputFolders()
      throws Exception {
    SegmentProcessorConfig config;

    try {
      new SegmentProcessorConfig.Builder().setSchema(_pinotSchema).build();
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

    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema).build();

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

    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema).build();
    // Single day, Single segment
    File outputSegmentDir = new File(_baseDir, "output_directory_single_day_single_segment");

    // default configs
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    SegmentProcessorFramework framework =
        new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 1);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(outputSegmentDir.listFiles()[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 10);

    // partitioning
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema)
        .setPartitionerConfigs(Lists.newArrayList(
            new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.ROUND_ROBIN)
                .setNumPartitions(3).build())).build();
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 3);
    int totalDocs = 0;
    for (File segment : outputSegmentDir.listFiles()) {
      segmentMetadata = new SegmentMetadataImpl(segment);
      totalDocs += segmentMetadata.getTotalDocs();
      assertTrue(segmentMetadata.getTotalDocs() == 3 || segmentMetadata.getTotalDocs() == 4);
    }
    assertEquals(totalDocs, 10);

    // record filtering
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema)
        .setRecordFilterConfig(
            new RecordFilterConfig.Builder().setRecordFilterType(RecordFilterFactory.RecordFilterType.FILTER_FUNCTION)
                .setFilterFunction("Groovy({campaign == \"abc\"}, campaign)").build()).build();
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 1);
    segmentMetadata = new SegmentMetadataImpl(outputSegmentDir.listFiles()[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 5);

    // filtered everything
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema)
        .setRecordFilterConfig(
            new RecordFilterConfig.Builder().setRecordFilterType(RecordFilterFactory.RecordFilterType.FILTER_FUNCTION)
                .setFilterFunction("Groovy({clicks > 0}, clicks)").build()).build();
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    try {
      framework.processSegments();
      fail("Should fail for empty map output");
    } catch (IllegalStateException e) {
      framework.cleanup();
    }

    // record transformation
    Map<String, String> recordTransformationMap = new HashMap<>();
    recordTransformationMap.put("clicks", "times(clicks, 0)");
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema)
        .setRecordTransformerConfig(
            new RecordTransformerConfig.Builder().setTransformFunctionsMap(recordTransformationMap).build()).build();
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 1);
    segmentMetadata = new SegmentMetadataImpl(outputSegmentDir.listFiles()[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 10);
    assertEquals(segmentMetadata.getColumnMetadataFor("clicks").getCardinality(), 1);

    // collection
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema)
        .setCollectorConfig(
            new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP).build()).build();
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 1);
    segmentMetadata = new SegmentMetadataImpl(outputSegmentDir.listFiles()[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    assertEquals(segmentMetadata.getColumnMetadataFor("campaign").getCardinality(), 3);

    // segment config
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema)
        .setSegmentConfig(new SegmentConfig.Builder().setMaxNumRecordsPerSegment(4).build()).build();
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    framework = new SegmentProcessorFramework(_singleDaySingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 3);
    totalDocs = 0;
    for (File segment : outputSegmentDir.listFiles()) {
      segmentMetadata = new SegmentMetadataImpl(segment);
      totalDocs += segmentMetadata.getTotalDocs();
      assertTrue(segmentMetadata.getTotalDocs() == 4 || segmentMetadata.getTotalDocs() == 2);
    }
    assertEquals(totalDocs, 10);
  }

  @Test
  public void testMultipleDaysSingleSegment()
      throws Exception {
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema).build();

    // Multiple day, Single segment
    File outputSegmentDir = new File(_baseDir, "output_directory_multiple_days_single_segment");

    // default configs
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    SegmentProcessorFramework framework =
        new SegmentProcessorFramework(_multipleDaysSingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 1);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(outputSegmentDir.listFiles()[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 10);

    // date partition
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema)
        .setPartitionerConfigs(Lists.newArrayList(
            new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
                .setColumnName("timeValue").build())).build();
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    framework = new SegmentProcessorFramework(_multipleDaysSingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 10);
    for (File segment : outputSegmentDir.listFiles()) {
      segmentMetadata = new SegmentMetadataImpl(segment);
      assertEquals(segmentMetadata.getTotalDocs(), 1);
    }
  }

  @Test
  public void testSingleDayMultipleSegments()
      throws Exception {
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema).build();

    // Single day, multiple segments
    File outputSegmentDir = new File(_baseDir, "output_directory_single_day_multiple_segments");

    // default configs
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    SegmentProcessorFramework framework =
        new SegmentProcessorFramework(_singleDayMultipleSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 1);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(outputSegmentDir.listFiles()[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 10);

    // collection
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema)
        .setCollectorConfig(
            new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP).build()).build();
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    framework = new SegmentProcessorFramework(_singleDayMultipleSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 1);
    segmentMetadata = new SegmentMetadataImpl(outputSegmentDir.listFiles()[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    assertEquals(segmentMetadata.getColumnMetadataFor("campaign").getCardinality(), 3);
  }

  @Test
  public void testMultipleDaysMultipleSegments()
      throws Exception {
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema).build();

    // Multiple day, multiple segments
    File outputSegmentDir = new File(_baseDir, "output_directory_multiple_days_multiple_segments");

    // default configs
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    SegmentProcessorFramework framework =
        new SegmentProcessorFramework(_multipleDaysMultipleSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 1);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(outputSegmentDir.listFiles()[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 10);

    // date partition
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema)
        .setPartitionerConfigs(Lists.newArrayList(
            new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TRANSFORM_FUNCTION)
                .setTransformFunction("round(timeValue, 86400000)").build())).build();
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    framework = new SegmentProcessorFramework(_multipleDaysSingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 3);
    int totalDocs = 0;
    for (File segment : outputSegmentDir.listFiles()) {
      segmentMetadata = new SegmentMetadataImpl(segment);
      totalDocs += segmentMetadata.getTotalDocs();
    }
    assertEquals(totalDocs, 10);

    // round date, partition, collect
    HashMap<String, String> recordTransformationMap = new HashMap<>();
    recordTransformationMap.put("timeValue", "round(timeValue, 86400000)");
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema)
        .setRecordTransformerConfig(
            new RecordTransformerConfig.Builder().setTransformFunctionsMap(recordTransformationMap).build())
        .setPartitionerConfigs(Lists.newArrayList(
            new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
                .setColumnName("timeValue").build())).setCollectorConfig(
            new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP).build()).build();
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    framework = new SegmentProcessorFramework(_multipleDaysSingleSegment, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 3);
    totalDocs = 0;
    for (File segment : outputSegmentDir.listFiles()) {
      segmentMetadata = new SegmentMetadataImpl(segment);
      totalDocs += segmentMetadata.getTotalDocs();
      assertEquals(segmentMetadata.getColumnMetadataFor("timeValue").getCardinality(), 1);
    }
    assertTrue(totalDocs < 10);
  }

  @Test
  public void testMultiValue()
      throws Exception {
    // Multi-value
    File outputSegmentDir = new File(_baseDir, "output_directory_multivalue");

    // collection
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchemaMV).setCollectorConfig(
            new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP).build()).build();
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    SegmentProcessorFramework framework = new SegmentProcessorFramework(_multiValueSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 1);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(outputSegmentDir.listFiles()[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 2);
  }

  @Test
  public void testTarredSegments()
      throws Exception {
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_pinotSchema).build();
    File outputSegmentDir = new File(_baseDir, "output_directory_tarred_seg");

    // default configs
    FileUtils.deleteQuietly(outputSegmentDir);
    assertTrue(outputSegmentDir.mkdirs());
    SegmentProcessorFramework framework = new SegmentProcessorFramework(_tarredSegments, config, outputSegmentDir);
    framework.processSegments();
    framework.cleanup();
    assertEquals(outputSegmentDir.listFiles().length, 1);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(outputSegmentDir.listFiles()[0]);
    assertEquals(segmentMetadata.getTotalDocs(), 10);
  }

  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(_baseDir);
  }
}
