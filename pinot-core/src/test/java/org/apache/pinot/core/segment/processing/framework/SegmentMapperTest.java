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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.core.segment.processing.mapper.SegmentMapper;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandler;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandlerConfig;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFileConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link SegmentMapper}
 */
public class SegmentMapperTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentMapperTest");

  private static final TableConfigBuilder TABLE_CONFIG_BUILDER =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("ts")
          .setNullHandlingEnabled(true).setFieldConfigList(Collections.singletonList(
              new FieldConfig("ts", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TIMESTAMP, null, null,
                  new TimestampConfig(Collections.singletonList(TimestampIndexGranularity.DAY)), null)));
  private static final Schema.SchemaBuilder SCHEMA_BUILDER =
      new Schema.SchemaBuilder().setSchemaName("myTable").addSingleValueDimension("campaign", DataType.STRING, "xyz")
          .addMetric("clicks", DataType.INT).addDateTime("ts", DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS");
  private static final List<Object[]> RAW_DATA =
      Arrays.asList(new Object[]{"abc", 1000, 1597719600000L}, new Object[]{"pqr", 2000, 1597773600000L},
          new Object[]{"abc", 1000, 1597777200000L}, new Object[]{"abc", 4000, 1597795200000L},
          new Object[]{"abc", 3000, 1597802400000L}, new Object[]{"pqr", 1000, 1597838400000L},
          new Object[]{null, 4000, 1597856400000L}, new Object[]{"pqr", 1000, 1597878000000L},
          new Object[]{"abc", 7000, 1597881600000L}, new Object[]{null, 6000, 1597892400000L});

  public TableConfig getTableConfig() {
    return TABLE_CONFIG_BUILDER.build();
  }

  public Schema getSchema() {
    return SCHEMA_BUILDER.build();
  }

  public List<Object[]> getRawData() {
    return RAW_DATA;
  }

  private File _indexDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    assertTrue(TEMP_DIR.mkdirs());

    // Segment directory
    File inputSegmentDir = new File(TEMP_DIR, "input_segment");
    assertTrue(inputSegmentDir.mkdirs());

    // Create test data
    List<GenericRow> inputRows = new ArrayList<>();
    for (Object[] rawRow : getRawData()) {
      GenericRow row = new GenericRow();
      if (rawRow[0] != null) {
        row.putValue("campaign", rawRow[0]);
      } else {
        row.putDefaultNullValue("campaign", "xyz");
      }
      row.putValue("clicks", rawRow[1]);
      row.putValue("ts", rawRow[2]);
      inputRows.add(row);
    }

    // Create test segment
    RecordReader recordReader = new GenericRowRecordReader(inputRows);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(getTableConfig(), getSchema());
    segmentGeneratorConfig.setOutDir(inputSegmentDir.getAbsolutePath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, recordReader);
    driver.build();

    File[] segmentFiles = inputSegmentDir.listFiles();
    assertTrue(segmentFiles != null && segmentFiles.length == 1);
    _indexDir = segmentFiles[0];
  }

  @Test(dataProvider = "segmentMapperConfigProvider")
  public void segmentMapperTest(SegmentProcessorConfig processorConfig, Map<String, List<Object[]>> partitionToRecords)
      throws Exception {
    File mapperOutputDir = new File(TEMP_DIR, "mapper_output");
    FileUtils.deleteQuietly(mapperOutputDir);
    assertTrue(mapperOutputDir.mkdirs());

    PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader();
    segmentRecordReader.init(_indexDir, null, null, true);
    SegmentMapper segmentMapper =
        new SegmentMapper(Collections.singletonList(new RecordReaderFileConfig(segmentRecordReader)),
            Collections.emptyList(), processorConfig, mapperOutputDir);
    Map<String, GenericRowFileManager> partitionToFileManagerMap = segmentMapper.map(1);
    segmentRecordReader.close();

    assertEquals(partitionToFileManagerMap.size(), partitionToRecords.size());
    for (Map.Entry<String, GenericRowFileManager> entry : partitionToFileManagerMap.entrySet()) {
      // Directory named after every partition
      String partition = entry.getKey();
      File partitionDir = new File(mapperOutputDir, partition);
      assertTrue(partitionDir.isDirectory());

      // Each partition directory should contain 2 files (offset & data)
      String[] fileNames = partitionDir.list();
      assertNotNull(fileNames);
      assertEquals(fileNames.length, 2);
      Arrays.sort(fileNames);
      assertEquals(fileNames[0], GenericRowFileManager.DATA_FILE_NAME);
      assertEquals(fileNames[1], GenericRowFileManager.OFFSET_FILE_NAME);

      GenericRowFileManager fileManager = entry.getValue();
      GenericRowFileReader fileReader = fileManager.getFileReader();
      int numRows = fileReader.getNumRows();
      List<Object[]> expectedRecords = partitionToRecords.get(partition);
      assertEquals(numRows, expectedRecords.size());
      GenericRow buffer = new GenericRow();
      for (int i = 0; i < numRows; i++) {
        fileReader.read(i, buffer);
        Object[] expectedValues = expectedRecords.get(i);
        assertEquals(buffer.getValue("campaign"), expectedValues[0]);
        assertEquals(buffer.getValue("clicks"), expectedValues[1]);
        assertEquals(buffer.getValue("ts"), expectedValues[2]);
        // TIMESTAMP index
        assertEquals(buffer.getValue("$ts$DAY"), (long) expectedValues[2] / 86400000 * 86400000);
        // Default null value
        if (expectedValues[0].equals("xyz")) {
          assertEquals(buffer.getNullValueFields(), Collections.singleton("campaign"));
        } else {
          assertEquals(buffer.getNullValueFields(), Collections.emptySet());
        }
        buffer.clear();
      }
      fileManager.cleanUp();
    }
  }

  /**
   * Provides several combinations of transform functions, partitioning, partition filters
   */
  @DataProvider(name = "segmentMapperConfigProvider")
  public Object[][] segmentMapperConfigProvider() {
    List<Object[]> outputData =
        Arrays.asList(new Object[]{"abc", 1000, 1597719600000L}, new Object[]{"pqr", 2000, 1597773600000L},
            new Object[]{"abc", 1000, 1597777200000L}, new Object[]{"abc", 4000, 1597795200000L},
            new Object[]{"abc", 3000, 1597802400000L}, new Object[]{"pqr", 1000, 1597838400000L},
            new Object[]{"xyz", 4000, 1597856400000L}, new Object[]{"pqr", 1000, 1597878000000L},
            new Object[]{"abc", 7000, 1597881600000L}, new Object[]{"xyz", 6000, 1597892400000L});

    List<Object[]> inputs = new ArrayList<>();

    // Default configs
    SegmentProcessorConfig config0 =
        new SegmentProcessorConfig.Builder().setTableConfig(getTableConfig()).setSchema(getSchema()).build();
    Map<String, List<Object[]>> expectedRecords0 = Collections.singletonMap("0", outputData);
    inputs.add(new Object[]{config0, expectedRecords0});

    // Round-robin partitioner
    SegmentProcessorConfig config1 =
        new SegmentProcessorConfig.Builder().setTableConfig(getTableConfig()).setSchema(getSchema())
            .setPartitionerConfigs(Collections.singletonList(
                new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.ROUND_ROBIN)
                    .setNumPartitions(3).build())).build();
    Map<String, List<Object[]>> expectedRecords1 = new HashMap<>();
    for (int i = 0; i < outputData.size(); i++) {
      expectedRecords1.computeIfAbsent("0_" + (i % 3), k -> new ArrayList<>()).add(outputData.get(i));
    }
    inputs.add(new Object[]{config1, expectedRecords1});

    // Partition by campaign
    SegmentProcessorConfig config2 =
        new SegmentProcessorConfig.Builder().setTableConfig(getTableConfig()).setSchema(getSchema())
            .setPartitionerConfigs(Collections.singletonList(
                new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
                    .setColumnName("campaign").build())).build();
    Map<String, List<Object[]>> expectedRecords2 =
        outputData.stream().collect(Collectors.groupingBy(r -> "0_" + r[0], Collectors.toList()));
    inputs.add(new Object[]{config2, expectedRecords2});

    // Transform function partition
    SegmentProcessorConfig config3 =
        new SegmentProcessorConfig.Builder().setTableConfig(getTableConfig()).setSchema(getSchema())
            .setPartitionerConfigs(Collections.singletonList(new PartitionerConfig.Builder().setPartitionerType(
                PartitionerFactory.PartitionerType.TRANSFORM_FUNCTION).setTransformFunction("toEpochDays(ts)").build()))
            .build();
    Map<String, List<Object[]>> expectedRecords3 =
        outputData.stream().collect(Collectors.groupingBy(r -> "0_" + ((long) r[2] / 86400000), Collectors.toList()));
    inputs.add(new Object[]{config3, expectedRecords3});

    // Partition by column and then table column partition config
    SegmentProcessorConfig config4 =
        new SegmentProcessorConfig.Builder().setTableConfig(getTableConfig()).setSchema(getSchema())
            .setPartitionerConfigs(Arrays.asList(
                new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
                    .setColumnName("campaign").build(), new PartitionerConfig.Builder().setPartitionerType(
                        PartitionerFactory.PartitionerType.TABLE_PARTITION_CONFIG).setColumnName("clicks")
                    .setColumnPartitionConfig(new ColumnPartitionConfig("Modulo", 3, null)).build())).build();
    Map<String, List<Object[]>> expectedRecords4 = new HashMap<>();
    for (Object[] record : outputData) {
      String partition = "0_" + record[0] + "_" + ((int) record[1] % 3);
      List<Object[]> objects = expectedRecords4.computeIfAbsent(partition, k -> new ArrayList<>());
      objects.add(record);
    }
    inputs.add(new Object[]{config4, expectedRecords4});

    // Time handling - filter out certain times
    SegmentProcessorConfig config5 =
        new SegmentProcessorConfig.Builder().setTableConfig(getTableConfig()).setSchema(getSchema())
            .setTimeHandlerConfig(
                new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597795200000L, 1597881600000L)
                    .build()).build();
    Map<String, List<Object[]>> expectedRecords5 =
        outputData.stream().filter(r -> ((long) r[2]) >= 1597795200000L && ((long) r[2]) < 1597881600000L)
            .collect(Collectors.groupingBy(r -> "0", Collectors.toList()));
    inputs.add(new Object[]{config5, expectedRecords5});

    // Time handling - round time to nearest day
    SegmentProcessorConfig config6 =
        new SegmentProcessorConfig.Builder().setTableConfig(getTableConfig()).setSchema(getSchema())
            .setTimeHandlerConfig(
                new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setRoundBucketMs(86400000).build()).build();
    Map<String, List<Object[]>> expectedRecords6 =
        outputData.stream().map(r -> new Object[]{r[0], r[1], (((long) r[2]) / 86400000) * 86400000})
            .collect(Collectors.groupingBy(r -> "0", Collectors.toList()));
    inputs.add(new Object[]{config6, expectedRecords6});

    // Time handling - partition time by day
    SegmentProcessorConfig config7 =
        new SegmentProcessorConfig.Builder().setTableConfig(getTableConfig()).setSchema(getSchema())
            .setTimeHandlerConfig(
                new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setPartitionBucketMs(86400000).build()).build();
    Map<String, List<Object[]>> expectedRecords7 = outputData.stream()
        .collect(Collectors.groupingBy(r -> Long.toString(((long) r[2]) / 86400000), Collectors.toList()));
    inputs.add(new Object[]{config7, expectedRecords7});

    // Time handling - filter out certain times, round time to nearest hour, partition by day
    SegmentProcessorConfig config8 =
        new SegmentProcessorConfig.Builder().setTableConfig(getTableConfig()).setSchema(getSchema())
            .setTimeHandlerConfig(
                new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597795200000L, 1597881600000L)
                    .setRoundBucketMs(3600000).setPartitionBucketMs(86400000).build()).build();
    Map<String, List<Object[]>> expectedRecords8 =
        outputData.stream().filter(r -> ((long) r[2]) >= 1597795200000L && ((long) r[2]) < 1597881600000L)
            .map(r -> new Object[]{r[0], r[1], (((long) r[2]) / 3600000) * 3600000})
            .collect(Collectors.groupingBy(r -> Long.toString(((long) r[2]) / 86400000), Collectors.toList()));
    inputs.add(new Object[]{config8, expectedRecords8});

    // Time handling - negate filter with certain times
    SegmentProcessorConfig config9 =
        new SegmentProcessorConfig.Builder().setTableConfig(getTableConfig()).setSchema(getSchema())
            .setTimeHandlerConfig(
                new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597795200000L, 1597881600000L)
                    .setNegateWindowFilter(true).build()).build();
    Map<String, List<Object[]>> expectedRecords9 =
        outputData.stream().filter(r -> !(((long) r[2]) >= 1597795200000L && ((long) r[2]) < 1597881600000L))
            .collect(Collectors.groupingBy(r -> "0", Collectors.toList()));
    inputs.add(new Object[]{config9, expectedRecords9});

    return inputs.toArray(new Object[0][]);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
