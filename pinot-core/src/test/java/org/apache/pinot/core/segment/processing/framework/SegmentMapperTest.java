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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.filter.RecordFilterConfig;
import org.apache.pinot.core.segment.processing.filter.RecordFilterFactory;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerConfig;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link SegmentMapper}
 */
public class SegmentMapperTest {

  private File _baseDir;
  private File _inputSegment;
  private Schema _pinotSchema;
  private final List<Object[]> _rawData = Lists
      .newArrayList(new Object[]{"abc", 1000, 1597719600000L}, new Object[]{"pqr", 2000, 1597773600000L},
          new Object[]{"abc", 1000, 1597777200000L}, new Object[]{"abc", 4000, 1597795200000L},
          new Object[]{"abc", 3000, 1597802400000L}, new Object[]{"pqr", 1000, 1597838400000L},
          new Object[]{"xyz", 4000, 1597856400000L}, new Object[]{"pqr", 1000, 1597878000000L},
          new Object[]{"abc", 7000, 1597881600000L}, new Object[]{"xyz", 6000, 1597892400000L});

  @BeforeClass
  public void before()
      throws Exception {
    _baseDir = new File(FileUtils.getTempDirectory(), "segment_mapper_test_" + System.currentTimeMillis());
    FileUtils.deleteQuietly(_baseDir);
    assertTrue(_baseDir.mkdirs());

    // Segment directory
    File inputSegmentDir = new File(_baseDir, "input_segment");
    assertTrue(inputSegmentDir.mkdirs());

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("timeValue").build();
    _pinotSchema = new Schema.SchemaBuilder().setSchemaName("mySchema")
        .addSingleValueDimension("campaign", FieldSpec.DataType.STRING).addMetric("clicks", FieldSpec.DataType.INT)
        .addDateTime("timeValue", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

    // Create test data
    List<GenericRow> inputRows = new ArrayList<>();
    for (Object[] rawRow : _rawData) {
      GenericRow row = new GenericRow();
      row.putValue("campaign", rawRow[0]);
      row.putValue("clicks", rawRow[1]);
      row.putValue("timeValue", rawRow[2]);
      inputRows.add(row);
    }

    // Create test segment
    RecordReader recordReader = new GenericRowRecordReader(inputRows);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, _pinotSchema);
    segmentGeneratorConfig.setTableName(tableConfig.getTableName());
    segmentGeneratorConfig.setOutDir(inputSegmentDir.getAbsolutePath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, recordReader);
    driver.build();

    assertEquals(inputSegmentDir.listFiles().length, 1);
    _inputSegment = inputSegmentDir.listFiles()[0];
  }

  @Test(dataProvider = "segmentMapperConfigProvider")
  public void segmentMapperTest(String mapperId, SegmentMapperConfig segmentMapperConfig,
      Map<String, List<Object[]>> partitionToRecords)
      throws Exception {

    File mapperOutputDir = new File(_baseDir, "mapper_output");
    FileUtils.deleteQuietly(mapperOutputDir);
    assertTrue(mapperOutputDir.mkdirs());
    SegmentMapper segmentMapper = new SegmentMapper(mapperId, _inputSegment, segmentMapperConfig, mapperOutputDir);
    segmentMapper.map();
    segmentMapper.cleanup();

    File[] partitionDirs = mapperOutputDir.listFiles();
    // as many directories in output as num partitions created + passed filter
    assertEquals(partitionDirs.length, partitionToRecords.size());
    for (File partitionDir : partitionDirs) {
      String partition = partitionDir.getName();
      // directory named after every partition
      assertTrue(partitionToRecords.containsKey(partition));
      // each partition directory has as many files as mapper
      File[] avroFiles = partitionDir.listFiles();
      assertEquals(avroFiles.length, 1);
      assertEquals(avroFiles[0].getName(), SegmentMapper.createMapperOutputFileName(mapperId));

      RecordReader avroRecordReader = new AvroRecordReader();
      avroRecordReader.init(avroFiles[0], _pinotSchema.getColumnNames(), null);
      int numRecords = 0;
      List<Object[]> expectedRecords = partitionToRecords.get(partition);
      GenericRow next = new GenericRow();
      while (avroRecordReader.hasNext()) {
        avroRecordReader.next(next);
        assertEquals(next.getValue("campaign"), expectedRecords.get(numRecords)[0]);
        assertEquals(next.getValue("clicks"), expectedRecords.get(numRecords)[1]);
        assertEquals(next.getValue("timeValue"), expectedRecords.get(numRecords)[2]);
        numRecords++;
      }
      assertEquals(numRecords, expectedRecords.size());
    }

    FileUtils.deleteQuietly(mapperOutputDir);
  }

  /**
   * Provides several combinations of transform functions, partitioning, partition filters
   */
  @DataProvider(name = "segmentMapperConfigProvider")
  public Object[][] segmentMapperConfigProvider() {
    String mapperId = "aMapperId";
    List<Object[]> outputData = new ArrayList<>();
    _rawData.forEach(r -> outputData.add(new Object[]{r[0], r[1], r[2]}));

    List<Object[]> inputs = new ArrayList<>();

    // default configs
    SegmentMapperConfig config1 = new SegmentMapperConfig(_pinotSchema, new RecordTransformerConfig.Builder().build(),
        new RecordFilterConfig.Builder().build(), Lists.newArrayList(new PartitionerConfig.Builder().build()));
    Map<String, List<Object[]>> expectedRecords1 = new HashMap<>();
    expectedRecords1.put("0", outputData);
    inputs.add(new Object[]{mapperId, config1, expectedRecords1});

    // round robin partitioner
    SegmentMapperConfig config12 = new SegmentMapperConfig(_pinotSchema, new RecordTransformerConfig.Builder().build(),
        new RecordFilterConfig.Builder().build(), Lists.newArrayList(
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.ROUND_ROBIN)
            .setNumPartitions(3).build()));
    Map<String, List<Object[]>> expectedRecords12 = new HashMap<>();
    IntStream.range(0, 3).forEach(i -> expectedRecords12.put(String.valueOf(i), new ArrayList<>()));
    for (int i = 0; i < outputData.size(); i++) {
      expectedRecords12.get(String.valueOf(i % 3)).add(outputData.get(i));
    }
    inputs.add(new Object[]{mapperId, config12, expectedRecords12});

    // partition by timeValue
    SegmentMapperConfig config2 = new SegmentMapperConfig(_pinotSchema, new RecordTransformerConfig.Builder().build(),
        new RecordFilterConfig.Builder().build(), Lists.newArrayList(
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
            .setColumnName("timeValue").build()));
    Map<String, List<Object[]>> expectedRecords2 =
        outputData.stream().collect(Collectors.groupingBy(r -> String.valueOf(r[2]), Collectors.toList()));
    inputs.add(new Object[]{mapperId, config2, expectedRecords2});

    // partition by campaign
    SegmentMapperConfig config3 = new SegmentMapperConfig(_pinotSchema, new RecordTransformerConfig.Builder().build(),
        new RecordFilterConfig.Builder().build(), Lists.newArrayList(
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
            .setColumnName("campaign").build()));
    Map<String, List<Object[]>> expectedRecords3 =
        outputData.stream().collect(Collectors.groupingBy(r -> String.valueOf(r[0]), Collectors.toList()));
    inputs.add(new Object[]{mapperId, config3, expectedRecords3});

    // transform function partition
    SegmentMapperConfig config4 = new SegmentMapperConfig(_pinotSchema, new RecordTransformerConfig.Builder().build(),
        new RecordFilterConfig.Builder().build(), Lists.newArrayList(
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TRANSFORM_FUNCTION)
            .setTransformFunction("toEpochDays(timeValue)").build()));
    Map<String, List<Object[]>> expectedRecords4 = outputData.stream()
        .collect(Collectors.groupingBy(r -> String.valueOf(((long) r[2]) / 86400000), Collectors.toList()));
    inputs.add(new Object[]{mapperId, config4, expectedRecords4});

    // partition by column and then table column partition config
    SegmentMapperConfig config41 = new SegmentMapperConfig(_pinotSchema, new RecordTransformerConfig.Builder().build(),
        new RecordFilterConfig.Builder().build(), Lists.newArrayList(
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
            .setColumnName("campaign").build(),
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TABLE_PARTITION_CONFIG)
            .setColumnName("clicks").setColumnPartitionConfig(new ColumnPartitionConfig("Modulo", 3)).build()));
    Map<String, List<Object[]>> expectedRecords41 = new HashMap<>();
    for (Object[] record : outputData) {
      String partition = record[0] + "_" + (int) record[1] % 3;
      List<Object[]> objects = expectedRecords41.computeIfAbsent(partition, k -> new ArrayList<>());
      objects.add(record);
    }
    inputs.add(new Object[]{mapperId, config41, expectedRecords41});

    // filter function which filters out nothing
    SegmentMapperConfig config5 = new SegmentMapperConfig(_pinotSchema, new RecordTransformerConfig.Builder().build(),
        new RecordFilterConfig.Builder().setRecordFilterType(RecordFilterFactory.RecordFilterType.FILTER_FUNCTION)
            .setFilterFunction("Groovy({campaign == \"foo\"}, campaign)").build(),
        Lists.newArrayList(new PartitionerConfig.Builder().build()));
    Map<String, List<Object[]>> expectedRecords5 = new HashMap<>();
    expectedRecords5.put("0", outputData);
    inputs.add(new Object[]{mapperId, config5, expectedRecords5});

    // filter function which filters out everything
    SegmentMapperConfig config6 = new SegmentMapperConfig(_pinotSchema, new RecordTransformerConfig.Builder().build(),
        new RecordFilterConfig.Builder().setRecordFilterType(RecordFilterFactory.RecordFilterType.FILTER_FUNCTION)
            .setFilterFunction("Groovy({timeValue > 0}, timeValue)").build(),
        Lists.newArrayList(new PartitionerConfig.Builder().build()));
    Map<String, List<Object[]>> expectedRecords6 = new HashMap<>();
    inputs.add(new Object[]{mapperId, config6, expectedRecords6});

    // filter function which filters out certain times
    SegmentMapperConfig config7 = new SegmentMapperConfig(_pinotSchema, new RecordTransformerConfig.Builder().build(),
        new RecordFilterConfig.Builder().setRecordFilterType(RecordFilterFactory.RecordFilterType.FILTER_FUNCTION)
            .setFilterFunction("Groovy({timeValue < 1597795200000L || timeValue >= 1597881600000L}, timeValue)")
            .build(), Lists.newArrayList(new PartitionerConfig.Builder().build()));
    Map<String, List<Object[]>> expectedRecords7 =
        outputData.stream().filter(r -> ((long) r[2]) >= 1597795200000L && ((long) r[2]) < 1597881600000L)
            .collect(Collectors.groupingBy(r -> "0", Collectors.toList()));
    inputs.add(new Object[]{mapperId, config7, expectedRecords7});

    // record transformation - round timeValue to nearest day
    Map<String, String> transformFunctionMap = new HashMap<>();
    transformFunctionMap.put("timeValue", "round(timeValue, 86400000)");
    SegmentMapperConfig config9 = new SegmentMapperConfig(_pinotSchema,
        new RecordTransformerConfig.Builder().setTransformFunctionsMap(transformFunctionMap).build(),
        new RecordFilterConfig.Builder().build(), Lists.newArrayList(new PartitionerConfig.Builder().build()));
    List<Object[]> transformedData = new ArrayList<>();
    outputData.forEach(r -> transformedData.add(new Object[]{r[0], r[1], (((long) r[2]) / 86400000) * 86400000}));
    Map<String, List<Object[]>> expectedRecords9 = new HashMap<>();
    expectedRecords9.put("0", transformedData);
    inputs.add(new Object[]{mapperId, config9, expectedRecords9});

    // record transformation - round timeValue to nearest day, partition on timeValue
    SegmentMapperConfig config10 = new SegmentMapperConfig(_pinotSchema,
        new RecordTransformerConfig.Builder().setTransformFunctionsMap(transformFunctionMap).build(),
        new RecordFilterConfig.Builder().build(), Lists.newArrayList(
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
            .setColumnName("timeValue").build()));
    Map<String, List<Object[]>> expectedRecords10 =
        transformedData.stream().collect(Collectors.groupingBy(r -> String.valueOf(r[2]), Collectors.toList()));
    inputs.add(new Object[]{mapperId, config10, expectedRecords10});

    // record transformation - round timeValue to nearest day, partition on timeValue, filter out timeValues
    SegmentMapperConfig config11 = new SegmentMapperConfig(_pinotSchema,
        new RecordTransformerConfig.Builder().setTransformFunctionsMap(transformFunctionMap).build(),
        new RecordFilterConfig.Builder().setRecordFilterType(RecordFilterFactory.RecordFilterType.FILTER_FUNCTION)
            .setFilterFunction("Groovy({timeValue < 1597795200000L|| timeValue >= 1597881600000}, timeValue)").build(), Lists.newArrayList(
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
            .setColumnName("timeValue").build()));
    Map<String, List<Object[]>> expectedRecords11 =
        transformedData.stream().filter(r -> ((long) r[2]) == 1597795200000L)
            .collect(Collectors.groupingBy(r -> "1597795200000", Collectors.toList()));
    inputs.add(new Object[]{mapperId, config11, expectedRecords11});

    return inputs.toArray(new Object[0][]);
  }

  @AfterClass
  public void after() {
    FileUtils.deleteQuietly(_baseDir);
  }
}
