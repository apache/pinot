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

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandler;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandlerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
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
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFileConfig;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * End-to-end tests for SegmentProcessorFramework
 */
public class SegmentProcessorFrameworkTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentProcessorFrameworkTest");

  private List<RecordReader> _singleSegment;
  private List<RecordReader> _multipleSegments;
  private List<RecordReader> _multiValueSegments;

  private TableConfig _tableConfig;
  private TableConfig _tableConfigNullValueEnabled;
  private TableConfig _tableConfigSegmentNameGeneratorEnabled;
  private TableConfig _tableConfigWithFixedSegmentName;

  private Schema _schema;
  private Schema _schemaMV;

  private final List<Object[]> _rawData =
      Arrays.asList(new Object[]{"abc", 1000, 1597719600000L}, new Object[]{null, 2000, 1597773600000L},
          new Object[]{"abc", null, 1597777200000L}, new Object[]{"abc", 4000, 1597795200000L},
          new Object[]{"abc", 3000, 1597802400000L}, new Object[]{null, null, 1597838400000L},
          new Object[]{"xyz", 4000, 1597856400000L}, new Object[]{null, 1000, 1597878000000L},
          new Object[]{"abc", 7000, 1597881600000L}, new Object[]{"xyz", 6000, 1597892400000L});

  private final List<Object[]> _rawDataMultiValue =
      Arrays.asList(new Object[]{new String[]{"a", "b"}, 1000, 1597795200000L},
          new Object[]{null, null, 1597795200000L}, new Object[]{null, 1000, 1597795200000L},
          new Object[]{new String[]{"a", "b"}, null, 1597795200000L});

  @BeforeClass
  public void setup()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    FileUtils.forceMkdir(TEMP_DIR);

    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("time").build();
    _tableConfigNullValueEnabled =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("time")
            .setNullHandlingEnabled(true).build();
    _tableConfigSegmentNameGeneratorEnabled =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("time").build();
    _tableConfigSegmentNameGeneratorEnabled.getIndexingConfig().setSegmentNameGeneratorType("normalizedDate");
    _tableConfigWithFixedSegmentName =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("time").build();
    _tableConfigWithFixedSegmentName.getIndexingConfig().setSegmentNameGeneratorType("fixed");

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

    // create segments in many folders
    _singleSegment = createInputSegments(new File(TEMP_DIR, "single_segment"), _rawData, 1, _schema);
    _multipleSegments = createInputSegments(new File(TEMP_DIR, "multiple_segments"), _rawData, 3, _schema);
    _multiValueSegments =
        createInputSegments(new File(TEMP_DIR, "multi_value_segment"), _rawDataMultiValue, 1, _schemaMV);
  }

  private List<RecordReader> createInputSegments(File inputDir, List<Object[]> rawData, int numSegments, Schema schema)
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

    List<RecordReader> segmentRecordReaders = new ArrayList<>(dataLists.size());
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
      PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader();
      segmentRecordReader.init(driver.getOutputDirectory(), null, null, true);
      segmentRecordReaders.add(segmentRecordReader);
    }
    return segmentRecordReaders;
  }

  private GenericRow getGenericRow(Object[] rawRow) {
    GenericRow row = new GenericRow();
    row.putValue("campaign", rawRow[0]);
    row.putValue("clicks", rawRow[1]);
    row.putValue("time", rawRow[2]);
    return row;
  }

  private void rewindRecordReaders(List<RecordReader> recordReaders)
      throws IOException {
    for (RecordReader recordReader : recordReaders) {
      recordReader.rewind();
    }
  }

  /**
   * Test lazy initialization of record readers. Here we create
   * RecoderReaderFileConfig and the actual reader is initialized during the
   * map phase.
   * @throws Exception
   */
  @Test
  public void testRecordReaderFileConfigInit() throws Exception {
    File workingDir = new File(TEMP_DIR, "segmentOutput");
    FileUtils.forceMkdir(workingDir);
    ClassLoader classLoader = getClass().getClassLoader();
    URL resource = classLoader.getResource("data/dimBaseballTeams.csv");
    RecordReaderFileConfig reader = new RecordReaderFileConfig(FileFormat.CSV,
        new File(resource.toURI()),
        null, null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").
        setTimeColumnName("time").build();

    Schema schema =
        new Schema.SchemaBuilder().setSchemaName("mySchema").addSingleValueDimension("teamId",
                DataType.STRING, "")
            .addSingleValueDimension("teamName", DataType.STRING, "")
            .addDateTime("time", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema).build();
    SegmentProcessorFramework framework = new SegmentProcessorFramework(config, workingDir, ImmutableList.of(reader),
        Collections.emptyList(), null);
    List<File> outputSegments = framework.process();
    assertEquals(outputSegments.size(), 1);
    ImmutableSegment segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
    SegmentMetadata segmentMetadata = segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 51);
  }

  @Test
  public void testSingleSegment()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "single_segment_output");
    FileUtils.forceMkdir(workingDir);

    // Default configs
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    SegmentProcessorFramework framework = new SegmentProcessorFramework(_singleSegment, config, workingDir);
    List<File> outputSegments = framework.process();
    assertEquals(outputSegments.size(), 1);
    ImmutableSegment segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
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
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_singleSegment);

    // Default configs - null value enabled
    config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schema).build();
    framework = new SegmentProcessorFramework(_singleSegment, config, workingDir);
    outputSegments = framework.process();
    assertEquals(outputSegments.size(), 1);
    String[] outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
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
    assertNull(timeDataSource.getNullValueVector());
    assertEquals(segmentMetadata.getName(), "myTable_1597719600000_1597892400000_0");
    segment.destroy();
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_singleSegment);

    // Fixed segment name
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigWithFixedSegmentName).setSchema(_schema)
        .setSegmentConfig(new SegmentConfig.Builder().setFixedSegmentName("myTable_segment_0001").build()).build();
    framework = new SegmentProcessorFramework(_singleSegment, config, workingDir);
    outputSegments = framework.process();
    assertEquals(outputSegments.size(), 1);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
    segmentMetadata = segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getName(), "myTable_segment_0001");
    segment.destroy();
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_singleSegment);

    // Time filter
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
            new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597795200000L, 1597881600000L).build())
        .build();
    framework = new SegmentProcessorFramework(_singleSegment, config, workingDir);
    outputSegments = framework.process();
    assertEquals(outputSegments.size(), 1);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 5);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597878000000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597795200000_1597878000000_0");
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_singleSegment);

    // Negate time filter
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
        new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597795200000L, 1597881600000L)
            .setNegateWindowFilter(true).build()).build();
    framework = new SegmentProcessorFramework(_singleSegment, config, workingDir);
    outputSegments = framework.process();
    assertEquals(outputSegments.size(), 1);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 5);
    assertEquals(timeMetadata.getMinValue(), 1597719600000L);
    assertEquals(timeMetadata.getMaxValue(), 1597892400000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597719600000_1597892400000_0");
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_singleSegment);

    // Time filter - filtered everything
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
            new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597968000000L, 1598054400000L).build())
        .build();
    framework = new SegmentProcessorFramework(_singleSegment, config, workingDir);
    outputSegments = framework.process();
    assertTrue(outputSegments.isEmpty());
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_singleSegment);

    // Time round
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema)
        .setTimeHandlerConfig(new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setRoundBucketMs(86400000).build())
        .build();
    framework = new SegmentProcessorFramework(_singleSegment, config, workingDir);
    outputSegments = framework.process();
    assertEquals(outputSegments.size(), 1);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), 10);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 3);
    assertEquals(timeMetadata.getMinValue(), 1597708800000L);
    assertEquals(timeMetadata.getMaxValue(), 1597881600000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597708800000_1597881600000_0");
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_singleSegment);

    // Time partition
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
        new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setPartitionBucketMs(86400000).build()).build();
    framework = new SegmentProcessorFramework(_singleSegment, config, workingDir);
    outputSegments = framework.process();
    assertEquals(outputSegments.size(), 3);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    outputSegments.sort(null);
    // segment 0
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 3);
    assertEquals(timeMetadata.getMinValue(), 1597719600000L);
    assertEquals(timeMetadata.getMaxValue(), 1597777200000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597719600000_1597777200000_0");
    // segment 1
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(1));
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 5);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597878000000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597795200000_1597878000000_1");
    // segment 2
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(2));
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 2);
    assertEquals(timeMetadata.getMinValue(), 1597881600000L);
    assertEquals(timeMetadata.getMaxValue(), 1597892400000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597881600000_1597892400000_2");
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_singleSegment);

    // Time filter, round, partition, rollup - null value enabled
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schema)
        .setTimeHandlerConfig(
            new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597708800000L, 1597881600000L)
                .setRoundBucketMs(86400000).setPartitionBucketMs(86400000).build()).setMergeType(MergeType.ROLLUP)
        .build();
    framework = new SegmentProcessorFramework(_singleSegment, config, workingDir);
    outputSegments = framework.process();
    assertEquals(outputSegments.size(), 2);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    outputSegments.sort(null);
    // segment 0
    segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
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
    assertNull(clicksDataSource.getNullValueVector());
    timeDataSource = segment.getDataSource("time");
    assertNull(timeDataSource.getNullValueVector());
    assertEquals(segmentMetadata.getName(), "myTable_1597708800000_1597708800000_0");
    segment.destroy();
    // segment 1
    segment = ImmutableSegmentLoader.load(outputSegments.get(1), ReadMode.mmap);
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
    assertNull(clicksDataSource.getNullValueVector());
    timeDataSource = segment.getDataSource("time");
    assertNull(timeDataSource.getNullValueVector());
    assertEquals(segmentMetadata.getName(), "myTable_1597795200000_1597795200000_1");
    segment.destroy();
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_singleSegment);

    // Time round, dedup
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema)
        .setTimeHandlerConfig(new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setRoundBucketMs(86400000).build())
        .setMergeType(MergeType.DEDUP).build();
    framework = new SegmentProcessorFramework(_singleSegment, config, workingDir);
    outputSegments = framework.process();
    assertEquals(outputSegments.size(), 1);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), 8);
    assertEquals(segmentMetadata.getName(), "myTable_1597708800000_1597881600000_0");
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_singleSegment);

    // Segment config
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setSegmentConfig(
        new SegmentConfig.Builder().setMaxNumRecordsPerSegment(4).setSegmentNamePrefix("myPrefix")
            .setSegmentNamePostfix("myPostfix").build()).build();
    framework = new SegmentProcessorFramework(_singleSegment, config, workingDir);
    outputSegments = framework.process();
    assertEquals(outputSegments.size(), 3);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    outputSegments.sort(null);
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), 4);
    assertEquals(segmentMetadata.getName(), "myPrefix_1597719600000_1597795200000_myPostfix_0");
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(1));
    assertEquals(segmentMetadata.getTotalDocs(), 4);
    assertEquals(segmentMetadata.getName(), "myPrefix_1597802400000_1597878000000_myPostfix_1");
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(2));
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    assertEquals(segmentMetadata.getName(), "myPrefix_1597881600000_1597892400000_myPostfix_2");
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_singleSegment);

    config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigSegmentNameGeneratorEnabled).setSchema(_schema)
            .setSegmentConfig(new SegmentConfig.Builder().setMaxNumRecordsPerSegment(4).setSegmentNamePrefix("myPrefix")
                .setSegmentNamePostfix("myPostfix").build()).build();
    framework = new SegmentProcessorFramework(_singleSegment, config, workingDir);
    outputSegments = framework.process();
    assertEquals(outputSegments.size(), 3);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    outputSegments.sort(null);
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), 4);
    assertEquals(segmentMetadata.getName(), "myPrefix_2020-08-18_2020-08-19_myPostfix_0");
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(1));
    assertEquals(segmentMetadata.getTotalDocs(), 4);
    assertEquals(segmentMetadata.getName(), "myPrefix_2020-08-19_2020-08-19_myPostfix_1");
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(2));
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    assertEquals(segmentMetadata.getName(), "myPrefix_2020-08-20_2020-08-20_myPostfix_2");
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_singleSegment);
  }

  @Test
  public void testMultipleSegments()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "multiple_segments_output");
    FileUtils.forceMkdir(workingDir);

    // Default configs
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    SegmentProcessorFramework framework = new SegmentProcessorFramework(_multipleSegments, config, workingDir);
    List<File> outputSegments = framework.process();
    assertEquals(outputSegments.size(), 1);
    String[] outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), 10);
    assertEquals(segmentMetadata.getName(), "myTable_1597719600000_1597892400000_0");
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_multipleSegments);

    // Time round, partition, rollup
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
        new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setRoundBucketMs(86400000).setPartitionBucketMs(86400000)
            .build()).setMergeType(MergeType.ROLLUP).build();
    framework = new SegmentProcessorFramework(_multipleSegments, config, workingDir);
    outputSegments = framework.process();
    assertEquals(outputSegments.size(), 3);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    outputSegments.sort(null);
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    assertEquals(segmentMetadata.getName(), "myTable_1597708800000_1597708800000_0");
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(1));
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    assertEquals(segmentMetadata.getName(), "myTable_1597795200000_1597795200000_1");
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(2));
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    assertEquals(segmentMetadata.getName(), "myTable_1597881600000_1597881600000_2");
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_multipleSegments);
  }

  @Test
  public void testMultiValue()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "output_directory_multi_value");
    FileUtils.forceMkdir(workingDir);

    // Rollup - null value enabled
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schemaMV)
            .setMergeType(MergeType.ROLLUP).build();
    SegmentProcessorFramework framework = new SegmentProcessorFramework(_multiValueSegments, config, workingDir);
    List<File> outputSegments = framework.process();
    assertEquals(outputSegments.size(), 1);
    String[] outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    ImmutableSegment segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
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
    assertNull(clicksDataSource.getNullValueVector());
    DataSource timeDataSource = segment.getDataSource("time");
    assertNull(timeDataSource.getNullValueVector());
    assertEquals(segmentMetadata.getName(), "myTable_1597795200000_1597795200000_0");
    segment.destroy();
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_multiValueSegments);

    // Dedup
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schemaMV)
        .setMergeType(MergeType.DEDUP).build();
    framework = new SegmentProcessorFramework(_multiValueSegments, config, workingDir);
    outputSegments = framework.process();
    assertEquals(outputSegments.size(), 1);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
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
    FileUtils.cleanDirectory(workingDir);
    rewindRecordReaders(_multiValueSegments);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    for (RecordReader recordReader : _singleSegment) {
      recordReader.close();
    }
    for (RecordReader recordReader : _multipleSegments) {
      recordReader.close();
    }
    for (RecordReader recordReader : _multiValueSegments) {
      recordReader.close();
    }
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
