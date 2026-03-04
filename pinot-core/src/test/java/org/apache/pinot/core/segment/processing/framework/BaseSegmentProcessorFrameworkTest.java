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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandler;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandlerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Abstract base class for SegmentProcessorFramework tests.
 * Contains all test logic and setup, with abstract methods for processing that
 * concrete implementations override to use either SPF or CSPF.
 */
public abstract class BaseSegmentProcessorFrameworkTest {
  protected static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentProcessorFrameworkTest");

  protected List<File> _singleSegment;
  protected List<File> _multipleSegments;
  protected List<File> _multiValueSegments;

  protected TableConfig _tableConfig;
  protected TableConfig _tableConfigWithComplexType;
  protected TableConfig _tableConfigWithoutTimeColumn;
  protected TableConfig _tableConfigNullValueEnabled;
  protected TableConfig _tableConfigSegmentNameGeneratorEnabled;
  protected TableConfig _tableConfigWithFixedSegmentName;

  protected Schema _schema;
  protected Schema _schemaMV;
  protected Schema _schemaWithComplexType;

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

  /**
   * Abstract method for processing segments.
   * Concrete implementations create appropriate readers (RecordReader for SPF, ColumnarDataSource for CSPF).
   */
  protected abstract List<File> processSegments(List<File> segmentDirs, SegmentProcessorConfig config, File workingDir)
      throws Exception;

  @BeforeClass
  public void setup()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    FileUtils.forceMkdir(TEMP_DIR);

    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("time").build();
    _tableConfigNullValueEnabled =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("time")
            .setNullHandlingEnabled(true).build();
    _tableConfigWithoutTimeColumn =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setNullHandlingEnabled(true).build();
    _tableConfigSegmentNameGeneratorEnabled =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("time").build();
    _tableConfigSegmentNameGeneratorEnabled.getIndexingConfig().setSegmentNameGeneratorType("normalizedDate");
    _tableConfigWithFixedSegmentName =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("time").build();
    _tableConfigWithFixedSegmentName.getIndexingConfig().setSegmentNameGeneratorType("fixed");

    IngestionConfig ingestionConfig = new IngestionConfig();
    List<String> fieldsToUnnest = new ArrayList<>();
    fieldsToUnnest.add("targetusers");
    ingestionConfig.setComplexTypeConfig(
        new ComplexTypeConfig(fieldsToUnnest, ".", null, null));
    _tableConfigWithComplexType = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTableComplex")
        .setTimeColumnName("time").setIngestionConfig(ingestionConfig).build();

    _schema =
        new Schema.SchemaBuilder().setSchemaName("mySchema")
            .addSingleValueDimension("campaign", DataType.STRING, "")
            .addSingleValueDimension("campaign.inner1", DataType.STRING)
            .addSingleValueDimension("campaign.inner1.inner2", DataType.STRING)
            // NOTE: Intentionally put 1000 as default value to test skipping null values during rollup
            .addMetric("clicks", DataType.INT, 1000)
            .addDateTime("time", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    _schemaMV =
        new Schema.SchemaBuilder().setSchemaName("mySchema")
            .addMultiValueDimension("campaign", DataType.STRING, "")
            // NOTE: Intentionally put 1000 as default value to test skipping null values during rollup
            .addMetric("clicks", DataType.INT, 1000)
            .addDateTime("time", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    _schemaWithComplexType =
        new Schema.SchemaBuilder().setSchemaName("mySchema")
            .addSingleValueDimension("campaign", DataType.JSON)
            .addSingleValueDimension("campaign.inner1", DataType.STRING)
            .addSingleValueDimension("campaign.inner1.inner2", DataType.STRING)
            .addSingleValueDimension("targetusers.user", DataType.STRING)
            // NOTE: Intentionally put 1000 as default value to test skipping null values during rollup
            .addMetric("clicks", DataType.INT, 1000)
            .addDateTime("time", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    // create segments in many folders
    _singleSegment = createInputSegments(new File(TEMP_DIR, "single_segment"), _rawData, 1, _schema);
    _multipleSegments = createInputSegments(new File(TEMP_DIR, "multiple_segments"), _rawData, 3, _schema);
    _multiValueSegments =
        createInputSegments(new File(TEMP_DIR, "multi_value_segment"), _rawDataMultiValue, 1, _schemaMV);
  }

  /**
   * Create input segments and return the list of segment directories.
   * This is changed from the original to return List<File> instead of List<RecordReader>.
   */
  private List<File> createInputSegments(File inputDir, List<Object[]> rawData, int numSegments, Schema schema)
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

    List<File> segmentDirs = new ArrayList<>(dataLists.size());
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
      segmentDirs.add(driver.getOutputDirectory());
    }
    return segmentDirs;
  }

  private GenericRow getGenericRow(Object[] rawRow) {
    GenericRow row = new GenericRow();
    row.putValue("campaign", rawRow[0]);
    row.putValue("clicks", rawRow[1]);
    row.putValue("time", rawRow[2]);
    return row;
  }

  @Test
  public void testSegmentGenerationWithComplexType() throws Exception {
    File workingDir = new File(TEMP_DIR, "single_segment_complex_type_output");
    FileUtils.forceMkdir(workingDir);

    // Create segments for complex type test
    GenericRow genericRow = new GenericRow();
    genericRow.putValue("a", 1L);
    Map<String, Object> map1 = new HashMap<>();
    genericRow.putValue("campaign", map1);
    map1.put("inner", "innerv");
    Map<String, Object> innerMap1 = new HashMap<>();
    innerMap1.put("inner2", "inner2v");

    map1.put("inner1", innerMap1);
    Map<String, Object> map2 = new HashMap<>();
    map2.put("c", 3);
    genericRow.putValue("map2", map2);

    //list with two map entries inside
    List<Map<String, Object>> list = new ArrayList<>();
    Map<String, Object> map3 = new HashMap<>();
    map3.put("user", "foobar");
    list.add(map3);
    Map<String, Object> map4 = new HashMap<>();
    map4.put("user", "barfoo");
    list.add(map4);
    genericRow.putValue("targetusers", list);

    List<GenericRow> inputRows = List.of(genericRow);

    // Create a segment with this data
    File complexTypeInputDir = new File(TEMP_DIR, "complex_type_input");
    FileUtils.forceMkdir(complexTypeInputDir);
    RecordReader recordReader = new GenericRowRecordReader(inputRows);
    SegmentGeneratorConfig segmentGeneratorConfig =
        new SegmentGeneratorConfig(_tableConfigWithComplexType, _schemaWithComplexType);
    segmentGeneratorConfig.setOutDir(complexTypeInputDir.getPath());
    segmentGeneratorConfig.setSequenceId(0);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, recordReader);
    driver.build();
    List<File> segmentDirs = List.of(driver.getOutputDirectory());

    // Default configs
    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder()
        .setTableConfig(_tableConfigWithComplexType).setSchema(_schemaWithComplexType).build();

    List<File> outputSegments = processSegments(segmentDirs, config, workingDir);
    ImmutableSegment segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
    SegmentMetadata segmentMetadata = segment.getSegmentMetadata();
    // Pick the column created from complex type
    ColumnMetadata campaignInner2Metadata = segmentMetadata.getColumnMetadataFor("campaign.inner1.inner2");
    // Verify we see a specific value parsed from the complexType
    Assert.assertEquals(campaignInner2Metadata.getMinValue().compareTo("inner2v"), 0);
    ColumnMetadata campaignMetadata = segmentMetadata.getColumnMetadataFor("campaign");
    Assert.assertEquals(
        campaignMetadata.getMinValue().compareTo("{\"inner1\":{\"inner2\":\"inner2v\"},\"inner\":\"innerv\"}"), 0);

    ColumnMetadata listMetadata = segmentMetadata.getColumnMetadataFor("targetusers.user");
    Assert.assertEquals(listMetadata.getMinValue().compareTo("barfoo"), 0);
  }

  @Test
  public void testSingleSegment()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "single_segment_output");
    FileUtils.forceMkdir(workingDir);

    // Default configs
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    List<File> outputSegments = processSegments(_singleSegment, config, workingDir);
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

    // Default configs - null value enabled
    config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schema).build();
    outputSegments = processSegments(_singleSegment, config, workingDir);
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

    // Fixed segment name
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigWithFixedSegmentName).setSchema(_schema)
        .setSegmentConfig(new SegmentConfig.Builder().setFixedSegmentName("myTable_segment_0001").build()).build();
    outputSegments = processSegments(_singleSegment, config, workingDir);
    assertEquals(outputSegments.size(), 1);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
    segmentMetadata = segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getName(), "myTable_segment_0001");
    segment.destroy();
    FileUtils.cleanDirectory(workingDir);

    // Segment config
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setSegmentConfig(
        new SegmentConfig.Builder().setMaxNumRecordsPerSegment(4).setSegmentNamePrefix("myPrefix")
            .setSegmentNamePostfix("myPostfix").build()).build();
    outputSegments = processSegments(_singleSegment, config, workingDir);
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

    config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigSegmentNameGeneratorEnabled).setSchema(_schema)
            .setSegmentConfig(new SegmentConfig.Builder().setMaxNumRecordsPerSegment(4).setSegmentNamePrefix("myPrefix")
                .setSegmentNamePostfix("myPostfix").build()).build();
    outputSegments = processSegments(_singleSegment, config, workingDir);
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
  }

  @Test
  public void testSingleSegmentWithTimeFiltersAndPartition()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "single_segment_output");
    FileUtils.forceMkdir(workingDir);

    // Time filter
    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder()
        .setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
            new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597795200000L, 1597881600000L).build())
        .build();
    List<File> outputSegments = processSegments(_singleSegment, config, workingDir);
    assertEquals(outputSegments.size(), 1);
    String[] outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    ColumnMetadata timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 5);
    assertEquals(timeMetadata.getMinValue(), 1597795200000L);
    assertEquals(timeMetadata.getMaxValue(), 1597878000000L);
    assertEquals(segmentMetadata.getName(), "myTable_1597795200000_1597878000000_0");
    FileUtils.cleanDirectory(workingDir);

    // Negate time filter
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
        new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597795200000L, 1597881600000L)
            .setNegateWindowFilter(true).build()).build();
    outputSegments = processSegments(_singleSegment, config, workingDir);
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

    // Time filter - filtered everything
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
            new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597968000000L, 1598054400000L).build())
        .build();
    outputSegments = processSegments(_singleSegment, config, workingDir);
    assertTrue(outputSegments.isEmpty());
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    FileUtils.cleanDirectory(workingDir);

    // Time round
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema)
        .setTimeHandlerConfig(new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setRoundBucketMs(86400000).build())
        .build();
    outputSegments = processSegments(_singleSegment, config, workingDir);
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

    // Time partition
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
        new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setPartitionBucketMs(86400000).build()).build();
    outputSegments = processSegments(_singleSegment, config, workingDir);
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
  }

  @Test
  public void testSingleSegmentWithRollup()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "single_segment_output");
    FileUtils.forceMkdir(workingDir);

    // Time filter, round, partition, rollup - null value enabled
    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder()
        .setTableConfig(_tableConfigNullValueEnabled).setSchema(_schema)
        .setTimeHandlerConfig(
            new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setTimeRange(1597708800000L, 1597881600000L)
                .setRoundBucketMs(86400000).setPartitionBucketMs(86400000).build()).setMergeType(MergeType.ROLLUP)
        .build();
    List<File> outputSegments = processSegments(_singleSegment, config, workingDir);
    assertEquals(outputSegments.size(), 2);
    String[] outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    outputSegments.sort(null);
    // segment 0
    ImmutableSegment segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
    SegmentMetadata segmentMetadata = segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    ColumnMetadata campaignMetadata = segmentMetadata.getColumnMetadataFor("campaign");
    assertEquals(campaignMetadata.getCardinality(), 2);
    assertEquals(campaignMetadata.getMinValue(), "");
    assertEquals(campaignMetadata.getMaxValue(), "abc");
    ColumnMetadata clicksMetadata = segmentMetadata.getColumnMetadataFor("clicks");
    assertEquals(clicksMetadata.getCardinality(), 2);
    assertEquals(clicksMetadata.getMinValue(), 1000);
    assertEquals(clicksMetadata.getMaxValue(), 2000);
    ColumnMetadata timeMetadata = segmentMetadata.getColumnMetadataFor("time");
    assertEquals(timeMetadata.getCardinality(), 1);
    assertEquals(timeMetadata.getMinValue(), 1597708800000L);
    assertEquals(timeMetadata.getMaxValue(), 1597708800000L);
    DataSource campaignDataSource = segment.getDataSource("campaign");
    NullValueVectorReader campaignNullValueVector = campaignDataSource.getNullValueVector();
    assertNotNull(campaignNullValueVector);
    assertEquals(campaignNullValueVector.getNullBitmap().toArray(), new int[]{0});
    DataSource clicksDataSource = segment.getDataSource("clicks");
    assertNull(clicksDataSource.getNullValueVector());
    DataSource timeDataSource = segment.getDataSource("time");
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
  }

  @Test
  public void testSingleSegmentWithDedup()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "single_segment_output");
    FileUtils.forceMkdir(workingDir);

    // Time round, dedup
    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema)
        .setTimeHandlerConfig(new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setRoundBucketMs(86400000).build())
        .setMergeType(MergeType.DEDUP).build();
    List<File> outputSegments = processSegments(_singleSegment, config, workingDir);
    assertEquals(outputSegments.size(), 1);
    String[] outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), 8);
    assertEquals(segmentMetadata.getName(), "myTable_1597708800000_1597881600000_0");
    FileUtils.cleanDirectory(workingDir);
  }

  /**
   * Helper method to create multiple input segments with controlled data distribution.
   */
  protected List<File> createMultipleInputSegments(File inputDir, List<List<Object[]>> segmentDataLists,
      Schema schema)
      throws Exception {
    assertTrue(inputDir.mkdirs());

    List<File> segmentDirs = new ArrayList<>(segmentDataLists.size());
    int idx = 0;
    for (List<Object[]> segmentData : segmentDataLists) {
      List<GenericRow> inputRows = new ArrayList<>();
      for (Object[] rawRow : segmentData) {
        inputRows.add(getGenericRow(rawRow));
      }
      RecordReader recordReader = new GenericRowRecordReader(inputRows);
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfigNullValueEnabled, schema);
      segmentGeneratorConfig.setOutDir(inputDir.getPath());
      segmentGeneratorConfig.setSequenceId(idx++);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(segmentGeneratorConfig, recordReader);
      driver.build();
      segmentDirs.add(driver.getOutputDirectory());
    }
    return segmentDirs;
  }

  /**
   * Helper method to create segment with invalid data for specific column/rows.
   */
  protected List<File> createSegmentWithInvalidData(File inputDir, List<Object[]> data, Schema schema,
      String invalidColumn, Set<Integer> invalidRowIndices)
      throws Exception {
    assertTrue(inputDir.mkdirs());

    List<GenericRow> inputRows = new ArrayList<>();
    for (int i = 0; i < data.size(); i++) {
      GenericRow row = new GenericRow();
      Object[] rawRow = data.get(i);
      row.putValue("campaign", rawRow[0]);

      // Set invalid value for specified rows and column
      if (invalidRowIndices.contains(i) && invalidColumn.equals("clicks")) {
        row.putValue("clicks", "INVALID");
      } else if (invalidRowIndices.contains(i) && invalidColumn.equals("time")) {
        row.putValue("time", "INVALID_TIME");
      } else {
        row.putValue("clicks", rawRow[1]);
        row.putValue("time", rawRow[2]);
      }

      inputRows.add(row);
    }

    // Add the invalid column as STRING type to accommodate invalid data in the segment
    Schema schemaWithInvalidColumn = new Schema.SchemaBuilder().build();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      schemaWithInvalidColumn.addField(fieldSpec);
    }
    schemaWithInvalidColumn.removeField(invalidColumn);
    schemaWithInvalidColumn.addField(new DimensionFieldSpec(invalidColumn, DataType.STRING, true));

    RecordReader recordReader = new GenericRowRecordReader(inputRows);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfigWithoutTimeColumn,
        schemaWithInvalidColumn);
    segmentGeneratorConfig.setOutDir(inputDir.getPath());
    segmentGeneratorConfig.setSequenceId(0);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, recordReader);
    driver.build();
    return List.of(driver.getOutputDirectory());
  }

  /**
   * Helper method to generate test data with specific partition values and times.
   */
  protected List<Object[]> generatePartitionedData(int numDocs, String campaignValue, long baseTime) {
    List<Object[]> data = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      data.add(new Object[]{campaignValue, 1000 + i, baseTime + i * 1000L});
    }
    return data;
  }

  @Test
  public void testMultipleDataSourcesWithPartitioningAndNulls()
      throws Exception {
    Schema schemaWithDefault = new Schema.SchemaBuilder().build();
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      schemaWithDefault.addField(fieldSpec);
    }
    schemaWithDefault.removeField("campaign");
    schemaWithDefault.addField(new DimensionFieldSpec("campaign", DataType.STRING, true));
    testMultipleDataSourcesWithPartitioningAndNulls(_tableConfigNullValueEnabled, schemaWithDefault, 12);
  }

  @Test
  public void testMultipleDataSourcesWithPartitioningAndDefaults()
      throws Exception {
    Schema schemaWithDefault = new Schema.SchemaBuilder().build();
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      schemaWithDefault.addField(fieldSpec);
    }
    schemaWithDefault.removeField("campaign");
    schemaWithDefault.addField(new DimensionFieldSpec("campaign", DataType.STRING, true, "abc"));
    testMultipleDataSourcesWithPartitioningAndNulls(_tableConfigNullValueEnabled, schemaWithDefault, 9);
  }

  private void testMultipleDataSourcesWithPartitioningAndNulls(TableConfig tableConf, Schema schema, int numOfSegments)
      throws Exception {
    File workingDir = new File(TEMP_DIR, "multiple_datasources_partitioning_output" + UUID.randomUUID());
    FileUtils.forceMkdir(workingDir);

    // Create 3 input segments with different sizes: 100, 50, 200 docs
    // Data spans 3 days and has multiple campaign values (3 distinct + nulls)
    long day1 = 1597708800000L; // 2020-08-18
    long day2 = 1597795200000L; // 2020-08-19
    long day3 = 1597881600000L; // 2020-08-20

    List<List<Object[]>> segmentDataLists = new ArrayList<>();

    // Segment 1: 100 docs
    List<Object[]> segment1Data = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      segment1Data.add(new Object[]{"abc", 1000 + i, day1 + i * 1000});
    }
    for (int i = 0; i < 25; i++) {
      segment1Data.add(new Object[]{"xyz", 2000 + i, day1 + i * 1000});
    }
    for (int i = 0; i < 20; i++) {
      segment1Data.add(new Object[]{"abc", 3000 + i, day2 + i * 1000});
    }
    for (int i = 0; i < 15; i++) {
      segment1Data.add(new Object[]{null, 4000 + i, day2 + i * 1000});
    }
    for (int i = 0; i < 10; i++) {
      segment1Data.add(new Object[]{"pqr", null, day3 + i * 1000});
    }
    // Shuffle to mix data
    Collections.shuffle(segment1Data, new Random());

    // Segment 2: 50 docs
    List<Object[]> segment2Data = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      segment2Data.add(new Object[]{"abc", 5000 + i, day1 + i * 1000});
    }
    for (int i = 0; i < 10; i++) {
      segment2Data.add(new Object[]{"xyz", 6000 + i, day2 + i * 1000});
    }
    for (int i = 0; i < 12; i++) {
      segment2Data.add(new Object[]{"pqr", 7000 + i, day2 + i * 1000});
    }
    for (int i = 0; i < 8; i++) {
      segment2Data.add(new Object[]{null, null, day1 + i * 1000});
    }
    for (int i = 0; i < 5; i++) {
      segment2Data.add(new Object[]{"abc", 8000 + i, day3 + i * 1000});
    }
    Collections.shuffle(segment2Data, new Random());

    // Segment 3: 200 docs
    List<Object[]> segment3Data = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      segment3Data.add(new Object[]{"abc", 9000 + i, day1 + i * 1000});
    }
    for (int i = 0; i < 40; i++) {
      segment3Data.add(new Object[]{"xyz", 10000 + i, day1 + i * 1000});
    }
    for (int i = 0; i < 35; i++) {
      segment3Data.add(new Object[]{"pqr", 11000 + i, day1 + i * 1000});
    }
    for (int i = 0; i < 30; i++) {
      segment3Data.add(new Object[]{"abc", null, day2 + i * 1000});
    }
    for (int i = 0; i < 25; i++) {
      segment3Data.add(new Object[]{null, 12000 + i, day3 + i * 1000});
    }
    for (int i = 0; i < 20; i++) {
      segment3Data.add(new Object[]{"xyz", 13000 + i, day3 + i * 1000});
    }
    Collections.shuffle(segment3Data, new Random());

    segmentDataLists.add(segment1Data);
    segmentDataLists.add(segment2Data);
    segmentDataLists.add(segment3Data);

    File inputDir = new File(TEMP_DIR, "multiple_datasources_partitioning_input" + UUID.randomUUID());
    List<File> inputSegments = createMultipleInputSegments(inputDir, segmentDataLists, schema);

    // Configure time + column partitioning
    PartitionerConfig columnPartitioner = new PartitionerConfig.Builder()
        .setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
        .setColumnName("campaign")
        .build();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder()
        .setTableConfig(tableConf)
        .setSchema(schema)
        .setTimeHandlerConfig(
            new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH)
                .setPartitionBucketMs(86400000) // 1 day
                .build())
        .setPartitionerConfigs(List.of(columnPartitioner))
        .build();

    List<File> outputSegments = processSegments(inputSegments, config, workingDir);

    // Verify output
    assertEquals(outputSegments.size(), numOfSegments, "Should create output segments");

    // Total docs should be 350 (100 + 50 + 200)
    int totalDocs = 0;
    for (File segmentDir : outputSegments) {
      SegmentMetadata metadata = new SegmentMetadataImpl(segmentDir);
      totalDocs += metadata.getTotalDocs();
    }
    assertEquals(totalDocs, 350, "Total document count should match sum of inputs");

    // Verify each segment has data for single day + single campaign
    for (File segmentDir : outputSegments) {
      SegmentMetadata metadata = new SegmentMetadataImpl(segmentDir);
      ColumnMetadata timeMetadata = metadata.getColumnMetadataFor("time");
      assertNotNull(timeMetadata, "Time column metadata should not be null");
      long startTime = (long) timeMetadata.getMinValue();
      long endTime = (long) timeMetadata.getMaxValue();
      // Verify time range is exactly 1 day
      assertTrue(endTime - startTime <= 86400000, "Segment time range should be 1 day");

      ColumnMetadata campaignMetadata = metadata.getColumnMetadataFor("campaign");
      assertNotNull(campaignMetadata, "Campaign column metadata should not be null");
      assertTrue(campaignMetadata.getCardinality() <= 1, "Campaign cardinality should be at most 1");
    }

    FileUtils.cleanDirectory(workingDir);
    FileUtils.cleanDirectory(inputDir);
  }

  @Test
  public void testSegmentSplittingWithinPartition()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "segment_splitting_within_partition_output");
    FileUtils.forceMkdir(workingDir);

    // Create input segment with 100 docs, all same partition value
    List<Object[]> testData = generatePartitionedData(100, "abc", 1597719600000L);
    File inputDir = new File(TEMP_DIR, "segment_splitting_within_partition_input");
    List<File> inputSegments = createInputSegments(inputDir, testData, 1, _schema);

    // Configure max 30 records per segment
    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder()
        .setTableConfig(_tableConfig)
        .setSchema(_schema)
        .setSegmentConfig(new SegmentConfig.Builder().setMaxNumRecordsPerSegment(30).build())
        .build();

    List<File> outputSegments = processSegments(inputSegments, config, workingDir);

    // Verify exactly 4 segments created (30+30+30+10)
    assertEquals(outputSegments.size(), 4, "Should create exactly 4 segments");

    outputSegments.sort(null);
    SegmentMetadata seg0 = new SegmentMetadataImpl(outputSegments.get(0));
    SegmentMetadata seg1 = new SegmentMetadataImpl(outputSegments.get(1));
    SegmentMetadata seg2 = new SegmentMetadataImpl(outputSegments.get(2));
    SegmentMetadata seg3 = new SegmentMetadataImpl(outputSegments.get(3));

    assertEquals(seg0.getTotalDocs(), 30, "Segment 0 should have 30 docs");
    assertEquals(seg1.getTotalDocs(), 30, "Segment 1 should have 30 docs");
    assertEquals(seg2.getTotalDocs(), 30, "Segment 2 should have 30 docs");
    assertEquals(seg3.getTotalDocs(), 10, "Segment 3 should have 10 docs");

    FileUtils.cleanDirectory(workingDir);
  }

  @Test
  public void testSegmentSplittingAcrossMultiplePartitions()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "segment_splitting_across_partitions_output");
    FileUtils.forceMkdir(workingDir);

    // Create input with 3 partitions: abc(80), xyz(70), pqr(45) = 195 docs total
    List<Object[]> testData = new ArrayList<>();
    testData.addAll(generatePartitionedData(80, "abc", 1597719600000L));
    testData.addAll(generatePartitionedData(70, "xyz", 1597719600000L));
    testData.addAll(generatePartitionedData(45, "pqr", 1597719600000L));

    File inputDir = new File(TEMP_DIR, "segment_splitting_across_partitions_input");
    List<File> inputSegments = createInputSegments(inputDir, testData, 1, _schema);

    // Configure column partitioner + max 30 records per segment
    PartitionerConfig columnPartitioner = new PartitionerConfig.Builder()
        .setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
        .setColumnName("campaign")
        .build();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder()
        .setTableConfig(_tableConfig)
        .setSchema(_schema)
        .setSegmentConfig(new SegmentConfig.Builder().setMaxNumRecordsPerSegment(30).build())
        .setPartitionerConfigs(List.of(columnPartitioner))
        .build();

    List<File> outputSegments = processSegments(inputSegments, config, workingDir);

    // Verify total 8 segments: abc(3), xyz(3), pqr(2)
    assertEquals(outputSegments.size(), 8, "Should create exactly 8 segments");

    // Verify total doc count
    int totalDocs = 0;
    for (File segmentDir : outputSegments) {
      SegmentMetadata metadata = new SegmentMetadataImpl(segmentDir);
      totalDocs += metadata.getTotalDocs();
    }
    assertEquals(totalDocs, 195, "Total docs should be 195");

    FileUtils.cleanDirectory(workingDir);
  }

  @Test
  public void testExactBoundaryCondition()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "exact_boundary_output");
    FileUtils.forceMkdir(workingDir);

    // Create input with exactly 90 docs, all same partition
    List<Object[]> testData = generatePartitionedData(90, "abc", 1597719600000L);
    File inputDir = new File(TEMP_DIR, "exact_boundary_input");
    List<File> inputSegments = createInputSegments(inputDir, testData, 1, _schema);

    // Configure max 30 records per segment
    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder()
        .setTableConfig(_tableConfig)
        .setSchema(_schema)
        .setSegmentConfig(new SegmentConfig.Builder().setMaxNumRecordsPerSegment(30).build())
        .build();

    List<File> outputSegments = processSegments(inputSegments, config, workingDir);

    // Verify exactly 3 segments (30+30+30), no empty 4th segment
    assertEquals(outputSegments.size(), 3, "Should create exactly 3 segments");

    for (File segmentDir : outputSegments) {
      SegmentMetadata metadata = new SegmentMetadataImpl(segmentDir);
      assertEquals(metadata.getTotalDocs(), 30, "Each segment should have exactly 30 docs");
    }

    FileUtils.cleanDirectory(workingDir);
  }

  @Test
  public void testContinueOnErrorDuringPartitionMapping()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "continue_on_error_partition_mapping_output");
    FileUtils.forceMkdir(workingDir);

    // Create data with 10 rows, rows 2, 5, 7 have invalid time
    List<Object[]> testData = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      testData.add(new Object[]{"abc", 1000 + i, 1597719600000L + i * 1000});
    }

    File inputDir = new File(TEMP_DIR, "continue_on_error_partition_mapping_input");
    Set<Integer> invalidRowIndices = new HashSet<>(Arrays.asList(2, 5, 7));
    List<File> inputSegments = createSegmentWithInvalidData(inputDir, testData, _schema, "time", invalidRowIndices);

    // Configure continueOnError = true with time partitioning
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setContinueOnError(true);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("myTable")
        .setTimeColumnName("time")
        .setIngestionConfig(ingestionConfig)
        .build();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder()
        .setTableConfig(tableConfig)
        .setSchema(_schema)
        .setTimeHandlerConfig(
            new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH)
                .setPartitionBucketMs(86400000)
                .build())
        .build();

    List<File> outputSegments = processSegments(inputSegments, config, workingDir);

    int totalDocs = 0;
    for (File segmentDir : outputSegments) {
      SegmentMetadata metadata = new SegmentMetadataImpl(segmentDir);
      totalDocs += metadata.getTotalDocs();
    }
    assertEquals(totalDocs, 10, "Should have 10 valid docs due to continue on error");

    FileUtils.cleanDirectory(workingDir);

    // Now create config with continueOnError = false and verify exception
    tableConfig.setIngestionConfig(new IngestionConfig());
    SegmentProcessorConfig configFailFast = new SegmentProcessorConfig.Builder()
        .setTableConfig(tableConfig)
        .setSchema(_schema)
        .setTimeHandlerConfig(
            new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH)
                .setPartitionBucketMs(86400000)
                .build())
        .build();
    boolean exceptionThrown = false;
    try {
      processSegments(inputSegments, configFailFast, workingDir);
    } catch (Exception e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown, "Processing should throw exception with continueOnError=false");
    FileUtils.cleanDirectory(workingDir);
  }

  @Test
  public void testContinueOnErrorDuringStatsAndIndexing()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "continue_on_error_stats_indexing_output");
    FileUtils.forceMkdir(workingDir);

    // Create data with 10 rows, rows 3, 5, 8 have invalid clicks value
    List<Object[]> testData = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      testData.add(new Object[]{"abc", 1000 + i, 1597719600000L + i * 1000});
    }

    File inputDir = new File(TEMP_DIR, "continue_on_error_stats_indexing_input");
    Set<Integer> invalidRowIndices = new HashSet<>(Arrays.asList(3, 5, 8));
    List<File> inputSegments = createSegmentWithInvalidData(inputDir, testData, _schema, "clicks", invalidRowIndices);

    // Configure continueOnError = true
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setContinueOnError(true);
    TableConfig tableConfigWithContinueOnError = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("myTable")
        .setTimeColumnName("time")
        .setIngestionConfig(ingestionConfig)
        .build();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder()
        .setTableConfig(tableConfigWithContinueOnError)
        .setSchema(_schema)
        .build();

    List<File> outputSegments = processSegments(inputSegments, config, workingDir);

    // Verify processing completes with all 10 docs
    assertEquals(outputSegments.size(), 1, "Should create 1 output segment");

    ImmutableSegment segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
    SegmentMetadata metadata = segment.getSegmentMetadata();
    assertEquals(metadata.getTotalDocs(), 10, "Should have all 10 docs");

    // Verify rows with invalid values use default (1000)
    try (PinotSegmentColumnReader clicksReader = new PinotSegmentColumnReader(segment, "clicks")) {
      for (int i = 0; i < 10; i++) {
        int clicksValue = (int) clicksReader.getValue(i);
        if (invalidRowIndices.contains(i)) {
          assertEquals(clicksValue, 1000, "Invalid rows should use default clicks value");
        } else {
          assertEquals(clicksValue, 1000 + i, "Valid rows should retain original clicks value");
        }
      }
    }

    segment.destroy();
    FileUtils.cleanDirectory(workingDir);

    // Now create config with continueOnError = false and verify exception
    tableConfigWithContinueOnError.setIngestionConfig(new IngestionConfig());
    SegmentProcessorConfig configFailFast = new SegmentProcessorConfig.Builder()
        .setTableConfig(tableConfigWithContinueOnError)
        .setSchema(_schema)
        .build();
    boolean exceptionThrown = false;
    try {
      processSegments(inputSegments, configFailFast, workingDir);
    } catch (Exception e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown, "Processing should throw exception with continueOnError=false");
    FileUtils.cleanDirectory(workingDir);
  }

  @Test
  public void testAllRecordsFilteredByTime()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "all_records_filtered_output");
    FileUtils.forceMkdir(workingDir);

    // Create segment with times in 2020
    List<Object[]> testData = generatePartitionedData(10, "abc", 1597719600000L);
    File inputDir = new File(TEMP_DIR, "all_records_filtered_input");
    List<File> inputSegments = createInputSegments(inputDir, testData, 1, _schema);

    // Configure time filter for far future (2030-2035)
    long futureStart = 1893456000000L; // Jan 1, 2030
    long futureEnd = 2051222400000L;   // Jan 1, 2035

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder()
        .setTableConfig(_tableConfig)
        .setSchema(_schema)
        .setTimeHandlerConfig(
            new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH)
                .setTimeRange(futureStart, futureEnd)
                .build())
        .build();

    List<File> outputSegments = processSegments(inputSegments, config, workingDir);

    // Verify empty list returned
    assertTrue(outputSegments.isEmpty(), "Should return empty list when all records filtered");

    FileUtils.cleanDirectory(workingDir);
  }

  @Test
  public void testEmptyPartitions()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "empty_partitions_output");
    FileUtils.forceMkdir(workingDir);

    // Create data with only abc and xyz values (no other partition values)
    List<Object[]> testData = new ArrayList<>();
    testData.addAll(generatePartitionedData(15, "abc", 1597719600000L));
    testData.addAll(generatePartitionedData(12, "xyz", 1597719600000L));

    File inputDir = new File(TEMP_DIR, "empty_partitions_input");
    List<File> inputSegments = createInputSegments(inputDir, testData, 1, _schema);

    // Configure column partitioner
    PartitionerConfig columnPartitioner = new PartitionerConfig.Builder()
        .setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
        .setColumnName("campaign")
        .build();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder()
        .setTableConfig(_tableConfig)
        .setSchema(_schema)
        .setPartitionerConfigs(List.of(columnPartitioner))
        .build();

    List<File> outputSegments = processSegments(inputSegments, config, workingDir);

    // Verify only 2 output segments (for abc and xyz, no empty partitions)
    assertEquals(outputSegments.size(), 2, "Should create exactly 2 output segments");

    // Verify total docs
    int totalDocs = 0;
    for (File segmentDir : outputSegments) {
      SegmentMetadata metadata = new SegmentMetadataImpl(segmentDir);
      totalDocs += metadata.getTotalDocs();
    }
    assertEquals(totalDocs, 27, "Total docs should be 27 (15 + 12)");

    FileUtils.cleanDirectory(workingDir);
  }

  @Test
  public void testTwoColumnPartitioningWithoutTime()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "two_column_partitioning_output");
    FileUtils.forceMkdir(workingDir);

    // Create test data with multiple combinations of campaign and clicks values
    // campaign: "abc", "xyz", "pqr"
    // clicks: different ranges to test partitioning
    List<Object[]> testData = new ArrayList<>();
    long baseTime = 1597719600000L;

    // Add data for abc campaign with 1000 clicks
    for (int i = 0; i < 20; i++) {
      testData.add(new Object[]{"abc", 1000, baseTime + i * 1000});
    }
    // Add data for abc campaign with 2000 clicks
    for (int i = 0; i < 15; i++) {
      testData.add(new Object[]{"abc", 2000, baseTime + i * 1000});
    }
    // Add data for xyz campaign with 1000 clicks
    for (int i = 0; i < 18; i++) {
      testData.add(new Object[]{"xyz", 1000, baseTime + i * 1000});
    }
    // Add data for xyz campaign with 3000 clicks
    for (int i = 0; i < 12; i++) {
      testData.add(new Object[]{"xyz", 3000, baseTime + i * 1000});
    }
    // Add data for pqr campaign with 2000 clicks
    for (int i = 0; i < 10; i++) {
      testData.add(new Object[]{"pqr", 2000, baseTime + i * 1000});
    }
    Collections.shuffle(testData, new Random());

    File inputDir = new File(TEMP_DIR, "two_column_partitioning_input");
    List<File> inputSegments = createInputSegments(inputDir, testData, 1, _schema);

    // Configure two column partitioners - one for campaign, one for clicks
    PartitionerConfig campaignPartitioner = new PartitionerConfig.Builder()
        .setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
        .setColumnName("campaign")
        .build();
    PartitionerConfig clicksPartitioner = new PartitionerConfig.Builder()
        .setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
        .setColumnName("clicks")
        .build();

    SegmentProcessorConfig config = new SegmentProcessorConfig.Builder()
        .setTableConfig(_tableConfig)
        .setSchema(_schema)
        .setPartitionerConfigs(List.of(campaignPartitioner, clicksPartitioner))
        .build();

    List<File> outputSegments = processSegments(inputSegments, config, workingDir);

    // Expected partitions:
    // abc + 1000-1099 (20 docs)
    // abc + 2000-2099 (15 docs)
    // xyz + 1000-1099 (18 docs)
    // xyz + 3000-3099 (12 docs)
    // pqr + 2000-2099 (10 docs)
    // Total: 5 segments
    assertEquals(outputSegments.size(), 5, "Should create exactly 5 segments for unique combinations");

    // Verify total document count
    int totalDocs = 0;
    for (File segmentDir : outputSegments) {
      SegmentMetadata metadata = new SegmentMetadataImpl(segmentDir);
      totalDocs += metadata.getTotalDocs();

      // Each segment should have only one campaign value (cardinality = 1)
      ColumnMetadata campaignMetadata = metadata.getColumnMetadataFor("campaign");
      assertNotNull(campaignMetadata, "Campaign column metadata should not be null");
      assertEquals(campaignMetadata.getCardinality(), 1,
          "Each segment should have exactly 1 campaign value");

      // Each segment should have clicks values from a specific partition
      ColumnMetadata clicksMetadata = metadata.getColumnMetadataFor("clicks");
      assertEquals(clicksMetadata.getCardinality(), 1,
          "Each segment should have exactly 1 clicks value");
    }
    assertEquals(totalDocs, 75, "Total docs should be 75 (20+15+18+12+10)");

    FileUtils.cleanDirectory(workingDir);
    FileUtils.cleanDirectory(inputDir);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
