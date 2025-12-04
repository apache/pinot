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
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandler;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandlerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.data.readers.RecordReaderFileConfig;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import static org.testng.Assert.assertFalse;


/**
 * End-to-end tests for SegmentProcessorFramework using RecordReader-based processing.
 */
public class SegmentProcessorFrameworkTest extends BaseSegmentProcessorFrameworkTest {

  @Override
  protected List<File> processSegments(List<File> segmentDirs, SegmentProcessorConfig config, File workingDir)
      throws Exception {
    List<RecordReaderFileConfig> recordReaderConfigs = new ArrayList<>();
    for (File segmentDir : segmentDirs) {
      PinotSegmentRecordReader reader = new PinotSegmentRecordReader();
      reader.init(segmentDir, null, null, true);
      recordReaderConfigs.add(new RecordReaderFileConfig(reader));
    }
    SegmentProcessorFramework framework =
        new SegmentProcessorFramework(config, workingDir, recordReaderConfigs, Collections.emptyList(), null);
    return framework.process();
  }

  /**
   * Test lazy initialization of record readers. Here we create
   * RecoderReaderFileConfig and the actual reader is initialized during the
   * map phase.
   */
  @Test
  public void testRecordReaderFileConfigInit() throws Exception {
    File workingDir = new File(TEMP_DIR, "segmentOutput");
    FileUtils.forceMkdir(workingDir);
    ClassLoader classLoader = getClass().getClassLoader();
    URL resource = classLoader.getResource("data/dimBaseballTeams.csv");
    assertNotNull(resource);
    RecordReader recordReader = RecordReaderFactory.getRecordReader(FileFormat.CSV, new File(resource.toURI()),
        null, null);
    RecordReaderFileConfig recordReaderFileConfig = new RecordReaderFileConfig(FileFormat.CSV,
        new File(resource.toURI()),
        null, null, recordReader);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").
        setTimeColumnName("time").build();

    Schema schema =
        new Schema.SchemaBuilder().setSchemaName("mySchema").addSingleValueDimension("teamId",
                FieldSpec.DataType.STRING, "")
            .addSingleValueDimension("teamName", FieldSpec.DataType.STRING, "")
            .addDateTime("time", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema).build();
    SegmentProcessorFramework framework = new SegmentProcessorFramework(config, workingDir,
        List.of(recordReaderFileConfig), Collections.emptyList(), null);
    List<File> outputSegments = framework.process();
    assertEquals(outputSegments.size(), 1);
    ImmutableSegment segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
    SegmentMetadata segmentMetadata = segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 52);
    // Verify reader is closed
    assertTrue(recordReaderFileConfig.isRecordReaderClosedFromRecordReaderFileConfig());
  }

  @Test
  public void testConfigurableMapperOutputSize()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "configurable_mapper_test_output");
    FileUtils.forceMkdir(workingDir);
    int expectedTotalDocsCount = 10;

    // Test 1 :  Default case i.e. no limit to mapper output file size (single segment).

    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    List<File> outputSegments = processSegments(_singleSegment, config, workingDir);
    assertEquals(outputSegments.size(), 1);
    String[] outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), expectedTotalDocsCount);
    assertEquals(segmentMetadata.getName(), "myTable_1597719600000_1597892400000_0");
    FileUtils.cleanDirectory(workingDir);

    // Test 2 :  Default case i.e. no limit to mapper output file size (multiple segments).
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    outputSegments = processSegments(_multipleSegments, config, workingDir);
    assertEquals(outputSegments.size(), 1);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), expectedTotalDocsCount);
    assertEquals(segmentMetadata.getName(), "myTable_1597719600000_1597892400000_0");
    FileUtils.cleanDirectory(workingDir);

    // Test 3 :  Test mapper with threshold output size (single segment).

    // Create a segmentConfig with intermediate mapper output size threshold set to the number of bytes in each row
    // from the data. In this way, we can test if each row is written to a separate segment.
    SegmentConfig segmentConfig =
        new SegmentConfig.Builder().setIntermediateFileSizeThreshold(16).setSegmentNamePrefix("testPrefix")
            .setSegmentNamePostfix("testPostfix").build();
    config = new SegmentProcessorConfig.Builder().setSegmentConfig(segmentConfig).setTableConfig(_tableConfig)
        .setSchema(_schema).build();
    outputSegments = processSegments(_singleSegment, config, workingDir);
    assertEquals(outputSegments.size(), expectedTotalDocsCount);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));

    // Verify that each segment has only one row, and the segment name is correct.

    for (int i = 0; i < expectedTotalDocsCount; i++) {
      segmentMetadata = new SegmentMetadataImpl(outputSegments.get(i));
      assertEquals(segmentMetadata.getTotalDocs(), 1);
      assertTrue(segmentMetadata.getName().matches("testPrefix_.*_testPostfix_" + i));
    }
    FileUtils.cleanDirectory(workingDir);

    // Test 4 :  Test mapper with threshold output size (multiple segments).

    // Create a segmentConfig with intermediate mapper output size threshold set to the number of bytes in each row
    // from the data. In this way, we can test if each row is written to a separate segment.
    segmentConfig = new SegmentConfig.Builder().setIntermediateFileSizeThreshold(16).setSegmentNamePrefix("testPrefix")
        .setSegmentNamePostfix("testPostfix").build();
    config = new SegmentProcessorConfig.Builder().setSegmentConfig(segmentConfig).setTableConfig(_tableConfig)
        .setSchema(_schema).build();
    outputSegments = processSegments(_multipleSegments, config, workingDir);
    assertEquals(outputSegments.size(), expectedTotalDocsCount);
    outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));

    // Verify that each segment has only one row, and the segment name is correct.

    for (int i = 0; i < expectedTotalDocsCount; i++) {
      segmentMetadata = new SegmentMetadataImpl(outputSegments.get(i));
      assertEquals(segmentMetadata.getTotalDocs(), 1);
      assertTrue(segmentMetadata.getName().matches("testPrefix_.*_testPostfix_" + i));
    }
    FileUtils.cleanDirectory(workingDir);

    // Test 5 :  Test with injected failure in mapper to verify output directory is cleaned up.

    List<File> testList = new ArrayList<>(_multipleSegments);
    segmentConfig = new SegmentConfig.Builder().setIntermediateFileSizeThreshold(16).setSegmentNamePrefix("testPrefix")
        .setSegmentNamePostfix("testPostfix").build();
    config = new SegmentProcessorConfig.Builder().setSegmentConfig(segmentConfig).setTableConfig(_tableConfig)
        .setSchema(_schema).build();
    processSegments(testList, config, workingDir);
    List<RecordReader> readers = new ArrayList<>();

    int index = 0;
    for (File segmentDir : testList) {
      if (index == 1) {
        // Inject failure
        readers.add(null);
      } else {
        PinotSegmentRecordReader reader = new PinotSegmentRecordReader();
        reader.init(segmentDir, null, null, true);
        readers.add(reader);
      }
      index++;
    }
    SegmentProcessorFramework failureTest = new SegmentProcessorFramework(readers, config, workingDir);
    assertThrows(NullPointerException.class, failureTest::process);
    assertTrue(FileUtils.isEmptyDirectory(workingDir));

    // Test 6: RecordReader should be closed when recordReader is created inside RecordReaderFileConfig (without mapper
    // output size threshold configured).

    ClassLoader classLoader = getClass().getClassLoader();
    URL resource = classLoader.getResource("data/dimBaseballTeams.csv");
    assertNotNull(resource);
    RecordReaderFileConfig recordReaderFileConfig =
        new RecordReaderFileConfig(FileFormat.CSV, new File(resource.toURI()), null, null, null);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("time").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("mySchema")
            .addSingleValueDimension("teamId", FieldSpec.DataType.STRING, "")
            .addSingleValueDimension("teamName", FieldSpec.DataType.STRING, "")
            .addDateTime("time", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

    config = new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema).build();

    SegmentProcessorFramework frameworkWithRecordReaderFileConfig =
        new SegmentProcessorFramework(config, workingDir, List.of(recordReaderFileConfig),
            Collections.emptyList(), null);
    outputSegments = frameworkWithRecordReaderFileConfig.process();

    // Verify the number of segments created and the total docs.
    assertEquals(outputSegments.size(), 1);
    ImmutableSegment segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
    segmentMetadata = segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 52);

    // Verify that the record reader is closed from RecordReaderFileConfig.
    assertTrue(recordReaderFileConfig.isRecordReaderClosedFromRecordReaderFileConfig());
    FileUtils.cleanDirectory(workingDir);

    // Test 7: RecordReader should not be closed when recordReader is passed to RecordReaderFileConfig. (Without
    // mapper output size threshold configured)

    RecordReader recordReader = recordReaderFileConfig.getRecordReader();
    recordReader.rewind();

    // Pass the recordReader to RecordReaderFileConfig.
    recordReaderFileConfig = new RecordReaderFileConfig(recordReader);
    SegmentProcessorFramework frameworkWithDelegateRecordReader =
        new SegmentProcessorFramework(config, workingDir, List.of(recordReaderFileConfig),
            Collections.emptyList(), null);
    outputSegments = frameworkWithDelegateRecordReader.process();

    // Verify the number of segments created and the total docs.
    assertEquals(outputSegments.size(), 1);
    segment = ImmutableSegmentLoader.load(outputSegments.get(0), ReadMode.mmap);
    segmentMetadata = segment.getSegmentMetadata();
    assertEquals(segmentMetadata.getTotalDocs(), 52);

    // Verify that the record reader is not closed from RecordReaderFileConfig.
    assertFalse(recordReaderFileConfig.isRecordReaderClosedFromRecordReaderFileConfig());
    FileUtils.cleanDirectory(workingDir);

    // Test 8: RecordReader should be closed when recordReader is created inside RecordReaderFileConfig (With mapper
    // output size threshold configured).

    expectedTotalDocsCount = 52;
    recordReaderFileConfig = new RecordReaderFileConfig(FileFormat.CSV, new File(resource.toURI()), null, null, null);

    segmentConfig = new SegmentConfig.Builder().setIntermediateFileSizeThreshold(19).setSegmentNamePrefix("testPrefix")
        .setSegmentNamePostfix("testPostfix").build();
    config = new SegmentProcessorConfig.Builder().setSegmentConfig(segmentConfig).setTableConfig(tableConfig)
        .setSchema(schema).build();

    frameworkWithRecordReaderFileConfig =
        new SegmentProcessorFramework(config, workingDir, List.of(recordReaderFileConfig),
            Collections.emptyList(), null);
    outputSegments = frameworkWithRecordReaderFileConfig.process();

    // Verify that each segment has only one row.
    for (int i = 0; i < expectedTotalDocsCount; i++) {
      segmentMetadata = new SegmentMetadataImpl(outputSegments.get(i));
      assertEquals(segmentMetadata.getTotalDocs(), 1);
    }

    // Verify that the record reader is closed from RecordReaderFileConfig.
    assertTrue(recordReaderFileConfig.isRecordReaderClosedFromRecordReaderFileConfig());
    FileUtils.cleanDirectory(workingDir);

    // Test 9: RecordReader should not be closed when recordReader is passed to RecordReaderFileConfig (With mapper
    // output size threshold configured).

    recordReader = recordReaderFileConfig.getRecordReader();
    recordReader.rewind();

    // Pass the recordReader to RecordReaderFileConfig.
    recordReaderFileConfig = new RecordReaderFileConfig(recordReader);
    frameworkWithDelegateRecordReader =
        new SegmentProcessorFramework(config, workingDir, List.of(recordReaderFileConfig),
            Collections.emptyList(), null);
    outputSegments = frameworkWithDelegateRecordReader.process();

    // Verify that each segment has only one row.
    for (int i = 0; i < expectedTotalDocsCount; i++) {
      segmentMetadata = new SegmentMetadataImpl(outputSegments.get(i));
      assertEquals(segmentMetadata.getTotalDocs(), 1);
    }

    // Verify that the record reader is not closed from RecordReaderFileConfig.
    assertFalse(recordReaderFileConfig.isRecordReaderClosedFromRecordReaderFileConfig());
    FileUtils.cleanDirectory(workingDir);
  }

  @Test
  public void testMultipleSegments()
      throws Exception {
    File workingDir = new File(TEMP_DIR, "multiple_segments_output");
    FileUtils.forceMkdir(workingDir);

    // Default configs
    SegmentProcessorConfig config =
        new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).build();
    List<File> outputSegments = processSegments(_multipleSegments, config, workingDir);
    assertEquals(outputSegments.size(), 1);
    String[] outputDirs = workingDir.list();
    assertTrue(outputDirs != null && outputDirs.length == 1, Arrays.toString(outputDirs));
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(outputSegments.get(0));
    assertEquals(segmentMetadata.getTotalDocs(), 10);
    assertEquals(segmentMetadata.getName(), "myTable_1597719600000_1597892400000_0");
    FileUtils.cleanDirectory(workingDir);

    // Time round, partition, rollup
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfig).setSchema(_schema).setTimeHandlerConfig(
        new TimeHandlerConfig.Builder(TimeHandler.Type.EPOCH).setRoundBucketMs(86400000).setPartitionBucketMs(86400000)
            .build()).setMergeType(MergeType.ROLLUP).build();
    outputSegments = processSegments(_multipleSegments, config, workingDir);
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
    List<File> outputSegments = processSegments(_multiValueSegments, config, workingDir);
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

    // Dedup
    config = new SegmentProcessorConfig.Builder().setTableConfig(_tableConfigNullValueEnabled).setSchema(_schemaMV)
        .setMergeType(MergeType.DEDUP).build();
    outputSegments = processSegments(_multiValueSegments, config, workingDir);
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
  }
}
