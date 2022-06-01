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
package org.apache.pinot.plugin.segmentwriter.filebased;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.segment.writer.SegmentWriter;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for {@link FileBasedSegmentWriter}
 */
public class FileBasedSegmentWriterTest {

  private static final String TABLE_NAME = "segmentWriter";
  private static final String TIME_COLUMN_NAME = "aLong";

  private File _tmpDir;
  private File _outputDir;
  private TableConfig _tableConfig;
  private IngestionConfig _ingestionConfig;
  private Schema _schema;

  @BeforeClass
  public void setup() {
    _tmpDir = new File(FileUtils.getTempDirectory(), FileBasedSegmentWriterTest.class.getName());
    FileUtils.deleteQuietly(_tmpDir);
    Preconditions.checkState(_tmpDir.mkdirs());
    _outputDir = new File(_tmpDir, "segmentWriterOutputDir");

    _ingestionConfig = new IngestionConfig();
    _ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(Collections.singletonList(
        Collections.singletonMap(BatchConfigProperties.OUTPUT_DIR_URI, _outputDir.getAbsolutePath())), "APPEND",
        "HOURLY"));
    _ingestionConfig.setTransformConfigs(Arrays.asList(new TransformConfig("aSimpleMap_str", "jsonFormat(aSimpleMap)"),
        new TransformConfig("anAdvancedMap_str", "jsonFormat(anAdvancedMap)")));
    _tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(_ingestionConfig)
            .setTimeColumnName(TIME_COLUMN_NAME).build();
    _schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("aString", FieldSpec.DataType.STRING)
        .addSingleValueDimension("aSimpleMap_str", FieldSpec.DataType.STRING)
        .addSingleValueDimension("anAdvancedMap_str", FieldSpec.DataType.STRING)
        .addSingleValueDimension("nullString", FieldSpec.DataType.STRING)
        .addSingleValueDimension("aBoolean", FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension("aBytes", FieldSpec.DataType.BYTES)
        .addMultiValueDimension("aStringList", FieldSpec.DataType.STRING)
        .addMultiValueDimension("anIntList", FieldSpec.DataType.INT)
        .addMultiValueDimension("aStringArray", FieldSpec.DataType.STRING)
        .addMultiValueDimension("aDoubleArray", FieldSpec.DataType.DOUBLE).addMetric("anInt", FieldSpec.DataType.INT)
        .addMetric("aFloat", FieldSpec.DataType.FLOAT).addMetric("aDouble", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
  }

  /**
   * Tests init on batchConfig combinations
   */
  @Test
  public void testBatchConfigs()
      throws Exception {
    SegmentWriter segmentWriter = new FileBasedSegmentWriter();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME).build();
    try {
      segmentWriter.init(tableConfig, _schema);
      Assert.fail("Should fail due to missing ingestionConfig");
    } catch (IllegalStateException e) {
      // expected
    }

    IngestionConfig ingestionConfig = new IngestionConfig();
    tableConfig.setIngestionConfig(ingestionConfig);
    try {
      segmentWriter.init(tableConfig, _schema);
      Assert.fail("Should fail due to missing batchIngestionConfig");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "APPEND", "HOURLY"));
    try {
      segmentWriter.init(tableConfig, _schema);
      Assert.fail("Should fail due to missing batchConfigMaps");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(Collections.emptyList(), "APPEND", "HOURLY"));
    try {
      segmentWriter.init(tableConfig, _schema);
      Assert.fail("Should fail due to missing batchConfigMaps");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Collections.singletonList(Collections.emptyMap()), "APPEND", "HOURLY"));
    try {
      segmentWriter.init(tableConfig, _schema);
      Assert.fail("Should fail due to missing outputDirURI in batchConfigMap");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(Collections.singletonList(
        Collections.singletonMap(BatchConfigProperties.OUTPUT_DIR_URI, _outputDir.getAbsolutePath())), "APPEND",
        "HOURLY"));
    segmentWriter.init(tableConfig, _schema);
    segmentWriter.close();
  }

  /**
   * Tests that {@link SegmentWriter} generates segments as expected
   */
  @Test
  public void testSegmentWriter()
      throws Exception {
    FileUtils.deleteQuietly(_outputDir);
    SegmentWriter segmentWriter = new FileBasedSegmentWriter();
    segmentWriter.init(_tableConfig, _schema);

    // write 3 records
    segmentWriter.collect(getGenericRow("record1", 1616238000000L));
    segmentWriter.collect(getGenericRow("record2", 1616241600000L));
    segmentWriter.collect(getGenericRow("record3", 1616241600000L));
    segmentWriter.flush();

    // verify num docs and cardinality of aString
    File segmentTar = new File(_outputDir, "segmentWriter_1616238000000_1616241600000.tar.gz");
    Assert.assertTrue(segmentTar.exists());
    TarGzCompressionUtils.untar(segmentTar, _outputDir);
    SegmentMetadataImpl segmentMetadata =
        new SegmentMetadataImpl(new File(_outputDir, "segmentWriter_1616238000000_1616241600000"));
    Assert.assertEquals(segmentMetadata.getTotalDocs(), 3);
    Assert.assertEquals(segmentMetadata.getColumnMetadataFor("aString").getCardinality(), 3);
    Assert.assertEquals(segmentMetadata.getColumnMetadataFor("aLong").getCardinality(), 2);
    Assert.assertEquals(segmentMetadata.getColumnMetadataFor("anInt").getCardinality(), 1);

    // write 2 records
    segmentWriter.collect(getGenericRow("record4", 1616245200000L));
    segmentWriter.collect(getGenericRow("record5", 1616245200000L));
    segmentWriter.flush();

    // verify num docs and cardinality of aString
    segmentTar = new File(_outputDir, "segmentWriter_1616245200000_1616245200000.tar.gz");
    Assert.assertTrue(segmentTar.exists());
    TarGzCompressionUtils.untar(segmentTar, _outputDir);
    segmentMetadata = new SegmentMetadataImpl(new File(_outputDir, "segmentWriter_1616245200000_1616245200000"));
    Assert.assertEquals(segmentMetadata.getTotalDocs(), 2);
    Assert.assertEquals(segmentMetadata.getColumnMetadataFor("aString").getCardinality(), 2);
    Assert.assertEquals(segmentMetadata.getColumnMetadataFor("aLong").getCardinality(), 1);
    Assert.assertEquals(segmentMetadata.getColumnMetadataFor("anInt").getCardinality(), 1);

    segmentWriter.close();
    FileUtils.deleteQuietly(_outputDir);
  }

  /**
   * Tests flushing on empty collection
   */
  @Test
  public void testEmptySegment()
      throws Exception {
    FileUtils.deleteQuietly(_outputDir);
    SegmentWriter segmentWriter = new FileBasedSegmentWriter();
    segmentWriter.init(_tableConfig, _schema);

    // write 0 records
    segmentWriter.flush();

    // verify num docs and cardinality of aString
    File[] files = _outputDir.listFiles();
    Assert.assertEquals(files.length, 1);
    File segmentTar = files[0];
    TarGzCompressionUtils.untar(segmentTar, _outputDir);
    SegmentMetadataImpl segmentMetadata =
        new SegmentMetadataImpl(new File(_outputDir, files[0].getName().split(Constants.TAR_GZ_FILE_EXT)[0]));
    Assert.assertEquals(segmentMetadata.getTotalDocs(), 0);
    Assert.assertEquals(segmentMetadata.getColumnMetadataFor("aString").getCardinality(), 0);
    Assert.assertEquals(segmentMetadata.getColumnMetadataFor("aLong").getCardinality(), 0);
    Assert.assertEquals(segmentMetadata.getColumnMetadataFor("anInt").getCardinality(), 0);

    segmentWriter.close();
    FileUtils.deleteQuietly(_outputDir);
  }

  /**
   * Tests various {@link org.apache.pinot.spi.ingestion.batch.BatchConfigProperties.SegmentNameGeneratorType}
   */
  @Test
  public void testSegmentNameGenerator()
      throws Exception {
    FileUtils.deleteQuietly(_outputDir);

    // FIXED segment name
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.OUTPUT_DIR_URI, _outputDir.getAbsolutePath());
    batchConfigMap.put(BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE,
        BatchConfigProperties.SegmentNameGeneratorType.FIXED);
    batchConfigMap.put(String.format("%s.%s", BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX,
        BatchConfigProperties.SEGMENT_NAME), "customSegmentName");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Collections.singletonList(batchConfigMap), "APPEND", "HOURLY"));
    ingestionConfig.setTransformConfigs(_ingestionConfig.getTransformConfigs());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setIngestionConfig(ingestionConfig).build();

    SegmentWriter segmentWriter = new FileBasedSegmentWriter();
    segmentWriter.init(tableConfig, _schema);

    // write 2 records
    segmentWriter.collect(getGenericRow("record1", 1616238000000L));
    segmentWriter.collect(getGenericRow("record2", 1616241600000L));
    segmentWriter.flush();

    // segment name should be customSegmentName
    File[] segmentTars = _outputDir.listFiles();
    Assert.assertEquals(segmentTars.length, 1);
    TarGzCompressionUtils.untar(segmentTars[0], _outputDir);
    Assert.assertEquals(segmentTars[0].getName(), "customSegmentName.tar.gz");
    FileUtils.deleteQuietly(_outputDir);
    segmentWriter.close();

    // NORMALIZED segment name
    batchConfigMap.put(BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE,
        BatchConfigProperties.SegmentNameGeneratorType.NORMALIZED_DATE);
    segmentWriter.init(tableConfig, _schema);

    // write 2 records
    segmentWriter.collect(getGenericRow("record1", 1616238000000L));
    segmentWriter.collect(getGenericRow("record2", 1616241600000L));
    segmentWriter.flush();

    // segment name should be normalized for hours since epoch
    segmentTars = _outputDir.listFiles();
    Assert.assertEquals(segmentTars.length, 1);
    TarGzCompressionUtils.untar(segmentTars[0], _outputDir);
    Assert.assertEquals(segmentTars[0].getName(), "segmentWriter_2021-03-20-11_2021-03-20-12.tar.gz");
    FileUtils.deleteQuietly(_outputDir);

    // SIMPLE segment name w/ sequenceId
    batchConfigMap.put(BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE,
        BatchConfigProperties.SegmentNameGeneratorType.SIMPLE);
    batchConfigMap.put(BatchConfigProperties.SEQUENCE_ID, "1001");
    segmentWriter.init(tableConfig, _schema);

    // write 2 records
    segmentWriter.collect(getGenericRow("record1", 1616238000000L));
    segmentWriter.collect(getGenericRow("record2", 1616241600000L));
    segmentWriter.flush();

    // segment name should be simple
    segmentTars = _outputDir.listFiles();
    Assert.assertEquals(segmentTars.length, 1);
    TarGzCompressionUtils.untar(segmentTars[0], _outputDir);
    Assert.assertEquals(segmentTars[0].getName(), "segmentWriter_1616238000000_1616241600000_1001.tar.gz");
    FileUtils.deleteQuietly(_outputDir);
  }

  /**
   * Tests segment overwrite config
   */
  @Test
  public void testOverwrite()
      throws Exception {
    FileUtils.deleteQuietly(_outputDir);

    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.OUTPUT_DIR_URI, _outputDir.getAbsolutePath());
    batchConfigMap.put(BatchConfigProperties.OVERWRITE_OUTPUT, "true");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Collections.singletonList(batchConfigMap), "APPEND", "HOURLY"));
    ingestionConfig.setTransformConfigs(_ingestionConfig.getTransformConfigs());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setIngestionConfig(ingestionConfig).build();

    SegmentWriter segmentWriter = new FileBasedSegmentWriter();
    segmentWriter.init(tableConfig, _schema);

    // write 3 records with same timestamp
    segmentWriter.collect(getGenericRow("record1", 1616238000000L));
    segmentWriter.collect(getGenericRow("record2", 1616238000000L));
    segmentWriter.collect(getGenericRow("record3", 1616238000000L));
    segmentWriter.flush();

    // verify 1 tar was created
    File[] segmentTars = _outputDir.listFiles();
    Assert.assertEquals(segmentTars.length, 1);
    Assert.assertEquals(segmentTars[0].getName(), "segmentWriter_1616238000000_1616238000000.tar.gz");
    TarGzCompressionUtils.untar(segmentTars[0], _outputDir);
    File segmentDir = new File(_outputDir, "segmentWriter_1616238000000_1616238000000");
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(segmentMetadata.getTotalDocs(), 3);
    FileUtils.deleteQuietly(segmentDir);

    // write 2 records with same timestamp again
    segmentWriter.collect(getGenericRow("record4", 1616238000000L));
    segmentWriter.collect(getGenericRow("record5", 1616238000000L));
    segmentWriter.flush();

    // verify tar was overwritten
    segmentTars = _outputDir.listFiles();
    Assert.assertEquals(segmentTars.length, 1);
    Assert.assertEquals(segmentTars[0].getName(), "segmentWriter_1616238000000_1616238000000.tar.gz");
    TarGzCompressionUtils.untar(segmentTars[0], _outputDir);
    segmentMetadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(segmentMetadata.getTotalDocs(), 2);
    FileUtils.deleteQuietly(segmentDir);

    segmentWriter.close();

    // unset overwrite
    tableConfig.setIngestionConfig(_ingestionConfig);
    segmentWriter.init(tableConfig, _schema);
    // write 4 records with same timestamp again
    segmentWriter.collect(getGenericRow("record6", 1616238000000L));
    segmentWriter.collect(getGenericRow("record7", 1616238000000L));
    segmentWriter.collect(getGenericRow("record8", 1616238000000L));
    segmentWriter.collect(getGenericRow("record9", 1616238000000L));
    try {
      segmentWriter.flush();
      Assert.fail();
    } catch (RuntimeException e) {
      // expected.
    }

    segmentWriter.close();
    FileUtils.deleteQuietly(_outputDir);
  }

  private static GenericRow getGenericRow(String aString, long aLong) {
    GenericRow row = new GenericRow();
    row.putValue("aString", aString);
    row.putValue("anInt", 100);
    row.putValue("aLong", aLong);
    row.putValue("aDouble", 10.5);
    row.putValue("aFloat", 2.0);
    row.putValue("aBoolean", true);
    row.putValue("aBytes", "foo".getBytes(StandardCharsets.UTF_8));
    List<String> stringList = new ArrayList<>();
    stringList.add("a");
    stringList.add("b");
    row.putValue("aStringList", stringList);
    List<Integer> intList = new ArrayList<>();
    intList.add(100);
    intList.add(200);
    row.putValue("anIntList", intList);
    row.putValue("aStringArray", new String[]{"x", "y", null});
    row.putValue("aDoubleArray", new Double[]{0.4, 0.5});
    Map<String, Object> simpleMap = new HashMap<>();
    simpleMap.put("name", "Mr. Foo");
    simpleMap.put("age", 100);
    simpleMap.put("phoneNumber", 9090909090L);
    row.putValue("aSimpleMap", simpleMap);
    Map<String, Object> advancedMap = new HashMap<>();
    advancedMap.put("list", Lists.newArrayList("p", "q", "r"));
    advancedMap.put("map", simpleMap);
    row.putValue("anAdvancedMap", advancedMap);
    row.putValue("nullString", null);

    return row;
  }

  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(_tmpDir);
  }
}
