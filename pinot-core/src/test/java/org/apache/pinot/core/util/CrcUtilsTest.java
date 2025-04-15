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
package org.apache.pinot.core.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.utils.CrcUtils;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class CrcUtilsTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "CrcUtilsTest");
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final String RAW_TABLE_NAME = "testTable";

  //@formatter:off
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension("column1", DataType.INT)
      .addSingleValueDimension("column5", DataType.STRING)
      .addMultiValueDimension("column6", DataType.INT)
      .addMetric("count", DataType.INT)
      .addDateTime("daysSinceEpoch", DataType.INT, "EPOCH|DAYS", "1:DAYS")
      .build();
  //@formatter:on

  @BeforeMethod
  public void setup()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @BeforeMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testCrc()
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    File avroFile = new File(TestUtils.getFileFromResourceUrl(resource));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName("daysSinceEpoch")
            .setInvertedIndexColumns(List.of("column1", "column5", "column6"))
            .setCreateInvertedIndexDuringSegmentGeneration(true)
            .setIngestionConfig(SegmentTestUtils.getSkipTimeCheckIngestionConfig()).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setInputFilePath(avroFile.getAbsolutePath());
    config.setSegmentVersion(SegmentVersion.v1);
    config.setOutDir(INDEX_DIR.getAbsolutePath());
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    File indexDir = driver.getOutputDirectory();
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), 2102337593L);

    new SegmentV1V2ToV3FormatConverter().convert(indexDir);
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), 3362640853L);
  }

  @Test
  public void testCrcWithNativeFstIndex()
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    File avroFile = new File(TestUtils.getFileFromResourceUrl(resource));
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig("column5", FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.FST), null,
            null));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName("daysSinceEpoch")
            .setInvertedIndexColumns(List.of("column1", "column5", "column6"))
            .setCreateInvertedIndexDuringSegmentGeneration(true).setFieldConfigList(fieldConfigs)
            .setIngestionConfig(SegmentTestUtils.getSkipTimeCheckIngestionConfig()).build();
    tableConfig.getIndexingConfig().setFSTIndexType(FSTType.NATIVE);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setInputFilePath(avroFile.getAbsolutePath());
    config.setSegmentVersion(SegmentVersion.v1);
    config.setOutDir(INDEX_DIR.getAbsolutePath());
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    File indexDir = driver.getOutputDirectory();
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), 289171778L);

    new SegmentV1V2ToV3FormatConverter().convert(indexDir);
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), 3409394291L);
  }

  @Test
  public void testCrcWithLuceneFstIndex()
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    File avroFile = new File(TestUtils.getFileFromResourceUrl(resource));
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig("column5", FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.FST), null,
            null));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName("daysSinceEpoch")
            .setInvertedIndexColumns(List.of("column1", "column5", "column6"))
            .setCreateInvertedIndexDuringSegmentGeneration(true).setFieldConfigList(fieldConfigs)
            .setIngestionConfig(SegmentTestUtils.getSkipTimeCheckIngestionConfig()).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setInputFilePath(avroFile.getAbsolutePath());
    config.setSegmentVersion(SegmentVersion.v1);
    config.setOutDir(INDEX_DIR.getAbsolutePath());
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    File indexDir = driver.getOutputDirectory();
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), 2627227852L);

    new SegmentV1V2ToV3FormatConverter().convert(indexDir);
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), 1229791705L);
  }

  @Test
  public void testCrcWithLuceneTextIndex()
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    File avroFile = new File(TestUtils.getFileFromResourceUrl(resource));
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig("column5", FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.TEXT), null,
            null));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName("daysSinceEpoch")
            .setInvertedIndexColumns(List.of("column1", "column5", "column6"))
            .setCreateInvertedIndexDuringSegmentGeneration(true).setFieldConfigList(fieldConfigs)
            .setIngestionConfig(SegmentTestUtils.getSkipTimeCheckIngestionConfig()).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setInputFilePath(avroFile.getAbsolutePath());
    config.setSegmentVersion(SegmentVersion.v1);
    config.setOutDir(INDEX_DIR.getAbsolutePath());
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    // Lucene text index data is not deterministic, thus leading to different segment crc across each test runs.
    // When using text index in RealTime table, different crc values can cause servers to have to download segments
    // from deep store to make segment replicas in sync.
    File indexDir = driver.getOutputDirectory();
    System.out.println(CrcUtils.forAllFilesInFolder(indexDir).computeCrc());

    new SegmentV1V2ToV3FormatConverter().convert(indexDir);
    System.out.println(CrcUtils.forAllFilesInFolder(indexDir).computeCrc());
  }
}
