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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.utils.CrcUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class CrcUtilsTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "CrcUtilsTest");
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String COMPLEX_SCHEMA_NAME = "data/test_crc_complex_schema.json";
  private static final String COMPLEX_TABLE_CONFIG_NAME = "data/test_crc_complex_table.json";
  private static final String COMPLEX_DATA_NAME = "data/test_crc_complex_sample_data.csv";

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
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(false), 2102337593L);

    new SegmentV1V2ToV3FormatConverter().convert(indexDir);
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(false), 3362640853L);
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
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(false), 289171778L);

    new SegmentV1V2ToV3FormatConverter().convert(indexDir);
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(false), 3409394291L);
  }

  @Test
  public void testCrcAllFilesWithLuceneFstIndex()
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
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(false), 2627227852L);

    new SegmentV1V2ToV3FormatConverter().convert(indexDir);
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(false), 1229791705L);
  }

  @Test
  public void testCrcAllFilesWithLuceneTextIndex()
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
    System.out.println(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(false));

    new SegmentV1V2ToV3FormatConverter().convert(indexDir);
    System.out.println(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(false));
  }

  @Test
  public void testCrcConsistencyWithExternalIndexesForAllSegmentVers()
      throws Exception {
    int numRuns = 5;

    URL schemaResource = getClass().getClassLoader().getResource(COMPLEX_SCHEMA_NAME);
    URL tableConfigResource = getClass().getClassLoader().getResource(COMPLEX_TABLE_CONFIG_NAME);
    URL dataResource = getClass().getClassLoader().getResource(COMPLEX_DATA_NAME);

    assertNotNull(schemaResource, "Schema file not found: " + COMPLEX_SCHEMA_NAME);
    assertNotNull(tableConfigResource, "Table config file not found: " + COMPLEX_TABLE_CONFIG_NAME);
    assertNotNull(dataResource, "Data file not found: " + COMPLEX_DATA_NAME);

    Schema schema = Schema.fromFile(new File(TestUtils.getFileFromResourceUrl(schemaResource)));
    TableConfig tableConfig = createTableConfig(new File(tableConfigResource.getFile()));
    File dataFile = new File(TestUtils.getFileFromResourceUrl(dataResource));

    SegmentVersion[] versionsToTest = new SegmentVersion[]{
        SegmentVersion.v1,
        SegmentVersion.v2,
        SegmentVersion.v3
    };

    for (SegmentVersion version : versionsToTest) {

      List<Long> crcs = new ArrayList<Long>();

      for (int i = 0; i < numRuns; i++) {
        FileUtils.deleteDirectory(INDEX_DIR);
        INDEX_DIR.mkdirs();

        SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
        config.setInputFilePath(dataFile.getAbsolutePath());
        config.setSegmentVersion(version);
        config.setOutDir(INDEX_DIR.getAbsolutePath());
        config.setFormat(FileFormat.CSV);
        config.setSegmentName("testCrcSegment");

        CSVRecordReaderConfig csvReaderConfig = new CSVRecordReaderConfig();
        csvReaderConfig.setMultiValueDelimiter('|');
        csvReaderConfig.setSkipHeader(false);

        config.setReaderConfig(csvReaderConfig);
        SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
        driver.init(config);
        driver.build();

        File indexDir = driver.getOutputDirectory();

        long currentDataCrc = readDataOnlyCrcFromMeta(indexDir);

        crcs.add(currentDataCrc);
        System.out.println(String.format("  [%s] Run %d Data CRC got from meta file: %d",
            version, i + 1,
            currentDataCrc));
      }

      Assert.assertFalse(crcs.isEmpty());
      long firstRunCrc = crcs.get(0);
      for (int i = 1; i < crcs.size(); i++) {
        Assert.assertEquals(crcs.get(i).longValue(), firstRunCrc,
            String.format("Determinism test Failed for %s! Run 1 vs Run %d", version, i + 1));
      }
    }
  }

  /**
   * Helper method to read the 'dataOnlyCrc' from the creation.meta file.
   * Logic corresponds to: output.writeLong(crc); output.writeLong(creationTime); output.writeLong(dataOnlyCrc);
   */
  private long readDataOnlyCrcFromMeta(File indexDir) throws IOException {
    File metaFile = new File(indexDir, V1Constants.SEGMENT_CREATION_META);

    if (!metaFile.exists()) {
      File[] files = indexDir.listFiles();
      if (files != null) {
        for (File f : files) {
          if (f.isDirectory()) {
            File nestedMeta = new File(f, V1Constants.SEGMENT_CREATION_META);
            if (nestedMeta.exists()) {
              metaFile = nestedMeta;
              break;
            }
          }
        }
      }
    }
    Assert.assertTrue(metaFile.exists(), "creation.meta file could not be found in: " + indexDir.getAbsolutePath());

    try (DataInputStream input = new DataInputStream(new FileInputStream(metaFile))) {
      long crc = input.readLong();           // 1st: Standard CRC (Skip)
      long creationTime = input.readLong();  // 2nd: Creation Time (Skip)
      return input.readLong();               // 3rd: Data Only CRC (Return this)
    }
  }
  private static TableConfig createTableConfig(File tableConfigFile)
      throws IOException {
    InputStream inputStream = new FileInputStream(tableConfigFile);
    Assert.assertNotNull(inputStream);
    return JsonUtils.inputStreamToObject(inputStream, TableConfig.class);
  }
}
