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
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.local.utils.CrcUtils;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FstIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class CrcUtilsTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "CrcUtilsTest");
  private static final String AVRO_DATA = "data/test_data-mv.avro";

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
    String filePath = TestUtils.getFileFromResourceUrl(resource);
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            TimeUnit.DAYS, "testTable");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    File indexDir = driver.getOutputDirectory();
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), 2708456273L);

    new SegmentV1V2ToV3FormatConverter().convert(indexDir);
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), 2796149869L);
  }

  @Test
  public void testCrcWithNativeFstIndex()
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = TestUtils.getFileFromResourceUrl(resource);
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            TimeUnit.DAYS, "testTable");
    FstIndexConfig fstIndexConfig = new FstIndexConfig(FSTType.NATIVE);
    config.setIndexOn(StandardIndexes.fst(), fstIndexConfig, "column5");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    File indexDir = driver.getOutputDirectory();
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), 3358657641L);

    new SegmentV1V2ToV3FormatConverter().convert(indexDir);
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), 961102604L);
  }

  @Test
  public void testCrcWithLuceneFstIndex()
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = TestUtils.getFileFromResourceUrl(resource);
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            TimeUnit.DAYS, "testTable");
    FstIndexConfig fstIndexConfig = new FstIndexConfig(FSTType.LUCENE);
    config.setIndexOn(StandardIndexes.fst(), fstIndexConfig, "column5");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    File indexDir = driver.getOutputDirectory();
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), 3294819300L);

    new SegmentV1V2ToV3FormatConverter().convert(indexDir);
    assertEquals(CrcUtils.forAllFilesInFolder(indexDir).computeCrc(), 2552900261L);
  }

  // @Test
  public void testCrcWithLuceneTextIndex()
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = TestUtils.getFileFromResourceUrl(resource);
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            TimeUnit.DAYS, "testTable");
    addTextIndex(config, "column5");
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

  private void addTextIndex(SegmentGeneratorConfig config, String colName) {
    FieldIndexConfigs fieldIndexConfigs = config.getIndexConfigsByColName().get(colName);
    TextIndexConfig textConfig = fieldIndexConfigs.getConfig(StandardIndexes.text());
    TextIndexConfig newTextConfig = new TextIndexConfigBuilder(textConfig).build();
    config.setIndexOn(StandardIndexes.text(), newTextConfig, colName);
  }
}
