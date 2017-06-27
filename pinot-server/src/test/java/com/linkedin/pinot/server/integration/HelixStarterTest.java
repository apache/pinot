/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.server.integration;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;
import com.linkedin.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import com.linkedin.pinot.server.util.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class HelixStarterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixStarterTest.class);

  private static final String AVRO_DATA = "data/test_sample_data.avro";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory() + File.separator + "HelixStarterTest");
  private static final ColumnarSegmentMetadataLoader columnarSegmentMetadataLoader =
      new ColumnarSegmentMetadataLoader();

  @BeforeClass
  public void setup() {
    FileUtils.deleteQuietly(INDEX_DIR);
    Assert.assertTrue(INDEX_DIR.mkdirs());
  }

  @Test
  public void testSingleHelixServerStartAndTakingSegment()
      throws Exception {
    Configuration pinotHelixProperties = new PropertiesConfiguration();
    pinotHelixProperties.addProperty("pinot.server.instance.id", "localhost:0000");
    ServerConf serverConf = DefaultHelixStarterServerConfig.getDefaultHelixServerConfig(pinotHelixProperties);
    ServerInstance serverInstance = new ServerInstance();
    serverInstance.init(serverConf, new MetricsRegistry());
    serverInstance.start();

    File segmentDir0 = new File(INDEX_DIR, "segment0");
    setupSegment(segmentDir0, "testTable0");
    File[] segment0Files = segmentDir0.listFiles();
    Assert.assertNotNull(segment0Files);
    File segmentDir1 = new File(INDEX_DIR, "segment1");
    setupSegment(segmentDir1, "testTable1");
    File[] segment1Files = segmentDir1.listFiles();
    Assert.assertNotNull(segment1Files);
    File segmentDir2 = new File(INDEX_DIR, "segment2");
    setupSegment(segmentDir2, "testTable2");
    File[] segment2Files = segmentDir2.listFiles();
    Assert.assertNotNull(segment2Files);

    DataManager instanceDataManager = serverInstance.getInstanceDataManager();
    instanceDataManager.addSegment(
        columnarSegmentMetadataLoader.loadIndexSegmentMetadataFromDir(segment0Files[0].getAbsolutePath()), null, null);
    instanceDataManager.addSegment(
        columnarSegmentMetadataLoader.loadIndexSegmentMetadataFromDir(segment1Files[0].getAbsolutePath()), null, null);
    instanceDataManager.addSegment(
        columnarSegmentMetadataLoader.loadIndexSegmentMetadataFromDir(segment2Files[0].getAbsolutePath()), null, null);
  }

  private void setupSegment(File segmentDir, String tableName)
      throws Exception {
    String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfig(new File(filePath), segmentDir, TimeUnit.DAYS,
            tableName, null);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    LOGGER.info("Table: {} built at path: {}", tableName, segmentDir.getAbsolutePath());
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
