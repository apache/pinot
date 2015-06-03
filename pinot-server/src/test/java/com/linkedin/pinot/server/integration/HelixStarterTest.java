/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.yammer.metrics.core.MetricsRegistry;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeTest;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;
import com.linkedin.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import com.linkedin.pinot.server.util.SegmentTestUtils;


public class HelixStarterTest {

  private final String AVRO_DATA = "data/test_sample_data.avro";
  private static File INDEX_DIR = new File(FileUtils.getTempDirectory() + File.separator + "HelixStarterTest");
  public static IndexSegment _indexSegment;
  private final ColumnarSegmentMetadataLoader _columnarSegmentMetadataLoader = new ColumnarSegmentMetadataLoader();

  @BeforeTest
  public void setup() throws Exception {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    INDEX_DIR.mkdirs();
  }

  private void setupSegment(File segmentDir, String tableName) throws Exception {
    System.out.println(getClass().getClassLoader().getResource(AVRO_DATA));
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));

    if (segmentDir.exists()) {
      FileUtils.deleteQuietly(segmentDir);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), segmentDir, TimeUnit.DAYS,
            tableName);

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }

  public void testSingleHelixServerStartAndTakingSegment() throws Exception {
    final Configuration pinotHelixProperties = new PropertiesConfiguration();
    final String instanceId = "localhost:0000";
    pinotHelixProperties.addProperty("pinot.server.instance.id", instanceId);
    final ServerConf serverConf = DefaultHelixStarterServerConfig.getDefaultHelixServerConfig(pinotHelixProperties);
    final ServerInstance serverInstance = new ServerInstance();

    serverInstance.init(serverConf, new MetricsRegistry());
    serverInstance.start();
    final DataManager instanceDataManager = serverInstance.getInstanceDataManager();
    final File segmentDir0 = new File(INDEX_DIR.getAbsolutePath() + "/segment0");
    System.out.println(segmentDir0);
    setupSegment(segmentDir0, "testTable0");
    final File segmentDir1 = new File(INDEX_DIR.getAbsolutePath() + "/segment1");
    System.out.println(segmentDir1);
    setupSegment(segmentDir1, "testTable1");
    final File segmentDir2 = new File(INDEX_DIR.getAbsolutePath() + "/segment2");
    System.out.println(segmentDir2);
    setupSegment(segmentDir2, "testTable2");

    instanceDataManager.addSegment(_columnarSegmentMetadataLoader.loadIndexSegmentMetadataFromDir(segmentDir0
        .listFiles()[0].getAbsolutePath()));
    instanceDataManager.addSegment(_columnarSegmentMetadataLoader.loadIndexSegmentMetadataFromDir(segmentDir1
        .listFiles()[0].getAbsolutePath()));
    instanceDataManager.addSegment(_columnarSegmentMetadataLoader.loadIndexSegmentMetadataFromDir(segmentDir2
        .listFiles()[0].getAbsolutePath()));

  }

}
