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

package com.linkedin.pinot.server.api.resources;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.integration.InstanceServerStarter;
import com.linkedin.pinot.server.starter.ServerInstance;
import com.linkedin.pinot.server.starter.helix.AdminApiApplication;
import com.linkedin.pinot.util.TestUtils;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResourceTestHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceTestHelper.class);
  public static final String DEFAULT_TABLE_NAME = "testTable";
  public static final String DEFAULT_AVRO_DATA_FILE = "data/test_data-mv.avro";

  ServerInstance serverInstance;
  AdminApiApplication apiApplication;
  File INDEX_DIR;
  IndexSegment indexSegment;

  Client client;
  WebTarget target;

  /**
   * Sets up Pinot server instance, index directory for creation of segments, creates a default segment
   * and starts pinot admin api service
   * This should be called only once in the  @BeforeClass method of a unit test.
   * Caller must ensure teardown() is called when the test completes (in @AfterClass)
   */
  public void setup() throws Exception {
    INDEX_DIR = Files.createTempDirectory(TableSizeResourceTest.class.getName() + "_segmentDir").toFile();
    File confFile = new File(
        TestUtils.getFileFromResourceUrl(InstanceServerStarter.class.getClassLoader().getResource("conf/pinot.properties")));
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setDelimiterParsingDisabled(false);
    config.load(confFile);
    ServerConf serverConf = new ServerConf(config);

    LOGGER.info("Trying to create a new ServerInstance!");
    serverInstance = new ServerInstance();
    LOGGER.info("Trying to initial ServerInstance!");
    serverInstance.init(serverConf, new MetricsRegistry());
    LOGGER.info("Trying to start ServerInstance!");
    serverInstance.start();
    apiApplication = new AdminApiApplication(serverInstance);
    apiApplication.start(Integer.parseInt(CommonConstants.Server.DEFAULT_ADMIN_API_PORT));
    client = ClientBuilder.newClient();
    target = client.target(apiApplication.getBaseUri().toString());
    setupSegment();
  }

  public void tearDown() throws Exception {

    if (apiApplication != null) {
      apiApplication.stop();
    }

    if (serverInstance != null) {
      serverInstance.shutDown();
    }

    if (INDEX_DIR != null) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    if (indexSegment != null) {
      indexSegment.destroy();
    }
  }

  public IndexSegment setupSegment()
      throws Exception {
    return setupSegment(DEFAULT_TABLE_NAME, DEFAULT_AVRO_DATA_FILE);
  }

  public IndexSegment setupSegment(String tableName, String avroDataFilePath)
      throws Exception {
    final String filePath =
        TestUtils
            .getFileFromResourceUrl(SegmentV1V2ToV3FormatConverter.class.getClassLoader().getResource(avroDataFilePath));

    // intentionally changed this to TimeUnit.Hours to make it non-default for testing
    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            TimeUnit.HOURS, tableName);
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    File segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
    indexSegment = ColumnarSegmentLoader.load(segmentDirectory, ReadMode.mmap);
    serverInstance.getInstanceDataManager().addSegment(indexSegment.getSegmentMetadata(), null, null);
    return indexSegment;
  }

}
