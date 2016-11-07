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

import com.linkedin.pinot.common.restlet.resources.TableSizeInfo;
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
import javax.ws.rs.core.Response;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TableSizeResourceTest {
  public static final Logger LOGGER = LoggerFactory.getLogger(TableSizeResourceTest.class);
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final String TABLE_NAME = "testTable";
  private static final String TABLE_SIZE_PATH = "/tables/" + TABLE_NAME + "/size";

  ServerInstance serverInstance;
  AdminApiApplication apiService;
  private File INDEX_DIR;
  private IndexSegment indexSegment;
  Client client;
  WebTarget target;

  @BeforeClass
  public void setupTest()
      throws Exception {

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
    apiService = new AdminApiApplication(serverInstance);
    apiService.start(Integer.parseInt(CommonConstants.Server.DEFAULT_ADMIN_API_PORT));
    client = ClientBuilder.newClient();
    target = client.target(apiService.getBaseUri().toString());
    setupSegment();
  }

  @AfterTest
  public void tearDownTest() {
    apiService.stop();
    serverInstance.shutDown();
    if (INDEX_DIR != null) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    if (indexSegment != null) {
      indexSegment.destroy();
    }
  }

  @Test
  public void testTableSizeNotFound() {
    Response response = target.path("table/unknownTable/size").request().get(Response.class);
    Assert.assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
  }

  public void setupSegment()
      throws Exception {
    final String filePath =
        TestUtils
            .getFileFromResourceUrl(SegmentV1V2ToV3FormatConverter.class.getClassLoader().getResource(AVRO_DATA));

    // intentionally changed this to TimeUnit.Hours to make it non-default for testing
    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            TimeUnit.HOURS, TABLE_NAME);
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    File segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
    indexSegment = ColumnarSegmentLoader.load(segmentDirectory, ReadMode.mmap);
    serverInstance.getInstanceDataManager().addSegment(indexSegment.getSegmentMetadata(), null, null);
  }

  @Test
  public void testTableSizeDetailed() {
    TableSizeInfo tableSizeInfo = target.path(TABLE_SIZE_PATH).request().get(TableSizeInfo.class);

    Assert.assertEquals(tableSizeInfo.tableName, TABLE_NAME);
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, indexSegment.getDiskSizeBytes());
    Assert.assertEquals(tableSizeInfo.segments.size(), 1);
    Assert.assertEquals(tableSizeInfo.segments.get(0).segmentName, indexSegment.getSegmentName());
    Assert.assertEquals(tableSizeInfo.segments.get(0).diskSizeInBytes,
        indexSegment.getDiskSizeBytes());
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, indexSegment.getDiskSizeBytes());
  }

  @Test
  public void testTableSizeNoDetails() {
    TableSizeInfo tableSizeInfo = target.path(TABLE_SIZE_PATH).queryParam("detailed", "false")
        .request().get(TableSizeInfo.class);
    Assert.assertEquals(tableSizeInfo.tableName, TABLE_NAME);
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, indexSegment.getDiskSizeBytes());
    Assert.assertEquals(tableSizeInfo.segments.size(), 0);
  }

  @Test
  public void testTableSizeOld() {
    TableSizeInfo tableSizeInfo = target.path("/table/" + TABLE_NAME + "/size").request().get(TableSizeInfo.class);

    Assert.assertEquals(tableSizeInfo.tableName, TABLE_NAME);
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, indexSegment.getDiskSizeBytes());
    Assert.assertEquals(tableSizeInfo.segments.size(), 1);
    Assert.assertEquals(tableSizeInfo.segments.get(0).segmentName, indexSegment.getSegmentName());
    Assert.assertEquals(tableSizeInfo.segments.get(0).diskSizeInBytes,
        indexSegment.getDiskSizeBytes());
    Assert.assertEquals(tableSizeInfo.diskSizeInBytes, indexSegment.getDiskSizeBytes());
  }
}
