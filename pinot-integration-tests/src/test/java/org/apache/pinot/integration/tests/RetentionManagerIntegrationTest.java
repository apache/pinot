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
package org.apache.pinot.integration.tests;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.util.TestUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RetentionManagerIntegrationTest extends BaseClusterIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetentionManagerIntegrationTest.class);

  protected List<File> _avroFiles;

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.RETENTION_MANAGER_INITIAL_DELAY_IN_SECONDS, 5000);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    try {
      LOGGER.info("Set segment.store.uri: {} for server with scheme: {}", _controllerConfig.getDataDir(),
          new URI(_controllerConfig.getDataDir()).getScheme());
      serverConf.setProperty("pinot.server.instance.segment.store.uri", "file:" + _controllerConfig.getDataDir());
      serverConf.setProperty("pinot.server.instance." + HelixInstanceDataManagerConfig.UPLOAD_SEGMENT_TO_DEEP_STORE,
          "true");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startController();
    startBroker();
    startServer();
    setupTable();
    waitForAllDocsLoaded(600_000L);
  }

  protected void setupTable()
      throws Exception {
    _avroFiles = unpackAvroData(_tempDir);
    startKafka();
    pushAvroIntoKafka(_avroFiles);

    Schema schema = createSchema();
    addSchema(schema);

    TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("2");
    addTableConfig(tableConfig);

    waitForDocsLoaded(600_000L, true, tableConfig.getTableName());
  }

  @Test
  public void testClusterConfigChangeListener()
      throws IOException, URISyntaxException {
    // Disable orphan segment deletion to ensure orphan segments are not cleaned up
    updateClusterConfig(Map.of(ControllerConf.ControllerPeriodicTasksConf.ENABLE_UNTRACKED_SEGMENT_DELETION, "false"));

    createFakeSegments(_controllerConfig.getDataDir(), getTableName(), "orphan_segment", 4);

    _controllerStarter.getRetentionManager().run();

    PinotFS pinotFS = PinotFSFactory.create(URIUtils.getUri(_controllerConfig.getDataDir()).getScheme());

    // Verify that 6 segments remain: 2 CONSUMING segments + 4 orphan segments
    TestUtils.waitForCondition((aVoid) -> {
      try {
        String[] fileList = pinotFS.listFiles(new URI(_controllerConfig.getDataDir() + "/" + getTableName()), false);
        return fileList.length == 6;
      } catch (IOException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }, 5000, 10000, "Expected 6 segments (2 CONSUMING + 4 orphan) but found different count");

    // Enable orphan segment deletion
    updateClusterConfig(Map.of(ControllerConf.ControllerPeriodicTasksConf.ENABLE_UNTRACKED_SEGMENT_DELETION, "true"));

    // Wait for the config change to take effect
    TestUtils.waitForCondition((aVoid) -> _controllerStarter.getRetentionManager().isUntrackedSegmentDeletionEnabled(),
        1000, 10000, "UntrackedSegmentDeletionEnabled is still false. Should have been set to true");

    _controllerStarter.getRetentionManager().run();

    // Verify that only 2 CONSUMING segments remain after orphan segment cleanup
    TestUtils.waitForCondition((aVoid) -> {
      try {
        String[] fileList = pinotFS.listFiles(new URI(_controllerConfig.getDataDir() + "/" + getTableName()), false);
        return fileList.length == 2;
      } catch (IOException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }, 5000, 100000, "Expected 2 CONSUMING segments after orphan cleanup but found different count");
  }

  private void createFakeSegments(String dataDir, String tableName, String orphanSegmentPrefix,
      int numberOfOrphanSegment)
      throws URISyntaxException, IOException {
    PinotFS pinotFS = PinotFSFactory.create(URIUtils.getUri(dataDir).getScheme());
    for (int i = 0; i < numberOfOrphanSegment; i++) {
      String segmentPath = dataDir + "/" + tableName + "/" + orphanSegmentPrefix + "_" + i;
      pinotFS.touch(new URI(segmentPath));
      File file = new File(segmentPath);
      if (!file.setLastModified(DateTime.now().minusDays(100).getMillis())) {
        LOGGER.warn("Failed to set last modified time for file: {}", segmentPath);
      }
    }
  }
}
