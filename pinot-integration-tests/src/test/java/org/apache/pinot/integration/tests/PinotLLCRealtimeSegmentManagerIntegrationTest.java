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
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Integration tests for {@link PinotLLCRealtimeSegmentManager}.
 */
public class PinotLLCRealtimeSegmentManagerIntegrationTest extends BaseClusterIntegrationTest {

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startKafka();
    startController();
    startBroker();
    startServer();

    List<File> avroFiles = unpackAvroData(_tempDir);
    pushAvroIntoKafka(avroFiles);

    Schema schema = createSchema();
    addSchema(schema);

    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    addTableConfig(tableConfig);

    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    try {
      dropRealtimeTable(getTableName());
      stopServer();
      stopBroker();
      stopController();
      stopKafka();
      stopZk();
    } finally {
      FileUtils.deleteQuietly(_tempDir);
    }
  }

  @Test
  public void testMaxSegmentCompletionTimeClusterConfigChange()
      throws Exception {
    PinotLLCRealtimeSegmentManager segmentManager = _helixResourceManager.getRealtimeSegmentManager();

    // Verify default value
    assertEquals(segmentManager.getMaxSegmentCompletionTimeMillis(),
        PinotLLCRealtimeSegmentManager.DEFAULT_MAX_SEGMENT_COMPLETION_TIME_MILLIS);

    // Update via cluster config and verify propagation through ZK change listener
    String configKey = PinotLLCRealtimeSegmentManager.MAX_SEGMENT_COMPLETION_TIME_MILLIS_KEY;
    updateClusterConfig(Map.of(configKey, "600000"));
    TestUtils.waitForCondition(
        aVoid -> segmentManager.getMaxSegmentCompletionTimeMillis() == 600_000L,
        1000, 10000,
        "Max segment completion time was not updated to 600000ms");

    // Update to another value
    updateClusterConfig(Map.of(configKey, "900000"));
    TestUtils.waitForCondition(
        aVoid -> segmentManager.getMaxSegmentCompletionTimeMillis() == 900_000L,
        1000, 10000,
        "Max segment completion time was not updated to 900000ms");

    // Delete the config and verify revert to default
    deleteClusterConfig(configKey);
    TestUtils.waitForCondition(
        aVoid -> segmentManager.getMaxSegmentCompletionTimeMillis()
            == PinotLLCRealtimeSegmentManager.DEFAULT_MAX_SEGMENT_COMPLETION_TIME_MILLIS,
        1000, 10000,
        "Max segment completion time was not reverted to default after config deletion");
  }
}
