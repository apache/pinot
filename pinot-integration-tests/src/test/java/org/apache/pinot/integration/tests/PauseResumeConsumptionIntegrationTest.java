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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.PauseStatus;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PauseResumeConsumptionIntegrationTest extends BaseClusterIntegrationTestSet {

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);
    File firstAvroFile = avroFiles.get(0);

    // Create and upload schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(firstAvroFile);
    addTableConfig(tableConfig);

    // push data to kafka; pause; verify...
    pushAvroIntoKafka(Collections.singletonList(firstAvroFile)); // 9,292 records are now available in the topic
    pause();
    verifyPause(9_292);
    pushAvroIntoKafka(avroFiles.subList(1, 2)); // 18,028 records are now available in the topic
    verifyPause(9_292);
    resume();
    verifyResume(18_028);
    pushAvroIntoKafka(avroFiles.subList(2, 3)); // 27,406 records are now available in the topic
    verifyResume(27_406);
    pause();
    verifyPause(27_406);
    pushAvroIntoKafka(avroFiles.subList(3, 4)); // 37,152 records are now available in the topic
    verifyPause(27_406);
    resume();
    verifyResume(37_152);
    pushAvroIntoKafka(avroFiles.subList(4, avroFiles.size())); // 115,545 records are now available in the topic
    verifyResume(115_545);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  private void pause() throws Exception {
    getControllerRequestClient().pauseConsumption(getTableName());
  }

  private void resume() throws Exception {
    getControllerRequestClient().resumeConsumption(getTableName());
  }

  private void verifyPause(int numRecordsInTable) throws Exception {
    verify(true, numRecordsInTable);
  }

  private void verifyResume(int numRecordsInTable) throws Exception {
    verify(false, numRecordsInTable);
  }

  private void verify(boolean pause, int expectedNumRecordsInTable) throws Exception {
    for (int i = 0; i < 60; i++) {
      PauseStatus pauseStatus = getControllerRequestClient().getPauseStatus(getTableName());
      if (pause && pauseStatus.getPauseFlag() && pauseStatus.getConsumingSegments().isEmpty()) {
        // pause completed
        assertCountStar(expectedNumRecordsInTable);
        return;
      }
      if (!pause && !pauseStatus.getPauseFlag() && pauseStatus.getConsumingSegments().size() == 2) {
        // resume completed
        assertCountStar(expectedNumRecordsInTable);
        return;
      }
      Thread.sleep(1000L);
    }
    Assert.fail("Table was not " + (pause ? "paused" : "resumed") + " in 60 seconds!");
  }

  private void assertCountStar(int expectedNumRecordsInTable) throws Exception {
    long actualNumRecordsInTable = 0;
    for (int i = 0; i < 60; i++) {
      actualNumRecordsInTable = getCurrentCountStarResult();
      if (actualNumRecordsInTable == expectedNumRecordsInTable) {
        return;
      }
      Thread.sleep(1000L);
    }
    Assert.assertEquals(actualNumRecordsInTable, expectedNumRecordsInTable);
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Override
  protected String getLoadMode() {
    return ReadMode.mmap.name();
  }

  @Override
  public void startController()
      throws Exception {
    Map<String, Object> properties = getDefaultControllerConfiguration();

    properties.put(ControllerConf.ALLOW_HLC_TABLES, false);
    properties.put(ControllerConf.ENABLE_SPLIT_COMMIT, true);

    startController(properties);
    enableResourceConfigForLeadControllerResource(true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_SPLIT_COMMIT, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_COMMIT_END_WITH_METADATA, true);
  }

  @Test
  public void testHardcodedQueries()
      throws Exception {
    super.testHardcodedQueries();
  }
}
