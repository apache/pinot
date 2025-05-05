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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.KEY_OF_ENABLE_DATA_BLOCK_PAYLOAD_SPLIT;
import static org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class MultiStageEngineSmallBufferTest extends BaseClusterIntegrationTestSet {

  private static final String SCHEMA_FILE_NAME = "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";
  private static final int NUM_SERVERS = 4;
  private static final int INBOUND_BLOCK_SIZE = 256;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();

    // Set the multi-stage max server query threads for the cluster, so that we can test the query queueing logic
    // in the MultiStageBrokerRequestHandler
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS, "30");

    startBroker();
    startServer();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Override
  protected void startServer()
      throws Exception {
    startServers(NUM_SERVERS);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }

  @Override
  protected String getSchemaFileName() {
    return SCHEMA_FILE_NAME;
  }

  @BeforeMethod
  @Override
  public void resetMultiStage() {
    setUseMultiStageQueryEngine(true);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES, INBOUND_BLOCK_SIZE);
    brokerConf.setProperty(KEY_OF_ENABLE_DATA_BLOCK_PAYLOAD_SPLIT, true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES, INBOUND_BLOCK_SIZE);
    serverConf.setProperty(KEY_OF_ENABLE_DATA_BLOCK_PAYLOAD_SPLIT, true);
  }

  @Test
  public void testConcurrentSplittedMailboxes()
      throws Exception {
    int numClients = 32;
    int numIterations = 50;

    String query =
        "SELECT ActualElapsedTime FROM mytable ORDER BY ActualElapsedTime DESC LIMIT 100";
    String expected = "[[678],[668],[662],[658],[651],[650],[647],[629],[625],[625],[621],[617],[610],[610],[607],[605]"
        + ",[603],[582],[578],[576],[574],[572],[572],[566],[565],[564],[558],[555],[555],[554],[554],[553],[552],[550]"
        + ",[549],[544],[543],[541],[540],[538],[537],[535],[533],[532],[526],[521],[520],[519],[518],[516],[515],[514]"
        + ",[508],[508],[507],[505],[505],[502],[502],[499],[499],[498],[495],[494],[494],[493],[490],[487],[487],[484]"
        + ",[484],[481],[481],[480],[479],[478],[478],[477],[473],[472],[471],[468],[465],[464],[464],[463],[461],[461]"
        + ",[460],[459],[459],[458],[457],[456],[453],[453],[451],[451],[450],[450]]";

    List<String> results = Collections.synchronizedList(new ArrayList<>());
    ExecutorService executorService = null;

    for (int itest = 0; itest < numIterations; itest++) {
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(numClients);
      try {
        executorService = Executors.newFixedThreadPool(numClients);
        for (int i = 0; i < numClients; i++) {
          executorService.submit(() -> {
            try {
              startLatch.await();
              JsonNode jsonNode = postQuery(query);
              assertNotNull(jsonNode);
              String actual = jsonNode.get("resultTable").get("rows").toString();
              results.add(actual);
            } catch (Exception e) {
              results.add("Error: " + e.getMessage());
              e.printStackTrace();
            } finally {
              doneLatch.countDown();
            }
          });
        }

        startLatch.countDown();
        doneLatch.await();

        assertEquals(numClients, results.size());
        for (String result : results) {
          assertEquals(expected, result);
        }
      } finally {
        if (executorService != null) {
          executorService.shutdown();
          executorService = null;
        }
        results.clear();
      }
    }
  }
}
