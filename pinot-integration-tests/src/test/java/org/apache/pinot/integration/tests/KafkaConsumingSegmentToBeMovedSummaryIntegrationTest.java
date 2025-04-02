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
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSummaryResult;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class KafkaConsumingSegmentToBeMovedSummaryIntegrationTest extends BaseRealtimeClusterIntegrationTest {

  @Override
  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();

    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    // Set max segment preprocess parallelism to 8
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, Integer.toString(8));
    // Set max segment startree preprocess parallelism to 6
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM, Integer.toString(6));

    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    Map<String, String> streamConfig = getStreamConfigs();
    streamConfig.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE, "1000000");
    streamConfig.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
    indexingConfig.setStreamConfigs(streamConfig);
    tableConfig.setIndexingConfig(indexingConfig);
    addTableConfig(tableConfig);

    // Push data into Kafka
    pushAvroIntoKafka(avroFiles);

    // create segments and upload them to controller
    createSegmentsAndUpload(avroFiles, schema, tableConfig);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    runValidationJob(600_000);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Test
  public void testConsumingSegmentSummary()
      throws Exception {
    String response = sendPostRequest(
        getControllerRequestURLBuilder().forTableRebalance(getTableName(), "REALTIME", true, false, true, false, -1));
    RebalanceResult result = JsonUtils.stringToObject(response, RebalanceResult.class);
    Assert.assertNotNull(result);
    Assert.assertNotNull(result.getRebalanceSummaryResult());
    Assert.assertNotNull(result.getRebalanceSummaryResult().getSegmentInfo());
    RebalanceSummaryResult.SegmentInfo segmentInfo = result.getRebalanceSummaryResult().getSegmentInfo();
    RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary consumingSegmentToBeMovedSummary =
        segmentInfo.getConsumingSegmentToBeMovedSummary();
    Assert.assertNotNull(consumingSegmentToBeMovedSummary);
    Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumConsumingSegmentsToBeMoved(), 0);
    Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumServersGettingConsumingSegmentsAdded(), 0);
    Assert.assertEquals(consumingSegmentToBeMovedSummary.getServerConsumingSegmentSummary().size(),
        0);

    startServer();
    response = sendPostRequest(
        getControllerRequestURLBuilder().forTableRebalance(getTableName(), "REALTIME", true, false, true, false, -1));
    result = JsonUtils.stringToObject(response, RebalanceResult.class);
    Assert.assertNotNull(result);
    Assert.assertNotNull(result.getRebalanceSummaryResult());
    Assert.assertNotNull(result.getRebalanceSummaryResult().getSegmentInfo());
    segmentInfo = result.getRebalanceSummaryResult().getSegmentInfo();
    consumingSegmentToBeMovedSummary = segmentInfo.getConsumingSegmentToBeMovedSummary();
    Assert.assertNotNull(consumingSegmentToBeMovedSummary);
    Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumConsumingSegmentsToBeMoved(), 1);
    Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumServersGettingConsumingSegmentsAdded(), 1);
    Assert.assertEquals(consumingSegmentToBeMovedSummary.getServerConsumingSegmentSummary().size(),
        1);
    Assert.assertTrue(consumingSegmentToBeMovedSummary
        .getServerConsumingSegmentSummary()
        .values()
        .stream()
        .allMatch(x -> x.getTotalOffsetsToCatchUpAcrossAllConsumingSegments() == 57801
            || x.getTotalOffsetsToCatchUpAcrossAllConsumingSegments() == 0));
    Assert.assertEquals(consumingSegmentToBeMovedSummary
        .getServerConsumingSegmentSummary()
        .values()
        .stream()
        .reduce(0, (a, b) -> a + b.getTotalOffsetsToCatchUpAcrossAllConsumingSegments(), Integer::sum), 57801);

    // set includeConsuming to false
    response = sendPostRequest(
        getControllerRequestURLBuilder().forTableRebalance(getTableName(), "REALTIME", true, false, false, false, -1));
    result = JsonUtils.stringToObject(response, RebalanceResult.class);
    Assert.assertNotNull(result);
    Assert.assertNotNull(result.getRebalanceSummaryResult());
    Assert.assertNotNull(result.getRebalanceSummaryResult().getSegmentInfo());
    segmentInfo = result.getRebalanceSummaryResult().getSegmentInfo();
    consumingSegmentToBeMovedSummary = segmentInfo.getConsumingSegmentToBeMovedSummary();
    Assert.assertNotNull(consumingSegmentToBeMovedSummary);
    Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumConsumingSegmentsToBeMoved(), 0);
    Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumServersGettingConsumingSegmentsAdded(), 0);
    Assert.assertEquals(consumingSegmentToBeMovedSummary.getServerConsumingSegmentSummary().size(),
        0);

    stopServer();
    response = sendPostRequest(
        getControllerRequestURLBuilder().forTableRebalance(getTableName(), "REALTIME", true, false, true, false, -1));
    RebalanceResult resultNoInfo = JsonUtils.stringToObject(response, RebalanceResult.class);
    Assert.assertNotNull(resultNoInfo);
    Assert.assertNotNull(resultNoInfo.getRebalanceSummaryResult());
    Assert.assertNotNull(resultNoInfo.getRebalanceSummaryResult().getSegmentInfo());
    segmentInfo = resultNoInfo.getRebalanceSummaryResult().getSegmentInfo();
    consumingSegmentToBeMovedSummary = segmentInfo.getConsumingSegmentToBeMovedSummary();
    Assert.assertNotNull(consumingSegmentToBeMovedSummary);
    Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumConsumingSegmentsToBeMoved(), 1);
    Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumServersGettingConsumingSegmentsAdded(), 1);
    Assert.assertNotNull(consumingSegmentToBeMovedSummary.getServerConsumingSegmentSummary());
    Assert.assertNull(consumingSegmentToBeMovedSummary.getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp());
  }
}
