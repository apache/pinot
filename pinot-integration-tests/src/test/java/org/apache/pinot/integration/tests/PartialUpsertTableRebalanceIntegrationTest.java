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

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.IdealState;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.api.resources.PauseStatus;
import org.apache.pinot.controller.api.resources.ServerRebalanceJobStatusResponse;
import org.apache.pinot.controller.api.resources.ServerReloadControllerJobStatusResponse;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class PartialUpsertTableRebalanceIntegrationTest extends BaseClusterIntegrationTest {
  private static final int NUM_SERVERS = 1;
  private static final String PRIMARY_KEY_COL = "clientId";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(DEFAULT_TABLE_NAME);

  // Segment 1 contains records of pk value 100000 (partition 0)
  private static final String UPLOADED_SEGMENT_1 = "mytable_10027_19736_0 %";
  // Segment 2 contains records of pk value 100001 (partition 1)
  private static final String UPLOADED_SEGMENT_2 = "mytable_10072_19919_1 %";
  // Segment 3 contains records of pk value 100002 (partition 1)
  private static final String UPLOADED_SEGMENT_3 = "mytable_10158_19938_2 %";

  private PinotHelixResourceManager _resourceManager;
  private TableRebalancer _tableRebalancer;
  private static List<File> _avroFiles;
  private TableConfig _tableConfig;
  private Schema _schema;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServers(NUM_SERVERS);

    // Start Kafka and push data into Kafka
    startKafka();

    _resourceManager = getControllerStarter().getHelixResourceManager();
    _tableRebalancer = new TableRebalancer(_resourceManager.getHelixZkManager());

    createSchemaAndTable();
  }

  @Test
  public void testRebalance()
      throws Exception {
    populateTables();

    verifyIdealState(5, NUM_SERVERS);

    // setup the rebalance config
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(false);
    rebalanceConfig.setMinAvailableReplicas(0);
    rebalanceConfig.setIncludeConsuming(true);

    // Add a new server
    BaseServerStarter serverStarter1 = startOneServer(1234);

    // Now we trigger a rebalance operation
    TableConfig tableConfig = _resourceManager.getTableConfig(REALTIME_TABLE_NAME);
    RebalanceResult rebalanceResult = _tableRebalancer.rebalance(tableConfig, rebalanceConfig);

    // Check the number of replicas after rebalancing
    int finalReplicas = _resourceManager.getServerInstancesForTable(getTableName(), TableType.REALTIME).size();

    // Check that a replica has been added
    assertEquals(finalReplicas, NUM_SERVERS + 1, "Rebalancing didn't correctly add the new server");

    waitForRebalanceToComplete(rebalanceResult, 600_000L);
    waitForAllDocsLoaded(600_000L);

    verifySegmentAssignment(rebalanceResult.getSegmentAssignment(), 5, finalReplicas);

    // Add a new server
    BaseServerStarter serverStarter2 = startOneServer(4567);
    rebalanceResult = _tableRebalancer.rebalance(tableConfig, rebalanceConfig);

    // Check the number of replicas after rebalancing
    finalReplicas = _resourceManager.getServerInstancesForTable(getTableName(), TableType.REALTIME).size();

    // Check that a replica has been added
    assertEquals(finalReplicas, NUM_SERVERS + 2, "Rebalancing didn't correctly add the new server");

    waitForRebalanceToComplete(rebalanceResult, 600_000L);
    waitForAllDocsLoaded(600_000L);

    // number of instances assigned can't be more than number of partitions for rf = 1
    verifySegmentAssignment(rebalanceResult.getSegmentAssignment(), 5, getNumKafkaPartitions());

    _resourceManager.updateInstanceTags(serverStarter1.getInstanceId(), "", false);
    _resourceManager.updateInstanceTags(serverStarter2.getInstanceId(), "", false);

    rebalanceConfig.setReassignInstances(true);
    rebalanceConfig.setDowntime(true);


    rebalanceResult = _tableRebalancer.rebalance(tableConfig, rebalanceConfig);

    verifySegmentAssignment(rebalanceResult.getSegmentAssignment(), 5, NUM_SERVERS);

    waitForRebalanceToComplete(rebalanceResult, 600_000L);
    waitForAllDocsLoaded(600_000L);

    _resourceManager.disableInstance(serverStarter1.getInstanceId());
    _resourceManager.disableInstance(serverStarter2.getInstanceId());

    _resourceManager.dropInstance(serverStarter1.getInstanceId());
    _resourceManager.dropInstance(serverStarter2.getInstanceId());

    serverStarter1.stop();
    serverStarter2.stop();
  }

  @Test
  public void testReload()
      throws Exception {
    pushAvroIntoKafka(_avroFiles);
    waitForAllDocsLoaded(600_000L, 300);

    String statusResponse = reloadRealtimeTable(getTableName());
    Map<String, String> statusResponseJson =
        JsonUtils.stringToObject(statusResponse, new TypeReference<Map<String, String>>() {
        });
    String reloadResponse = statusResponseJson.get("status");
    int jsonStartIndex = reloadResponse.indexOf("{");
    String trimmedResponse = reloadResponse.substring(jsonStartIndex);
    Map<String, Map<String, String>> reloadStatus =
        JsonUtils.stringToObject(trimmedResponse, new TypeReference<Map<String, Map<String, String>>>() {
        });
    String reloadJobId = reloadStatus.get(REALTIME_TABLE_NAME).get("reloadJobId");
    waitForReloadToComplete(reloadJobId, 600_000L);
    waitForAllDocsLoaded(600_000L, 300);
    verifyIdealState(4, NUM_SERVERS); // 4 because reload triggers commit of consuming segments
  }

  @AfterMethod
  public void afterMethod()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    getControllerRequestClient().pauseConsumption(realtimeTableName);
    TestUtils.waitForCondition((aVoid) -> {
      try {
        PauseStatus pauseStatus = getControllerRequestClient().getPauseStatus(realtimeTableName);
        return pauseStatus.getConsumingSegments().isEmpty();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to drop the segments");

    // Test dropping all segments one by one
    List<String> segments = listSegments(realtimeTableName);
    for (String segment : segments) {
      dropSegment(realtimeTableName, segment);
    }

    // NOTE: There is a delay to remove the segment from property store
    TestUtils.waitForCondition((aVoid) -> {
      try {
        return listSegments(realtimeTableName).isEmpty();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to drop the segments");

    stopServer();
    stopKafka(); // to clean up the topic
    startServers(NUM_SERVERS);
    startKafka();
    getControllerRequestClient().resumeConsumption(realtimeTableName);
  }

  protected void verifySegmentAssignment(Map<String, Map<String, String>> segmentAssignment, int numSegmentsExpected,
      int numInstancesExpected) {
    assertEquals(segmentAssignment.size(), numSegmentsExpected);

    int maxSequenceNumber = 0;
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        maxSequenceNumber = Math.max(maxSequenceNumber, llcSegmentName.getSequenceNumber());
      }
    }

    Map<Integer, String> serverForPartition = new HashMap<>();
    Set<String> uniqueServers = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();

      // Verify that all segments have the correct state
      assertEquals(instanceStateMap.size(), 1);
      Map.Entry<String, String> instanceIdAndState = instanceStateMap.entrySet().iterator().next();
      String state = instanceIdAndState.getValue();
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        if (llcSegmentName.getSequenceNumber() < maxSequenceNumber) {
          assertEquals(state, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE);
        } else {
          assertEquals(state, CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING);
        }
      } else {
        assertEquals(state, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE);
      }

      // Verify that all segments of the same partition are mapped to the same server
      String instanceId = instanceIdAndState.getKey();
      int partitionId = getSegmentPartitionId(segmentName);
      uniqueServers.add(instanceId);
      serverForPartition.compute(partitionId, (key, value) -> {
        if (value == null) {
          return instanceId;
        } else {
          assertEquals(instanceId, value);
          return value;
        }
      });
    }

    assertEquals(uniqueServers.size(), numInstancesExpected);
  }

  protected void verifyIdealState(int numSegmentsExpected, int numInstancesExpected) {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, REALTIME_TABLE_NAME);
    verifySegmentAssignment(idealState.getRecord().getMapFields(), numSegmentsExpected, numInstancesExpected);
  }

  protected void populateTables()
      throws Exception {
    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(_avroFiles, _tableConfig, _schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), TableType.REALTIME, _tarDir);

    pushAvroIntoKafka(_avroFiles);
    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected void createSchemaAndTable()
      throws Exception {
    // Unpack the Avro files
    _avroFiles = unpackAvroData(_tempDir);

    // Create and upload schema and table config
    _schema = createSchema();
    addSchema(_schema);
    _tableConfig = createUpsertTableConfig(_avroFiles.get(0), PRIMARY_KEY_COL, null, getNumKafkaPartitions());
    _tableConfig.getValidationConfig().setDeletedSegmentsRetentionPeriod(null);

    addTableConfig(_tableConfig);
  }

  @AfterClass
  public void tearDown() {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }

  @Override
  protected String getSchemaFileName() {
    return "upsert_upload_segment_test.schema";
  }

  @Override
  protected String getAvroTarFileName() {
    return "upsert_upload_segment_test.tar.gz";
  }

  @Override
  protected String getPartitionColumn() {
    return PRIMARY_KEY_COL;
  }

  @Override
  protected long getCountStarResult() {
    // Three distinct records are expected with pk values of 100000, 100001, 100002
    return 3;
  }

  protected void waitForAllDocsLoaded(long timeoutMs, long expectedCount)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getCurrentCountStarResultWithoutUpsert() == expectedCount;
      } catch (Exception e) {
        return null;
      }
    }, 1000L, timeoutMs, "Failed to load all documents");
  }

  private static int getSegmentPartitionId(String segmentName) {
    switch (segmentName) {
      case UPLOADED_SEGMENT_1:
        return 0;
      case UPLOADED_SEGMENT_2:
      case UPLOADED_SEGMENT_3:
        return 1;
      default:
        return new LLCSegmentName(segmentName).getPartitionGroupId();
    }
  }

  protected void waitForRebalanceToComplete(RebalanceResult rebalanceResult, long timeoutMs)
      throws Exception {
    String jobId = rebalanceResult.getJobId();
    if (rebalanceResult.getStatus() != RebalanceResult.Status.IN_PROGRESS) {
      return;
    }

    TestUtils.waitForCondition(aVoid -> {
      try {
        String requestUrl = getControllerRequestURLBuilder().forTableRebalanceStatus(jobId);
        try {
          SimpleHttpResponse httpResponse =
              HttpClient.wrapAndThrowHttpException(getHttpClient().sendGetRequest(new URL(requestUrl).toURI(), null));

          ServerRebalanceJobStatusResponse serverRebalanceJobStatusResponse =
              JsonUtils.stringToObject(httpResponse.getResponse(), ServerRebalanceJobStatusResponse.class);
          String status = serverRebalanceJobStatusResponse.getTableRebalanceProgressStats().getStatus();
          return status.equals(RebalanceResult.Status.DONE.toString()) || status.equals(
              RebalanceResult.Status.FAILED.toString());
        } catch (HttpErrorStatusException | URISyntaxException e) {
          throw new IOException(e);
        }
      } catch (Exception e) {
        return null;
      }
    }, 1000L, timeoutMs, "Failed to load all segments after rebalance");
  }

  protected void waitForReloadToComplete(String reloadJobId, long timeoutMs)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        String requestUrl = getControllerRequestURLBuilder().forControllerJobStatus(reloadJobId);
        try {
          SimpleHttpResponse httpResponse =
              HttpClient.wrapAndThrowHttpException(_httpClient.sendGetRequest(new URL(requestUrl).toURI(), null));
          ServerReloadControllerJobStatusResponse segmentReloadStatusValue =
              JsonUtils.stringToObject(httpResponse.getResponse(), ServerReloadControllerJobStatusResponse.class);
          return segmentReloadStatusValue.getSuccessCount() == segmentReloadStatusValue.getTotalSegmentCount();
        } catch (HttpErrorStatusException | URISyntaxException e) {
          throw new IOException(e);
        }
      } catch (Exception e) {
        return null;
      }
    }, 1000L, timeoutMs, "Failed to load all segments after reload");
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        boolean c1 = getCurrentCountStarResultWithoutUpsert() == getCountStarResultWithoutUpsert();
        boolean c2 = getCurrentCountStarResult() == getCountStarResult();
        // verify there are no null rows
        boolean c3 =
            getCurrentCountStarResultWithoutNulls(getTableName(), _schema) == getCountStarResultWithoutUpsert();
        return c1 && c2 && c3;
      } catch (Exception e) {
        return null;
      }
    }, 100L, timeoutMs, "Failed to load all documents");
  }

  private long getCurrentCountStarResultWithoutUpsert() {
    return getPinotConnection().execute("SELECT COUNT(*) FROM " + getTableName() + " OPTION(skipUpsert=true)")
        .getResultSet(0).getLong(0);
  }

  private long getCountStarResultWithoutUpsert() {
    // 3 Avro files, each with 100 documents, one copy from streaming source, one copy from batch source
    return 600;
  }

  protected long getCurrentCountStarResultWithoutNulls(String tableName, Schema schema) {
    StringBuilder queryFilter = new StringBuilder(" WHERE ");
    for (String column : schema.getColumnNames()) {
      if (schema.getFieldSpecFor(column).isSingleValueField()) {
        queryFilter.append(column).append(" IS NOT NULL AND ");
      }
    }

    // remove last AND
    queryFilter = new StringBuilder(queryFilter.substring(0, queryFilter.length() - 5));

    ResultSetGroup resultSetGroup =
        getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName + queryFilter + " OPTION(skipUpsert=true)");
    if (resultSetGroup.getResultSetCount() > 0) {
      return resultSetGroup.getResultSet(0).getLong(0);
    }
    return 0;
  }
}
