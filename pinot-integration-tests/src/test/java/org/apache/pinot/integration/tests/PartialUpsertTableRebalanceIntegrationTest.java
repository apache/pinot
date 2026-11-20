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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.partition.function.MurmurPartitionFunction;
import org.apache.pinot.common.restlet.resources.PinotTableReloadStatusResponse;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.restlet.resources.RebalanceResult;
import org.apache.pinot.common.restlet.resources.ServerRebalanceJobStatusResponse;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentBuildTimeLeaseExtender;
import org.apache.pinot.integration.tests.models.DelayedTableUpsertMetadataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ConsumingSegmentConsistencyModeListener;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


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
    // Start Kafka
    startKafka();
    startController();
    startBroker();
    startServers(NUM_SERVERS);

    _resourceManager = _controllerStarter.getHelixResourceManager();
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
    BaseServerStarter serverStarter1 = startOneServer(NUM_SERVERS);

    // Now we trigger a rebalance operation
    TableConfig tableConfig = _resourceManager.getTableConfig(REALTIME_TABLE_NAME);
    RebalanceResult rebalanceResult = _tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);

    // Check the number of servers after rebalancing
    int finalServers = _resourceManager.getServerInstancesForTable(getTableName(), TableType.REALTIME).size();

    // Check that a server has been added
    assertEquals(finalServers, NUM_SERVERS + 1, "Rebalancing didn't correctly add the new server");

    waitForRebalanceToComplete(rebalanceResult, 600_000L);
    waitForAllDocsLoaded(600_000L);

    verifySegmentAssignment(rebalanceResult.getSegmentAssignment(), 5, finalServers);

    // Add a new server
    BaseServerStarter serverStarter2 = startOneServer(NUM_SERVERS + 1);
    rebalanceResult = _tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);

    waitForRebalanceToComplete(rebalanceResult, 600_000L);
    waitForAllDocsLoaded(600_000L);

    // number of instances assigned can't be more than number of partitions for rf = 1
    finalServers = _resourceManager.getServerInstancesForTable(getTableName(), TableType.REALTIME).size();
    assertEquals(finalServers, getNumKafkaPartitions(), "Rebalancing didn't correctly add the new server");
    verifySegmentAssignment(rebalanceResult.getSegmentAssignment(), 5, getNumKafkaPartitions());

    _resourceManager.updateInstanceTags(serverStarter1.getInstanceId(), "", false);
    _resourceManager.updateInstanceTags(serverStarter2.getInstanceId(), "", false);

    rebalanceConfig.setReassignInstances(true);
    rebalanceConfig.setDowntime(true);

    rebalanceResult = _tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);

    verifySegmentAssignment(rebalanceResult.getSegmentAssignment(), 5, NUM_SERVERS);

    waitForRebalanceToComplete(rebalanceResult, 600_000L);
    waitForAllDocsLoaded(600_000L);

    serverStarter1.stop();
    serverStarter2.stop();
    // Re-init the static executor because stopping servers shuts it down; required for subsequent operations.
    SegmentBuildTimeLeaseExtender.initExecutor();
    TestUtils.waitForCondition(aVoid -> _resourceManager.dropInstance(serverStarter1.getInstanceId()).isSuccessful()
            && _resourceManager.dropInstance(serverStarter2.getInstanceId()).isSuccessful(), 60_000L,
        "Failed to drop servers");
  }

  @Test
  public void testRebalanceWithBatching()
      throws Exception {
    populateTables();

    verifyIdealState(5, NUM_SERVERS);

    // setup the rebalance config
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(false);
    rebalanceConfig.setMinAvailableReplicas(0);
    rebalanceConfig.setIncludeConsuming(true);
    rebalanceConfig.setBatchSizePerServer(1);

    // Add a new server
    BaseServerStarter serverStarter1 = startOneServer(NUM_SERVERS);

    // Now we trigger a rebalance operation
    TableConfig tableConfig = _resourceManager.getTableConfig(REALTIME_TABLE_NAME);
    RebalanceResult rebalanceResult = _tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);

    // Check the number of servers after rebalancing
    int finalServers = _resourceManager.getServerInstancesForTable(getTableName(), TableType.REALTIME).size();

    // Check that a server has been added
    assertEquals(finalServers, NUM_SERVERS + 1, "Rebalancing didn't correctly add the new server");

    waitForRebalanceToComplete(rebalanceResult, 600_000L);
    waitForAllDocsLoaded(600_000L);

    verifySegmentAssignment(rebalanceResult.getSegmentAssignment(), 5, finalServers);

    // Add a new server
    BaseServerStarter serverStarter2 = startOneServer(NUM_SERVERS + 1);
    rebalanceResult = _tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);

    waitForRebalanceToComplete(rebalanceResult, 600_000L);
    waitForAllDocsLoaded(600_000L);

    // number of instances assigned can't be more than number of partitions for rf = 1
    finalServers = _resourceManager.getServerInstancesForTable(getTableName(), TableType.REALTIME).size();
    assertEquals(finalServers, getNumKafkaPartitions(), "Rebalancing didn't correctly add the new server");
    verifySegmentAssignment(rebalanceResult.getSegmentAssignment(), 5, getNumKafkaPartitions());

    _resourceManager.updateInstanceTags(serverStarter1.getInstanceId(), "", false);
    _resourceManager.updateInstanceTags(serverStarter2.getInstanceId(), "", false);

    rebalanceConfig.setReassignInstances(true);
    rebalanceConfig.setDowntime(true);

    rebalanceResult = _tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);

    verifySegmentAssignment(rebalanceResult.getSegmentAssignment(), 5, NUM_SERVERS);

    waitForRebalanceToComplete(rebalanceResult, 600_000L);
    waitForAllDocsLoaded(600_000L);

    serverStarter1.stop();
    serverStarter2.stop();
    // Re-init the static executor because stopping servers shuts it down; required for subsequent operations.
    SegmentBuildTimeLeaseExtender.initExecutor();
    TestUtils.waitForCondition(aVoid -> _resourceManager.dropInstance(serverStarter1.getInstanceId()).isSuccessful()
            && _resourceManager.dropInstance(serverStarter2.getInstanceId()).isSuccessful(), 60_000L,
        "Failed to drop servers");
  }

  @Test
  public void testRebalanceWaitsForOlderSegmentBeforeIncrement()
      throws Exception {
    String tableName = "partialUpsertReadiness";
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    String topic = getKafkaTopic() + "-readiness";
    String tenant = "readinessTenant";
    List<BaseServerStarter> servers = new ArrayList<>();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<RebalanceResult> rebalance = null;
    DelayedTableUpsertMetadataManager delayedManager = null;
    boolean tableAdded = false;
    boolean schemaAdded = false;
    boolean topicCreated = false;
    try {
      BaseServerStarter source = startOneServer(NUM_SERVERS);
      servers.add(source);
      _resourceManager.updateInstanceTags(source.getInstanceId(), tenant + "_REALTIME", false);
      PinotConfiguration destinationConf = getServerConf(NUM_SERVERS + 1);
      destinationConf.setProperty(CommonConstants.Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "."
              + CommonConstants.Server.Upsert.CONFIG_PREFIX + "."
              + CommonConstants.Server.Upsert.DEFAULT_METADATA_MANAGER_CLASS,
          DelayedTableUpsertMetadataManager.class.getName());
      BaseServerStarter destination = startOneServer(destinationConf);
      servers.add(destination);
      _resourceManager.updateInstanceTags(destination.getInstanceId(), tenant + "_REALTIME", false);

      createKafkaTopic(topic);
      topicCreated = true;
      Schema schema = new Schema.SchemaBuilder().setSchemaName(tableName)
          .addSingleValueDimension(PRIMARY_KEY_COL, DataType.INT)
          .addMetric("salary", DataType.INT)
          .addDateTime(getTimeColumnName(), DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
          .setPrimaryKeyColumns(List.of(PRIMARY_KEY_COL)).build();
      addSchema(schema);
      schemaAdded = true;
      UpsertConfig upsert = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
      upsert.setPartialUpsertStrategies(Map.of("salary", UpsertConfig.Strategy.INCREMENT));
      TableConfig tableConfig = createCSVUpsertTableConfig(tableName, topic, 2,
          getCSVDecoderProperties(",", PRIMARY_KEY_COL + ",salary," + getTimeColumnName()), upsert, PRIMARY_KEY_COL);
      tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), tenant, null));
      addTableConfig(tableConfig);
      tableAdded = true;
      waitForClusterStateSettled(tableNameWithType, 2, 1, 60_000L);

      Map<String, Map<String, String>> initialAssignment =
          HelixHelper.getTableIdealState(_helixManager, tableNameWithType).getRecord().getMapFields();
      assertEquals(initialAssignment.values().stream()
          .filter(instances -> instances.containsKey(source.getInstanceId())).count(), 1L);
      assertEquals(initialAssignment.values().stream()
          .filter(instances -> instances.containsKey(destination.getInstanceId())).count(), 1L);
      int partition = initialAssignment.entrySet().stream()
          .filter(entry -> entry.getValue().containsKey(source.getInstanceId()))
          .mapToInt(entry -> new LLCSegmentName(entry.getKey()).getPartitionGroupId()).findFirst().orElseThrow();

      // Warm both servers on this same table, so the old table-wide readiness latch has already passed.
      // S1 owns the base keys; S2 contains disjoint keys, leaving the target key's history only in S1.
      MurmurPartitionFunction partitionFunction = new MurmurPartitionFunction(2, null);
      int[][] keys = new int[2][];
      for (int p = 0; p < 2; p++) {
        int partitionId = p;
        keys[p] = IntStream.range(0, 100)
            .filter(key -> partitionFunction.getPartition(Integer.toString(key)) == partitionId).limit(2).toArray();
        assertEquals(keys[p].length, 2);
      }
      long timestamp = System.currentTimeMillis();
      pushCsvIntoKafka(List.of(keys[0][0] + ",100," + timestamp, keys[1][0] + ",100," + timestamp), topic, 0);
      waitForSalarySum(tableName, 200);
      getOrCreateAdminClient().getTableClient().forceCommit(tableName);
      waitForClusterStateSettled(tableNameWithType, 4, 1, 60_000L);
      pushCsvIntoKafka(List.of(keys[0][1] + ",1," + (timestamp + 1), keys[1][1] + ",1," + (timestamp + 1)), topic, 0);
      waitForSalarySum(tableName, 202);
      getOrCreateAdminClient().getTableClient().forceCommit(tableName);
      waitForClusterStateSettled(tableNameWithType, 6, 1, 60_000L);

      Map<String, Map<String, String>> assignment =
          HelixHelper.getTableIdealState(_helixManager, tableNameWithType).getRecord().getMapFields();
      String baseSegment = findReadinessSegment(assignment, partition, 0);
      String predecessor = findReadinessSegment(assignment, partition, 1);
      String consuming = findReadinessSegment(assignment, partition, 2);
      RealtimeTableDataManager destinationTable = (RealtimeTableDataManager) destination.getServerInstance()
          .getInstanceDataManager().getTableDataManager(tableNameWithType);
      delayedManager = (DelayedTableUpsertMetadataManager) destinationTable.getTableUpsertMetadataManager();
      delayedManager.delaySegment(baseSegment);

      // Move the source's partition onto the already-warm destination. Run the synchronous rebalancer separately
      // so this thread can hold S1 until S2 has loaded and S3 has reached its readiness wait.
      _resourceManager.updateInstanceTags(source.getInstanceId(), "", false);
      RebalanceConfig config = new RebalanceConfig();
      config.setDryRun(false);
      config.setReassignInstances(true);
      config.setIncludeConsuming(true);
      config.setForceCommit(false);
      config.setMinAvailableReplicas(0);
      rebalance = executor.submit(() -> _tableRebalancer.rebalance(tableConfig, config, null));
      assertTrue(delayedManager.awaitDelayedLoad(60, TimeUnit.SECONDS), "S1 never reached the destination");
      TestUtils.waitForCondition(aVoid -> {
        SegmentDataManager manager = destinationTable.getSegmentDataManager(predecessor);
        return manager instanceof ImmutableSegmentDataManager
            && ((ImmutableSegmentDataManager) manager).isPartitionMetadataInitialized()
            && destinationTable.getSegmentDataManager(consuming) instanceof RealtimeSegmentDataManager
            && isWaitingForReadiness(tableNameWithType, partition);
      }, 100L, 60_000L, "S3 did not wait for S1 after its immediate predecessor S2 was loaded");
      RealtimeSegmentDataManager consumer =
          (RealtimeSegmentDataManager) destinationTable.getSegmentDataManager(consuming);
      String initialOffset = consumer.getCurrentOffset().toString();
      assertEquals(consumer.getSegment().getNumDocsIndexed(), 0);

      pushCsvIntoKafka(List.of(keys[partition][0] + ",7," + (timestamp + 2),
          keys[partition][0] + ",11," + (timestamp + 3)), topic, 0);
      assertTrue(isWaitingForReadiness(tableNameWithType, partition));
      assertEquals(consumer.getCurrentOffset().toString(), initialOffset);
      assertEquals(consumer.getSegment().getNumDocsIndexed(), 0);
      assertFalse(rebalance.isDone(), "Rebalance completed while S1 metadata was still blocked");

      delayedManager.releaseDelayedLoad();
      assertEquals(rebalance.get(60, TimeUnit.SECONDS).getStatus(), RebalanceResult.Status.DONE);
      waitForClusterStateSettled(tableNameWithType, 6, 1, 60_000L);
      waitForSalarySum(tableName, 220);
      assertEquals(getPinotConnection().execute("SELECT salary FROM " + tableName + " WHERE " + PRIMARY_KEY_COL
          + " = " + keys[partition][0]).getResultSet(0).getInt(0), 118);
    } finally {
      if (delayedManager != null) {
        delayedManager.releaseDelayedLoad();
      }
      if (rebalance != null) {
        rebalance.cancel(true);
      }
      executor.shutdownNow();
      try {
        assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS), "Rebalance executor did not stop");
      } finally {
        try {
          if (tableAdded) {
            dropRealtimeTable(tableName);
            waitForEVToDisappear(tableNameWithType);
          }
        } finally {
          try {
            stopReadinessServers(servers);
          } finally {
            SegmentBuildTimeLeaseExtender.initExecutor();
            try {
              if (schemaAdded) {
                deleteSchema(tableName);
              }
            } finally {
              if (topicCreated) {
                deleteKafkaTopic(topic);
              }
            }
          }
        }
      }
    }
  }

  private void stopReadinessServers(List<BaseServerStarter> servers) {
    AssertionError failure = new AssertionError("Failed to clean up readiness test servers");
    for (BaseServerStarter server : servers) {
      try {
        server.stop();
      } catch (Exception | AssertionError e) {
        failure.addSuppressed(e);
      }
    }
    for (BaseServerStarter server : servers) {
      try {
        TestUtils.waitForCondition(aVoid -> _resourceManager.dropInstance(server.getInstanceId()).isSuccessful(),
            60_000L, "Failed to drop readiness test server");
      } catch (Exception | AssertionError e) {
        failure.addSuppressed(e);
      }
    }
    if (failure.getSuppressed().length > 0) {
      throw failure;
    }
  }

  private static String findReadinessSegment(Map<String, Map<String, String>> assignment, int partition, int sequence) {
    return assignment.keySet().stream().map(LLCSegmentName::new)
        .filter(name -> name.getPartitionGroupId() == partition && name.getSequenceNumber() == sequence)
        .map(LLCSegmentName::getSegmentName).findFirst().orElseThrow();
  }

  private void waitForSalarySum(String tableName, int expected) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getPinotConnection().execute("SELECT SUM(salary) FROM " + tableName)
            .getResultSet(0).getDouble(0) == expected;
      } catch (Exception e) {
        return false;
      }
    }, 100L, 60_000L, "Unexpected salary total for " + tableName + ": expected " + expected);
  }

  private static boolean isWaitingForReadiness(String tableNameWithType, int partition) {
    return MetricValueUtils.getPartitionGaugeValue(ServerMetrics.get(), tableNameWithType, partition,
        ServerGauge.CONSUMERS_WAITING_FOR_READINESS) > 0;
  }

  @Test
  public void testReload()
      throws Exception {
    pushAvroIntoKafka(_avroFiles);
    waitForAllDocsLoaded(600_000L, 300);

    // Partial-upsert tables need PROTECTED consuming segment consistency mode for reload to force-commit
    // consuming segments; RESTRICTED skips force-commit and reload never completes. Set explicitly here
    // rather than relying on the default so this test doesn't depend on it.
    try {
      updateClusterConfig(
          Map.of(CommonConstants.ConfigChangeListenerConstants.CONSUMING_SEGMENT_CONSISTENCY_MODE,
              ConsumingSegmentConsistencyModeListener.Mode.PROTECTED.name()));
      Thread.sleep(5000); // Allow server to pick up cluster config from ZK

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
      // Reload job status only tracks per-segment reload completion, not IdealState settlement.
      // For partial-upsert reload the CONSUMING segment is force-committed and a new CONSUMING
      // segment is created; wait until IdealState settles (expected count, no OFFLINE) and
      // ExternalView catches up, sustained across a few samples.
      waitForClusterStateSettled(REALTIME_TABLE_NAME, 4, 5, 60_000L);
      verifyIdealState(4, NUM_SERVERS); // 4 because reload triggers commit of consuming segments
    } finally {
      deleteClusterConfig(CommonConstants.ConfigChangeListenerConstants.CONSUMING_SEGMENT_CONSISTENCY_MODE);
    }
  }

  @AfterMethod
  public void afterMethod()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());

    // Drop the table entirely to clean up all segments and server-side upsert state.
    // This is more reliable than the pause/drop-segments/restart cycle because it uses
    // the standard table lifecycle and avoids issues with stale controller/server state.
    dropRealtimeTable(getTableName());
    waitForTableDataManagerRemoved(realtimeTableName);
    waitForEVToDisappear(realtimeTableName);

    // Delete and recreate the Kafka topic for a clean stream
    deleteKafkaTopic(getKafkaTopic());
    createKafkaTopic(getKafkaTopic());

    // Recreate the table — this triggers fresh consuming segment creation
    addTableConfig(_tableConfig);
  }

  protected void verifySegmentAssignment(Map<String, Map<String, String>> segmentAssignment, int numSegmentsExpected,
      int numInstancesExpected) {
    assertEquals(segmentAssignment.size(), numSegmentsExpected);

    int maxSequenceNumber = 0;
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      LLCSegmentName llcSegmentName = LLCSegmentName.of(entry.getKey());
      if (llcSegmentName != null) {
        maxSequenceNumber = Math.max(maxSequenceNumber, llcSegmentName.getSequenceNumber());
      }
    }

    Map<Integer, String> serverForPartition = new HashMap<>();
    Set<String> uniqueServers = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();

      // Verify that all segments have the correct state
      assertEquals(instanceStateMap.size(), 1,
          "Segment " + segmentName + " has unexpected replica count. instanceStateMap=" + instanceStateMap
              + ", assignment=" + segmentAssignment);
      Map.Entry<String, String> instanceIdAndState = instanceStateMap.entrySet().iterator().next();
      String state = instanceIdAndState.getValue();
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName != null) {
        if (llcSegmentName.getSequenceNumber() < maxSequenceNumber) {
          assertEquals(state, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE,
              "Older segment " + segmentName + " on instance " + instanceIdAndState.getKey()
                  + " expected ONLINE. maxSeq=" + maxSequenceNumber + ", assignment=" + segmentAssignment);
        } else {
          assertEquals(state, CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING,
              "Newest segment " + segmentName + " on instance " + instanceIdAndState.getKey()
                  + " expected CONSUMING. maxSeq=" + maxSequenceNumber + ", assignment=" + segmentAssignment);
        }
      } else {
        assertEquals(state, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE,
            "Uploaded segment " + segmentName + " on instance " + instanceIdAndState.getKey()
                + " expected ONLINE. assignment=" + segmentAssignment);
      }

      // Verify that all segments of the same partition are mapped to the same server
      String instanceId = instanceIdAndState.getKey();
      int partitionId = getSegmentPartitionId(segmentName);
      uniqueServers.add(instanceId);
      String prevInstance = serverForPartition.computeIfAbsent(partitionId, k -> instanceId);
      assertEquals(instanceId, prevInstance);
    }

    assertEquals(uniqueServers.size(), numInstancesExpected);
  }

  protected void verifyIdealState(int numSegmentsExpected, int numInstancesExpected) {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, REALTIME_TABLE_NAME);
    verifySegmentAssignment(idealState.getRecord().getMapFields(), numSegmentsExpected, numInstancesExpected);
  }

  /// Waits until the cluster state has settled after a reload/rebalance:
  ///
  /// - IdealState has `expectedSegments` entries.
  /// - No segment in IdealState has any instance in OFFLINE state.
  /// - ExternalView equals IdealState (queries route via ExternalView, so this is required
  ///   before asserting on IdealState).
  ///
  /// All conditions must hold for `stableSamples` consecutive polls, because IdealState can
  /// transiently look correct between two intermediate states during force-commit +
  /// new-consuming-segment creation.
  protected void waitForClusterStateSettled(String tableNameWithType, int expectedSegments, int stableSamples,
      long timeoutMs) {
    AtomicInteger streak = new AtomicInteger(0);
    TestUtils.waitForCondition(aVoid -> {
      IdealState idealState = HelixHelper.getTableIdealState(_helixManager, tableNameWithType);
      if (idealState == null) {
        streak.set(0);
        return false;
      }
      Map<String, Map<String, String>> isMap = idealState.getRecord().getMapFields();
      if (isMap.size() != expectedSegments) {
        streak.set(0);
        return false;
      }
      boolean anyOffline = isMap.values().stream()
          .flatMap(m -> m.values().stream())
          .anyMatch(CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE::equals);
      if (anyOffline) {
        streak.set(0);
        return false;
      }
      ExternalView externalView =
          HelixHelper.getExternalViewForResource(_resourceManager.getHelixAdmin(),
              _resourceManager.getHelixClusterName(), tableNameWithType);
      if (externalView == null || !isMap.equals(externalView.getRecord().getMapFields())) {
        streak.set(0);
        return false;
      }
      return streak.incrementAndGet() >= stableSamples;
    }, 200L, timeoutMs,
        "Cluster state never settled to " + expectedSegments
            + " segments with no OFFLINE and ExternalView == IdealState for table " + tableNameWithType);
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
    UpsertConfig upsertConfig = _tableConfig.getUpsertConfig();
    assert upsertConfig != null;
    upsertConfig.setMode(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    _tableConfig.getIndexingConfig().setNullHandlingEnabled(true);

    addTableConfig(_tableConfig);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
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
        ServerRebalanceJobStatusResponse serverRebalanceJobStatusResponse = getOrCreateAdminClient()
            .getRebalanceClient().getRebalanceStatusObject(jobId);
        RebalanceResult.Status status = serverRebalanceJobStatusResponse.getTableRebalanceProgressStats().getStatus();
        return status != RebalanceResult.Status.IN_PROGRESS;
      } catch (Exception e) {
        return null;
      }
    }, 1000L, timeoutMs, "Failed to load all segments after rebalance");
  }

  protected void waitForReloadToComplete(String reloadJobId, long timeoutMs)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        PinotTableReloadStatusResponse segmentReloadStatusValue = getOrCreateAdminClient().getSegmentClient()
            .getSegmentReloadStatusObject(reloadJobId);
        return segmentReloadStatusValue.getSuccessCount() == segmentReloadStatusValue.getTotalSegmentCount();
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
