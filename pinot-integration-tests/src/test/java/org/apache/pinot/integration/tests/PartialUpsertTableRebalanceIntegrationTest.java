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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.restlet.resources.PinotTableReloadStatusResponse;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.restlet.resources.RebalanceResult;
import org.apache.pinot.common.restlet.resources.ServerRebalanceJobStatusResponse;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.core.data.manager.realtime.SegmentBuildTimeLeaseExtender;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class PartialUpsertTableRebalanceIntegrationTest extends SharedRichClusterIntegrationTest {
  private static final int NUM_SERVERS = 1;
  private static final String PRIMARY_KEY_COL = "clientId";
  private static final String SHARED_TABLE_NAME = "partial_upsert_table_rebalance";
  private static final String SHARED_KAFKA_TOPIC = "partial-upsert-table-rebalance";

  // Segment 1 contains records of pk value 100000 (partition 0)
  private static final String UPLOADED_SEGMENT_1_SUFFIX = "_10027_19736_0 %";
  // Segment 2 contains records of pk value 100001 (partition 1)
  private static final String UPLOADED_SEGMENT_2_SUFFIX = "_10072_19919_1 %";
  // Segment 3 contains records of pk value 100002 (partition 1)
  private static final String UPLOADED_SEGMENT_3_SUFFIX = "_10158_19938_2 %";

  private final String _resourceSuffix = Long.toUnsignedString(RANDOM.nextLong(), Character.MAX_RADIX);
  private final List<BaseServerStarter> _extraServerStarters = new ArrayList<>();

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;
  private PinotHelixResourceManager _resourceManager;
  private TableRebalancer _tableRebalancer;
  private List<File> _avroFiles;
  private TableConfig _tableConfig;
  private Schema _schema;

  @BeforeClass(alwaysRun = true)
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    startZk();
    // Start Kafka
    startKafka();
    startController();
    startBroker();
    startServers(NUM_SERVERS);

    validateSharedTopology();

    _resourceManager = _controllerStarter.getHelixResourceManager();
    _tableRebalancer = new TableRebalancer(_resourceManager.getHelixZkManager());

    cleanRealtimeTableAndSchema();
    resetKafkaTopic();
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
    BaseServerStarter serverStarter1 = startExtraServer(NUM_SERVERS);

    // Now we trigger a rebalance operation
    TableConfig tableConfig = _resourceManager.getTableConfig(getRealtimeTableName());
    RebalanceResult rebalanceResult = _tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);

    // Check the number of servers after rebalancing
    int finalServers = _resourceManager.getServerInstancesForTable(getTableName(), TableType.REALTIME).size();

    // Check that a server has been added
    assertEquals(finalServers, NUM_SERVERS + 1, "Rebalancing didn't correctly add the new server");

    waitForRebalanceToComplete(rebalanceResult, 600_000L);
    waitForAllDocsLoaded(600_000L);

    verifySegmentAssignment(rebalanceResult.getSegmentAssignment(), 5, finalServers);

    // Add a new server
    BaseServerStarter serverStarter2 = startExtraServer(NUM_SERVERS + 1);
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

    cleanUpExtraServers();
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
    BaseServerStarter serverStarter1 = startExtraServer(NUM_SERVERS);

    // Now we trigger a rebalance operation
    TableConfig tableConfig = _resourceManager.getTableConfig(getRealtimeTableName());
    RebalanceResult rebalanceResult = _tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);

    // Check the number of servers after rebalancing
    int finalServers = _resourceManager.getServerInstancesForTable(getTableName(), TableType.REALTIME).size();

    // Check that a server has been added
    assertEquals(finalServers, NUM_SERVERS + 1, "Rebalancing didn't correctly add the new server");

    waitForRebalanceToComplete(rebalanceResult, 600_000L);
    waitForAllDocsLoaded(600_000L);

    verifySegmentAssignment(rebalanceResult.getSegmentAssignment(), 5, finalServers);

    // Add a new server
    BaseServerStarter serverStarter2 = startExtraServer(NUM_SERVERS + 1);
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

    cleanUpExtraServers();
  }

  @Test
  public void testReload()
      throws Exception {
    pushAvroIntoKafka(_avroFiles);
    waitForAllDocsLoaded(600_000L, 300);

    // Partial-upsert tables need PROTECTED consuming segment consistency mode for reload to force-commit
    // consuming segments; RESTRICTED (default) skips force-commit and reload never completes.
    String clusterConfigKey = CommonConstants.ConfigChangeListenerConstants.CONSUMING_SEGMENT_CONSISTENCY_MODE;
    String originalClusterConfigValue = getClusterConfig(clusterConfigKey);
    try {
      updateClusterConfig(
          Map.of(clusterConfigKey, ConsumingSegmentConsistencyModeListener.Mode.PROTECTED.name()));
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
      String reloadJobId = reloadStatus.get(getRealtimeTableName()).get("reloadJobId");
      waitForReloadToComplete(reloadJobId, 600_000L);
      waitForAllDocsLoaded(600_000L, 300);
      verifyIdealState(4, NUM_SERVERS); // 4 because reload triggers commit of consuming segments
    } finally {
      restoreClusterConfig(clusterConfigKey, originalClusterConfigValue);
    }
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::dropRealtimeTableIfPresent);
    exception = runCleanup(exception, this::cleanUpExtraServers);
    exception = runCleanup(exception, this::resetKafkaTopicIfStarted);
    if (_tableConfig != null) {
      exception = runCleanup(exception, () -> addTableConfig(_tableConfig));
    }
    if (exception != null) {
      throw exception;
    }
  }

  private void dropRealtimeTableIfPresent()
      throws Exception {
    if (_resourceManager == null) {
      return;
    }
    String realtimeTableName = getRealtimeTableName();
    // Drop the table entirely to clean up all segments and server-side upsert state.
    // This is more reliable than the pause/drop-segments/restart cycle because it uses
    // the standard table lifecycle and avoids issues with stale controller/server state.
    if (_resourceManager.getTableConfig(realtimeTableName) == null
        && !_resourceManager.hasRealtimeTable(getTableName())) {
      return;
    }
    dropRealtimeTable(getTableName());
    waitForTableDataManagerRemoved(realtimeTableName);
    waitForEVToDisappear(realtimeTableName);
  }

  private void resetKafkaTopicIfStarted() {
    if (_kafkaStarters == null || _kafkaStarters.isEmpty()) {
      return;
    }
    resetKafkaTopic();
  }

  private void resetKafkaTopic() {
    deleteKafkaTopicIfPresent();
    createKafkaTopic(getKafkaTopic());
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
      String prevInstance = serverForPartition.computeIfAbsent(partitionId, k -> instanceId);
      assertEquals(instanceId, prevInstance);
    }

    assertEquals(uniqueServers.size(), numInstancesExpected);
  }

  protected void verifyIdealState(int numSegmentsExpected, int numInstancesExpected) {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, getRealtimeTableName());
    verifySegmentAssignment(idealState.getRecord().getMapFields(), numSegmentsExpected, numInstancesExpected);
  }

  protected void populateTables()
      throws Exception {
    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(_avroFiles, _tableConfig, _schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), TableType.REALTIME, _classTarDir);

    pushAvroIntoKafka(_avroFiles);
    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected void createSchemaAndTable()
      throws Exception {
    // Unpack the Avro files
    _avroFiles = unpackAvroData(_classTempDir);

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

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanRealtimeTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::cleanUpExtraServers);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::closeAdminClientInSharedMode);
    if (!isSharedRichClusterEnabled()) {
      exception = runCleanup(exception, this::stopServerIfStarted);
      exception = runCleanup(exception, this::stopBrokerIfStarted);
      exception = runCleanup(exception, this::stopControllerIfStarted);
      exception = runCleanup(exception, this::stopKafkaIfStarted);
      exception = runCleanup(exception, this::stopZk);
    }
    exception = runCleanup(exception, this::deleteClassTempDir);
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME + "_" + _resourceSuffix : super.getTableName();
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC + "-" + _resourceSuffix : super.getKafkaTopic();
  }

  @Override
  protected boolean shouldStartSharedKafka() {
    return true;
  }

  @Override
  protected int getSharedNumBrokers() {
    return 1;
  }

  @Override
  protected int getSharedNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected boolean shouldStartSharedMinion() {
    return false;
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
    if (segmentName.endsWith(UPLOADED_SEGMENT_1_SUFFIX)) {
      return 0;
    }
    if (segmentName.endsWith(UPLOADED_SEGMENT_2_SUFFIX) || segmentName.endsWith(UPLOADED_SEGMENT_3_SUFFIX)) {
      return 1;
    }
    return new LLCSegmentName(segmentName).getPartitionGroupId();
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

  private String getRealtimeTableName() {
    return TableNameBuilder.REALTIME.tableNameWithType(getTableName());
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-" + _resourceSuffix)
        : _tempDir;
  }

  private void validateSharedTopology() {
    if (!isSharedRichClusterEnabled()) {
      return;
    }
    assertEquals(_brokerStarters.size(), 1, "Shared rich cluster broker count mismatch");
    assertEquals(_serverStarters.size(), NUM_SERVERS, "Shared rich cluster server count mismatch");
    assertNotNull(_controllerStarter, "Shared rich cluster controller is not started");
    assertNotNull(_kafkaStarters, "Shared rich cluster Kafka is not started");
    assertFalse(_kafkaStarters.isEmpty(), "Shared rich cluster Kafka is not started");
    assertNull(_minionStarter, "Shared rich cluster minion should not be started");
  }

  private BaseServerStarter startExtraServer(int serverId)
      throws Exception {
    BaseServerStarter serverStarter = startOneServer(serverId);
    _extraServerStarters.add(serverStarter);
    return serverStarter;
  }

  private void cleanUpExtraServers()
      throws Exception {
    if (_extraServerStarters.isEmpty()) {
      return;
    }
    Exception exception = null;
    for (BaseServerStarter serverStarter : new ArrayList<>(_extraServerStarters)) {
      Exception serverException = null;
      serverException = runCleanup(serverException, serverStarter::stop);
      SegmentBuildTimeLeaseExtender.initExecutor();
      if (_resourceManager != null && isInstancePresent(serverStarter.getInstanceId())) {
        serverException = runCleanup(serverException, () -> TestUtils.waitForCondition(
            aVoid -> _resourceManager.dropInstance(serverStarter.getInstanceId()).isSuccessful(), 60_000L,
            "Failed to drop server " + serverStarter.getInstanceId()));
      }
      if (serverException == null) {
        _extraServerStarters.remove(serverStarter);
      } else if (exception == null) {
        exception = serverException;
      } else {
        exception.addSuppressed(serverException);
      }
    }
    restoreSharedOwnerServerStarters();
    if (exception != null) {
      throw exception;
    }
  }

  private boolean isInstancePresent(String instanceId) {
    try {
      return _helixAdmin != null && _helixAdmin.getInstancesInCluster(getHelixClusterName()).contains(instanceId);
    } catch (Exception e) {
      return false;
    }
  }

  private void restoreSharedOwnerServerStarters() {
    if (!isSharedRichClusterEnabled() || _sharedRichClusterTestSuite == null || _sharedRichClusterTestSuite == this) {
      return;
    }
    _sharedRichClusterTestSuite._serverStarters.clear();
    _sharedRichClusterTestSuite._serverStarters.addAll(_serverStarters);
    _sharedRichClusterTestSuite._serverGrpcPort = _serverGrpcPort;
    _sharedRichClusterTestSuite._serverAdminApiPort = _serverAdminApiPort;
    _sharedRichClusterTestSuite._serverNettyPort = _serverNettyPort;
    attachSharedRichCluster();
  }

  private void cleanRealtimeTableAndSchema()
      throws Exception {
    dropRealtimeTableIfPresent();
    if (_resourceManager != null && _resourceManager.getSchema(getTableName()) != null) {
      deleteSchema(getTableName());
    }
  }

  private void deleteKafkaTopicIfPresent() {
    if (isKafkaTopicPresent()) {
      deleteKafkaTopic(getKafkaTopic());
    }
  }

  private boolean isKafkaTopicPresent() {
    if (_kafkaStarters == null || _kafkaStarters.isEmpty()) {
      return false;
    }
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      return adminClient.listTopics().names().get(5, TimeUnit.SECONDS).contains(getKafkaTopic());
    } catch (Exception e) {
      return false;
    }
  }

  private void closePinotConnections() {
    if (_pinotConnection != null) {
      _pinotConnection.close();
      _pinotConnection = null;
    }
    if (_pinotConnectionV2 != null) {
      _pinotConnectionV2.close();
      _pinotConnectionV2 = null;
    }
  }

  private void closeAdminClientInSharedMode()
      throws IOException {
    if (isSharedRichClusterEnabled()) {
      getOrCreateAdminClient().close();
    }
  }

  private void stopServerIfStarted() {
    if (!_serverStarters.isEmpty()) {
      stopServer();
    }
  }

  private void stopBrokerIfStarted() {
    if (!_brokerStarters.isEmpty()) {
      stopBroker();
    }
  }

  private void stopControllerIfStarted() {
    if (_controllerStarter != null) {
      stopController();
    }
  }

  private void stopKafkaIfStarted() {
    if (_kafkaStarters != null && !_kafkaStarters.isEmpty()) {
      stopKafka();
    }
  }

  private void deleteClassTempDir()
      throws IOException {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private String getClusterConfig(String key) {
    return _helixManager.getConfigAccessor().get(getClusterConfigScope(), key);
  }

  private void restoreClusterConfig(String key, String originalValue)
      throws IOException {
    if (originalValue == null) {
      deleteClusterConfig(key);
    } else {
      updateClusterConfig(Map.of(key, originalValue));
    }
  }

  private HelixConfigScope getClusterConfigScope() {
    return new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
        .build();
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
