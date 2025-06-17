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

package org.apache.pinot.controller.helix.core.rebalance.tenant;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSummaryResult;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class TenantRebalancerTest extends ControllerTest {

  private static final String DEFAULT_TENANT_NAME = "DefaultTenant";
  private static final String TENANT_NAME = "TestTenant";
  private static final String RAW_TABLE_NAME_A = "testTableA";
  private static final String OFFLINE_TABLE_NAME_A = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_A);
  private static final String RAW_TABLE_NAME_B = "testTableB";
  private static final String OFFLINE_TABLE_NAME_B = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_B);
  private static final int NUM_REPLICAS = 3;
  ExecutorService _executorService;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    _executorService = Executors.newFixedThreadPool(3);
  }

  @Test
  public void testRebalanceBasic()
      throws Exception {
    int numServers = 3;
    for (int i = 0; i < numServers; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + i, true);
    }

    DefaultTenantRebalancer tenantRebalancer =
        new DefaultTenantRebalancer(_tableRebalanceManager, _helixResourceManager, _executorService);

    // tag all servers and brokers to test tenant
    addTenantTagToInstances(TENANT_NAME);

    // create 2 schemas
    addDummySchema(RAW_TABLE_NAME_A);
    addDummySchema(RAW_TABLE_NAME_B);

    // create 2 tables, one on each of test tenant and default tenant
    createTableWithSegments(RAW_TABLE_NAME_A, DEFAULT_TENANT_NAME);
    createTableWithSegments(RAW_TABLE_NAME_B, TENANT_NAME);

    // Add 3 more servers which will be tagged to default tenant
    int numServersToAdd = 3;
    for (int i = 0; i < numServersToAdd; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + (numServers + i), true);
    }

    Map<String, Map<String, String>> oldSegmentAssignment =
        _helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME_B).getRecord().getMapFields();

    // rebalance the tables on test tenant
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(TENANT_NAME);
    config.setVerboseResult(true);
    TenantRebalanceResult result = tenantRebalancer.rebalance(config);
    RebalanceResult rebalanceResult = result.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_B);
    Map<String, Map<String, String>> rebalancedAssignment = rebalanceResult.getSegmentAssignment();
    // assignment should not change, with a NO_OP status as no now server is added to test tenant
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    assertEquals(oldSegmentAssignment, rebalancedAssignment);

    // rebalance the tables on default tenant
    config.setTenantName(DEFAULT_TENANT_NAME);
    result = tenantRebalancer.rebalance(config);
    // rebalancing default tenant should distribute the segment of table A over 6 servers
    rebalanceResult = result.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_A);
    InstancePartitions partitions = rebalanceResult.getInstanceAssignment().get(InstancePartitionsType.OFFLINE);
    assertEquals(partitions.getPartitionToInstancesMap().get("0_0").size(), 6);

    // ensure the ideal state and external view converges
    assertTrue(waitForCompletion(result.getJobId()));
    TenantRebalanceProgressStats progressStats = getProgress(result.getJobId());
    assertTrue(progressStats.getTableRebalanceJobIdMap().containsKey(OFFLINE_TABLE_NAME_A));
    assertEquals(progressStats.getTableStatusMap().get(OFFLINE_TABLE_NAME_A),
        TenantRebalanceProgressStats.TableStatus.PROCESSED.name());
    Map<String, Map<String, String>> idealState =
        _helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME_A).getRecord().getMapFields();
    Map<String, Map<String, String>> externalView =
        _helixResourceManager.getTableExternalView(OFFLINE_TABLE_NAME_A).getRecord().getMapFields();
    assertEquals(idealState, externalView);

    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME_A);
    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME_B);
    for (int i = 0; i < numServers + numServersToAdd; i++) {
      stopAndDropFakeInstance(SERVER_INSTANCE_ID_PREFIX + i);
    }
  }

  @Test
  public void testIncludeAndExcludeTables()
      throws Exception {
    int numServers = 3;
    for (int i = 0; i < numServers; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + i, true);
    }

    DefaultTenantRebalancer tenantRebalancer =
        new DefaultTenantRebalancer(_tableRebalanceManager, _helixResourceManager, _executorService);

    // tag all servers and brokers to test tenant
    addTenantTagToInstances(TENANT_NAME);

    // create 2 schemas
    addDummySchema(RAW_TABLE_NAME_A);
    addDummySchema(RAW_TABLE_NAME_B);

    // create 2 tables, one on each of test tenant and default tenant
    createTableWithSegments(RAW_TABLE_NAME_A, TENANT_NAME);
    createTableWithSegments(RAW_TABLE_NAME_B, TENANT_NAME);

    // Add 3 more servers which will be tagged to default tenant
    int numServersToAdd = 3;
    for (int i = 0; i < numServersToAdd; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + (numServers + i), true);
    }

    addTenantTagToInstances(TENANT_NAME);

    // rebalance the tables on test tenant
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(TENANT_NAME);
    config.setVerboseResult(true);
    config.setDryRun(true);

    // leave allow and block tables empty
    config.setIncludeTables(Collections.emptySet());
    config.setExcludeTables(Collections.emptySet());

    TenantRebalanceResult tenantRebalanceResult = tenantRebalancer.rebalance(config);

    RebalanceResult rebalanceResultA = tenantRebalanceResult.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_A);
    RebalanceResult rebalanceResultB = tenantRebalanceResult.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_B);

    assertNotNull(rebalanceResultA);
    assertNotNull(rebalanceResultB);

    assertEquals(rebalanceResultA.getStatus(), RebalanceResult.Status.DONE);
    assertEquals(rebalanceResultB.getStatus(), RebalanceResult.Status.DONE);

    // block table B
    config.setExcludeTables(Collections.singleton(OFFLINE_TABLE_NAME_B));

    tenantRebalanceResult = tenantRebalancer.rebalance(config);

    rebalanceResultA = tenantRebalanceResult.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_A);
    rebalanceResultB = tenantRebalanceResult.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_B);

    assertNotNull(rebalanceResultA);
    assertNull(rebalanceResultB);
    assertEquals(rebalanceResultA.getStatus(), RebalanceResult.Status.DONE);

    // allow all tables explicitly, block table B, this should result the same as above case
    Set<String> includeTables = new HashSet<>();
    includeTables.add(OFFLINE_TABLE_NAME_A);
    includeTables.add(OFFLINE_TABLE_NAME_B);
    config.setIncludeTables(includeTables);
    config.setExcludeTables(Collections.singleton(OFFLINE_TABLE_NAME_B));

    tenantRebalanceResult = tenantRebalancer.rebalance(config);

    rebalanceResultA = tenantRebalanceResult.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_A);
    rebalanceResultB = tenantRebalanceResult.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_B);

    assertNotNull(rebalanceResultA);
    assertNull(rebalanceResultB);
    assertEquals(rebalanceResultA.getStatus(), RebalanceResult.Status.DONE);

    // allow table B
    config.setIncludeTables(Collections.singleton(OFFLINE_TABLE_NAME_B));
    config.setExcludeTables(Collections.emptySet());

    tenantRebalanceResult = tenantRebalancer.rebalance(config);

    rebalanceResultA = tenantRebalanceResult.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_A);
    rebalanceResultB = tenantRebalanceResult.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_B);

    assertNull(rebalanceResultA);
    assertNotNull(rebalanceResultB);

    assertEquals(rebalanceResultB.getStatus(), RebalanceResult.Status.DONE);

    // allow table B, also block table B
    config.setIncludeTables(Collections.singleton(OFFLINE_TABLE_NAME_B));
    config.setExcludeTables(Collections.singleton(OFFLINE_TABLE_NAME_B));

    tenantRebalanceResult = tenantRebalancer.rebalance(config);

    assertTrue(tenantRebalanceResult.getRebalanceTableResults().isEmpty());

    // allow a non-existing table
    config.setIncludeTables(Collections.singleton("TableDoesNotExist_OFFLINE"));
    config.setExcludeTables(Collections.emptySet());

    tenantRebalanceResult = tenantRebalancer.rebalance(config);

    assertTrue(tenantRebalanceResult.getRebalanceTableResults().isEmpty());

    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME_A);
    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME_B);

    for (int i = 0; i < numServers + numServersToAdd; i++) {
      stopAndDropFakeInstance(SERVER_INSTANCE_ID_PREFIX + i);
    }
  }

  @Test
  public void testGetTenantTables()
      throws Exception {
    int numServers = 1;
    for (int i = 0; i < numServers; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + i, true);
    }
    addTenantTagToInstances(TENANT_NAME);
    final String tableNameA = "testGetTenantTables_table_A";
    final String tableNameB = "testGetTenantTables_table_B";
    final String tableNameC = "testGetTenantTables_table_C";
    final String tableNameD = "testGetTenantTables_table_D";
    final String tableNameE = "testGetTenantTables_table_E";
    addDummySchema(tableNameA);
    addDummySchema(tableNameB);
    addDummySchema(tableNameC);
    addDummySchema(tableNameD);
    addDummySchema(tableNameE);

    DefaultTenantRebalancer tenantRebalancer =
        new DefaultTenantRebalancer(_tableRebalanceManager, _helixResourceManager, _executorService);

    // table A set tenantConfig.tenants.server to tenantName
    // SHOULD be selected as tenant's table
    TableConfig tableConfigA = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableNameA)
        .setServerTenant(TENANT_NAME).setBrokerTenant(DEFAULT_TENANT_NAME).build();
    // table B set tenantConfig.tagOverrideConfig.realtimeConsuming to tenantName
    // SHOULD be selected as tenant's table
    TableConfig tableConfigB = new TableConfigBuilder(TableType.REALTIME).setTableName(tableNameB)
        .setServerTenant(DEFAULT_TENANT_NAME)
        .setBrokerTenant(DEFAULT_TENANT_NAME)
        .setTagOverrideConfig(new TagOverrideConfig(TagNameUtils.getRealtimeTagForTenant(TENANT_NAME), null))
        .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap())
        .build();
    // table C set instanceAssignmentConfigMap.OFFLINE.tagPoolConfig.tag to tenantName
    // SHOULD be selected as tenant's table
    TableConfig tableConfigC = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableNameC)
        .setServerTenant(DEFAULT_TENANT_NAME)
        .setBrokerTenant(DEFAULT_TENANT_NAME)
        .setInstanceAssignmentConfigMap(
            Collections.singletonMap("OFFLINE", new InstanceAssignmentConfig(
                new InstanceTagPoolConfig(TagNameUtils.getOfflineTagForTenant(TENANT_NAME), false, 0, null), null,
                new InstanceReplicaGroupPartitionConfig(true, 0, 1, 0, 0, 0, false, null), null, true
            ))).build();
    // table D set tierConfigList[0].serverTag to tenantName
    // SHOULD be selected as tenant's table
    TableConfig tableConfigD = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableNameD)
        .setServerTenant(DEFAULT_TENANT_NAME)
        .setBrokerTenant(DEFAULT_TENANT_NAME)
        .setTierConfigList(Collections.singletonList(
            new TierConfig("dummyTier", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "7d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE,
                TagNameUtils.getOfflineTagForTenant(TENANT_NAME), null, null))).build();
    // table E set to default tenant
    // SHOULD NOT be selected as tenant's table
    TableConfig tableConfigE = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableNameE)
        .setServerTenant(DEFAULT_TENANT_NAME).setBrokerTenant(DEFAULT_TENANT_NAME).setNumReplicas(NUM_REPLICAS).build();
    // Create the table
    _helixResourceManager.addTable(tableConfigA);
    _helixResourceManager.addTable(tableConfigB);
    _helixResourceManager.addTable(tableConfigC);
    _helixResourceManager.addTable(tableConfigD);
    _helixResourceManager.addTable(tableConfigE);
    Set<String> tenantTables = tenantRebalancer.getTenantTables(TENANT_NAME);
    assertEquals(tenantTables.size(), 4);
    assertTrue(tenantTables.contains(TableNameBuilder.OFFLINE.tableNameWithType(tableNameA)));
    assertTrue(tenantTables.contains(TableNameBuilder.REALTIME.tableNameWithType(tableNameB)));
    assertTrue(tenantTables.contains(TableNameBuilder.OFFLINE.tableNameWithType(tableNameC)));
    assertTrue(tenantTables.contains(TableNameBuilder.OFFLINE.tableNameWithType(tableNameD)));
    assertFalse(tenantTables.contains(TableNameBuilder.OFFLINE.tableNameWithType(tableNameE)));

    _helixResourceManager.deleteOfflineTable(tableNameA);
    _helixResourceManager.deleteRealtimeTable(tableNameB);
    _helixResourceManager.deleteOfflineTable(tableNameC);
    _helixResourceManager.deleteOfflineTable(tableNameD);
    _helixResourceManager.deleteOfflineTable(tableNameE);
    for (int i = 0; i < numServers; i++) {
      stopAndDropFakeInstance(SERVER_INSTANCE_ID_PREFIX + i);
    }
  }

  @Test
  public void testCreateTableQueue()
      throws Exception {
    int numServers = NUM_REPLICAS + 1;
    for (int i = 0; i < numServers; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + i, true);
    }

    DefaultTenantRebalancer tenantRebalancer =
        new DefaultTenantRebalancer(_tableRebalanceManager, _helixResourceManager, _executorService);

    // tag all servers and brokers to test tenant
    addTenantTagToInstances(TENANT_NAME);

    // create a schema
    addDummySchema(RAW_TABLE_NAME_A);
    addDummySchema(RAW_TABLE_NAME_B);

    // create a table on test tenant
    createTableWithSegments(RAW_TABLE_NAME_A, TENANT_NAME);
    createDimTableWithSegments(RAW_TABLE_NAME_B, TENANT_NAME);

    // Add 3 more servers which will be tagged to default tenant
    int numServersToAdd = 3;
    for (int i = 0; i < numServersToAdd; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + (numServers + i), true);
    }
    addTenantTagToInstances(TENANT_NAME);

    // rebalance the tables on test tenant, this should be a pure scale out
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(TENANT_NAME);
    config.setVerboseResult(true);
    config.setDryRun(true);
    TenantRebalanceResult dryRunResult = tenantRebalancer.rebalance(config);

    RebalanceSummaryResult.ServerInfo serverInfo =
        dryRunResult.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_B).getRebalanceSummaryResult().getServerInfo();
    assertEquals(serverInfo.getServersAdded().size(), 3);
    assertEquals(serverInfo.getServersRemoved().size(), 0);

    Queue<String> tableQueue = tenantRebalancer.createTableQueue(config, dryRunResult.getRebalanceTableResults());
    // Dimension Table B should be rebalance first since it is a dim table, and we're doing scale out
    assertEquals(tableQueue.poll(), OFFLINE_TABLE_NAME_B);
    assertEquals(tableQueue.poll(), OFFLINE_TABLE_NAME_A);

    // untag server 0, now the rebalance is not a pure scale in/out
    _helixResourceManager.updateInstanceTags(SERVER_INSTANCE_ID_PREFIX + 0, "", false);
    dryRunResult = tenantRebalancer.rebalance(config);

    serverInfo =
        dryRunResult.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_B).getRebalanceSummaryResult().getServerInfo();
    assertEquals(serverInfo.getServersAdded().size(), 3);
    assertEquals(serverInfo.getServersRemoved().size(), 1);

    tableQueue = tenantRebalancer.createTableQueue(config, dryRunResult.getRebalanceTableResults());
    // Dimension table B should be rebalance first in this case. (it does not matter whether dimension tables are
    // rebalanced first or last in this case, simply because we defaulted it to be first while non-pure scale in/out)
    assertEquals(tableQueue.poll(), OFFLINE_TABLE_NAME_B);
    assertEquals(tableQueue.poll(), OFFLINE_TABLE_NAME_A);

    // untag the added servers, now the rebalance is a pure scale in
    for (int i = numServers; i < numServers + numServersToAdd; i++) {
      _helixResourceManager.updateInstanceTags(SERVER_INSTANCE_ID_PREFIX + i, "", false);
    }
    dryRunResult = tenantRebalancer.rebalance(config);

    serverInfo =
        dryRunResult.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_B).getRebalanceSummaryResult().getServerInfo();
    assertEquals(serverInfo.getServersAdded().size(), 0);
    assertEquals(serverInfo.getServersRemoved().size(), 1);

    tableQueue = tenantRebalancer.createTableQueue(config, dryRunResult.getRebalanceTableResults());
    // Dimension table B should be rebalance last in this case
    assertEquals(tableQueue.poll(), OFFLINE_TABLE_NAME_A);
    assertEquals(tableQueue.poll(), OFFLINE_TABLE_NAME_B);

    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME_A);
    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME_B);

    for (int i = 0; i < numServers + numServersToAdd; i++) {
      stopAndDropFakeInstance(SERVER_INSTANCE_ID_PREFIX + i);
    }
  }

  private boolean waitForCompletion(String jobId) {
    int retries = 5;
    while (retries > 0) {
      try {
        TenantRebalanceProgressStats stats = getProgress(jobId);
        if (stats != null && stats.getRemainingTables() == 0) {
          return true;
        }
        retries--;
        Thread.sleep(2000);
      } catch (JsonProcessingException | InterruptedException e) {
        return false;
      }
    }
    return false;
  }

  private TenantRebalanceProgressStats getProgress(String jobId)
      throws JsonProcessingException {
    Map<String, String> controllerJobZKMetadata =
        _helixResourceManager.getControllerJobZKMetadata(jobId, ControllerJobTypes.TENANT_REBALANCE);
    if (controllerJobZKMetadata == null) {
      return null;
    }
    return JsonUtils.stringToObject(
        controllerJobZKMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS),
        TenantRebalanceProgressStats.class);
  }

  private void createTableWithSegments(String rawTableName, String tenant)
      throws IOException {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName)
        .setServerTenant(tenant).setBrokerTenant(tenant).setNumReplicas(NUM_REPLICAS).build();
    // Create the table
    _helixResourceManager.addTable(tableConfig);
    // Add the segments
    int numSegments = 10;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    for (int i = 0; i < numSegments; i++) {
      _helixResourceManager.addNewSegment(offlineTableName,
          SegmentMetadataMockUtils.mockSegmentMetadata(rawTableName, "segment_" + i), null);
    }
  }

  private void createDimTableWithSegments(String rawTableName, String tenant)
      throws IOException {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName)
        .setServerTenant(tenant).setBrokerTenant(tenant).setIsDimTable(true).build();
    // Create the table
    _helixResourceManager.addTable(tableConfig);
    // Add the segments
    int numSegments = 10;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    for (int i = 0; i < numSegments; i++) {
      _helixResourceManager.addNewSegment(offlineTableName,
          SegmentMetadataMockUtils.mockSegmentMetadata(rawTableName, "segment_" + i), null);
    }
  }

  private void addTenantTagToInstances(String testTenant) {
    String offlineTag = TagNameUtils.getOfflineTagForTenant(testTenant);
    String realtimeTag = TagNameUtils.getRealtimeTagForTenant(testTenant);
    String brokerTag = TagNameUtils.getBrokerTagForTenant(testTenant);
    _helixResourceManager.getAllInstances().forEach(instance -> {
      List<String> existingTags = _helixResourceManager.getHelixInstanceConfig(instance).getTags();
      if (instance.startsWith(SERVER_INSTANCE_ID_PREFIX)) {
        existingTags.add(offlineTag);
        existingTags.add(realtimeTag);
      } else if (instance.startsWith(BROKER_INSTANCE_ID_PREFIX)) {
        existingTags.add(brokerTag);
      }
      _helixResourceManager.updateInstanceTags(instance, String.join(",", existingTags), true);
    });
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
    _executorService.shutdown();
  }
}
