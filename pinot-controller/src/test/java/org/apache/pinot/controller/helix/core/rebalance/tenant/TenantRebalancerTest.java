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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.tuple.Pair;
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

    Queue<TenantRebalancer.TenantTableRebalanceJobContext> tableQueue =
        tenantRebalancer.createTableQueue(config, dryRunResult.getRebalanceTableResults());
    // Dimension Table B should be rebalance first since it is a dim table, and we're doing scale out
    TenantRebalancer.TenantTableRebalanceJobContext jobContext = tableQueue.poll();
    assertNotNull(jobContext);
    assertEquals(jobContext.getTableName(), OFFLINE_TABLE_NAME_B);
    jobContext = tableQueue.poll();
    assertNotNull(jobContext);
    assertEquals(jobContext.getTableName(), OFFLINE_TABLE_NAME_A);

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
    jobContext = tableQueue.poll();
    assertNotNull(jobContext);
    assertEquals(jobContext.getTableName(), OFFLINE_TABLE_NAME_B);
    jobContext = tableQueue.poll();
    assertNotNull(jobContext);
    assertEquals(jobContext.getTableName(), OFFLINE_TABLE_NAME_A);

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
    jobContext = tableQueue.poll();
    assertNotNull(jobContext);
    assertEquals(jobContext.getTableName(), OFFLINE_TABLE_NAME_A);
    jobContext = tableQueue.poll();
    assertNotNull(jobContext);
    assertEquals(jobContext.getTableName(), OFFLINE_TABLE_NAME_B);

    // set table B in parallel blacklist, so that it ends up in sequential queue, and table A in parallel queue
    Pair<ConcurrentLinkedQueue<TenantRebalancer.TenantTableRebalanceJobContext>,
        Queue<TenantRebalancer.TenantTableRebalanceJobContext>>
        queues =
        tenantRebalancer.createParallelAndSequentialQueues(config, dryRunResult.getRebalanceTableResults(), null,
            Collections.singleton(OFFLINE_TABLE_NAME_B));
    Queue<TenantRebalancer.TenantTableRebalanceJobContext> parallelQueue = queues.getLeft();
    Queue<TenantRebalancer.TenantTableRebalanceJobContext> sequentialQueue = queues.getRight();
    jobContext = parallelQueue.poll();
    assertNotNull(jobContext);
    assertEquals(jobContext.getTableName(), OFFLINE_TABLE_NAME_A);
    assertNull(parallelQueue.poll());
    jobContext = sequentialQueue.poll();
    assertNotNull(jobContext);
    assertEquals(jobContext.getTableName(), OFFLINE_TABLE_NAME_B);
    assertNull(sequentialQueue.poll());

    // set table B in parallel whitelist, so that it ends up in parallel queue, and table A in sequential queue
    queues = tenantRebalancer.createParallelAndSequentialQueues(config, dryRunResult.getRebalanceTableResults(),
        Collections.singleton(OFFLINE_TABLE_NAME_B), null);
    parallelQueue = queues.getLeft();
    sequentialQueue = queues.getRight();
    jobContext = parallelQueue.poll();
    assertNotNull(jobContext);
    assertEquals(jobContext.getTableName(), OFFLINE_TABLE_NAME_B);
    assertNull(parallelQueue.poll());
    jobContext = sequentialQueue.poll();
    assertNotNull(jobContext);
    assertEquals(jobContext.getTableName(), OFFLINE_TABLE_NAME_A);
    assertNull(sequentialQueue.poll());

    // set both tables in parallel whitelist, and table B in parallel blacklist, so that B ends up in sequential
    // queue, and table A in parallel queue
    queues = tenantRebalancer.createParallelAndSequentialQueues(config, dryRunResult.getRebalanceTableResults(),
        Set.of(OFFLINE_TABLE_NAME_A, OFFLINE_TABLE_NAME_B), Collections.singleton(OFFLINE_TABLE_NAME_B));
    parallelQueue = queues.getLeft();
    sequentialQueue = queues.getRight();
    jobContext = parallelQueue.poll();
    assertNotNull(jobContext);
    assertEquals(jobContext.getTableName(), OFFLINE_TABLE_NAME_A);
    assertNull(parallelQueue.poll());
    jobContext = sequentialQueue.poll();
    assertNotNull(jobContext);
    assertEquals(jobContext.getTableName(), OFFLINE_TABLE_NAME_B);
    assertNull(sequentialQueue.poll());

    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME_A);
    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME_B);

    for (int i = 0; i < numServers + numServersToAdd; i++) {
      stopAndDropFakeInstance(SERVER_INSTANCE_ID_PREFIX + i);
    }
  }

  @Test
  public void testTenantRebalanceResultAggregation()
      throws Exception {
    // Test data setup
    String jobId = "test-job-123";
    String testTenant = "TestTenant";
    String offlineTag = TagNameUtils.getOfflineTagForTenant(testTenant);
    String realtimeTag = TagNameUtils.getRealtimeTagForTenant(testTenant);

    // Create mock RebalanceResult objects for different tables
    Map<String, RebalanceResult> tableResults = new HashMap<>();

    // Table A: Scale out scenario - adding servers
    RebalanceResult tableAResult = createMockRebalanceResult("tableA_job",
        RebalanceResult.Status.DONE, "Table A rebalanced successfully",
        createMockServerInfo(
            2, // servers getting new segments
            new RebalanceSummaryResult.RebalanceChangeInfo(3, 5), // 3 -> 5 servers
            Set.of("server4", "server5"), // servers added
            null, // servers removed
            Set.of("server1", "server2", "server3"), // servers unchanged
            Set.of("server4", "server5"), // servers getting new segments
            Map.of(
                "server1", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 10, 10, 0, 0, 10, List.of(offlineTag)),
                "server2", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 10, 10, 0, 0, 10, List.of(offlineTag)),
                "server3", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 10, 10, 0, 0, 10, List.of(offlineTag)),
                "server4", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.ADDED, 5, 0, 5, 0, 0, List.of(offlineTag)),
                "server5", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.ADDED, 5, 0, 5, 0, 0, List.of(offlineTag))
            )
        ),
        createMockSegmentInfo(
            10, // segments to be moved
            0,  // segments to be deleted
            5,  // max segments added to single server
            1000L, // average segment size
            10000L, // total data to be moved
            new RebalanceSummaryResult.RebalanceChangeInfo(3, 4), // replication factor unchanged
            new RebalanceSummaryResult.RebalanceChangeInfo(10, 10), // segments in single replica
            new RebalanceSummaryResult.RebalanceChangeInfo(30, 40), // segments across all replicas
            null // no consuming segments for offline table
        ),
        List.of(new RebalanceSummaryResult.TagInfo(offlineTag, 10, 30, 5))
    );
    tableResults.put("tableA_OFFLINE", tableAResult);

    // Table B: Scale in scenario - removing servers
    RebalanceResult tableBResult = createMockRebalanceResult("tableB_job",
        RebalanceResult.Status.DONE, "Table B rebalanced successfully",
        createMockServerInfo(
            0, // servers getting new segments
            new RebalanceSummaryResult.RebalanceChangeInfo(4, 3), // 4 -> 3 servers
            null, // servers added
            Set.of("server6"), // servers removed
            Set.of("server7", "server8", "server9"), // servers unchanged
            null, // servers getting new segments
            Map.of(
                "server6", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.REMOVED, 0, 8, 0, 8, 0, List.of(realtimeTag)),
                "server7", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 12, 8, 4, 0, 8, List.of(realtimeTag)),
                "server8", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 12, 8, 4, 0, 8, List.of(realtimeTag)),
                "server9", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 8, 8, 0, 0, 8, List.of(realtimeTag))
            )
        ),
        createMockSegmentInfo(
            8,  // segments to be moved
            8,  // segments to be deleted
            4,  // max segments added to single server
            2000L, // average segment size
            16000L, // total data to be moved
            new RebalanceSummaryResult.RebalanceChangeInfo(2, 2), // replication factor unchanged
            new RebalanceSummaryResult.RebalanceChangeInfo(16, 16), // segments in single replica
            new RebalanceSummaryResult.RebalanceChangeInfo(32, 32), // segments across all replicas
            null // no consuming segments for offline table
        ),
        List.of(new RebalanceSummaryResult.TagInfo(realtimeTag, 8, 24, 3))
    );
    tableResults.put("tableB_REALTIME", tableBResult);

    // Table C: No operation scenario
    RebalanceResult tableCResult = createMockRebalanceResult("tableC_job",
        RebalanceResult.Status.NO_OP, "Table C - no rebalancing needed",
        createMockServerInfo(
            0, // servers getting new segments
            new RebalanceSummaryResult.RebalanceChangeInfo(2, 2), // 2 -> 2 servers
            null, // servers added
            null, // servers removed
            Set.of("server10", "server11"), // servers unchanged
            null, // servers getting new segments
            Map.of(
                "server10", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 6, 6, 0, 0, 6, List.of(offlineTag)),
                "server11", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 6, 6, 0, 0, 6, List.of(offlineTag))
            )
        ),
        createMockSegmentInfo(
            0,  // segments to be moved
            0,  // segments to be deleted
            0,  // max segments added to single server
            0L, // average segment size
            0L, // total data to be moved
            new RebalanceSummaryResult.RebalanceChangeInfo(2, 2), // replication factor unchanged
            new RebalanceSummaryResult.RebalanceChangeInfo(6, 6), // segments in single replica
            new RebalanceSummaryResult.RebalanceChangeInfo(12, 12), // segments across all replicas
            null // no consuming segments for offline table
        ),
        List.of(new RebalanceSummaryResult.TagInfo(offlineTag, 0, 12, 2))
    );
    tableResults.put("tableC_OFFLINE", tableCResult);

    // Create TenantRebalanceResult and verify aggregations
    TenantRebalanceResult tenantResult = new TenantRebalanceResult(jobId, tableResults, true);

    // Verify basic properties
    assertEquals(tenantResult.getJobId(), jobId);
    assertEquals(tenantResult.getTotalTables(), 3);

    // Verify status summary aggregation
    Map<RebalanceResult.Status, Integer> statusSummary = tenantResult.getStatusSummary();
    assertEquals(statusSummary.get(RebalanceResult.Status.DONE), Integer.valueOf(2));
    assertEquals(statusSummary.get(RebalanceResult.Status.NO_OP), Integer.valueOf(1));
    assertNull(statusSummary.get(RebalanceResult.Status.FAILED));

    // Verify aggregated rebalance summary
    RebalanceSummaryResult aggregatedSummary = tenantResult.getAggregatedRebalanceSummary();
    assertNotNull(aggregatedSummary);

    // Verify aggregated server info
    RebalanceSummaryResult.ServerInfo serverInfo = aggregatedSummary.getServerInfo();
    assertNotNull(serverInfo);
    assertEquals(serverInfo.getNumServersGettingNewSegments(), 4); // server4, server5, server7, and server8
    assertEquals(serverInfo.getNumServers().getValueBeforeRebalance(), 9); // 3 + 4 + 2 unique servers before
    assertEquals(serverInfo.getNumServers().getExpectedValueAfterRebalance(), 10); // 5 + 3 + 2 unique servers after

    // Verify server sets
    assertEquals(serverInfo.getServersAdded(), Set.of("server4", "server5"));
    assertEquals(serverInfo.getServersRemoved(), Set.of("server6"));
    assertEquals(serverInfo.getServersUnchanged(),
        Set.of("server1", "server2", "server3", "server7", "server8", "server9", "server10", "server11"));
    assertEquals(serverInfo.getServersGettingNewSegments(), Set.of("server4", "server5", "server7", "server8"));

    // Verify server segment change info aggregation
    Map<String, RebalanceSummaryResult.ServerSegmentChangeInfo> serverSegmentChangeInfo =
        serverInfo.getServerSegmentChangeInfo();
    assertNotNull(serverSegmentChangeInfo);

    // Verify a few key servers
    RebalanceSummaryResult.ServerSegmentChangeInfo server1Info = serverSegmentChangeInfo.get("server1");
    assertEquals(server1Info.getTotalSegmentsBeforeRebalance(), 10);
    assertEquals(server1Info.getTotalSegmentsAfterRebalance(), 10);
    assertEquals(server1Info.getSegmentsAdded(), 0);
    assertEquals(server1Info.getSegmentsDeleted(), 0);
    assertEquals(server1Info.getSegmentsUnchanged(), 10);
    assertEquals(server1Info.getServerStatus(), RebalanceSummaryResult.ServerStatus.UNCHANGED);

    RebalanceSummaryResult.ServerSegmentChangeInfo server4Info = serverSegmentChangeInfo.get("server4");
    assertEquals(server4Info.getTotalSegmentsBeforeRebalance(), 0);
    assertEquals(server4Info.getTotalSegmentsAfterRebalance(), 5);
    assertEquals(server4Info.getSegmentsAdded(), 5);
    assertEquals(server4Info.getSegmentsDeleted(), 0);
    assertEquals(server4Info.getSegmentsUnchanged(), 0);
    assertEquals(server4Info.getServerStatus(), RebalanceSummaryResult.ServerStatus.ADDED);

    RebalanceSummaryResult.ServerSegmentChangeInfo server6Info = serverSegmentChangeInfo.get("server6");
    assertEquals(server6Info.getTotalSegmentsBeforeRebalance(), 8);
    assertEquals(server6Info.getTotalSegmentsAfterRebalance(), 0);
    assertEquals(server6Info.getSegmentsAdded(), 0);
    assertEquals(server6Info.getSegmentsDeleted(), 8);
    assertEquals(server6Info.getSegmentsUnchanged(), 0);
    assertEquals(server6Info.getServerStatus(), RebalanceSummaryResult.ServerStatus.REMOVED);

    // Verify aggregated segment info
    RebalanceSummaryResult.SegmentInfo segmentInfo = aggregatedSummary.getSegmentInfo();
    assertNotNull(segmentInfo);
    assertEquals(segmentInfo.getTotalSegmentsToBeMoved(), 18); // 10 + 8 + 0
    assertEquals(segmentInfo.getTotalSegmentsToBeDeleted(), 8); // 0 + 8 + 0
    assertEquals(segmentInfo.getMaxSegmentsAddedToASingleServer(), 5); // max(5, 4, 0)
    assertEquals(segmentInfo.getEstimatedAverageSegmentSizeInBytes(),
        (10000L + 16000L) / 18); // weighted average: (10*1000 + 8*2000 + 0*0) / 18
    assertEquals(segmentInfo.getTotalEstimatedDataToBeMovedInBytes(), 26000L); // 10000 + 16000 + 0

    // Verify segment counts aggregation
    assertEquals(segmentInfo.getNumSegmentsInSingleReplica().getValueBeforeRebalance(), 32); // 10 + 16 + 6
    assertEquals(segmentInfo.getNumSegmentsInSingleReplica().getExpectedValueAfterRebalance(), 32); // 10 + 16 + 6
    assertEquals(segmentInfo.getNumSegmentsAcrossAllReplicas().getValueBeforeRebalance(), 74); // 30 + 32 + 12
    assertEquals(segmentInfo.getNumSegmentsAcrossAllReplicas().getExpectedValueAfterRebalance(), 84); // 40 + 32 + 12

    // Verify aggregated tag info
    List<RebalanceSummaryResult.TagInfo> tagsInfo = aggregatedSummary.getTagsInfo();
    assertNotNull(tagsInfo);
    assertEquals(tagsInfo.size(), 2);

    // Find and verify offline tag info
    RebalanceSummaryResult.TagInfo offlineTagInfo = tagsInfo.stream()
        .filter(tag -> tag.getTagName().equals(offlineTag))
        .findFirst()
        .orElse(null);
    assertNotNull(offlineTagInfo);
    assertEquals(offlineTagInfo.getNumSegmentsToDownload(), 10); // 10 + 0 + 0 (only table A and C have offline tags)
    assertEquals(offlineTagInfo.getNumSegmentsUnchanged(), 42); // 30 + 0 + 12
    assertEquals(offlineTagInfo.getNumServerParticipants(), 7); // servers 1,2,3,4,5,10,11

    // Find and verify realtime tag info
    RebalanceSummaryResult.TagInfo realtimeTagInfo = tagsInfo.stream()
        .filter(tag -> tag.getTagName().equals(realtimeTag))
        .findFirst()
        .orElse(null);
    assertNotNull(realtimeTagInfo);
    assertEquals(realtimeTagInfo.getNumSegmentsToDownload(), 8); // only table B has realtime tags
    assertEquals(realtimeTagInfo.getNumSegmentsUnchanged(), 24);
    assertEquals(realtimeTagInfo.getNumServerParticipants(), 3); // servers 7,8,9 (server6 is removed)

    // Verify original table results are preserved
    assertEquals(tenantResult.getRebalanceTableResults().size(), 3);
    assertEquals(tenantResult.getRebalanceTableResults().get("tableA_OFFLINE").getStatus(),
        RebalanceResult.Status.DONE);
    assertEquals(tenantResult.getRebalanceTableResults().get("tableB_REALTIME").getStatus(),
        RebalanceResult.Status.DONE);
    assertEquals(tenantResult.getRebalanceTableResults().get("tableC_OFFLINE").getStatus(),
        RebalanceResult.Status.NO_OP);
  }

  @Test
  public void testTenantRebalanceResultAggregationWithOverlappingServers()
      throws Exception {
    // Test data setup with overlapping servers between tables
    String jobId = "test-job-overlapping-456";
    String testTenant = "TestTenant";
    String offlineTag = TagNameUtils.getOfflineTagForTenant(testTenant);
    String realtimeTag = TagNameUtils.getRealtimeTagForTenant(testTenant);

    // Create mock RebalanceResult objects with overlapping servers
    Map<String, RebalanceResult> tableResults = new HashMap<>();

    // Table A (offline): servers 1,2,3 -> 1,2,3,4 (add server 4)
    // Server 2 and 3 will also appear in Table B
    RebalanceResult tableAResult = createMockRebalanceResult("tableA_job",
        RebalanceResult.Status.DONE, "Table A rebalanced successfully",
        createMockServerInfo(
            1, // servers getting new segments (server 4)
            new RebalanceSummaryResult.RebalanceChangeInfo(3, 3),
            Set.of("server4"), // servers added
            Set.of("server1"), // servers removed
            Set.of("server2", "server3"), // servers unchanged
            Set.of("server4"), // servers getting new segments
            Map.of(
                "server1", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.REMOVED, 0, 8, 0, 8, 0, List.of(offlineTag)),
                "server2", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 8, 8, 0, 0, 8, List.of(offlineTag)),
                "server3", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 8, 8, 0, 0, 8, List.of(offlineTag, realtimeTag)),
                "server4", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.ADDED, 8, 0, 8, 0, 0, List.of(offlineTag, realtimeTag))
            )
        ),
        createMockSegmentInfo(
            8, // segments to be moved
            8, // segments to be deleted
            8, // max segments added to single server
            1500L, // average segment size
            12000L, // total data to be moved
            new RebalanceSummaryResult.RebalanceChangeInfo(3, 3), // replication factor unchanged
            new RebalanceSummaryResult.RebalanceChangeInfo(8, 8), // segments in single replica
            new RebalanceSummaryResult.RebalanceChangeInfo(24, 24), // segments across all replicas
            null // no consuming segments for offline table
        ),
        List.of(new RebalanceSummaryResult.TagInfo(offlineTag, 8, 24, 3))
    );
    tableResults.put("tableA_OFFLINE", tableAResult);

    // Table B (realtime): servers 2,3,4 -> 3,4,5 (remove server 2, add server 5)
    // Server 2 is being removed from Table B but still exists in Table A
    // Server 3 and 4 overlap with Table A
    RebalanceResult tableBResult = createMockRebalanceResult("tableB_job",
        RebalanceResult.Status.DONE, "Table B rebalanced successfully",
        createMockServerInfo(
            1, // servers getting new segments (server 5)
            new RebalanceSummaryResult.RebalanceChangeInfo(3, 3), // 3 -> 3 servers (same count)
            Set.of("server5"), // servers added
            Set.of("server2"), // servers removed (but still exists in Table A)
            Set.of("server3", "server4"), // servers unchanged
            Set.of("server5"), // servers getting new segments
            Map.of(
                "server2", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.REMOVED, 0, 6, 0, 6, 0, List.of(offlineTag)),
                "server3", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 9, 6, 3, 0, 6, List.of(offlineTag, realtimeTag)),
                "server4", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 9, 6, 3, 0, 6, List.of(offlineTag, realtimeTag)),
                "server5", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.ADDED, 9, 0, 9, 0, 0, List.of(offlineTag, realtimeTag))
            )
        ),
        createMockSegmentInfo(
            15, // segments to be moved (6 from server2 + 6 to server5)
            6, // segments to be deleted
            9, // max segments added to single server
            1800L, // average segment size
            27000L, // total data to be moved
            new RebalanceSummaryResult.RebalanceChangeInfo(3, 3), // replication factor unchanged
            new RebalanceSummaryResult.RebalanceChangeInfo(6, 9), // segments in single replica
            new RebalanceSummaryResult.RebalanceChangeInfo(18, 27), // segments across all replicas
            null // no consuming segments for realtime table
        ),
        List.of(new RebalanceSummaryResult.TagInfo(realtimeTag, 15, 12, 3))
    );
    tableResults.put("tableB_REALTIME", tableBResult);

    // Table C (offline): servers 4,5,6 -> 4,5,6 (no change)
    // Server 4 and 5 overlap with previous tables
    RebalanceResult tableCResult = createMockRebalanceResult("tableC_job",
        RebalanceResult.Status.NO_OP, "Table C - no rebalancing needed",
        createMockServerInfo(
            0, // servers getting new segments
            new RebalanceSummaryResult.RebalanceChangeInfo(3, 3), // 3 -> 3 servers
            null, // servers added
            null, // servers removed
            Set.of("server4", "server5", "server6"), // servers unchanged
            null, // servers getting new segments
            Map.of(
                "server4", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 4, 4, 0, 0, 4, List.of(offlineTag, realtimeTag)),
                "server5", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 4, 4, 0, 0, 4, List.of(offlineTag, realtimeTag)),
                "server6", new RebalanceSummaryResult.ServerSegmentChangeInfo(
                    RebalanceSummaryResult.ServerStatus.UNCHANGED, 4, 4, 0, 0, 4, List.of(offlineTag))
            )
        ),
        createMockSegmentInfo(
            0, // segments to be moved
            0, // segments to be deleted
            0, // max segments added to single server
            0L, // average segment size
            0L, // total data to be moved
            new RebalanceSummaryResult.RebalanceChangeInfo(2, 2), // replication factor unchanged
            new RebalanceSummaryResult.RebalanceChangeInfo(6, 6), // segments in single replica
            new RebalanceSummaryResult.RebalanceChangeInfo(12, 12), // segments across all replicas
            null // no consuming segments for offline table
        ),
        List.of(new RebalanceSummaryResult.TagInfo(offlineTag, 0, 12, 3))
    );
    tableResults.put("tableC_OFFLINE", tableCResult);

    // Create TenantRebalanceResult and verify aggregations with overlapping servers
    TenantRebalanceResult tenantResult = new TenantRebalanceResult(jobId, tableResults, true);

    // Verify basic properties
    assertEquals(tenantResult.getJobId(), jobId);
    assertEquals(tenantResult.getTotalTables(), 3);

    // Verify status summary aggregation
    Map<RebalanceResult.Status, Integer> statusSummary = tenantResult.getStatusSummary();
    assertEquals(statusSummary.get(RebalanceResult.Status.DONE), Integer.valueOf(2));
    assertEquals(statusSummary.get(RebalanceResult.Status.NO_OP), Integer.valueOf(1));
    assertNull(statusSummary.get(RebalanceResult.Status.FAILED));

    // Verify aggregated rebalance summary
    RebalanceSummaryResult aggregatedSummary = tenantResult.getAggregatedRebalanceSummary();
    assertNotNull(aggregatedSummary);

    // Verify aggregated server info with overlapping servers
    RebalanceSummaryResult.ServerInfo serverInfo = aggregatedSummary.getServerInfo();
    assertNotNull(serverInfo);

    // Servers getting new segments: server4 (from Table A), server3, server4, server5 (from Table B)
    assertEquals(serverInfo.getNumServersGettingNewSegments(), 3);

    // Unique servers across all tables: 1,2,3,4,5,6
    // Before rebalance: server1,2,3 (Table A) + server2,3,4 (Table B) + server4,5,6 (Table C) = 6 unique servers
    assertEquals(serverInfo.getNumServers().getValueBeforeRebalance(), 6);
    // After rebalance: server2,3,4 (Table A) + server3,4,5 (Table B) + server4,5,6 (Table C) = 5 unique servers
    assertEquals(serverInfo.getNumServers().getExpectedValueAfterRebalance(), 5);

    // Verify server sets - with overlapping servers, aggregation should be more complex
    assertTrue(serverInfo.getServersAdded().isEmpty());
    // Server1 is removed from Table A, Server2 is removed from Table B but still exists in TableA so it shouldn't be
    // in serversRemoved
    assertEquals(serverInfo.getServersRemoved(), Set.of("server1"));
    // Servers that are unchanged in at least one table
    assertEquals(serverInfo.getServersUnchanged(), Set.of("server2", "server3", "server4", "server5", "server6"));
    assertEquals(serverInfo.getServersGettingNewSegments(), Set.of("server3", "server4", "server5"));

    // Verify server segment change info aggregation with overlapping servers
    Map<String, RebalanceSummaryResult.ServerSegmentChangeInfo> serverSegmentChangeInfo =
        serverInfo.getServerSegmentChangeInfo();
    assertNotNull(serverSegmentChangeInfo);

    // Verify overlapping servers have combined information
    // Server 2: exists in Table A (unchanged) and Table B (removed)
    RebalanceSummaryResult.ServerSegmentChangeInfo server1Info = serverSegmentChangeInfo.get("server1");
    assertEquals(server1Info.getTotalSegmentsBeforeRebalance(), 8); // 8 (Table A) + 6 (Table B)
    assertEquals(server1Info.getTotalSegmentsAfterRebalance(), 0); // 8 (Table A) + 0 (Table B)
    assertEquals(server1Info.getSegmentsAdded(), 0);
    assertEquals(server1Info.getSegmentsDeleted(), 8); // removed from Table B
    assertEquals(server1Info.getSegmentsUnchanged(), 0); // unchanged in Table A
    // Server status should reflect the most significant change (removed from one table)
    assertEquals(server1Info.getServerStatus(), RebalanceSummaryResult.ServerStatus.REMOVED);

    // Server 2: exists in Table A (unchanged) and Table B (removed)
    RebalanceSummaryResult.ServerSegmentChangeInfo server2Info = serverSegmentChangeInfo.get("server2");
    assertEquals(server2Info.getTotalSegmentsBeforeRebalance(), 14); // 8 (Table A) + 6 (Table B)
    assertEquals(server2Info.getTotalSegmentsAfterRebalance(), 8); // 8 (Table A) + 0 (Table B)
    assertEquals(server2Info.getSegmentsAdded(), 0);
    assertEquals(server2Info.getSegmentsDeleted(), 6); // removed from Table B
    assertEquals(server2Info.getSegmentsUnchanged(), 8); // unchanged in Table A
    // Server status should reflect the most significant change (removed from one table)
    assertEquals(server2Info.getServerStatus(), RebalanceSummaryResult.ServerStatus.UNCHANGED);

    // Server 3: exists in Table A (unchanged) and Table B (unchanged)
    RebalanceSummaryResult.ServerSegmentChangeInfo server3Info = serverSegmentChangeInfo.get("server3");
    assertEquals(server3Info.getTotalSegmentsBeforeRebalance(), 14); // 8 (Table A) + 6 (Table B)
    assertEquals(server3Info.getTotalSegmentsAfterRebalance(), 17); // 8 (Table A) + 9 (Table B)
    assertEquals(server3Info.getSegmentsAdded(), 3); // 0 (Table A) + 3 (Table B)
    assertEquals(server3Info.getSegmentsDeleted(), 0);
    assertEquals(server3Info.getSegmentsUnchanged(), 14); // 8 (Table A) + 6 (Table B)
    assertEquals(server3Info.getServerStatus(), RebalanceSummaryResult.ServerStatus.UNCHANGED);

    // Server 4: exists in Table A (added), Table B (unchanged), Table C (unchanged)
    RebalanceSummaryResult.ServerSegmentChangeInfo server4Info = serverSegmentChangeInfo.get("server4");
    assertEquals(server4Info.getTotalSegmentsBeforeRebalance(), 10); // 0 (Table A) + 6 (Table B) + 4 (Table C)
    assertEquals(server4Info.getTotalSegmentsAfterRebalance(), 21); // 8 (Table A) + 9 (Table B) + 4 (Table C)
    assertEquals(server4Info.getSegmentsAdded(), 11); // 8 (Table A) + 3 (Table B) + 0 (Table C)
    assertEquals(server4Info.getSegmentsDeleted(), 0);
    assertEquals(server4Info.getSegmentsUnchanged(), 10); // 0 (Table A) + 6 (Table B) + 4 (Table C)
    assertEquals(server4Info.getServerStatus(), RebalanceSummaryResult.ServerStatus.UNCHANGED);

    // Server 5: exists in Table B (added) and Table C (unchanged)
    RebalanceSummaryResult.ServerSegmentChangeInfo server5Info = serverSegmentChangeInfo.get("server5");
    assertEquals(server5Info.getTotalSegmentsBeforeRebalance(), 4); // 0 (Table B) + 4 (Table C)
    assertEquals(server5Info.getTotalSegmentsAfterRebalance(), 13); // 9 (Table B) + 4 (Table C)
    assertEquals(server5Info.getSegmentsAdded(), 9); // 9 (Table B) + 0 (Table C)
    assertEquals(server5Info.getSegmentsDeleted(), 0);
    assertEquals(server5Info.getSegmentsUnchanged(), 4); // 0 (Table B) + 4 (Table C)
    assertEquals(server5Info.getServerStatus(), RebalanceSummaryResult.ServerStatus.UNCHANGED);

    // Verify aggregated segment info
    RebalanceSummaryResult.SegmentInfo segmentInfo = aggregatedSummary.getSegmentInfo();
    assertNotNull(segmentInfo);
    assertEquals(segmentInfo.getTotalSegmentsToBeMoved(), 23); // 8 + 15 + 0
    assertEquals(segmentInfo.getTotalSegmentsToBeDeleted(), 14); // 8 + 6 + 0
    assertEquals(segmentInfo.getMaxSegmentsAddedToASingleServer(),
        11); // server4 has 11 segments added (8 from Table A, 3 from Table B)
    // Weighted average: (6*1500 + 12*1800 + 0*0) / 18 = 30600 / 18 = 1700
    assertEquals(segmentInfo.getEstimatedAverageSegmentSizeInBytes(), (12000L + 27000L) / 23);
    assertEquals(segmentInfo.getTotalEstimatedDataToBeMovedInBytes(), 12000L + 27000L); // 9000 + 21600 + 0

    // Verify aggregated tag info
    List<RebalanceSummaryResult.TagInfo> tagsInfo = aggregatedSummary.getTagsInfo();
    assertNotNull(tagsInfo);
    assertEquals(tagsInfo.size(), 2);

    // Find and verify offline tag info (Table A and C)
    RebalanceSummaryResult.TagInfo offlineTagInfo = tagsInfo.stream()
        .filter(tag -> tag.getTagName().equals(offlineTag))
        .findFirst()
        .orElse(null);
    assertNotNull(offlineTagInfo);
    assertEquals(offlineTagInfo.getNumSegmentsToDownload(), 8); // 8 (Table A) + 0 (Table C)
    assertEquals(offlineTagInfo.getNumSegmentsUnchanged(), 36); // server 1-6
    assertEquals(offlineTagInfo.getNumServerParticipants(), 5); // servers 2,3,4,5,6 (unique count from both tables)

    // Find and verify realtime tag info (Table B only)
    RebalanceSummaryResult.TagInfo realtimeTagInfo = tagsInfo.stream()
        .filter(tag -> tag.getTagName().equals(realtimeTag))
        .findFirst()
        .orElse(null);
    assertNotNull(realtimeTagInfo);
    assertEquals(realtimeTagInfo.getNumSegmentsToDownload(), 15); // only server 3,4,5 from Table B
    assertEquals(realtimeTagInfo.getNumSegmentsUnchanged(), 12);
    assertEquals(realtimeTagInfo.getNumServerParticipants(), 3); // servers 3,4,5 (server2 is removed)

    // Verify original table results are preserved
    assertEquals(tenantResult.getRebalanceTableResults().size(), 3);
    assertEquals(tenantResult.getRebalanceTableResults().get("tableA_OFFLINE").getStatus(),
        RebalanceResult.Status.DONE);
    assertEquals(tenantResult.getRebalanceTableResults().get("tableB_REALTIME").getStatus(),
        RebalanceResult.Status.DONE);
    assertEquals(tenantResult.getRebalanceTableResults().get("tableC_OFFLINE").getStatus(),
        RebalanceResult.Status.NO_OP);
  }

  private RebalanceResult createMockRebalanceResult(String jobId, RebalanceResult.Status status, String description,
      RebalanceSummaryResult.ServerInfo serverInfo, RebalanceSummaryResult.SegmentInfo segmentInfo,
      List<RebalanceSummaryResult.TagInfo> tagsInfo) {
    RebalanceSummaryResult summaryResult = new RebalanceSummaryResult(serverInfo, segmentInfo, tagsInfo);
    return new RebalanceResult(jobId, status, description, null, null, null, null, summaryResult);
  }

  private RebalanceSummaryResult.ServerInfo createMockServerInfo(int numServersGettingNewSegments,
      RebalanceSummaryResult.RebalanceChangeInfo numServers, Set<String> serversAdded, Set<String> serversRemoved,
      Set<String> serversUnchanged, Set<String> serversGettingNewSegments,
      Map<String, RebalanceSummaryResult.ServerSegmentChangeInfo> serverSegmentChangeInfo) {
    return new RebalanceSummaryResult.ServerInfo(numServersGettingNewSegments, numServers, serversAdded,
        serversRemoved, serversUnchanged, serversGettingNewSegments, serverSegmentChangeInfo);
  }

  private RebalanceSummaryResult.SegmentInfo createMockSegmentInfo(int totalSegmentsToBeMoved,
      int totalSegmentsToBeDeleted, int maxSegmentsAddedToASingleServer, long estimatedAverageSegmentSizeInBytes,
      long totalEstimatedDataToBeMovedInBytes, RebalanceSummaryResult.RebalanceChangeInfo replicationFactor,
      RebalanceSummaryResult.RebalanceChangeInfo numSegmentsInSingleReplica,
      RebalanceSummaryResult.RebalanceChangeInfo numSegmentsAcrossAllReplicas,
      RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary consumingSegmentToBeMovedSummary) {
    return new RebalanceSummaryResult.SegmentInfo(totalSegmentsToBeMoved, totalSegmentsToBeDeleted,
        maxSegmentsAddedToASingleServer, estimatedAverageSegmentSizeInBytes, totalEstimatedDataToBeMovedInBytes,
        replicationFactor, numSegmentsInSingleReplica, numSegmentsAcrossAllReplicas, consumingSegmentToBeMovedSummary);
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
