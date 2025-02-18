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
package org.apache.pinot.controller.helix.core.rebalance;

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.testng.Assert.*;


@Test(groups = "stateless")
public class TableRebalancerClusterStatelessTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final int NUM_REPLICAS = 3;
  private static final String SEGMENT_NAME_PREFIX = "segment_";

  private static final String TIERED_TABLE_NAME = "testTable";
  private static final String OFFLINE_TIERED_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TIERED_TABLE_NAME);
  private static final String NO_TIER_NAME = "noTier";
  private static final String TIER_A_NAME = "tierA";
  private static final String TIER_B_NAME = "tierB";
  private static final String TIER_FIXED_NAME = "tierFixed";

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
  }

  /**
   * Dropping instance from cluster requires waiting for live instance gone and removing instance related ZNodes, which
   * are not the purpose of the test, so combine different rebalance scenarios into one test:
   * 1. NO_OP rebalance
   * 2. Add servers and rebalance
   * 3. Migrate to replica-group based segment assignment and rebalance
   * 4. Migrate back to non-replica-group based segment assignment and rebalance
   * 5. Remove (disable) servers and rebalance
   */
  @Test
  public void testRebalance()
      throws Exception {
    int numServers = 3;
    for (int i = 0; i < numServers; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + i, true);
    }

    ExecutorService executorService = Executors.newFixedThreadPool(10);
    DefaultRebalancePreChecker preChecker = new DefaultRebalancePreChecker();
    preChecker.init(_helixResourceManager, executorService);
    TableRebalancer tableRebalancer = new TableRebalancer(_helixManager, null, null, preChecker,
        _helixResourceManager.getTableSizeReader());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS).build();

    // Rebalance should fail without creating the table
    RebalanceResult rebalanceResult = tableRebalancer.rebalance(tableConfig, new RebalanceConfig(), null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.FAILED);
    assertNull(rebalanceResult.getRebalanceSummaryResult());

    // Rebalance with dry-run summary should fail without creating the table
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.FAILED);
    assertNull(rebalanceResult.getRebalanceSummaryResult());

    // Create the table
    addDummySchema(RAW_TABLE_NAME);
    _helixResourceManager.addTable(tableConfig);

    // Add the segments
    int numSegments = 10;
    for (int i = 0; i < numSegments; i++) {
      _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(RAW_TABLE_NAME, SEGMENT_NAME_PREFIX + i), null);
    }
    Map<String, Map<String, String>> oldSegmentAssignment =
        _helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME).getRecord().getMapFields();

    // Rebalance with dry-run summary should return NO_OP status
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    RebalanceSummaryResult rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceSummaryResult);
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 0);
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 3);

    // Dry-run mode should not change the IdealState
    assertEquals(_helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME).getRecord().getMapFields(),
        oldSegmentAssignment);

    // Rebalance should return NO_OP status
    rebalanceResult = tableRebalancer.rebalance(tableConfig, new RebalanceConfig(), null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);

    // All servers should be assigned to the table
    Map<InstancePartitionsType, InstancePartitions> instanceAssignment = rebalanceResult.getInstanceAssignment();
    assertEquals(instanceAssignment.size(), 1);
    InstancePartitions instancePartitions = instanceAssignment.get(InstancePartitionsType.OFFLINE);
    assertEquals(instancePartitions.getNumReplicaGroups(), 1);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    // Math.abs("testTable_OFFLINE".hashCode()) % 3 = 2
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 2, SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 1));

    // Segment assignment should not change
    assertEquals(rebalanceResult.getSegmentAssignment(), oldSegmentAssignment);

    // Add 3 more servers
    int numServersToAdd = 3;
    for (int i = 0; i < numServersToAdd; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + (numServers + i), true);
    }

    // Rebalance in dry-run summary mode with added servers
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);
    rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceSummaryResult);
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 14);
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 6);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServersGettingNewSegments(), 3);

    Map<String, RebalanceSummaryResult.ServerSegmentChangeInfo> serverSegmentChangeInfoMap =
        rebalanceSummaryResult.getServerInfo().getServerSegmentChangeInfo();
    assertNotNull(serverSegmentChangeInfoMap);
    for (int i = 0; i < numServers; i++) {
      // Original servers should be losing some segments
      String newServer = SERVER_INSTANCE_ID_PREFIX + i;
      RebalanceSummaryResult.ServerSegmentChangeInfo serverSegmentChange = serverSegmentChangeInfoMap.get(newServer);
      assertTrue(serverSegmentChange.getSegmentsDeleted() > 0);
      assertTrue(serverSegmentChange.getSegmentsUnchanged() > 0);
      assertTrue(serverSegmentChange.getTotalSegmentsBeforeRebalance() > 0);
      assertTrue(serverSegmentChange.getTotalSegmentsAfterRebalance() > 0);
      assertEquals(serverSegmentChange.getSegmentsAdded(), 0);
    }
    for (int i = 0; i < numServersToAdd; i++) {
      // New servers should only get new segments
      String newServer = SERVER_INSTANCE_ID_PREFIX + (numServers + i);
      RebalanceSummaryResult.ServerSegmentChangeInfo serverSegmentChange = serverSegmentChangeInfoMap.get(newServer);
      assertTrue(serverSegmentChange.getSegmentsAdded() > 0);
      assertEquals(serverSegmentChange.getTotalSegmentsBeforeRebalance(), 0);
      assertEquals(serverSegmentChange.getTotalSegmentsAfterRebalance(), serverSegmentChange.getSegmentsAdded());
      assertEquals(serverSegmentChange.getSegmentsDeleted(), 0);
      assertEquals(serverSegmentChange.getSegmentsUnchanged(), 0);
    }

    // Dry-run mode should not change the IdealState
    assertEquals(_helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME).getRecord().getMapFields(),
        oldSegmentAssignment);

    // Rebalance in dry-run mode
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setPreChecks(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);
    Map<String, String> preCheckResult = rebalanceResult.getPreChecksResult();
    assertNotNull(preCheckResult);
    assertEquals(preCheckResult.size(), 2);
    assertTrue(preCheckResult.containsKey(DefaultRebalancePreChecker.NEEDS_RELOAD_STATUS));
    assertTrue(preCheckResult.containsKey(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT));
    // Sending request to servers should fail for all, so needsPreprocess should be set to "error" to indicate that a
    // manual check is needed
    assertEquals(preCheckResult.get(DefaultRebalancePreChecker.NEEDS_RELOAD_STATUS), "error");
    assertEquals(preCheckResult.get(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT), "false");

    // All servers should be assigned to the table
    instanceAssignment = rebalanceResult.getInstanceAssignment();
    assertEquals(instanceAssignment.size(), 1);
    instancePartitions = instanceAssignment.get(InstancePartitionsType.OFFLINE);
    assertEquals(instancePartitions.getNumReplicaGroups(), 1);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    // Math.abs("testTable_OFFLINE".hashCode()) % 6 = 2
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 2, SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 1));

    // Segments should be moved to the new added servers
    Map<String, Map<String, String>> newSegmentAssignment = rebalanceResult.getSegmentAssignment();
    Map<String, IntIntPair> instanceToNumSegmentsToMoveMap =
        SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(oldSegmentAssignment, newSegmentAssignment);
    assertEquals(instanceToNumSegmentsToMoveMap.size(), numServers + numServersToAdd);
    for (int i = 0; i < numServersToAdd; i++) {
      IntIntPair numSegmentsToMove = instanceToNumSegmentsToMoveMap.get(SERVER_INSTANCE_ID_PREFIX + (numServers + i));
      assertNotNull(numSegmentsToMove);
      assertTrue(numSegmentsToMove.leftInt() > 0);
      assertEquals(numSegmentsToMove.rightInt(), 0);
    }
    for (int i = 0; i < numServers; i++) {
      IntIntPair numSegmentsToMove = instanceToNumSegmentsToMoveMap.get(SERVER_INSTANCE_ID_PREFIX + i);
      assertNotNull(numSegmentsToMove);
      assertEquals(numSegmentsToMove.leftInt(), 0);
      assertTrue(numSegmentsToMove.rightInt() > 0);
    }

    // Dry-run mode should not change the IdealState
    assertEquals(_helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME).getRecord().getMapFields(),
        oldSegmentAssignment);

    // Rebalance dry-run summary with 3 min available replicas should not be impacted since actual rebalance does not
    // occur
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceConfig.setPreChecks(true);
    rebalanceConfig.setMinAvailableReplicas(3);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);
    rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceSummaryResult);
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 6);
    assertNotNull(rebalanceResult.getPreChecksResult());

    // Rebalance with 3 min available replicas should fail as the table only have 3 replicas
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setMinAvailableReplicas(3);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.FAILED);

    // IdealState should not change for FAILED rebalance
    assertEquals(_helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME).getRecord().getMapFields(),
        oldSegmentAssignment);

    // Rebalance with 2 min available replicas should succeed
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setMinAvailableReplicas(2);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);

    // Result should be the same as the result in dry-run mode
    instanceAssignment = rebalanceResult.getInstanceAssignment();
    assertEquals(instanceAssignment.size(), 1);
    assertEquals(instanceAssignment.get(InstancePartitionsType.OFFLINE).getPartitionToInstancesMap(),
        instancePartitions.getPartitionToInstancesMap());
    assertEquals(rebalanceResult.getSegmentAssignment(), newSegmentAssignment);

    // Update the table config to use replica-group based assignment
    InstanceTagPoolConfig tagPoolConfig =
        new InstanceTagPoolConfig(TagNameUtils.getOfflineTagForTenant(null), false, 0, null);
    InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, NUM_REPLICAS, 0, 0, 0, false, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig, null, false)));
    _helixResourceManager.updateTableConfig(tableConfig);

    // Try dry-run summary mode
    // No need to reassign instances because instances should be automatically assigned when updating the table config
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);
    rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceSummaryResult);
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 11);
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 6);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 6);

    serverSegmentChangeInfoMap = rebalanceSummaryResult.getServerInfo().getServerSegmentChangeInfo();
    assertNotNull(serverSegmentChangeInfoMap);
    for (int i = 0; i < numServers + numServersToAdd; i++) {
      String newServer = SERVER_INSTANCE_ID_PREFIX + i;
      RebalanceSummaryResult.ServerSegmentChangeInfo serverSegmentChange = serverSegmentChangeInfoMap.get(newServer);
      assertEquals(serverSegmentChange.getTotalSegmentsAfterRebalance(), 5);
      // Ensure not all segments moved
      assertTrue(serverSegmentChange.getSegmentsUnchanged() > 0);
      // Ensure all segments has something assigned prior to rebalance
      assertTrue(serverSegmentChange.getTotalSegmentsBeforeRebalance() > 0);
    }

    // Dry-run mode should not change the IdealState
    assertEquals(_helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME).getRecord().getMapFields(),
        newSegmentAssignment);

    // Try actual rebalance
    // No need to reassign instances because instances should be automatically assigned when updating the table config
    rebalanceResult = tableRebalancer.rebalance(tableConfig, new RebalanceConfig(), null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);

    // There should be 3 replica-groups, each with 2 servers
    instanceAssignment = rebalanceResult.getInstanceAssignment();
    assertEquals(instanceAssignment.size(), 1);
    instancePartitions = instanceAssignment.get(InstancePartitionsType.OFFLINE);
    assertEquals(instancePartitions.getNumReplicaGroups(), NUM_REPLICAS);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    // Math.abs("testTable_OFFLINE".hashCode()) % 6 = 2
    // [i2, i3, i4, i5, i0, i1]
    //  r0  r1  r2  r0  r1  r2
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 2, SERVER_INSTANCE_ID_PREFIX + 5));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 0));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 4, SERVER_INSTANCE_ID_PREFIX + 1));

    // The assignment are based on replica-group 0 and mirrored to all the replica-groups, so server of index 0, 1, 5
    // should have the same segments assigned, and server of index 2, 3, 4 should have the same segments assigned, each
    // with 5 segments
    newSegmentAssignment = rebalanceResult.getSegmentAssignment();
    int numSegmentsOnServer0 = 0;
    for (int i = 0; i < numSegments; i++) {
      String segmentName = SEGMENT_NAME_PREFIX + i;
      Map<String, String> instanceStateMap = newSegmentAssignment.get(segmentName);
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
      if (instanceStateMap.containsKey(SERVER_INSTANCE_ID_PREFIX + 0)) {
        numSegmentsOnServer0++;
        assertEquals(instanceStateMap.get(SERVER_INSTANCE_ID_PREFIX + 0), ONLINE);
        assertEquals(instanceStateMap.get(SERVER_INSTANCE_ID_PREFIX + 1), ONLINE);
        assertEquals(instanceStateMap.get(SERVER_INSTANCE_ID_PREFIX + 5), ONLINE);
      } else {
        assertEquals(instanceStateMap.get(SERVER_INSTANCE_ID_PREFIX + 2), ONLINE);
        assertEquals(instanceStateMap.get(SERVER_INSTANCE_ID_PREFIX + 3), ONLINE);
        assertEquals(instanceStateMap.get(SERVER_INSTANCE_ID_PREFIX + 4), ONLINE);
      }
    }
    assertEquals(numSegmentsOnServer0, numSegments / 2);

    // Update the table config to use non-replica-group based assignment
    tableConfig.setInstanceAssignmentConfigMap(null);
    _helixResourceManager.updateTableConfig(tableConfig);

    // Try dry-run summary mode without reassignment to ensure that existing instance partitions are used
    // no movement should occur
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceSummaryResult);
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 0);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 6);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 6);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServersGettingNewSegments(), 0);

    // Without instances reassignment, the rebalance should return status NO_OP, and the existing instance partitions
    // should be used
    rebalanceResult = tableRebalancer.rebalance(tableConfig, new RebalanceConfig(), null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    assertEquals(rebalanceResult.getInstanceAssignment(), instanceAssignment);
    assertEquals(rebalanceResult.getSegmentAssignment(), newSegmentAssignment);

    // Try dry-run summary mode with reassignment
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceConfig.setReassignInstances(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);
    rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceSummaryResult);
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    // No move expected since already balanced
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 0);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 6);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 6);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServersGettingNewSegments(), 0);

    // With instances reassignment, the rebalance should return status DONE, the existing instance partitions should be
    // removed, and the default instance partitions should be used
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setReassignInstances(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);
    assertNull(InstancePartitionsUtils.fetchInstancePartitions(_propertyStore,
        InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME)));

    // All servers should be assigned to the table
    instanceAssignment = rebalanceResult.getInstanceAssignment();
    assertEquals(instanceAssignment.size(), 1);
    instancePartitions = instanceAssignment.get(InstancePartitionsType.OFFLINE);
    assertEquals(instancePartitions.getNumReplicaGroups(), 1);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    // Math.abs("testTable_OFFLINE".hashCode()) % 6 = 2
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 2, SERVER_INSTANCE_ID_PREFIX + 3, SERVER_INSTANCE_ID_PREFIX + 4,
            SERVER_INSTANCE_ID_PREFIX + 5, SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 1));

    // Segment assignment should not change as it is already balanced
    assertEquals(rebalanceResult.getSegmentAssignment(), newSegmentAssignment);

    // Remove the tag from the added servers
    for (int i = 0; i < numServersToAdd; i++) {
      _helixAdmin.removeInstanceTag(getHelixClusterName(), SERVER_INSTANCE_ID_PREFIX + (numServers + i),
          TagNameUtils.getOfflineTagForTenant(null));
    }

    // Try dry-run summary mode
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceConfig.setDowntime(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);
    rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceSummaryResult);
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 15);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 6);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServersGettingNewSegments(), 3);

    // Rebalance with downtime should succeed
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDowntime(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);

    // All servers with tag should be assigned to the table
    instanceAssignment = rebalanceResult.getInstanceAssignment();
    assertEquals(instanceAssignment.size(), 1);
    instancePartitions = instanceAssignment.get(InstancePartitionsType.OFFLINE);
    assertEquals(instancePartitions.getNumReplicaGroups(), 1);
    assertEquals(instancePartitions.getNumPartitions(), 1);
    // Math.abs("testTable_OFFLINE".hashCode()) % 3 = 2
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList(SERVER_INSTANCE_ID_PREFIX + 2, SERVER_INSTANCE_ID_PREFIX + 0, SERVER_INSTANCE_ID_PREFIX + 1));

    // New segment assignment should not contain servers without tag
    newSegmentAssignment = rebalanceResult.getSegmentAssignment();
    for (int i = 0; i < numSegments; i++) {
      String segmentName = SEGMENT_NAME_PREFIX + i;
      Map<String, String> instanceStateMap = newSegmentAssignment.get(segmentName);
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
      for (int j = 0; j < numServersToAdd; j++) {
        assertFalse(instanceStateMap.containsKey(SERVER_INSTANCE_ID_PREFIX + (numServers + j)));
      }
    }

    // Try summary mode without dry-run set
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setSummary(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.FAILED);
    assertNull(rebalanceResult.getRebalanceSummaryResult());
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());

    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME);
    executorService.shutdown();
  }

  /**
   * Tests rebalance with tier configs
   * Add 10 segments, with segment metadata end time 3 days apart starting from now to 30 days ago
   * 1. run rebalance - should see no change
   * 2. add nodes for tiers and run rebalance - should see no change
   * 3. add tier config and run rebalance - should see changed assignment
   */
  @Test
  public void testRebalanceWithTiers()
      throws Exception {
    int numServers = 3;
    for (int i = 0; i < numServers; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(NO_TIER_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + i, false);
    }
    _helixResourceManager.createServerTenant(new Tenant(TenantRole.SERVER, NO_TIER_NAME, numServers, numServers, 0));

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TIERED_TABLE_NAME).setNumReplicas(NUM_REPLICAS)
            .setServerTenant(NO_TIER_NAME).build();
    // Create the table
    addDummySchema(TIERED_TABLE_NAME);
    _helixResourceManager.addTable(tableConfig);

    // Add the segments
    int numSegments = 10;
    long nowInDays = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis());
    // keep decreasing end time from today in steps of 3. 3 segments don't move. 3 segment on tierA. 4 segments on tierB
    for (int i = 0; i < numSegments; i++) {
      _helixResourceManager.addNewSegment(OFFLINE_TIERED_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadataWithEndTimeInfo(TIERED_TABLE_NAME, SEGMENT_NAME_PREFIX + i,
              nowInDays), null);
      nowInDays -= 3;
    }
    Map<String, Map<String, String>> oldSegmentAssignment =
        _helixResourceManager.getTableIdealState(OFFLINE_TIERED_TABLE_NAME).getRecord().getMapFields();

    TableRebalancer tableRebalancer = new TableRebalancer(_helixManager);

    // Try dry-run summary mode
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    RebalanceResult rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    RebalanceSummaryResult rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceSummaryResult);
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 0);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServersGettingNewSegments(), 0);

    // Run actual table rebalance
    rebalanceResult = tableRebalancer.rebalance(tableConfig, new RebalanceConfig(), null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    // Segment assignment should not change
    assertEquals(rebalanceResult.getSegmentAssignment(), oldSegmentAssignment);

    // add 3 nodes tierA, 3 nodes tierB
    for (int i = 0; i < 3; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + i, false);
    }
    _helixResourceManager.createServerTenant(new Tenant(TenantRole.SERVER, TIER_A_NAME, 3, 3, 0));
    for (int i = 0; i < 3; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(TIER_B_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + i, false);
    }
    _helixResourceManager.createServerTenant(new Tenant(TenantRole.SERVER, TIER_B_NAME, 3, 3, 0));

    // Try dry-run summary mode, should be no-op
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceSummaryResult);
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 0);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServersGettingNewSegments(), 0);

    // rebalance is NOOP and no change in assignment caused by new instances
    rebalanceResult = tableRebalancer.rebalance(tableConfig, new RebalanceConfig(), null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    // Segment assignment should not change
    assertEquals(rebalanceResult.getSegmentAssignment(), oldSegmentAssignment);

    // add tier config
    ArrayList<String> fixedTierSegments =
        Lists.newArrayList(SEGMENT_NAME_PREFIX + 6, SEGMENT_NAME_PREFIX + 3, SEGMENT_NAME_PREFIX + 1);
    tableConfig.setTierConfigsList(Lists.newArrayList(
        new TierConfig(TIER_A_NAME, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "7d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, TIER_A_NAME + "_OFFLINE", null, null),
        new TierConfig(TIER_B_NAME, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "15d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, TIER_B_NAME + "_OFFLINE", null, null),
        new TierConfig(TIER_FIXED_NAME, TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null, fixedTierSegments,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, NO_TIER_NAME + "_OFFLINE", null, null)));
    _helixResourceManager.updateTableConfig(tableConfig);

    // Try dry-run summary mode, some moves should occur
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);
    rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceSummaryResult);
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 15);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 9);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServersGettingNewSegments(), 6);

    // rebalance should change assignment
    rebalanceResult = tableRebalancer.rebalance(tableConfig, new RebalanceConfig(), null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);

    // check that segments have moved to tiers
    Map<String, Map<String, String>> tierSegmentAssignment = rebalanceResult.getSegmentAssignment();
    for (Map.Entry<String, Map<String, String>> entry : tierSegmentAssignment.entrySet()) {
      String segment = entry.getKey();
      int segId = Integer.parseInt(segment.split("_")[1]);
      Map<String, String> instanceStateMap = entry.getValue();
      String expectedPrefix;
      if (fixedTierSegments.contains(segment)) {
        expectedPrefix = NO_TIER_NAME + "_" + SERVER_INSTANCE_ID_PREFIX;
      } else if (segId > 4) {
        expectedPrefix = TIER_B_NAME + "_" + SERVER_INSTANCE_ID_PREFIX;
      } else if (segId > 2) {
        expectedPrefix = TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX;
      } else {
        expectedPrefix = NO_TIER_NAME + "_" + SERVER_INSTANCE_ID_PREFIX;
      }
      for (String instance : instanceStateMap.keySet()) {
        assertTrue(instance.startsWith(expectedPrefix));
      }
    }
    _helixResourceManager.deleteOfflineTable(TIERED_TABLE_NAME);
  }

  @Test
  public void testRebalanceWithTiersAndInstanceAssignments()
      throws Exception {
    int numServers = 3;
    for (int i = 0; i < numServers; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(
          "replicaAssignment" + NO_TIER_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + i, false);
    }
    _helixResourceManager.createServerTenant(
        new Tenant(TenantRole.SERVER, "replicaAssignment" + NO_TIER_NAME, numServers, numServers, 0));

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TIERED_TABLE_NAME).setNumReplicas(NUM_REPLICAS)
            .setServerTenant("replicaAssignment" + NO_TIER_NAME).build();
    // Create the table
    addDummySchema(TIERED_TABLE_NAME);
    _helixResourceManager.addTable(tableConfig);

    // Add the segments
    int numSegments = 10;
    long nowInDays = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis());

    for (int i = 0; i < numSegments; i++) {
      _helixResourceManager.addNewSegment(OFFLINE_TIERED_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadataWithEndTimeInfo(TIERED_TABLE_NAME, SEGMENT_NAME_PREFIX + i,
              nowInDays), null);
    }
    Map<String, Map<String, String>> oldSegmentAssignment =
        _helixResourceManager.getTableIdealState(OFFLINE_TIERED_TABLE_NAME).getRecord().getMapFields();

    TableRebalancer tableRebalancer = new TableRebalancer(_helixManager);

    // Try dry-run summary mode
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    RebalanceResult rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    RebalanceSummaryResult rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceSummaryResult);
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 0);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServersGettingNewSegments(), 0);

    rebalanceResult = tableRebalancer.rebalance(tableConfig, new RebalanceConfig(), null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    // Segment assignment should not change
    assertEquals(rebalanceResult.getSegmentAssignment(), oldSegmentAssignment);

    // add 6 nodes tierA
    for (int i = 0; i < 6; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(
          "replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + i, false);
    }
    _helixResourceManager.createServerTenant(new Tenant(TenantRole.SERVER, "replicaAssignment" + TIER_A_NAME, 6, 6, 0));

    // Try dry-run summary mode
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceResult.getRebalanceSummaryResult());
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 0);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServersGettingNewSegments(), 0);

    // rebalance is NOOP and no change in assignment caused by new instances
    rebalanceResult = tableRebalancer.rebalance(tableConfig, new RebalanceConfig(), null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    // Segment assignment should not change
    assertEquals(rebalanceResult.getSegmentAssignment(), oldSegmentAssignment);

    // add tier config
    tableConfig.setTierConfigsList(Lists.newArrayList(
        new TierConfig(TIER_A_NAME, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "0d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "replicaAssignment" + TIER_A_NAME + "_OFFLINE", null, null)));
    _helixResourceManager.updateTableConfig(tableConfig);

    // Try dry-run summary mode
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);
    rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceResult.getRebalanceSummaryResult());
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 30);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 3);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 6);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServersGettingNewSegments(), 6);

    // rebalance should change assignment
    rebalanceResult = tableRebalancer.rebalance(tableConfig, new RebalanceConfig(), null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);

    // check that segments have moved to tier a
    Map<String, Map<String, String>> tierSegmentAssignment = rebalanceResult.getSegmentAssignment();
    for (Map.Entry<String, Map<String, String>> entry : tierSegmentAssignment.entrySet()) {
      Map<String, String> instanceStateMap = entry.getValue();
      for (String instance : instanceStateMap.keySet()) {
        assertTrue(instance.startsWith("replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX));
      }
    }

    // Test rebalance with tier instance assignment
    InstanceTagPoolConfig tagPoolConfig =
        new InstanceTagPoolConfig(TagNameUtils.getOfflineTagForTenant("replicaAssignment" + TIER_A_NAME), false, 0,
            null);
    InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, NUM_REPLICAS, 0, 0, 0, false, null);
    tableConfig.setInstanceAssignmentConfigMap(Collections.singletonMap(TIER_A_NAME,
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig, null, false)));
    _helixResourceManager.updateTableConfig(tableConfig);

    // Try dry-run summary mode
    rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);
    rebalanceConfig.setSummary(true);
    rebalanceResult = tableRebalancer.rebalance(tableConfig, rebalanceConfig, null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);
    rebalanceSummaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(rebalanceSummaryResult);
    assertNotNull(rebalanceSummaryResult.getServerInfo());
    assertNotNull(rebalanceSummaryResult.getSegmentInfo());
    assertNull(rebalanceResult.getInstanceAssignment());
    assertNull(rebalanceResult.getTierInstanceAssignment());
    assertNull(rebalanceResult.getSegmentAssignment());
    assertEquals(rebalanceSummaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 13);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), 6);
    assertEquals(rebalanceSummaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), 6);

    rebalanceResult = tableRebalancer.rebalance(tableConfig, new RebalanceConfig(), null);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.DONE);
    assertTrue(rebalanceResult.getTierInstanceAssignment().containsKey(TIER_A_NAME));

    InstancePartitions instancePartitions = rebalanceResult.getTierInstanceAssignment().get(TIER_A_NAME);

    // Math.abs("testTable_OFFLINE".hashCode()) % 6 = 2
    // [i2, i3, i4, i5, i0, i1]
    //  r0  r1  r2  r0  r1  r2
    assertEquals(instancePartitions.getInstances(0, 0),
        Arrays.asList("replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 2,
            "replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 5));
    assertEquals(instancePartitions.getInstances(0, 1),
        Arrays.asList("replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 3,
            "replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 0));
    assertEquals(instancePartitions.getInstances(0, 2),
        Arrays.asList("replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 4,
            "replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 1));

    // The assignment are based on replica-group 0 and mirrored to all the replica-groups, so server of index 0, 1, 5
    // should have the same segments assigned, and server of index 2, 3, 4 should have the same segments assigned, each
    // with 5 segments
    Map<String, Map<String, String>> newSegmentAssignment = rebalanceResult.getSegmentAssignment();
    int numSegmentsOnServer0 = 0;
    for (int i = 0; i < numSegments; i++) {
      String segmentName = SEGMENT_NAME_PREFIX + i;
      Map<String, String> instanceStateMap = newSegmentAssignment.get(segmentName);
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
      if (instanceStateMap.containsKey("replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 0)) {
        numSegmentsOnServer0++;
        assertEquals(instanceStateMap.get("replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 0),
            ONLINE);
        assertEquals(instanceStateMap.get("replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 1),
            ONLINE);
        assertEquals(instanceStateMap.get("replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 5),
            ONLINE);
      } else {
        assertEquals(instanceStateMap.get("replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 2),
            ONLINE);
        assertEquals(instanceStateMap.get("replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 3),
            ONLINE);
        assertEquals(instanceStateMap.get("replicaAssignment" + TIER_A_NAME + "_" + SERVER_INSTANCE_ID_PREFIX + 4),
            ONLINE);
      }
    }
    assertEquals(numSegmentsOnServer0, numSegments / 2);

    _helixResourceManager.deleteOfflineTable(TIERED_TABLE_NAME);
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
