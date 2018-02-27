/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core;

import com.google.common.collect.BiMap;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.HLCSegmentName;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerTest;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotHelixResourceManagerTest extends ControllerTest {
  private static final int BASE_SERVER_ADMIN_PORT = 10000;
  private static final int NUM_INSTANCES = 5;
  private static final String BROKER_TENANT_NAME = "brokerTenant";
  private static final String SERVER_TENANT_NAME = "serverTenant";
  private static final String TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);

  private final String _helixClusterName = getHelixClusterName();

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    ControllerConf config = getDefaultControllerConfiguration();
    config.setTenantIsolationEnabled(false);
    startController(config);

    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(_helixClusterName,
        ZkStarter.DEFAULT_ZK_STR, NUM_INSTANCES, false);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(_helixClusterName, ZkStarter.DEFAULT_ZK_STR,
        NUM_INSTANCES, false, BASE_SERVER_ADMIN_PORT);

    // Create server tenant on all Servers
    Tenant serverTenant = new Tenant.TenantBuilder(SERVER_TENANT_NAME).setRole(TenantRole.SERVER)
        .setOfflineInstances(NUM_INSTANCES)
        .build();
    _helixResourceManager.createServerTenant(serverTenant);
  }

  @Test
  public void testGetInstanceEndpoints() {
    Set<String> servers = _helixResourceManager.getAllInstancesForServerTenant(SERVER_TENANT_NAME);
    BiMap<String, String> endpoints = _helixResourceManager.getDataInstanceAdminEndpoints(servers);
    for (int i = 0; i < NUM_INSTANCES; i++) {
      Assert.assertTrue(endpoints.inverse().containsKey("localhost:" + String.valueOf(BASE_SERVER_ADMIN_PORT + i)));
    }
  }

  @Test
  public void testRebuildBrokerResourceFromHelixTags() throws Exception {
    // Create broker tenant on 3 Brokers
    Tenant brokerTenant =
        new Tenant.TenantBuilder(BROKER_TENANT_NAME).setRole(TenantRole.BROKER).setTotalInstances(3).build();
    _helixResourceManager.createBrokerTenant(brokerTenant);

    // Create the table
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNumReplicas(3)
        .setBrokerTenant(BROKER_TENANT_NAME)
        .setServerTenant(SERVER_TENANT_NAME)
        .build();
    _helixResourceManager.addTable(tableConfig);

    // Check that the BrokerResource ideal state has 3 Brokers assigned to the table
    IdealState idealState = _helixResourceManager.getHelixAdmin()
        .getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), 3);

    // Untag all Brokers assigned to broker tenant
    for (String brokerInstance : _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      _helixAdmin.removeInstanceTag(_helixClusterName, brokerInstance,
          ControllerTenantNameBuilder.getBrokerTenantNameForTenant(BROKER_TENANT_NAME));
      _helixAdmin.addInstanceTag(_helixClusterName, brokerInstance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }

    // Rebuilding the broker tenant should update the ideal state size
    _helixResourceManager.rebuildBrokerResourceFromHelixTags(OFFLINE_TABLE_NAME);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), 0);

    // Create broker tenant on 5 Brokers
    brokerTenant.setNumberOfInstances(5);
    _helixResourceManager.createBrokerTenant(brokerTenant);

    // Rebuilding the broker tenant should update the ideal state size
    _helixResourceManager.rebuildBrokerResourceFromHelixTags(OFFLINE_TABLE_NAME);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), 5);

    // Untag all Brokers for other tests
    for (String brokerInstance : _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      _helixAdmin.removeInstanceTag(_helixClusterName, brokerInstance,
          ControllerTenantNameBuilder.getBrokerTenantNameForTenant(BROKER_TENANT_NAME));
      _helixAdmin.addInstanceTag(_helixClusterName, brokerInstance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }

    // Delete the table
    _helixResourceManager.deleteOfflineTable(TABLE_NAME);
  }

  @Test
  public void testRetrieveMetadata() throws Exception {
    String segmentName = "testSegment";

    // Test retrieving OFFLINE segment ZK metadata
    {
      OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
      offlineSegmentZKMetadata.setTableName(OFFLINE_TABLE_NAME);
      offlineSegmentZKMetadata.setSegmentName(segmentName);
      ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata);
      List<OfflineSegmentZKMetadata> retrievedMetadataList =
          _helixResourceManager.getOfflineSegmentMetadata(OFFLINE_TABLE_NAME);
      Assert.assertEquals(retrievedMetadataList.size(), 1);
      OfflineSegmentZKMetadata retrievedMetadata = retrievedMetadataList.get(0);
      Assert.assertEquals(retrievedMetadata.getTableName(), OFFLINE_TABLE_NAME);
      Assert.assertEquals(retrievedMetadata.getSegmentName(), segmentName);
    }

    // Test retrieving REALTIME segment ZK metadata
    {
      RealtimeSegmentZKMetadata realtimeMetadata = new RealtimeSegmentZKMetadata();
      realtimeMetadata.setTableName(REALTIME_TABLE_NAME);
      realtimeMetadata.setSegmentName(segmentName);
      realtimeMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      ZKMetadataProvider.setRealtimeSegmentZKMetadata(_propertyStore, realtimeMetadata);
      List<RealtimeSegmentZKMetadata> retrievedMetadataList =
          _helixResourceManager.getRealtimeSegmentMetadata(REALTIME_TABLE_NAME);
      Assert.assertEquals(retrievedMetadataList.size(), 1);
      RealtimeSegmentZKMetadata retrievedMetadata = retrievedMetadataList.get(0);
      Assert.assertEquals(retrievedMetadata.getTableName(), REALTIME_TABLE_NAME);
      Assert.assertEquals(retrievedMetadata.getSegmentName(), segmentName);
      Assert.assertEquals(realtimeMetadata.getStatus(), CommonConstants.Segment.Realtime.Status.DONE);
    }
  }

  @Test
  public void testRebalance() throws Exception {
    // We use reflections to call the real rebalance method so that we don't have to set up tableconfig.
    Method method = PinotHelixResourceManager.class.getDeclaredMethod("getRebalancedIdealState", IdealState.class, int.class, String.class, String.class);
    method.setAccessible(true);
    final String tableName = "someTable";
    final String offlineTenant = tableName + "_OFFLINE";
    final String realtimeTenant = tableName + "_REALTIME";
    final String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    final String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    // s1, s2, and s3 are initial set of servers in which offline segments exist
    InstanceConfig s1 = new InstanceConfig("Server_s1_9000");
    InstanceConfig s2 = new InstanceConfig("Server_s2_9000");
    InstanceConfig s3 = new InstanceConfig("Server_s3_9000");
    // s4, s5 and s6 are initial segments in which realtime segments exist.
    InstanceConfig s4 = new InstanceConfig("Server_s4_9000");
    InstanceConfig s5 = new InstanceConfig("Server_s5_9000");
    InstanceConfig s6 = new InstanceConfig("Server_s6_9000");
    InstanceConfig s7 = new InstanceConfig("Server_s7_9000"); // extra server into which realtime segments are rebalanced
    InstanceConfig s8 = new InstanceConfig("Server_s8_9000"); // extra server into which offline segments are rebalanced
    InstanceConfig s9 = new InstanceConfig("Server_s9_9000"); // extra server that replaces s4 in realtime rebalance test
    // Tag s1 through s3 as offline servers
    _helixAdmin.addInstance(_helixClusterName, s1);
    _helixAdmin.addInstanceTag(_helixClusterName, s1.getInstanceName(), offlineTenant);
    _helixAdmin.addInstance(_helixClusterName, s2);
    _helixAdmin.addInstanceTag(_helixClusterName, s2.getInstanceName(), offlineTenant);
    _helixAdmin.addInstance(_helixClusterName, s3);
    _helixAdmin.addInstanceTag(_helixClusterName, s3.getInstanceName(), offlineTenant);

    // Tag S4 through S6 as realtime servers.
    _helixAdmin.addInstance(_helixClusterName, s4);
    _helixAdmin.addInstanceTag(_helixClusterName, s4.getInstanceName(), realtimeTenant);
    _helixAdmin.addInstance(_helixClusterName, s5);
    _helixAdmin.addInstanceTag(_helixClusterName, s5.getInstanceName(), realtimeTenant);
    _helixAdmin.addInstance(_helixClusterName, s6);
    _helixAdmin.addInstanceTag(_helixClusterName, s6.getInstanceName(), realtimeTenant);

    // s7 is an realtime server into which we will re-balance. Not tagged yet.
    _helixAdmin.addInstance(_helixClusterName, s7);
    // s8 is an offline server into which we will re-balance. Not tagged yet.
    _helixAdmin.addInstance(_helixClusterName, s8);
    // s9 is used for realtime instance that replaces s4
    _helixAdmin.addInstance(_helixClusterName, s9);


    // Test an offline table decreasing number of replicas
    {
      final int oldNumReplicas = 3;
      final int newNumReplicas = 2;
      final String seg1 = "seg1";
      final String seg2 = "seg2";
      IdealState idealState = PinotTableIdealStateBuilder.buildEmptyIdealStateFor(offlineTableName, oldNumReplicas);
      idealState.setPartitionState(seg1, s1.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg1, s2.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg1, s3.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg2, s1.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg2, s2.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg2, s3.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      IdealState newIS =
          (IdealState) method.invoke(_helixResourceManager, idealState, Integer.valueOf(newNumReplicas), offlineTableName, offlineTenant);
      verifyIdealState(newIS, seg1, newNumReplicas);
      verifyIdealState(newIS, seg2, newNumReplicas);
      Assert.assertEquals(newIS.getReplicas(), String.valueOf(newNumReplicas));
    }

    // Test a realtime table re-balancing with the same number of replicas.
    {
      // Add s7 as another instance tagged for the realtime table
      _helixAdmin.addInstanceTag(_helixClusterName, s7.getInstanceName(), realtimeTenant);
      final int numReplicas = 3;
      final int kafkaPartition = 1;
      IdealState idealState  = PinotTableIdealStateBuilder.buildEmptyIdealStateFor(realtimeTableName, numReplicas);
      idealState.setReplicas(String.valueOf(numReplicas));
      long now = System.currentTimeMillis();
      LLCSegmentName seg1 = new LLCSegmentName(tableName, kafkaPartition, 1, now);
      LLCSegmentName seg2 = new LLCSegmentName(tableName, kafkaPartition, 2, now);
      LLCSegmentName seg3 = new LLCSegmentName(tableName, kafkaPartition, 3, now);
      LLCSegmentName seg4 = new LLCSegmentName(tableName, kafkaPartition, 4, now);
      LLCSegmentName seg5 = new LLCSegmentName(tableName, kafkaPartition, 5, now);
      HLCSegmentName seg6 = new HLCSegmentName("myTable_REALTIME_1442428556382_0", "0", "1");

      idealState.setPartitionState(seg1.toString(), s4.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg1.toString(), s5.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg1.toString(), s6.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);

      idealState.setPartitionState(seg2.toString(), s4.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg2.toString(), s5.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg2.toString(), s6.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);

      idealState.setPartitionState(seg3.toString(), s4.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      idealState.setPartitionState(seg3.toString(), s5.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      idealState.setPartitionState(seg3.toString(), s6.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);

      idealState.setPartitionState(seg4.toString(), s4.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
      idealState.setPartitionState(seg4.toString(), s5.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
      idealState.setPartitionState(seg4.toString(), s6.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);

      idealState.setPartitionState(seg5.toString(), s4.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
      idealState.setPartitionState(seg5.toString(), s5.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      idealState.setPartitionState(seg5.toString(), s6.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);

      idealState.setPartitionState(seg6.toString(), s5.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);

      Set<String> currentHosts = new HashSet<>();
      for (String segment : idealState.getRecord().getMapFields().keySet()) {
        currentHosts.addAll(idealState.getRecord().getMapFields().get(segment).keySet());
      }
      Assert.assertTrue(currentHosts.contains(s4.getId()));
      Assert.assertTrue(currentHosts.contains(s5.getId()));
      Assert.assertTrue(currentHosts.contains(s6.getId()));
      Assert.assertFalse(currentHosts.contains(s7.getId()));

      IdealState newIS =
          (IdealState) method.invoke(_helixResourceManager, idealState, Integer.valueOf(numReplicas), realtimeTableName, realtimeTenant);

      // There must be 6 segments.
      Assert.assertEquals(newIS.getPartitionSet().size(), 6);
      Assert.assertEquals(newIS.getReplicas(), String.valueOf(numReplicas));

      currentHosts.clear();
      // Make sure that new server (s7),and the old ones (s4, s5, s6) are there in the statemap
      for (String segment : newIS.getRecord().getMapFields().keySet()) {
        currentHosts.addAll(newIS.getRecord().getMapFields().get(segment).keySet());
      }
      Assert.assertTrue(currentHosts.contains(s4.getId()));
      Assert.assertTrue(currentHosts.contains(s5.getId()));
      Assert.assertTrue(currentHosts.contains(s6.getId()));
      Assert.assertTrue(currentHosts.contains(s7.getId()));

      // All LLC segments should have 3 replicas.
      verifyIdealState(newIS, seg1.toString(), numReplicas);
      verifyIdealState(newIS, seg2.toString(), numReplicas);
      verifyIdealState(newIS, seg3.toString(), numReplicas);
      verifyIdealState(newIS, seg4.toString(), numReplicas);
      verifyIdealState(newIS, seg5.toString(), numReplicas);

      // Ensure that seg3 is in CONSUMING state, and has not moved around.
      {
        Map<String, String> stateMap = newIS.getInstanceStateMap(seg3.toString());
        Assert.assertEquals(stateMap.get(s4.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
        Assert.assertEquals(stateMap.get(s5.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
        Assert.assertEquals(stateMap.get(s6.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      }

      // Ensure that seg4 is still offline with the same mapping.
      {
        Map<String, String> stateMap = newIS.getInstanceStateMap(seg4.toString());
        Assert.assertEquals(stateMap.get(s4.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
        Assert.assertEquals(stateMap.get(s5.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
        Assert.assertEquals(stateMap.get(s6.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
      }
      // Ensure that seg5 is still with same mapping
      {
        Map<String, String> stateMap = newIS.getInstanceStateMap(seg5.toString());
        Assert.assertEquals(stateMap.get(s4.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
        Assert.assertEquals(stateMap.get(s5.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
        Assert.assertEquals(stateMap.get(s6.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
      }
      // Make sure that seg6 stayed as is.
      {
        Map<String, String> stateMap = newIS.getInstanceStateMap(seg6.toString());
        Assert.assertEquals(stateMap.size(), 1);
        Assert.assertEquals(stateMap.get(s5.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);
      }
    }

    // Test an offline rebalance  with offline table
    {
      // Add s8 as another instance tagged for offline
      _helixAdmin.addInstanceTag(_helixClusterName, s8.getInstanceName(), offlineTenant);
      final int numReplicas = 3;
      final String seg1 = "seg1";
      final String seg2 = "seg2";
      IdealState idealState = PinotTableIdealStateBuilder.buildEmptyIdealStateFor(offlineTableName, numReplicas);

      idealState.setPartitionState(seg1, s1.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg1, s2.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg1, s3.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);

      idealState.setPartitionState(seg2, s1.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg2, s2.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg2, s3.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);

      IdealState newIS = (IdealState) method.invoke(_helixResourceManager, idealState, Integer.valueOf(numReplicas),
          offlineTableName, offlineTenant);
      // There must be 2 segments.
      Assert.assertEquals(newIS.getPartitionSet().size(), 2);
      Assert.assertEquals(newIS.getReplicas(), String.valueOf(numReplicas));

      // Make sure that new server (s8),and the old ones (s1, s2, s3) are there in the statemap
      Set<String> currentHosts = new HashSet<>();
      for (String segment : newIS.getRecord().getMapFields().keySet()) {
        currentHosts.addAll(newIS.getRecord().getMapFields().get(segment).keySet());
      }
      Assert.assertTrue(currentHosts.contains(s1.getId()));
      Assert.assertTrue(currentHosts.contains(s2.getId()));
      Assert.assertTrue(currentHosts.contains(s3.getId()));
      Assert.assertTrue(currentHosts.contains(s8.getId()));
      verifyIdealState(newIS, seg1, numReplicas);
      verifyIdealState(newIS, seg2, numReplicas);
    }
    // Increase number of replicas for offline rebalance
    {
      // There must already be 4 instances tagged, as per previous test.
      final int initialNumReplicas = 3;
      final int targetNumReplicas = 4;
      final String seg1 = "seg1";
      final String seg2 = "seg2";
      IdealState idealState = PinotTableIdealStateBuilder.buildEmptyIdealStateFor(offlineTableName, initialNumReplicas);

      idealState.setPartitionState(seg1, s1.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg1, s2.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg1, s3.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);

      idealState.setPartitionState(seg2, s1.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg2, s2.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg2, s3.getId(), CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);

      IdealState newIS = (IdealState) method.invoke(_helixResourceManager, idealState, Integer.valueOf(
          targetNumReplicas),
          offlineTableName, offlineTenant);
      // There must be 2 segments.
      Assert.assertEquals(newIS.getPartitionSet().size(), 2);
      Assert.assertEquals(newIS.getReplicas(), String.valueOf(targetNumReplicas));

      // Make sure that all servers are in the statemap
      Set<String> currentHosts = new HashSet<>();
      for (String segment : newIS.getRecord().getMapFields().keySet()) {
        currentHosts.addAll(newIS.getRecord().getMapFields().get(segment).keySet());
      }
      Assert.assertTrue(currentHosts.contains(s1.getId()));
      Assert.assertTrue(currentHosts.contains(s2.getId()));
      Assert.assertTrue(currentHosts.contains(s3.getId()));
      Assert.assertTrue(currentHosts.contains(s8.getId()));
      verifyIdealState(newIS, seg1, targetNumReplicas);
      verifyIdealState(newIS, seg2, targetNumReplicas);
    }

    // Increase replicas for realtime rebalance.
    {
      // We already have 4 instances tagged. Only ONLINE replicas should change. The other ones should not.
      final int initialNumReplicas = 3;
      final int targetNumReplicas = 4;
      final int kafkaPartition = 1;
      IdealState idealState  = PinotTableIdealStateBuilder.buildEmptyIdealStateFor(realtimeTableName,
          initialNumReplicas);
      idealState.setReplicas(String.valueOf(initialNumReplicas));
      long now = System.currentTimeMillis();
      LLCSegmentName seg1 = new LLCSegmentName(tableName, kafkaPartition, 1, now);
      LLCSegmentName seg2 = new LLCSegmentName(tableName, kafkaPartition, 2, now);
      LLCSegmentName seg3 = new LLCSegmentName(tableName, kafkaPartition, 3, now);
      LLCSegmentName seg4 = new LLCSegmentName(tableName, kafkaPartition, 4, now);
      LLCSegmentName seg5 = new LLCSegmentName(tableName, kafkaPartition, 5, now);
      HLCSegmentName seg6 = new HLCSegmentName("myTable_REALTIME_1442428556382_0", "0", "1");

      idealState.setPartitionState(seg1.toString(), s4.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg1.toString(), s5.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg1.toString(), s6.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);

      idealState.setPartitionState(seg2.toString(), s4.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg2.toString(), s5.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);
      idealState.setPartitionState(seg2.toString(), s6.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);

      idealState.setPartitionState(seg3.toString(), s4.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      idealState.setPartitionState(seg3.toString(), s5.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      idealState.setPartitionState(seg3.toString(), s6.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);

      idealState.setPartitionState(seg4.toString(), s4.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
      idealState.setPartitionState(seg4.toString(), s5.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
      idealState.setPartitionState(seg4.toString(), s6.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);

      idealState.setPartitionState(seg5.toString(), s4.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
      idealState.setPartitionState(seg5.toString(), s5.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      idealState.setPartitionState(seg5.toString(), s6.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);

      idealState.setPartitionState(seg6.toString(), s5.getId(), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);

      IdealState newIS =
          (IdealState) method.invoke(_helixResourceManager, idealState, Integer.valueOf(targetNumReplicas), realtimeTableName, realtimeTenant);

      // There must be 6 segments.
      Assert.assertEquals(newIS.getPartitionSet().size(), 6);
      Assert.assertEquals(newIS.getReplicas(), String.valueOf(targetNumReplicas));

      // Make sure that all servers are there in the statemap.
      Set<String> currentHosts = new HashSet<>();
      for (String segment : newIS.getRecord().getMapFields().keySet()) {
        currentHosts.addAll(newIS.getRecord().getMapFields().get(segment).keySet());
      }
      Assert.assertTrue(currentHosts.contains(s4.getId()));
      Assert.assertTrue(currentHosts.contains(s5.getId()));
      Assert.assertTrue(currentHosts.contains(s6.getId()));
      Assert.assertTrue(currentHosts.contains(s7.getId()));

      // All ONLINE LLC segments should have 4 replicas.
      verifyIdealState(newIS, seg1.toString(), targetNumReplicas);
      verifyIdealState(newIS, seg2.toString(), targetNumReplicas);

      // Other LLC segments should have 3 replicas.
      verifyIdealState(newIS, seg3.toString(), initialNumReplicas);
      verifyIdealState(newIS, seg4.toString(), initialNumReplicas);
      verifyIdealState(newIS, seg5.toString(), initialNumReplicas);

      // Ensure that seg3 is in CONSUMING state, and has not moved around.
      {
        Map<String, String> stateMap = newIS.getInstanceStateMap(seg3.toString());
        Assert.assertEquals(stateMap.get(s4.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
        Assert.assertEquals(stateMap.get(s5.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
        Assert.assertEquals(stateMap.get(s6.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      }

      // Ensure that seg4 is still offline with the same mapping.
      {
        Map<String, String> stateMap = newIS.getInstanceStateMap(seg4.toString());
        Assert.assertEquals(stateMap.get(s4.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
        Assert.assertEquals(stateMap.get(s5.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
        Assert.assertEquals(stateMap.get(s6.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
      }
      // Ensure that seg5 is still with same mapping
      {
        Map<String, String> stateMap = newIS.getInstanceStateMap(seg5.toString());
        Assert.assertEquals(stateMap.get(s4.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
        Assert.assertEquals(stateMap.get(s5.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
        Assert.assertEquals(stateMap.get(s6.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
      }
      // Make sure that seg6 stayed as is.
      {
        Map<String, String> stateMap = newIS.getInstanceStateMap(seg6.toString());
        Assert.assertEquals(stateMap.size(), 1);
        Assert.assertEquals(stateMap.get(s5.getId()), CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE);
      }

      // Add a new instance and remove the old instance.
      _helixAdmin.addInstanceTag(_helixClusterName, s9.getInstanceName(), realtimeTenant);
      idealState = newIS;

      newIS =
          (IdealState) method.invoke(_helixResourceManager, idealState, Integer.valueOf(targetNumReplicas), realtimeTableName, realtimeTenant);

      currentHosts.clear();
      for (String segment : newIS.getRecord().getMapFields().keySet()) {
        currentHosts.addAll(newIS.getRecord().getMapFields().get(segment).keySet());
      }
      Assert.assertTrue(currentHosts.contains(s4.getId()));
      Assert.assertTrue(currentHosts.contains(s5.getId()));
      Assert.assertTrue(currentHosts.contains(s6.getId()));
      Assert.assertTrue(currentHosts.contains(s7.getId()));
      Assert.assertTrue(currentHosts.contains(s9.getId()));

      // Untag s4, and rebalance.
      _helixAdmin.removeInstanceTag(_helixClusterName, s4.getInstanceName(), realtimeTenant);
      idealState = newIS;

      newIS =
          (IdealState) method.invoke(_helixResourceManager, idealState, Integer.valueOf(targetNumReplicas), realtimeTableName, realtimeTenant);

      currentHosts.clear();
      for (String segment : newIS.getRecord().getMapFields().keySet()) {
        currentHosts.addAll(newIS.getRecord().getMapFields().get(segment).keySet());
      }
      Assert.assertTrue(currentHosts.contains(s5.getId()));
      Assert.assertTrue(currentHosts.contains(s6.getId()));
      Assert.assertTrue(currentHosts.contains(s7.getId()));
      Assert.assertTrue(currentHosts.contains(s9.getId()));

      // s4 should be present only in HLC or CONSUMING segments if any, but not online LLC segments (because these are the ones we rebalance)
      Map<String, Map<String, String>> mapFields = newIS.getRecord().getMapFields();

      for (String segmentId : mapFields.keySet()) {
        if (SegmentName.isLowLevelConsumerSegmentName(segmentId)) {
          Map<String, String> stateMap = mapFields.get(segmentId);
          Set<String> serverSet = stateMap.keySet();
          Collection<String> stateSet = stateMap.values();
          if (stateSet.contains(CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE)) {
            Assert.assertFalse(serverSet.contains(s4.getId()));
          }
        }
      }
    }
  }

  private void verifyIdealState(IdealState idealState, String segmentName, int numReplicas) {
    Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();
    Map<String, String> stateMap = mapFields.get(segmentName);
    Assert.assertEquals(stateMap.size(), numReplicas);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
