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
package org.apache.pinot.controller.helix.core;

import com.google.common.collect.BiMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Helix.*;
import static org.apache.pinot.common.utils.CommonConstants.Server.*;


public class PinotHelixResourceManagerTest {
  private static final int NUM_INSTANCES = 2;
  private static final String BROKER_TENANT_NAME = "rBrokerTenant";
  private static final String SERVER_TENANT_NAME = "rServerTenant";
  private static final String TABLE_NAME = "resourceTestTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);

  private static final String SEGMENTS_REPLACE_TEST_TABLE_NAME = "segmentsReplaceTestTable";
  private static final String OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME =
      TableNameBuilder.OFFLINE.tableNameWithType(SEGMENTS_REPLACE_TEST_TABLE_NAME);

  private static final int CONNECTION_TIMEOUT_IN_MILLISECOND = 10_000;
  private static final int MAX_TIMEOUT_IN_MILLISECOND = 5_000;
  private static final int MAXIMUM_NUMBER_OF_CONTROLLER_INSTANCES = 10;
  private static final long TIMEOUT_IN_MS = 10_000L;

  @BeforeClass
  public void setUp() throws Exception {
    ControllerTestUtils.setupClusterAndValidate();

    // Create server tenant on all Servers
    Tenant serverTenant = new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, NUM_INSTANCES, NUM_INSTANCES, 0);
    ControllerTestUtils.getHelixResourceManager().createServerTenant(serverTenant);

    // Enable lead controller resource
    ControllerTestUtils.enableResourceConfigForLeadControllerResource(true);
  }

  @Test
  public void testGetInstanceEndpoints() throws InvalidConfigException {
    Set<String> servers = ControllerTestUtils.getHelixResourceManager().getAllInstancesForServerTenant(SERVER_TENANT_NAME);
    BiMap<String, String> endpoints = ControllerTestUtils.getHelixResourceManager().getDataInstanceAdminEndpoints(servers);

    // check that we have endpoints for all instances.
    Assert.assertEquals(endpoints.size(), NUM_INSTANCES);

    // check actual endpoint names
    for (String key : endpoints.keySet()) {
      int port = DEFAULT_ADMIN_API_PORT + Integer.parseInt(key.substring("Server_localhost_".length()));
      Assert.assertEquals(endpoints.get(key), "http://localhost:" + port);
    }
  }

  @Test
  public void testGetInstanceConfigs() throws Exception {
    Set<String> servers = ControllerTestUtils.getHelixResourceManager().getAllInstancesForServerTenant(SERVER_TENANT_NAME);
    for (String server : servers) {
      InstanceConfig cachedInstanceConfig = ControllerTestUtils.getHelixResourceManager().getHelixInstanceConfig(server);
      InstanceConfig realInstanceConfig = ControllerTestUtils
          .getHelixAdmin().getInstanceConfig(ControllerTestUtils.getHelixClusterName(), server);
      Assert.assertEquals(cachedInstanceConfig, realInstanceConfig);
    }

    ZkClient zkClient = new ZkClient(ControllerTestUtils.getHelixResourceManager().getHelixZkURL(), CONNECTION_TIMEOUT_IN_MILLISECOND,
        CONNECTION_TIMEOUT_IN_MILLISECOND, new ZNRecordSerializer());

    modifyExistingInstanceConfig(zkClient);
    addAndRemoveNewInstanceConfig(zkClient);

    zkClient.close();
  }

  private void modifyExistingInstanceConfig(ZkClient zkClient) throws InterruptedException {
    String instanceName = "Server_localhost_" + new Random().nextInt(NUM_INSTANCES);
    String instanceConfigPath = PropertyPathBuilder.instanceConfig(ControllerTestUtils.getHelixClusterName(), instanceName);
    Assert.assertTrue(zkClient.exists(instanceConfigPath));
    ZNRecord znRecord = zkClient.readData(instanceConfigPath, null);

    InstanceConfig cachedInstanceConfig = ControllerTestUtils.getHelixResourceManager().getHelixInstanceConfig(instanceName);
    String originalPort = cachedInstanceConfig.getPort();
    Assert.assertNotNull(originalPort);
    String newPort = Long.toString(System.currentTimeMillis());
    Assert.assertTrue(!newPort.equals(originalPort));

    // Set new port to this instance config.
    znRecord.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_PORT.toString(), newPort);
    zkClient.writeData(instanceConfigPath, znRecord);

    long maxTime = System.currentTimeMillis() + MAX_TIMEOUT_IN_MILLISECOND;
    InstanceConfig latestCachedInstanceConfig = ControllerTestUtils.getHelixResourceManager().getHelixInstanceConfig(instanceName);
    String latestPort = latestCachedInstanceConfig.getPort();
    while (!newPort.equals(latestPort) && System.currentTimeMillis() < maxTime) {
      Thread.sleep(100L);
      latestCachedInstanceConfig = ControllerTestUtils.getHelixResourceManager().getHelixInstanceConfig(instanceName);
      latestPort = latestCachedInstanceConfig.getPort();
    }
    Assert.assertTrue(System.currentTimeMillis() < maxTime, "Timeout when waiting for adding instance config");

    // Set original port back to this instance config.
    znRecord.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_PORT.toString(), originalPort);
    zkClient.writeData(instanceConfigPath, znRecord);
  }

  private void addAndRemoveNewInstanceConfig(ZkClient zkClient) {
    int biggerRandomNumber = ControllerTestUtils.TOTAL_NUM_SERVER_INSTANCES + new Random().nextInt(
        ControllerTestUtils.TOTAL_NUM_SERVER_INSTANCES);
    String instanceName = "Server_localhost_" + biggerRandomNumber;
    String instanceConfigPath = PropertyPathBuilder.instanceConfig(ControllerTestUtils.getHelixClusterName(), instanceName);
    Assert.assertFalse(zkClient.exists(instanceConfigPath));
    List<String> instances = ControllerTestUtils.getHelixResourceManager().getAllInstances();
    Assert.assertFalse(instances.contains(instanceName));

    // Add new instance.
    Instance instance = new Instance("localhost", biggerRandomNumber, InstanceType.SERVER,
        Collections.singletonList(UNTAGGED_SERVER_INSTANCE), null, 0, 0, false);
    ControllerTestUtils.getHelixResourceManager().addInstance(instance);

    List<String> allInstances = ControllerTestUtils.getHelixResourceManager().getAllInstances();
    Assert.assertTrue(allInstances.contains(instanceName));

    // Remove new instance.
    ControllerTestUtils.getHelixResourceManager().dropInstance(instanceName);

    allInstances = ControllerTestUtils.getHelixResourceManager().getAllInstances();
    Assert.assertFalse(allInstances.contains(instanceName));
  }

  @Test
  public void testRebuildBrokerResourceFromHelixTags() throws Exception {
    // Create broker tenant
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, NUM_INSTANCES, 0, 0);
    PinotResourceManagerResponse response = ControllerTestUtils.getHelixResourceManager().createBrokerTenant(brokerTenant);
    Assert.assertTrue(response.isSuccessful());

    // Create the table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNumReplicas(ControllerTestUtils.MIN_NUM_REPLICAS)
        .setBrokerTenant(BROKER_TENANT_NAME)
        .setServerTenant(SERVER_TENANT_NAME)
        .build();
    ControllerTestUtils.getHelixResourceManager().addTable(tableConfig);

    IdealState idealState = ControllerTestUtils.getHelixResourceManager().getHelixAdmin()
        .getResourceIdealState(ControllerTestUtils.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);

    // Untag all Brokers assigned to broker tenant
    untagBrokers();

    Assert.assertEquals(ControllerTestUtils.getHelixResourceManager().getOnlineUnTaggedBrokerInstanceList().size(), ControllerTestUtils.NUM_BROKER_INSTANCES);

    // Rebuilding the broker tenant should update the ideal state size
    response = ControllerTestUtils.getHelixResourceManager().rebuildBrokerResourceFromHelixTags(OFFLINE_TABLE_NAME);
    Assert.assertTrue(response.isSuccessful());
    idealState =
        ControllerTestUtils
            .getHelixAdmin().getResourceIdealState(ControllerTestUtils.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), 0);

    // Create broker tenant on Brokers
    brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, ControllerTestUtils.NUM_BROKER_INSTANCES, 0, 0);
    response = ControllerTestUtils.getHelixResourceManager().createBrokerTenant(brokerTenant);
    Assert.assertTrue(response.isSuccessful());

    // Rebuilding the broker tenant should update the ideal state size
    response = ControllerTestUtils.getHelixResourceManager().rebuildBrokerResourceFromHelixTags(OFFLINE_TABLE_NAME);
    Assert.assertTrue(response.isSuccessful());
    idealState =
        ControllerTestUtils
            .getHelixAdmin().getResourceIdealState(ControllerTestUtils.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), ControllerTestUtils.NUM_BROKER_INSTANCES);

    // Delete the table
    ControllerTestUtils.getHelixResourceManager().deleteOfflineTable(TABLE_NAME);

    // Untag the brokers
    untagBrokers();
    Assert.assertEquals(ControllerTestUtils.getHelixResourceManager().getOnlineUnTaggedBrokerInstanceList().size(), ControllerTestUtils.NUM_BROKER_INSTANCES);
  }

  @Test
  public void testRetrieveMetadata() {
    String segmentName = "testSegment";

    // Test retrieving OFFLINE segment ZK metadata
    {
      OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
      offlineSegmentZKMetadata.setSegmentName(segmentName);
      ZKMetadataProvider.setOfflineSegmentZKMetadata(ControllerTestUtils.getPropertyStore(), OFFLINE_TABLE_NAME, offlineSegmentZKMetadata);
      List<OfflineSegmentZKMetadata> retrievedMetadataList =
          ControllerTestUtils.getHelixResourceManager().getOfflineSegmentMetadata(OFFLINE_TABLE_NAME);
      Assert.assertEquals(retrievedMetadataList.size(), 1);
      OfflineSegmentZKMetadata retrievedMetadata = retrievedMetadataList.get(0);
      Assert.assertEquals(retrievedMetadata.getSegmentName(), segmentName);
    }

    // Test retrieving REALTIME segment ZK metadata
    {
      RealtimeSegmentZKMetadata realtimeMetadata = new RealtimeSegmentZKMetadata();
      realtimeMetadata.setSegmentName(segmentName);
      realtimeMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      ZKMetadataProvider.setRealtimeSegmentZKMetadata(ControllerTestUtils.getPropertyStore(), REALTIME_TABLE_NAME, realtimeMetadata);
      List<RealtimeSegmentZKMetadata> retrievedMetadataList =
          ControllerTestUtils.getHelixResourceManager().getRealtimeSegmentMetadata(REALTIME_TABLE_NAME);
      Assert.assertEquals(retrievedMetadataList.size(), 1);
      RealtimeSegmentZKMetadata retrievedMetadata = retrievedMetadataList.get(0);
      Assert.assertEquals(retrievedMetadata.getSegmentName(), segmentName);
      Assert.assertEquals(realtimeMetadata.getStatus(), CommonConstants.Segment.Realtime.Status.DONE);
    }
  }

  @Test
  void testRetrieveTenantNames() {
    // Create broker tenant on 1 Broker
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 1, 0, 0);
    PinotResourceManagerResponse response = ControllerTestUtils.getHelixResourceManager().createBrokerTenant(brokerTenant);
    Assert.assertTrue(response.isSuccessful());

    Set<String> brokerTenantNames = ControllerTestUtils.getHelixResourceManager().getAllBrokerTenantNames();
    // Two tenant names expected: [brokerTenant, DefaultTenant]
    Assert.assertEquals(brokerTenantNames.size(), 2);
    Assert.assertTrue(brokerTenantNames.contains(BROKER_TENANT_NAME));

    String testBrokerInstance =
        ControllerTestUtils.getHelixResourceManager().getAllInstancesForBrokerTenant(BROKER_TENANT_NAME).iterator().next();
    ControllerTestUtils
        .getHelixAdmin().addInstanceTag(ControllerTestUtils.getHelixClusterName(), testBrokerInstance, "wrong_tag");

    brokerTenantNames = ControllerTestUtils.getHelixResourceManager().getAllBrokerTenantNames();
    Assert.assertEquals(brokerTenantNames.size(), 2);
    Assert.assertTrue(brokerTenantNames.contains(BROKER_TENANT_NAME));

    ControllerTestUtils
        .getHelixAdmin().removeInstanceTag(ControllerTestUtils.getHelixClusterName(), testBrokerInstance, "wrong_tag");

    // Server tenant is already created during setup.
    Set<String> serverTenantNames = ControllerTestUtils.getHelixResourceManager().getAllServerTenantNames();
    // Two tenant names expected: [DefaultTenant, serverTenant]
    Assert.assertEquals(serverTenantNames.size(), 2);
    Assert.assertTrue(serverTenantNames.contains(SERVER_TENANT_NAME));

    String testServerInstance =
        ControllerTestUtils.getHelixResourceManager().getAllInstancesForServerTenant(SERVER_TENANT_NAME).iterator().next();
    ControllerTestUtils
        .getHelixAdmin().addInstanceTag(ControllerTestUtils.getHelixClusterName(), testServerInstance, "wrong_tag");

    serverTenantNames = ControllerTestUtils.getHelixResourceManager().getAllServerTenantNames();
    Assert.assertEquals(serverTenantNames.size(), 2);
    Assert.assertTrue(serverTenantNames.contains(SERVER_TENANT_NAME));

    ControllerTestUtils
        .getHelixAdmin().removeInstanceTag(ControllerTestUtils.getHelixClusterName(), testServerInstance, "wrong_tag");

    untagBrokers();
    Assert.assertEquals(ControllerTestUtils.getHelixResourceManager().getOnlineUnTaggedBrokerInstanceList().size(), ControllerTestUtils.NUM_BROKER_INSTANCES);
  }

  @Test
  public void testLeadControllerResource() {
    IdealState leadControllerResourceIdealState = ControllerTestUtils.getHelixResourceManager().getHelixAdmin()
        .getResourceIdealState(ControllerTestUtils.getHelixClusterName(), CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    Assert.assertTrue(leadControllerResourceIdealState.isValid());
    Assert.assertTrue(leadControllerResourceIdealState.isEnabled());
    Assert.assertEquals(leadControllerResourceIdealState.getInstanceGroupTag(),
        CommonConstants.Helix.CONTROLLER_INSTANCE);
    Assert.assertEquals(leadControllerResourceIdealState.getNumPartitions(),
        CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
    Assert.assertEquals(leadControllerResourceIdealState.getReplicas(),
        Integer.toString(LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT));
    Assert.assertEquals(leadControllerResourceIdealState.getRebalanceMode(), IdealState.RebalanceMode.FULL_AUTO);
    Assert.assertTrue(leadControllerResourceIdealState.getInstanceSet(
        leadControllerResourceIdealState.getPartitionSet().iterator().next()).isEmpty());

    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView = ControllerTestUtils.getHelixResourceManager().getHelixAdmin()
          .getResourceExternalView(ControllerTestUtils.getHelixClusterName(), CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);
      for (String partition : leadControllerResourceExternalView.getPartitionSet()) {
        Map<String, String> stateMap = leadControllerResourceExternalView.getStateMap(partition);
        Map.Entry<String, String> entry = stateMap.entrySet().iterator().next();
        boolean result =
            (LeadControllerUtils.generateParticipantInstanceId(
                ControllerTestUtils.LOCAL_HOST, ControllerTestUtils.getControllerPort())).equals(entry.getKey());
        result &= MasterSlaveSMD.States.MASTER.name().equals(entry.getValue());
        if (!result) {
          return false;
        }
      }
      return true;
    }, TIMEOUT_IN_MS, "Failed to assign controller hosts to lead controller resource in " + TIMEOUT_IN_MS + " ms.");
  }

  @Test
  public void testLeadControllerAssignment() {
    // Given a number of instances (from 1 to 10), make sure all the instances got assigned to lead controller resource.
    for (int nInstances = 1; nInstances <= MAXIMUM_NUMBER_OF_CONTROLLER_INSTANCES; nInstances++) {
      List<String> instanceNames = new ArrayList<>(nInstances);
      List<Integer> ports = new ArrayList<>(nInstances);
      for (int i = 0; i < nInstances; i++) {
        instanceNames.add(LeadControllerUtils.generateParticipantInstanceId(ControllerTestUtils.LOCAL_HOST, i));
        ports.add(i);
      }

      List<String> partitions = new ArrayList<>(NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
      for (int i = 0; i < NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE; i++) {
        partitions.add(LeadControllerUtils.generatePartitionName(i));
      }

      LinkedHashMap<String, Integer> states = new LinkedHashMap<>(2);
      states.put(MasterSlaveSMD.States.OFFLINE.name(), 0);
      states.put(MasterSlaveSMD.States.SLAVE.name(), LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT - 1);
      states.put(MasterSlaveSMD.States.MASTER.name(), 1);

      CrushEdRebalanceStrategy crushEdRebalanceStrategy = new CrushEdRebalanceStrategy();
      crushEdRebalanceStrategy.init(LEAD_CONTROLLER_RESOURCE_NAME, partitions, states, Integer.MAX_VALUE);

      ClusterDataCache clusterDataCache = new ClusterDataCache();
      PropertyKey.Builder keyBuilder = new PropertyKey.Builder(ControllerTestUtils.getHelixClusterName());
      HelixDataAccessor accessor = ControllerTestUtils.getHelixManager().getHelixDataAccessor();
      ClusterConfig clusterConfig = accessor.getProperty(keyBuilder.clusterConfig());
      clusterDataCache.setClusterConfig(clusterConfig);

      Map<String, InstanceConfig> instanceConfigMap = new HashMap<>(nInstances);
      for (int i = 0; i < nInstances; i++) {
        String instanceName = instanceNames.get(i);
        int port = ports.get(i);
        instanceConfigMap.put(instanceName, new InstanceConfig(instanceName
            + ", {HELIX_ENABLED=true, HELIX_ENABLED_TIMESTAMP=1559546216610, HELIX_HOST=Controller_localhost, HELIX_PORT="
            + port + "}{}{TAG_LIST=[controller]}"));
      }
      clusterDataCache.setInstanceConfigMap(instanceConfigMap);
      ZNRecord znRecord =
          crushEdRebalanceStrategy.computePartitionAssignment(instanceNames, instanceNames, new HashMap<>(0),
              clusterDataCache);

      Assert.assertNotNull(znRecord);
      Map<String, List<String>> listFields = znRecord.getListFields();
      Assert.assertEquals(listFields.size(), NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);

      Map<String, Integer> instanceToMasterAssignmentCountMap = new HashMap<>();
      int maxCount = 0;
      for (List<String> assignments : listFields.values()) {
        Assert.assertEquals(assignments.size(), LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT);
        if (!instanceToMasterAssignmentCountMap.containsKey(assignments.get(0))) {
          instanceToMasterAssignmentCountMap.put(assignments.get(0), 1);
        } else {
          instanceToMasterAssignmentCountMap.put(assignments.get(0),
              instanceToMasterAssignmentCountMap.get(assignments.get(0)) + 1);
        }
        maxCount = Math.max(instanceToMasterAssignmentCountMap.get(assignments.get(0)), maxCount);
      }
      Assert.assertEquals(instanceToMasterAssignmentCountMap.size(), nInstances,
          "Not all the instances got assigned to the resource!");
      for (Integer count : instanceToMasterAssignmentCountMap.values()) {
        Assert.assertTrue((maxCount - count == 0 || maxCount - count == 1), "Instance assignment isn't distributed");
      }
    }
  }

  @Test
  public void testSegmentReplacement() throws IOException {
    // Create broker tenant on 1 Brokers
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 1, 0, 0);
    PinotResourceManagerResponse response = ControllerTestUtils.getHelixResourceManager().createBrokerTenant(brokerTenant);
    Assert.assertTrue(response.isSuccessful());

    // Create the table
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME)
            .setNumReplicas(2)
            .setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME)
            .build();

    ControllerTestUtils.getHelixResourceManager().addTable(tableConfig);

    for (int i = 0; i < 5; i++) {
      ControllerTestUtils.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "s" + i),
          "downloadUrl");
    }
    List<String> segmentsForTable = ControllerTestUtils.getHelixResourceManager().getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentsForTable.size(), 5);

    List<String> segmentsFrom = new ArrayList<>();
    List<String> segmentsTo = Arrays.asList("s5", "s6");

    String lineageEntryId =
        ControllerTestUtils
            .getHelixResourceManager().startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom,
            segmentsTo);
    SegmentLineage segmentLineage =
        SegmentLineageAccessHelper.getSegmentLineage(ControllerTestUtils.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 1);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsFrom(), new ArrayList<>());
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsTo(), segmentsTo);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getState(), LineageEntryState.IN_PROGRESS);

    // Check invalid segmentsTo
    segmentsFrom = Arrays.asList("s1", "s2");
    segmentsTo = Arrays.asList("s3", "s4");
    try {
      ControllerTestUtils
          .getHelixResourceManager().startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom,
          segmentsTo);
    } catch (Exception e) {
      // expected
    }
    segmentsFrom = Arrays.asList("s1", "s2");
    segmentsTo = Arrays.asList("s2");
    try {
      ControllerTestUtils
          .getHelixResourceManager().startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom,
          segmentsTo);
    } catch (Exception e) {
      // expected
    }

    // Check invalid segmentsFrom
    segmentsFrom = Arrays.asList("s1", "s6");
    segmentsTo = Arrays.asList("merged1", "merged2");
    try {
      ControllerTestUtils
          .getHelixResourceManager().startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom,
          segmentsTo);
    } catch (Exception e) {
      // expected
    }

    segmentsFrom = Arrays.asList("s1", "s2");
    String lineageEntryId2 =
        ControllerTestUtils
            .getHelixResourceManager().startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom,
            segmentsTo);
    segmentLineage =
        SegmentLineageAccessHelper.getSegmentLineage(ControllerTestUtils.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 2);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getSegmentsFrom(), segmentsFrom);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getSegmentsTo(), segmentsTo);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getState(), LineageEntryState.IN_PROGRESS);

    try {
      ControllerTestUtils
          .getHelixResourceManager().startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom,
          segmentsTo);
    } catch (Exception e) {
      // expected
    }

    // Invalid table
    try {
      ControllerTestUtils.getHelixResourceManager().endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId);
    } catch (Exception e) {
      // expected
    }

    // Invalid lineage entry id
    try {
      ControllerTestUtils.getHelixResourceManager().endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "aaa");
    } catch (Exception e) {
      // expected
    }

    // Merged segment not available in the table
    try {
      ControllerTestUtils
          .getHelixResourceManager().endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, lineageEntryId);
    } catch (Exception e) {
      // expected
    }

    // Try after adding merged segments to the table
    ControllerTestUtils.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "s5"), "downloadUrl");
    ControllerTestUtils.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "s6"), "downloadUrl");

    ControllerTestUtils
        .getHelixResourceManager().endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, lineageEntryId);
    segmentLineage =
        SegmentLineageAccessHelper.getSegmentLineage(ControllerTestUtils.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 2);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsFrom(), new ArrayList<>());
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsTo(), Arrays.asList("s5", "s6"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getState(), LineageEntryState.COMPLETED);

    ControllerTestUtils.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "merged1"),
        "downloadUrl");
    ControllerTestUtils.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "merged2"),
        "downloadUrl");

    ControllerTestUtils
        .getHelixResourceManager().endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, lineageEntryId2);
    segmentLineage =
        SegmentLineageAccessHelper.getSegmentLineage(ControllerTestUtils.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 2);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getSegmentsFrom(), Arrays.asList("s1", "s2"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getSegmentsTo(),
        Arrays.asList("merged1", "merged2"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getState(), LineageEntryState.COMPLETED);
  }

  private void untagBrokers() {
    for (String brokerInstance : ControllerTestUtils.getHelixResourceManager().getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      ControllerTestUtils.getHelixAdmin().removeInstanceTag(ControllerTestUtils.getHelixClusterName(), brokerInstance,
          TagNameUtils.getBrokerTagForTenant(BROKER_TENANT_NAME));
      ControllerTestUtils.getHelixAdmin().addInstanceTag(ControllerTestUtils.getHelixClusterName(), brokerInstance,
          CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}
