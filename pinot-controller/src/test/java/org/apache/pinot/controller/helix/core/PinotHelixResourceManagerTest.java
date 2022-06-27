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
import java.util.HashSet;
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
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.api.resources.InstanceInfo;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE;
import static org.apache.pinot.spi.utils.CommonConstants.Server.DEFAULT_ADMIN_API_PORT;
import static org.testng.Assert.fail;


public class PinotHelixResourceManagerTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final int NUM_REALTIME_SERVER_INSTANCES = 2;
  private static final int NUM_OFFLINE_SERVER_INSTANCES = 2;
  private static final int NUM_INSTANCES = NUM_REALTIME_SERVER_INSTANCES + NUM_OFFLINE_SERVER_INSTANCES;
  private static final String BROKER_TENANT_NAME = "rBrokerTenant";
  private static final String SERVER_TENANT_NAME = "rServerTenant";
  private static final String TABLE_NAME = "resourceTestTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);

  private static final String SEGMENTS_REPLACE_TEST_TABLE_NAME = "segmentsReplaceTestTable";
  private static final String OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME =
      TableNameBuilder.OFFLINE.tableNameWithType(SEGMENTS_REPLACE_TEST_TABLE_NAME);

  private static final String SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME = "segmentsReplaceTestRefreshTable";
  private static final String OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME =
      TableNameBuilder.OFFLINE.tableNameWithType(SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);

  private static final int CONNECTION_TIMEOUT_IN_MILLISECOND = 10_000;
  private static final int MAXIMUM_NUMBER_OF_CONTROLLER_INSTANCES = 10;
  private static final long TIMEOUT_IN_MS = 60_000L;

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();

    // Create server tenant on all Servers
    Tenant serverTenant = new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, NUM_INSTANCES, NUM_OFFLINE_SERVER_INSTANCES,
        NUM_REALTIME_SERVER_INSTANCES);
    TEST_INSTANCE.getHelixResourceManager().createServerTenant(serverTenant);

    // Enable lead controller resource
    TEST_INSTANCE.enableResourceConfigForLeadControllerResource(true);
  }

  @Test
  public void testGetInstanceEndpoints()
      throws InvalidConfigException {
    Set<String> servers =
        TEST_INSTANCE.getHelixResourceManager().getAllInstancesForServerTenant(SERVER_TENANT_NAME);
    BiMap<String, String> endpoints =
        TEST_INSTANCE.getHelixResourceManager().getDataInstanceAdminEndpoints(servers);

    // check that we have endpoints for all instances.
    Assert.assertEquals(endpoints.size(), NUM_INSTANCES);

    // check actual endpoint names
    for (String key : endpoints.keySet()) {
      int port = DEFAULT_ADMIN_API_PORT + Integer.parseInt(key.substring("Server_localhost_".length()));
      Assert.assertEquals(endpoints.get(key), "http://localhost:" + port);
    }
  }

  @Test
  public void testGetInstanceConfigs()
      throws Exception {
    Set<String> servers =
        TEST_INSTANCE.getHelixResourceManager().getAllInstancesForServerTenant(SERVER_TENANT_NAME);
    for (String server : servers) {
      InstanceConfig cachedInstanceConfig =
          TEST_INSTANCE.getHelixResourceManager().getHelixInstanceConfig(server);
      InstanceConfig realInstanceConfig =
          TEST_INSTANCE.getHelixAdmin().getInstanceConfig(TEST_INSTANCE.getHelixClusterName(), server);
      Assert.assertEquals(cachedInstanceConfig, realInstanceConfig);
    }

    ZkClient zkClient =
        new ZkClient(TEST_INSTANCE.getHelixResourceManager().getHelixZkURL(), CONNECTION_TIMEOUT_IN_MILLISECOND,
            CONNECTION_TIMEOUT_IN_MILLISECOND, new ZNRecordSerializer());

    modifyExistingInstanceConfig(zkClient);
    addAndRemoveNewInstanceConfig(zkClient);

    zkClient.close();
  }

  private void modifyExistingInstanceConfig(ZkClient zkClient)
      throws InterruptedException {
    String instanceName = "Server_localhost_" + new Random().nextInt(NUM_INSTANCES);
    String instanceConfigPath =
        PropertyPathBuilder.instanceConfig(TEST_INSTANCE.getHelixClusterName(), instanceName);
    Assert.assertTrue(zkClient.exists(instanceConfigPath));
    ZNRecord znRecord = zkClient.readData(instanceConfigPath, null);

    InstanceConfig cachedInstanceConfig =
        TEST_INSTANCE.getHelixResourceManager().getHelixInstanceConfig(instanceName);
    String originalPort = cachedInstanceConfig.getPort();
    Assert.assertNotNull(originalPort);
    String newPort = Long.toString(System.currentTimeMillis());
    Assert.assertTrue(!newPort.equals(originalPort));

    // Set new port to this instance config.
    znRecord.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_PORT.toString(), newPort);
    zkClient.writeData(instanceConfigPath, znRecord);

    long maxTime = System.currentTimeMillis() + TIMEOUT_IN_MS;
    InstanceConfig latestCachedInstanceConfig =
        TEST_INSTANCE.getHelixResourceManager().getHelixInstanceConfig(instanceName);
    String latestPort = latestCachedInstanceConfig.getPort();
    while (!newPort.equals(latestPort) && System.currentTimeMillis() < maxTime) {
      Thread.sleep(100L);
      latestCachedInstanceConfig = TEST_INSTANCE.getHelixResourceManager().getHelixInstanceConfig(instanceName);
      latestPort = latestCachedInstanceConfig.getPort();
    }
    Assert.assertTrue(System.currentTimeMillis() < maxTime, "Timeout when waiting for adding instance config");

    // Set original port back to this instance config.
    znRecord.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_PORT.toString(), originalPort);
    zkClient.writeData(instanceConfigPath, znRecord);
  }

  private void addAndRemoveNewInstanceConfig(ZkClient zkClient) {
    int biggerRandomNumber = TEST_INSTANCE.TOTAL_NUM_SERVER_INSTANCES + new Random()
        .nextInt(TEST_INSTANCE.TOTAL_NUM_SERVER_INSTANCES);
    String instanceName = "Server_localhost_" + biggerRandomNumber;
    String instanceConfigPath =
        PropertyPathBuilder.instanceConfig(TEST_INSTANCE.getHelixClusterName(), instanceName);
    Assert.assertFalse(zkClient.exists(instanceConfigPath));
    List<String> instances = TEST_INSTANCE.getHelixResourceManager().getAllInstances();
    Assert.assertFalse(instances.contains(instanceName));

    // Add new instance.
    Instance instance = new Instance("localhost", biggerRandomNumber, InstanceType.SERVER,
        Collections.singletonList(UNTAGGED_SERVER_INSTANCE), null, 0, 0, 0, 0, false);
    TEST_INSTANCE.getHelixResourceManager().addInstance(instance, false);

    List<String> allInstances = TEST_INSTANCE.getHelixResourceManager().getAllInstances();
    Assert.assertTrue(allInstances.contains(instanceName));

    // Remove new instance.
    TEST_INSTANCE.getHelixResourceManager().dropInstance(instanceName);

    allInstances = TEST_INSTANCE.getHelixResourceManager().getAllInstances();
    Assert.assertFalse(allInstances.contains(instanceName));
  }

  @Test
  public void testRebuildBrokerResourceFromHelixTags()
      throws Exception {
    // Create broker tenant
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, NUM_INSTANCES, 0, 0);
    PinotResourceManagerResponse response =
        TEST_INSTANCE.getHelixResourceManager().createBrokerTenant(brokerTenant);
    Assert.assertTrue(response.isSuccessful());

    // Create the table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNumReplicas(TEST_INSTANCE.MIN_NUM_REPLICAS).setBrokerTenant(BROKER_TENANT_NAME)
        .setServerTenant(SERVER_TENANT_NAME).build();
    TEST_INSTANCE.getHelixResourceManager().addTable(tableConfig);

    IdealState idealState = TEST_INSTANCE.getHelixResourceManager().getHelixAdmin()
        .getResourceIdealState(TEST_INSTANCE.getHelixClusterName(),
            CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);

    // Untag all Brokers assigned to broker tenant
    untagBrokers();

    Assert.assertEquals(TEST_INSTANCE.getHelixResourceManager().getOnlineUnTaggedBrokerInstanceList().size(),
        TEST_INSTANCE.NUM_BROKER_INSTANCES);

    // Rebuilding the broker tenant should update the ideal state size
    response = TEST_INSTANCE.getHelixResourceManager().rebuildBrokerResourceFromHelixTags(OFFLINE_TABLE_NAME);
    Assert.assertTrue(response.isSuccessful());
    idealState = TEST_INSTANCE.getHelixAdmin().getResourceIdealState(TEST_INSTANCE.getHelixClusterName(),
        CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), 0);

    // Create broker tenant on Brokers
    brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, TEST_INSTANCE.NUM_BROKER_INSTANCES, 0, 0);
    response = TEST_INSTANCE.getHelixResourceManager().createBrokerTenant(brokerTenant);
    Assert.assertTrue(response.isSuccessful());

    // Rebuilding the broker tenant should update the ideal state size
    response = TEST_INSTANCE.getHelixResourceManager().rebuildBrokerResourceFromHelixTags(OFFLINE_TABLE_NAME);
    Assert.assertTrue(response.isSuccessful());
    idealState = TEST_INSTANCE.getHelixAdmin().getResourceIdealState(TEST_INSTANCE.getHelixClusterName(),
        CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(),
        TEST_INSTANCE.NUM_BROKER_INSTANCES);

    // Delete the table
    TEST_INSTANCE.getHelixResourceManager().deleteOfflineTable(TABLE_NAME);

    // Untag the brokers
    untagBrokers();
    Assert.assertEquals(TEST_INSTANCE.getHelixResourceManager().getOnlineUnTaggedBrokerInstanceList().size(),
        TEST_INSTANCE.NUM_BROKER_INSTANCES);
  }

  @Test
  public void testRetrieveSegmentZKMetadata() {
    String segmentName = "testSegment";

    // Test retrieving OFFLINE segment ZK metadata
    {
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      ZKMetadataProvider
          .setSegmentZKMetadata(TEST_INSTANCE.getPropertyStore(), OFFLINE_TABLE_NAME, segmentZKMetadata);
      List<SegmentZKMetadata> retrievedSegmentsZKMetadata =
          TEST_INSTANCE.getHelixResourceManager().getSegmentsZKMetadata(OFFLINE_TABLE_NAME);
      Assert.assertEquals(retrievedSegmentsZKMetadata.size(), 1);
      SegmentZKMetadata retrievedSegmentZKMetadata = retrievedSegmentsZKMetadata.get(0);
      Assert.assertEquals(retrievedSegmentZKMetadata.getSegmentName(), segmentName);
    }

    // Test retrieving REALTIME segment ZK metadata
    {
      SegmentZKMetadata realtimeMetadata = new SegmentZKMetadata(segmentName);
      realtimeMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      ZKMetadataProvider
          .setSegmentZKMetadata(TEST_INSTANCE.getPropertyStore(), REALTIME_TABLE_NAME, realtimeMetadata);
      List<SegmentZKMetadata> retrievedSegmentsZKMetadata =
          TEST_INSTANCE.getHelixResourceManager().getSegmentsZKMetadata(REALTIME_TABLE_NAME);
      Assert.assertEquals(retrievedSegmentsZKMetadata.size(), 1);
      SegmentZKMetadata retrievedSegmentZKMetadata = retrievedSegmentsZKMetadata.get(0);
      Assert.assertEquals(retrievedSegmentZKMetadata.getSegmentName(), segmentName);
      Assert.assertEquals(realtimeMetadata.getStatus(), CommonConstants.Segment.Realtime.Status.DONE);
    }
  }

  @Test
  void testRetrieveTenantNames() {
    // Create broker tenant on 1 Broker
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 1, 0, 0);
    PinotResourceManagerResponse response =
        TEST_INSTANCE.getHelixResourceManager().createBrokerTenant(brokerTenant);
    Assert.assertTrue(response.isSuccessful());

    Set<String> brokerTenantNames = TEST_INSTANCE.getHelixResourceManager().getAllBrokerTenantNames();
    // Two tenant names expected: [brokerTenant, DefaultTenant]
    Assert.assertEquals(brokerTenantNames.size(), 2);
    Assert.assertTrue(brokerTenantNames.contains(BROKER_TENANT_NAME));

    String testBrokerInstance =
        TEST_INSTANCE.getHelixResourceManager().getAllInstancesForBrokerTenant(BROKER_TENANT_NAME).iterator()
            .next();
    TEST_INSTANCE.getHelixAdmin()
        .addInstanceTag(TEST_INSTANCE.getHelixClusterName(), testBrokerInstance, "wrong_tag");

    brokerTenantNames = TEST_INSTANCE.getHelixResourceManager().getAllBrokerTenantNames();
    Assert.assertEquals(brokerTenantNames.size(), 2);
    Assert.assertTrue(brokerTenantNames.contains(BROKER_TENANT_NAME));

    TEST_INSTANCE.getHelixAdmin()
        .removeInstanceTag(TEST_INSTANCE.getHelixClusterName(), testBrokerInstance, "wrong_tag");

    // Server tenant is already created during setup.
    Set<String> serverTenantNames = TEST_INSTANCE.getHelixResourceManager().getAllServerTenantNames();
    // Two tenant names expected: [DefaultTenant, serverTenant]
    Assert.assertEquals(serverTenantNames.size(), 2);
    Assert.assertTrue(serverTenantNames.contains(SERVER_TENANT_NAME));

    String testServerInstance =
        TEST_INSTANCE.getHelixResourceManager().getAllInstancesForServerTenant(SERVER_TENANT_NAME).iterator()
            .next();
    TEST_INSTANCE.getHelixAdmin()
        .addInstanceTag(TEST_INSTANCE.getHelixClusterName(), testServerInstance, "wrong_tag");

    serverTenantNames = TEST_INSTANCE.getHelixResourceManager().getAllServerTenantNames();
    Assert.assertEquals(serverTenantNames.size(), 2);
    Assert.assertTrue(serverTenantNames.contains(SERVER_TENANT_NAME));

    TEST_INSTANCE.getHelixAdmin()
        .removeInstanceTag(TEST_INSTANCE.getHelixClusterName(), testServerInstance, "wrong_tag");

    untagBrokers();
    Assert.assertEquals(TEST_INSTANCE.getHelixResourceManager().getOnlineUnTaggedBrokerInstanceList().size(),
        TEST_INSTANCE.NUM_BROKER_INSTANCES);
  }

  @Test
  public void testLeadControllerResource() {
    IdealState leadControllerResourceIdealState = TEST_INSTANCE.getHelixResourceManager().getHelixAdmin()
        .getResourceIdealState(TEST_INSTANCE.getHelixClusterName(),
            CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    Assert.assertTrue(leadControllerResourceIdealState.isValid());
    Assert.assertTrue(leadControllerResourceIdealState.isEnabled());
    Assert.assertEquals(leadControllerResourceIdealState.getInstanceGroupTag(),
        CommonConstants.Helix.CONTROLLER_INSTANCE);
    Assert.assertEquals(leadControllerResourceIdealState.getNumPartitions(),
        CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
    Assert.assertEquals(leadControllerResourceIdealState.getReplicas(),
        Integer.toString(LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT));
    Assert.assertEquals(leadControllerResourceIdealState.getRebalanceMode(), IdealState.RebalanceMode.FULL_AUTO);
    Assert.assertTrue(leadControllerResourceIdealState
        .getInstanceSet(leadControllerResourceIdealState.getPartitionSet().iterator().next()).isEmpty());

    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView = TEST_INSTANCE.getHelixResourceManager().getHelixAdmin()
          .getResourceExternalView(TEST_INSTANCE.getHelixClusterName(),
              CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);
      for (String partition : leadControllerResourceExternalView.getPartitionSet()) {
        Map<String, String> stateMap = leadControllerResourceExternalView.getStateMap(partition);
        Map.Entry<String, String> entry = stateMap.entrySet().iterator().next();
        boolean result = (LeadControllerUtils
            .generateParticipantInstanceId(TEST_INSTANCE.LOCAL_HOST, TEST_INSTANCE.getControllerPort()))
            .equals(entry.getKey());
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
        instanceNames.add(LeadControllerUtils.generateParticipantInstanceId(TEST_INSTANCE.LOCAL_HOST, i));
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
      PropertyKey.Builder keyBuilder = new PropertyKey.Builder(TEST_INSTANCE.getHelixClusterName());
      HelixDataAccessor accessor = TEST_INSTANCE.getHelixManager().getHelixDataAccessor();
      ClusterConfig clusterConfig = accessor.getProperty(keyBuilder.clusterConfig());
      clusterDataCache.setClusterConfig(clusterConfig);

      Map<String, InstanceConfig> instanceConfigMap = new HashMap<>(nInstances);
      for (int i = 0; i < nInstances; i++) {
        String instanceName = instanceNames.get(i);
        int port = ports.get(i);
        instanceConfigMap.put(instanceName, new InstanceConfig(instanceName
            + ", {HELIX_ENABLED=true, HELIX_ENABLED_TIMESTAMP=1559546216610, HELIX_HOST=Controller_localhost, "
            + "HELIX_PORT=" + port + "}{}{TAG_LIST=[controller]}"));
      }
      clusterDataCache.setInstanceConfigMap(instanceConfigMap);
      ZNRecord znRecord = crushEdRebalanceStrategy
          .computePartitionAssignment(instanceNames, instanceNames, new HashMap<>(0), clusterDataCache);

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
          instanceToMasterAssignmentCountMap
              .put(assignments.get(0), instanceToMasterAssignmentCountMap.get(assignments.get(0)) + 1);
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
  public void testSegmentReplacement()
      throws Exception {
    // Create broker tenant on 1 Brokers
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 1, 0, 0);
    PinotResourceManagerResponse response =
        TEST_INSTANCE.getHelixResourceManager().createBrokerTenant(brokerTenant);
    Assert.assertTrue(response.isSuccessful());

    testSegmentReplacementRegular();
    testSegmentReplacementForRefresh();
  }

  private void testSegmentReplacementRegular()
      throws IOException {

    // Create the table
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME)
            .setNumReplicas(2).setBrokerTenant(BROKER_TENANT_NAME).setServerTenant(SERVER_TENANT_NAME).build();

    TEST_INSTANCE.getHelixResourceManager().addTable(tableConfig);

    for (int i = 0; i < 5; i++) {
      TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "s" + i),
          "downloadUrl");
    }
    List<String> segmentsForTable =
        TEST_INSTANCE.getHelixResourceManager().getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, false);
    Assert.assertEquals(segmentsForTable.size(), 5);

    List<String> segmentsFrom = new ArrayList<>();
    List<String> segmentsTo = Arrays.asList("s5", "s6");

    String lineageEntryId = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, false);
    SegmentLineage segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 1);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsFrom(), new ArrayList<>());
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsTo(), segmentsTo);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getState(), LineageEntryState.IN_PROGRESS);

    // Check invalid segmentsTo
    segmentsFrom = Arrays.asList("s1", "s2");
    segmentsTo = Arrays.asList("s3", "s4");
    try {
      TEST_INSTANCE.getHelixResourceManager()
          .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, false);
      fail();
    } catch (Exception e) {
      // expected
    }
    segmentsFrom = Arrays.asList("s1", "s2");
    segmentsTo = Arrays.asList("s2");
    try {
      TEST_INSTANCE.getHelixResourceManager()
          .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, false);
      fail();
    } catch (Exception e) {
      // expected
    }

    // Check invalid segmentsFrom
    segmentsFrom = Arrays.asList("s1", "s6");
    try {
      TEST_INSTANCE.getHelixResourceManager()
          .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, false);
      fail();
    } catch (Exception e) {
      // expected
    }

    // Invalid table
    try {
      TEST_INSTANCE.getHelixResourceManager().endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId);
      fail();
    } catch (Exception e) {
      // expected
    }

    // Invalid lineage entry id
    try {
      TEST_INSTANCE.getHelixResourceManager().endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "aaa");
      fail();
    } catch (Exception e) {
      // expected
    }

    // Merged segment not available in the table
    try {
      TEST_INSTANCE.getHelixResourceManager()
          .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, lineageEntryId);
      fail();
    } catch (Exception e) {
      // expected
    }

    // Try after adding merged segments to the table
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "s5"), "downloadUrl");
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "s6"), "downloadUrl");

    TEST_INSTANCE.getHelixResourceManager()
        .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, lineageEntryId);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 1);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsFrom(), new ArrayList<>());
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsTo(), Arrays.asList("s5", "s6"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getState(), LineageEntryState.COMPLETED);

    // Start the new segment replacement
    segmentsFrom = Arrays.asList("s1", "s2");
    segmentsTo = Arrays.asList("merged_t1_0", "merged_t1_1");
    String lineageEntryId2 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, false);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 2);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getSegmentsFrom(), Arrays.asList("s1", "s2"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getSegmentsTo(),
        Arrays.asList("merged_t1_0", "merged_t1_1"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getState(), LineageEntryState.IN_PROGRESS);

    // Upload partial data
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "merged_t1_0"),
        "downloadUrl");

    IdealState idealState =
        TEST_INSTANCE.getHelixResourceManager().getTableIdealState(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertTrue(!idealState.getInstanceSet("merged_t1_0").isEmpty());

    // Try to revert the entry with partial data uploaded without forceRevert
    try {
      TEST_INSTANCE.getHelixResourceManager()
          .revertReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, lineageEntryId2, false);
      fail();
    } catch (Exception e) {
      // expected
    }

    // Try to revert the entry with partial data uploaded with forceRevert
    TEST_INSTANCE.getHelixResourceManager()
        .revertReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, lineageEntryId2, true);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getState(), LineageEntryState.REVERTED);

    // 'merged_t1_0' segment should be proactively cleaned up
    idealState =
        TEST_INSTANCE.getHelixResourceManager().getTableIdealState(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertTrue(idealState.getInstanceSet("merged_t1_0").isEmpty());

    // Start new segment replacement since the above entry is reverted
    segmentsFrom = Arrays.asList("s1", "s2");
    segmentsTo = Arrays.asList("merged_t2_0", "merged_t2_1");
    String lineageEntryId3 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, false);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 3);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getSegmentsFrom(), segmentsFrom);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getSegmentsTo(), segmentsTo);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getState(), LineageEntryState.IN_PROGRESS);

    // Upload partial data
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "merged_t2_0"),
        "downloadUrl");

    // Without force cleanup, 'startReplaceSegments' should fail because of duplicate segments on 'segmentFrom'.
    segmentsTo = Arrays.asList("merged_t3_0", "merged_t3_1");
    try {
      TEST_INSTANCE.getHelixResourceManager()
          .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, false);
      fail();
    } catch (Exception e) {
      // expected
    }

    // Test force clean up case
    String lineageEntryId4 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, true);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 4);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getSegmentsFrom(), Arrays.asList("s1", "s2"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getSegmentsTo(),
        Arrays.asList("merged_t2_0", "merged_t2_1"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getState(), LineageEntryState.REVERTED);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId4).getSegmentsFrom(), Arrays.asList("s1", "s2"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId4).getSegmentsTo(),
        Arrays.asList("merged_t3_0", "merged_t3_1"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId4).getState(), LineageEntryState.IN_PROGRESS);

    // 'merged_t2_0' segment should be proactively cleaned up
    idealState =
        TEST_INSTANCE.getHelixResourceManager().getTableIdealState(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertTrue(idealState.getInstanceSet("merged_t2_0").isEmpty());

    // Upload segments again
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "merged_t3_0"),
        "downloadUrl");
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "merged_t3_1"),
        "downloadUrl");

    // Finish the replacement
    TEST_INSTANCE.getHelixResourceManager()
        .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, lineageEntryId4);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 4);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId4).getSegmentsFrom(), Arrays.asList("s1", "s2"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId4).getSegmentsTo(),
        Arrays.asList("merged_t3_0", "merged_t3_1"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId4).getState(), LineageEntryState.COMPLETED);

    // Check empty segmentsFrom won't revert previous lineage with empty segmentsFrom
    // Start a new segment replacement with empty segmentsFrom.
    segmentsFrom = new ArrayList<>();
    segmentsTo = Arrays.asList("s7", "s8");
    String lineageEntryId5 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, false);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 5);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getSegmentsFrom(), segmentsFrom);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getSegmentsTo(), segmentsTo);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getState(), LineageEntryState.IN_PROGRESS);

    // Assuming the replacement fails in the middle, rerunning the protocol with the same segmentsTo will go through,
    // and remove the previous lineage entry.
    String lineageEntryId6 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, true);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 5);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getSegmentsFrom(), segmentsFrom);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getSegmentsTo(), segmentsTo);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getState(), LineageEntryState.IN_PROGRESS);

    // Upload partial data
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "s7"), "downloadUrl");

    // Start another new segment replacement with empty segmentsFrom,
    // and check that previous lineages with empty segmentsFrom are not reverted.
    segmentsTo = Arrays.asList("s9", "s10");
    String lineageEntryId7 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, true);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getState(), LineageEntryState.IN_PROGRESS);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getState(), LineageEntryState.IN_PROGRESS);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getState(), LineageEntryState.COMPLETED);

    // Finish the replacement
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "s9"), "downloadUrl");
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "s10"), "downloadUrl");

    TEST_INSTANCE.getHelixResourceManager()
        .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, lineageEntryId7);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 6);
    Assert.assertTrue(segmentLineage.getLineageEntry(lineageEntryId7).getSegmentsFrom().isEmpty());
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getSegmentsTo(), Arrays.asList("s9", "s10"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getState(), LineageEntryState.COMPLETED);

    // Check partial overlap reverts previous lineage
    // Start a new segment replacement with non-empty segmentsFrom.
    segmentsFrom = Arrays.asList("s9", "s10");
    segmentsTo = Arrays.asList("s11", "s12");
    String lineageEntryId8 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, false);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 7);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getSegmentsFrom(), segmentsFrom);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getSegmentsTo(), segmentsTo);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getState(), LineageEntryState.IN_PROGRESS);

    // Upload partial data
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "s11"), "downloadUrl");

    // Start another new segment replacement with segmentsFrom overlapping with previous lineage
    // and check that previous lineages with overlapped segmentsFrom are reverted.
    segmentsFrom = Arrays.asList("s0", "s9");
    segmentsTo = Arrays.asList("s13", "s14");
    String lineageEntryId9 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, segmentsFrom, segmentsTo, true);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getState(), LineageEntryState.REVERTED);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getState(), LineageEntryState.IN_PROGRESS);

    // Finish the replacement
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "s13"), "downloadUrl");
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, "s14"), "downloadUrl");

    TEST_INSTANCE.getHelixResourceManager()
        .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, lineageEntryId9);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 8);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getSegmentsFrom(), Arrays.asList("s0", "s9"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getSegmentsTo(), Arrays.asList("s13", "s14"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getState(), LineageEntryState.COMPLETED);

    // Check endReplaceSegments is idempotent
    TEST_INSTANCE.getHelixResourceManager()
        .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME, lineageEntryId9);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getState(), LineageEntryState.COMPLETED);
  }

  private void testSegmentReplacementForRefresh()
      throws IOException, InterruptedException {

    // Create the table
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", "DAILY"));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME)
            .setNumReplicas(2).setBrokerTenant(BROKER_TENANT_NAME).setServerTenant(SERVER_TENANT_NAME)
            .setIngestionConfig(ingestionConfig).build();

    TEST_INSTANCE.getHelixResourceManager().addTable(tableConfig);

    for (int i = 0; i < 3; i++) {
      TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s" + i),
          "downloadUrl");
    }
    List<String> segmentsForTable = TEST_INSTANCE.getHelixResourceManager()
        .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false);
    Assert.assertEquals(segmentsForTable.size(), 3);

    List<String> segmentsFrom = Arrays.asList("s0", "s1", "s2");
    List<String> segmentsTo = Arrays.asList("s3", "s4", "s5");

    // Start segment replacement protocol with (s0, s1, s2) -> (s3, s4, s5)
    String lineageEntryId = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, segmentsFrom, segmentsTo, false);
    SegmentLineage segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 1);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsFrom(),
        Arrays.asList("s0", "s1", "s2"));
    Assert
        .assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsTo(), Arrays.asList("s3", "s4", "s5"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getState(), LineageEntryState.IN_PROGRESS);
    Assert.assertEquals(new HashSet<>(TEST_INSTANCE.getHelixResourceManager()
            .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false)),
        new HashSet<>(Arrays.asList("s0", "s1", "s2")));

    // Add new segments
    for (int i = 3; i < 6; i++) {
      TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s" + i),
          "downloadUrl");
    }

    Assert.assertEquals(TEST_INSTANCE.getHelixResourceManager()
        .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false).size(), 6);
    Assert.assertEquals(new HashSet<>(TEST_INSTANCE.getHelixResourceManager()
            .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, true)),
        new HashSet<>(Arrays.asList("s0", "s1", "s2")));

    // Call end segment replacements
    TEST_INSTANCE.getHelixResourceManager()
        .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, lineageEntryId);

    Assert.assertEquals(TEST_INSTANCE.getHelixResourceManager()
        .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false).size(), 6);
    Assert.assertEquals(new HashSet<>(TEST_INSTANCE.getHelixResourceManager()
            .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, true)),
        new HashSet<>(Arrays.asList("s3", "s4", "s5")));

    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 1);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsFrom(),
        Arrays.asList("s0", "s1", "s2"));
    Assert
        .assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsTo(), Arrays.asList("s3", "s4", "s5"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getState(), LineageEntryState.COMPLETED);

    // Start the new protocol with "forceCleanup = false" so there will be no proactive clean-up happening
    // (s3, s4, s5) -> (s6, s7, s8)
    segmentsFrom = Arrays.asList("s3", "s4", "s5");
    segmentsTo = Arrays.asList("s6", "s7", "s8");
    String lineageEntryId2 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, segmentsFrom, segmentsTo, false);

    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 2);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getSegmentsFrom(),
        Arrays.asList("s3", "s4", "s5"));
    Assert
        .assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getSegmentsTo(), Arrays.asList("s6", "s7", "s8"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getState(), LineageEntryState.IN_PROGRESS);
    Assert.assertEquals(TEST_INSTANCE.getHelixResourceManager()
        .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false).size(), 6);
    Assert.assertEquals(new HashSet<>(TEST_INSTANCE.getHelixResourceManager()
            .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, true)),
        new HashSet<>(Arrays.asList("s3", "s4", "s5")));
    // Try to revert the first entry should fail
    try {
      TEST_INSTANCE.getHelixResourceManager()
          .revertReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, lineageEntryId, false);
      fail();
    } catch (Exception e) {
      // expected
    }

    // Add partial segments to indicate incomplete protocol
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s6"),
        "downloadUrl");

    Assert.assertEquals(TEST_INSTANCE.getHelixResourceManager()
        .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false).size(), 7);
    Assert.assertEquals(new HashSet<>(TEST_INSTANCE.getHelixResourceManager()
            .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, true)),
        new HashSet<>(Arrays.asList("s3", "s4", "s5")));

    // Start the new protocol (s3, s4, s5) -> (s9, s10, s11) with "forceCleanup = true" to check if 2 different
    // proactive clean-up mechanism works:
    //
    // 1. the previous lineage entry (s3, s4, s5) -> (s6, s7, s8) should be "REVERTED"
    // 2. the older segments (s0, s1, s2) need to be cleaned up because we are about to upload the 3rd data snapshot
    segmentsTo = Arrays.asList("s9", "s10", "s11");
    String lineageEntryId3 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, segmentsFrom, segmentsTo, true);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 3);

    // Check that the previous entry gets reverted
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getState(), LineageEntryState.REVERTED);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getSegmentsFrom(),
        Arrays.asList("s3", "s4", "s5"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getSegmentsTo(),
        Arrays.asList("s9", "s10", "s11"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getState(), LineageEntryState.IN_PROGRESS);

    // Check that the segments from the older lineage gets deleted
    Collections.disjoint(
        TEST_INSTANCE.getHelixResourceManager().getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false),
        Arrays.asList("s6", "s7", "s8"));
    Assert.assertEquals(TEST_INSTANCE.getHelixResourceManager()
        .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false).size(), 3);
    Assert.assertEquals(new HashSet<>(TEST_INSTANCE.getHelixResourceManager()
            .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, true)),
        new HashSet<>(Arrays.asList("s3", "s4", "s5")));

    // Try to invoke end segment replacement for the reverted entry
    try {
      TEST_INSTANCE.getHelixResourceManager()
          .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, lineageEntryId2);
      fail();
    } catch (Exception e) {
      // expected
    }

    // Add new segments
    for (int i = 9; i < 12; i++) {
      TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s" + i),
          "downloadUrl");
    }

    // Call end segment replacements
    TEST_INSTANCE.getHelixResourceManager()
        .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, lineageEntryId3);
    Assert.assertEquals(TEST_INSTANCE.getHelixResourceManager()
        .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false).size(), 6);
    Assert.assertEquals(new HashSet<>(TEST_INSTANCE.getHelixResourceManager()
            .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, true)),
        new HashSet<>(Arrays.asList("s9", "s10", "s11")));

    // We clean up "segmentsTo" for the lineage entry with "REVERTED" state in 2 places:
    // 1. revertReplaceSegments API will delete segmentsTo
    // 2. startReplaceSegments API will also try to clean up segmentsTo for REVERTED lineage

    // Call revert segment replacements (s3, s4, s5) <- (s9, s10, s11) to check if the revertReplaceSegments correctly
    // deleted (s9, s10, s11).
    TEST_INSTANCE.getHelixResourceManager()
        .revertReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, lineageEntryId3, false);
    waitForSegmentsToDelete(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, 3, TIMEOUT_IN_MS);
    Assert.assertEquals(new HashSet<>(TEST_INSTANCE.getHelixResourceManager()
            .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, true)),
        new HashSet<>(Arrays.asList("s3", "s4", "s5")));

    // Re-upload (s9, s10, s11) to test the segment clean up from startReplaceSegments.
    for (int i = 9; i < 12; i++) {
      TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s" + i),
          "downloadUrl");
    }
    Assert.assertEquals(TEST_INSTANCE.getHelixResourceManager()
        .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false).size(), 6);

    // Call startReplaceSegments with (s3, s4, s5) -> (s12, s13, s14). This call should clean up the (s9, s10, s11).
    segmentsTo = Arrays.asList("s12", "s13", "s14");
    String lineageEntryId4 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, segmentsFrom, segmentsTo, true);
    Collections.disjoint(
        TEST_INSTANCE.getHelixResourceManager().getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false),
        Arrays.asList("s9", "s10", "s11"));
    Assert.assertEquals(new HashSet<>(TEST_INSTANCE.getHelixResourceManager()
            .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false)),
        new HashSet<>(Arrays.asList("s3", "s4", "s5")));

    // Upload the new segments (s12, s13, s14)
    for (int i = 12; i < 15; i++) {
      TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s" + i),
          "downloadUrl");
    }

    // Call endReplaceSegments to start to use (s12, s13, s14)
    TEST_INSTANCE.getHelixResourceManager()
        .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, lineageEntryId4);
    Assert.assertEquals(new HashSet<>(TEST_INSTANCE.getHelixResourceManager()
            .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, false)),
        new HashSet<>(Arrays.asList("s3", "s4", "s5", "s12", "s13", "s14")));
    Assert.assertEquals(new HashSet<>(TEST_INSTANCE.getHelixResourceManager()
            .getSegmentsFor(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, true)),
        new HashSet<>(Arrays.asList("s12", "s13", "s14")));

    // Check empty segmentsFrom won't revert previous lineage with empty segmentsFrom
    // Start a new segment replacement with empty segmentsFrom.
    segmentsFrom = new ArrayList<>();
    segmentsTo = Arrays.asList("s15", "s16");
    String lineageEntryId5 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, segmentsFrom, segmentsTo, false);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getSegmentsFrom(), segmentsFrom);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getSegmentsTo(), segmentsTo);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getState(), LineageEntryState.IN_PROGRESS);

    // Upload partial data
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s15"),
        "downloadUrl");

    // Start another new segment replacement with empty segmentsFrom,
    // and check that previous lineages with empty segmentsFrom are not reverted.
    segmentsTo = Arrays.asList("s17", "s18");
    String lineageEntryId6 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, segmentsFrom, segmentsTo, true);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getState(), LineageEntryState.IN_PROGRESS);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getState(), LineageEntryState.IN_PROGRESS);

    // Finish the replacement
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s17"),
        "downloadUrl");
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s18"),
        "downloadUrl");

    TEST_INSTANCE.getHelixResourceManager()
        .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, lineageEntryId6);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert.assertTrue(segmentLineage.getLineageEntry(lineageEntryId6).getSegmentsFrom().isEmpty());
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getSegmentsTo(), Arrays.asList("s17", "s18"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getState(), LineageEntryState.COMPLETED);

    // Check partial overlap of segmentsFrom reverts previous lineage
    // Start a new segment replacement with non-empty segmentsFrom.
    segmentsFrom = Arrays.asList("s17", "s18");
    segmentsTo = Arrays.asList("s19", "s20");
    String lineageEntryId7 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, segmentsFrom, segmentsTo, false);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getSegmentsFrom(), segmentsFrom);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getSegmentsTo(), segmentsTo);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getState(), LineageEntryState.IN_PROGRESS);

    // Upload partial data
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s19"),
        "downloadUrl");

    // Start another new segment replacement with segmentsFrom overlapping with previous lineage
    // and check that previous lineages with overlapped segmentsFrom are reverted.
    segmentsFrom = Arrays.asList("s14", "s17");
    segmentsTo = Arrays.asList("s21", "s22");
    String lineageEntryId8 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, segmentsFrom, segmentsTo, true);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getState(), LineageEntryState.REVERTED);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getState(), LineageEntryState.IN_PROGRESS);

    // Finish the replacement
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s21"),
        "downloadUrl");
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s22"),
        "downloadUrl");

    TEST_INSTANCE.getHelixResourceManager()
        .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, lineageEntryId8);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getSegmentsFrom(), Arrays.asList("s14", "s17"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getSegmentsTo(), Arrays.asList("s21", "s22"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getState(), LineageEntryState.COMPLETED);

    // Check partial overlap of segmentsTo reverts previous lineage
    // Start a new segment replacement with non-empty segmentsFrom.
    segmentsFrom = Arrays.asList("s21", "s22");
    segmentsTo = Arrays.asList("s23", "s24");
    String lineageEntryId9 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, segmentsFrom, segmentsTo, false);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getSegmentsFrom(), segmentsFrom);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getSegmentsTo(), segmentsTo);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getState(), LineageEntryState.IN_PROGRESS);

    // Upload data
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s23"),
        "downloadUrl");
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s24"),
        "downloadUrl");

    // Start another new segment replacement with segmentsTo overlapping with previous lineage
    // and check that previous lineages with overlapped segmentsTo are reverted.
    segmentsFrom = Arrays.asList("s21", "s22");
    segmentsTo = Arrays.asList("s24", "s25");
    String lineageEntryId10 = TEST_INSTANCE.getHelixResourceManager()
        .startReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, segmentsFrom, segmentsTo, true);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getState(), LineageEntryState.REVERTED);
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getSegmentsTo(), Arrays.asList("s23"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getState(), LineageEntryState.IN_PROGRESS);

    // Finish the replacement
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s24"),
        "downloadUrl");
    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, "s25"),
        "downloadUrl");

    TEST_INSTANCE.getHelixResourceManager()
        .endReplaceSegments(OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME, lineageEntryId10);
    segmentLineage = SegmentLineageAccessHelper
        .getSegmentLineage(TEST_INSTANCE.getPropertyStore(), OFFLINE_SEGMENTS_REPLACE_TEST_REFRESH_TABLE_NAME);
    Assert
        .assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getSegmentsFrom(), Arrays.asList("s21", "s22"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getSegmentsTo(), Arrays.asList("s24", "s25"));
    Assert.assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getState(), LineageEntryState.COMPLETED);
  }

  private void waitForSegmentsToDelete(String tableNameWithType, int expectedNumSegmentsAfterDelete,
      long timeOutInMillis)
      throws InterruptedException {
    long endTimeMs = System.currentTimeMillis() + timeOutInMillis;
    do {
      if (TEST_INSTANCE.getHelixResourceManager().getSegmentsFor(tableNameWithType, false).size()
          == expectedNumSegmentsAfterDelete) {
        return;
      } else {
        Thread.sleep(500L);
      }
    } while (System.currentTimeMillis() < endTimeMs);
    throw new RuntimeException("Timeout while waiting for segments to be deleted");
  }

  @Test
  public void testGetTableToLiveBrokersMapping()
      throws IOException {
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 2, 0, 0);
    PinotResourceManagerResponse response =
        TEST_INSTANCE.getHelixResourceManager().createBrokerTenant(brokerTenant);
    Assert.assertTrue(response.isSuccessful());
    // Create the table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNumReplicas(TEST_INSTANCE.MIN_NUM_REPLICAS).setBrokerTenant(BROKER_TENANT_NAME)
        .setServerTenant(SERVER_TENANT_NAME).build();
    TEST_INSTANCE.getHelixResourceManager().addTable(tableConfig);
    // Introduce a wait here for the EV is updated with live brokers for a table.
    TestUtils.waitForCondition(aVoid -> {
      ExternalView externalView = TEST_INSTANCE.getHelixResourceManager().getHelixAdmin()
          .getResourceExternalView(TEST_INSTANCE.getHelixClusterName(),
              CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      int onlineBrokersCnt = 0;
      Map<String, String> brokerToStateMap = externalView.getStateMap(OFFLINE_TABLE_NAME);
      if (brokerToStateMap == null) {
        return false;
      }
      for (Map.Entry<String, String> entry : brokerToStateMap.entrySet()) {
        if ("ONLINE".equalsIgnoreCase(entry.getValue())) {
          onlineBrokersCnt++;
        }
      }
      return onlineBrokersCnt == 2;
    }, TIMEOUT_IN_MS, "");

    Map<String, List<InstanceInfo>> tableToBrokersMapping =
        TEST_INSTANCE.getHelixResourceManager().getTableToLiveBrokersMapping();

    Assert.assertEquals(tableToBrokersMapping.size(), 1);
    Assert.assertEquals(tableToBrokersMapping.get(OFFLINE_TABLE_NAME).size(), 2);

    // Delete the table
    TEST_INSTANCE.getHelixResourceManager().deleteOfflineTable(TABLE_NAME);
    TEST_INSTANCE.getHelixResourceManager().deleteRealtimeTable(TABLE_NAME);
    // Clean up.
    untagBrokers();
  }

  @Test
  public void testGetLiveBrokersForTable()
      throws IOException, TableNotFoundException {
    // Create broker tenant
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 2, 0, 0);
    PinotResourceManagerResponse response =
        TEST_INSTANCE.getHelixResourceManager().createBrokerTenant(brokerTenant);
    Assert.assertTrue(response.isSuccessful());
    // Create the table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNumReplicas(TEST_INSTANCE.MIN_NUM_REPLICAS).setBrokerTenant(BROKER_TENANT_NAME)
        .setServerTenant(SERVER_TENANT_NAME).build();
    TEST_INSTANCE.getHelixResourceManager().addTable(tableConfig);
    // Introduce a wait here for the EV is updated with live brokers for a table.
    TestUtils.waitForCondition(aVoid -> {
      ExternalView externalView = TEST_INSTANCE.getHelixResourceManager().getHelixAdmin()
          .getResourceExternalView(TEST_INSTANCE.getHelixClusterName(),
              CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      int onlineBrokersCnt = 0;
      Map<String, String> brokerToStateMap = externalView.getStateMap(OFFLINE_TABLE_NAME);
      if (brokerToStateMap == null) {
        return false;
      }
      for (Map.Entry<String, String> entry : brokerToStateMap.entrySet()) {
        if ("ONLINE".equalsIgnoreCase(entry.getValue())) {
          onlineBrokersCnt++;
        }
      }
      return onlineBrokersCnt == 2;
    }, TIMEOUT_IN_MS, "");
    // Test retrieving the live broker for table
    List<String> liveBrokersForTable =
        TEST_INSTANCE.getHelixResourceManager().getLiveBrokersForTable(OFFLINE_TABLE_NAME);
    Assert.assertEquals(liveBrokersForTable.size(), 2);
    for (String broker : liveBrokersForTable) {
      Assert.assertTrue(broker.startsWith("Broker_localhost"));
    }

    // Test retrieving the live broker for table without table-type suffix.
    liveBrokersForTable = TEST_INSTANCE.getHelixResourceManager().getLiveBrokersForTable(TABLE_NAME);
    Assert.assertEquals(liveBrokersForTable.size(), 2);

    // Test retrieving the live broker for table with non-existent table-type.
    try {
      TEST_INSTANCE.getHelixResourceManager().getLiveBrokersForTable(REALTIME_TABLE_NAME);
      Assert.fail("Method call above should have failed");
    } catch (TableNotFoundException tableNotFoundException) {
      Assert.assertTrue(tableNotFoundException.getMessage().contains(REALTIME_TABLE_NAME));
    }

    // Create the realtime table.
    TEST_INSTANCE.addDummySchema(REALTIME_TABLE_NAME);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setNumReplicas(TEST_INSTANCE.MIN_NUM_REPLICAS).setBrokerTenant(BROKER_TENANT_NAME)
        .setStreamConfigs(FakeStreamConfigUtils.getDefaultHighLevelStreamConfigs().getStreamConfigsMap())
        .setSchemaName(REALTIME_TABLE_NAME).setServerTenant(SERVER_TENANT_NAME).build();
    TEST_INSTANCE.getHelixResourceManager().addTable(tableConfig);
    // Wait for EV to be updated with realtime table.
    TestUtils.waitForCondition(aVoid -> {
      ExternalView externalView = TEST_INSTANCE.getHelixResourceManager().getHelixAdmin()
          .getResourceExternalView(TEST_INSTANCE.getHelixClusterName(),
              CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      int onlineBrokersCnt = 0;
      Map<String, String> brokerToStateMap = externalView.getStateMap(REALTIME_TABLE_NAME);
      if (brokerToStateMap == null) {
        return false;
      }
      for (Map.Entry<String, String> entry : brokerToStateMap.entrySet()) {
        if ("ONLINE".equalsIgnoreCase(entry.getValue())) {
          onlineBrokersCnt++;
        }
      }
      return onlineBrokersCnt == 2;
    }, TIMEOUT_IN_MS, "");

    // Test retrieving using table name without type suffix.
    liveBrokersForTable = TEST_INSTANCE.getHelixResourceManager().getLiveBrokersForTable(TABLE_NAME);
    Assert.assertEquals(liveBrokersForTable.size(), 2);

    // Test case when table with given name doesn't exist.
    String fakeNonExistentTableName = "fake_non_existent_table_name";
    try {
      TEST_INSTANCE.getHelixResourceManager().getLiveBrokersForTable(fakeNonExistentTableName);
      Assert.fail("Method call above should have failed");
    } catch (TableNotFoundException tableNotFoundException) {
      Assert.assertTrue(tableNotFoundException.getMessage().contains(fakeNonExistentTableName));
    }

    try {
      TEST_INSTANCE.getHelixResourceManager().getLiveBrokersForTable(fakeNonExistentTableName + "_OFFLINE");
      Assert.fail("Method call above should have failed");
    } catch (TableNotFoundException tableNotFoundException) {
      Assert.assertTrue(tableNotFoundException.getMessage().contains(fakeNonExistentTableName + "_OFFLINE"));
    }

    // Delete the table
    TEST_INSTANCE.getHelixResourceManager().deleteOfflineTable(TABLE_NAME);
    TEST_INSTANCE.getHelixResourceManager().deleteRealtimeTable(TABLE_NAME);
    // Clean up.
    untagBrokers();
  }

  private void untagBrokers() {
    for (String brokerInstance : TEST_INSTANCE.getHelixResourceManager()
        .getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      TEST_INSTANCE.getHelixAdmin().removeInstanceTag(TEST_INSTANCE.getHelixClusterName(), brokerInstance,
          TagNameUtils.getBrokerTagForTenant(BROKER_TENANT_NAME));
      TEST_INSTANCE.getHelixAdmin().addInstanceTag(TEST_INSTANCE.getHelixClusterName(), brokerInstance,
          CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
