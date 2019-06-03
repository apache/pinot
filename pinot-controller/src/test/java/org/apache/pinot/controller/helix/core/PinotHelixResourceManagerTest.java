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
import java.util.ArrayList;
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
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.config.TagOverrideConfig;
import org.apache.pinot.common.config.Tenant;
import org.apache.pinot.common.config.TenantConfig;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.TenantRole;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerRequestBuilderUtil;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Helix.*;
import static org.apache.pinot.controller.helix.core.PinotHelixResourceManager.*;


public class PinotHelixResourceManagerTest extends ControllerTest {
  private static final int BASE_SERVER_ADMIN_PORT = 10000;
  private static final int NUM_INSTANCES = 5;
  private static final String BROKER_TENANT_NAME = "brokerTenant";
  private static final String SERVER_TENANT_NAME = "serverTenant";
  private static final String TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
  private static final int CONNECTION_TIMEOUT_IN_MILLISECOND = 10_000;
  private static final int MAX_TIMEOUT_IN_MILLISECOND = 5_000;
  private static final int MAXIMUM_NUMBER_OF_CONTROLLER_INSTANCES = 10;

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

    _helixAdmin.enableResource(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, true);
  }

  @Test
  public void testGetInstanceEndpoints() throws InvalidConfigException {
    Set<String> servers = _helixResourceManager.getAllInstancesForServerTenant(SERVER_TENANT_NAME);
    BiMap<String, String> endpoints = _helixResourceManager.getDataInstanceAdminEndpoints(servers);
    for (int i = 0; i < NUM_INSTANCES; i++) {
      Assert.assertTrue(endpoints.inverse().containsKey("localhost:" + String.valueOf(BASE_SERVER_ADMIN_PORT + i)));
    }
  }

  @Test
  public void testGetInstanceConfigs() throws Exception {
    Set<String> servers = _helixResourceManager.getAllInstancesForServerTenant(SERVER_TENANT_NAME);
    for (String server : servers) {
      InstanceConfig cachedInstanceConfig = _helixResourceManager.getHelixInstanceConfig(server);
      InstanceConfig realInstanceConfig = _helixAdmin.getInstanceConfig(_helixClusterName, server);
      Assert.assertEquals(cachedInstanceConfig, realInstanceConfig);
    }

    ZkClient zkClient = new ZkClient(_helixResourceManager.getHelixZkURL(), CONNECTION_TIMEOUT_IN_MILLISECOND,
        CONNECTION_TIMEOUT_IN_MILLISECOND, new ZNRecordSerializer());

    modifyExistingInstanceConfig(zkClient);
    addAndRemoveNewInstanceConfig(zkClient);

    zkClient.close();
  }

  private void modifyExistingInstanceConfig(ZkClient zkClient) throws InterruptedException {
    String instanceName = "Server_localhost_" + new Random().nextInt(NUM_INSTANCES);
    String instanceConfigPath = PropertyPathBuilder.instanceConfig(_helixClusterName, instanceName);
    Assert.assertTrue(zkClient.exists(instanceConfigPath));
    ZNRecord znRecord = zkClient.readData(instanceConfigPath, null);

    InstanceConfig cachedInstanceConfig = _helixResourceManager.getHelixInstanceConfig(instanceName);
    String originalPort = cachedInstanceConfig.getPort();
    Assert.assertNotNull(originalPort);
    String newPort = Long.toString(System.currentTimeMillis());
    Assert.assertTrue(!newPort.equals(originalPort));

    // Set new port to this instance config.
    znRecord.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_PORT.toString(), newPort);
    zkClient.writeData(instanceConfigPath, znRecord);

    long maxTime = System.currentTimeMillis() + MAX_TIMEOUT_IN_MILLISECOND;
    InstanceConfig latestCachedInstanceConfig = _helixResourceManager.getHelixInstanceConfig(instanceName);
    String latestPort = latestCachedInstanceConfig.getPort();
    while (!newPort.equals(latestPort) && System.currentTimeMillis() < maxTime) {
      Thread.sleep(100L);
      latestCachedInstanceConfig = _helixResourceManager.getHelixInstanceConfig(instanceName);
      latestPort = latestCachedInstanceConfig.getPort();
    }
    Assert.assertTrue(System.currentTimeMillis() < maxTime, "Timeout when waiting for adding instance config");

    // Set original port back to this instance config.
    znRecord.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_PORT.toString(), originalPort);
    zkClient.writeData(instanceConfigPath, znRecord);
  }

  private void addAndRemoveNewInstanceConfig(ZkClient zkClient) throws Exception {
    int biggerRandomNumber = NUM_INSTANCES + new Random().nextInt(NUM_INSTANCES);
    String instanceName = "Server_localhost_" + String.valueOf(biggerRandomNumber);
    String instanceConfigPath = PropertyPathBuilder.instanceConfig(_helixClusterName, instanceName);
    Assert.assertFalse(zkClient.exists(instanceConfigPath));
    List<String> instances = _helixResourceManager.getAllInstances();
    Assert.assertFalse(instances.contains(instanceName));

    // Add new ZNode.
    ZNRecord znRecord = new ZNRecord(instanceName);
    zkClient.createPersistent(instanceConfigPath, znRecord);

    List<String> latestAllInstances = _helixResourceManager.getAllInstances();
    long maxTime = System.currentTimeMillis() + MAX_TIMEOUT_IN_MILLISECOND;
    while (!latestAllInstances.contains(instanceName) && System.currentTimeMillis() < maxTime) {
      Thread.sleep(100L);
      latestAllInstances = _helixResourceManager.getAllInstances();
    }
    Assert.assertTrue(System.currentTimeMillis() < maxTime, "Timeout when waiting for adding instance config");

    // Remove new ZNode.
    zkClient.delete(instanceConfigPath);

    latestAllInstances = _helixResourceManager.getAllInstances();
    maxTime = System.currentTimeMillis() + MAX_TIMEOUT_IN_MILLISECOND;
    while (latestAllInstances.contains(instanceName) && System.currentTimeMillis() < maxTime) {
      Thread.sleep(100L);
      latestAllInstances = _helixResourceManager.getAllInstances();
    }
    Assert.assertTrue(System.currentTimeMillis() < maxTime, "Timeout when waiting for removing instance config");
  }

  @Test
  public void testRebuildBrokerResourceFromHelixTags() throws Exception {
    // Create broker tenant on 3 Brokers
    Tenant brokerTenant =
        new Tenant.TenantBuilder(BROKER_TENANT_NAME).setRole(TenantRole.BROKER).setTotalInstances(3).build();
    _helixResourceManager.createBrokerTenant(brokerTenant);

    // Create the table
    TableConfig tableConfig = new TableConfig.Builder(TableType.OFFLINE).setTableName(TABLE_NAME)
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
          TagNameUtils.getBrokerTagForTenant(BROKER_TENANT_NAME));
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
  void testRetrieveTenantNames() {
    // Create broker tenant on 1 Broker
    Tenant brokerTenant =
        new Tenant.TenantBuilder(BROKER_TENANT_NAME).setRole(TenantRole.BROKER).setTotalInstances(1).build();
    _helixResourceManager.createBrokerTenant(brokerTenant);

    Set<String> brokerTenantNames = _helixResourceManager.getAllBrokerTenantNames();
    Assert.assertEquals(brokerTenantNames.size(), 1);
    Assert.assertEquals(brokerTenantNames.iterator().next(), BROKER_TENANT_NAME);

    String testBrokerInstance =
        _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME).iterator().next();
    _helixAdmin.addInstanceTag(_helixClusterName, testBrokerInstance, "wrong_tag");

    brokerTenantNames = _helixResourceManager.getAllBrokerTenantNames();
    Assert.assertEquals(brokerTenantNames.size(), 1);
    Assert.assertEquals(brokerTenantNames.iterator().next(), BROKER_TENANT_NAME);

    _helixAdmin.removeInstanceTag(_helixClusterName, testBrokerInstance, "wrong_tag");

    // Server tenant is already created during setup.
    Set<String> serverTenantNames = _helixResourceManager.getAllServerTenantNames();
    Assert.assertEquals(serverTenantNames.size(), 1);
    Assert.assertEquals(serverTenantNames.iterator().next(), SERVER_TENANT_NAME);

    String testServerInstance =
        _helixResourceManager.getAllInstancesForServerTenant(SERVER_TENANT_NAME).iterator().next();
    _helixAdmin.addInstanceTag(_helixClusterName, testServerInstance, "wrong_tag");

    serverTenantNames = _helixResourceManager.getAllServerTenantNames();
    Assert.assertEquals(serverTenantNames.size(), 1);
    Assert.assertEquals(serverTenantNames.iterator().next(), SERVER_TENANT_NAME);

    _helixAdmin.removeInstanceTag(_helixClusterName, testServerInstance, "wrong_tag");
  }

  @Test
  public void testValidateTenantConfigs() {
    String tableNameWithType = "testTable_OFFLINE";
    TableType tableType = TableType.OFFLINE;
    int numReplica = 2;
    TableConfig tableConfig = null;
    String brokerTag = "aBrokerTag";
    String serverTag = "aServerTag";

    // null table config
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e1) {
      // expected
    } catch (Exception e2) {
      Assert.fail("Expected InvalidTableConfigException");
    }

    // null tenant config
    tableConfig = new TableConfig.Builder(tableType).setTableName(tableNameWithType).setNumReplicas(numReplica).build();
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e1) {
      // expected
    } catch (Exception e2) {
      Assert.fail("Expected InvalidTableConfigException");
    }

    // null broker tenant
    TenantConfig tenantConfig = new TenantConfig();
    tenantConfig.setServer(serverTag);
    tableConfig.setTenantConfig(tenantConfig);
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e1) {
      // expected
    } catch (Exception e2) {
      Assert.fail("Expected InvalidTableConfigException");
    }

    // null server tenant
    tenantConfig.setServer(null);
    tenantConfig.setBroker(brokerTag);
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e1) {
      // expected
    } catch (Exception e2) {
      Assert.fail("Expected InvalidTableConfigException");
    }

    // empty broker instances list
    tenantConfig.setServer(serverTag);
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e1) {
      // expected
    } catch (Exception e2) {
      Assert.fail("Expected InvalidTableConfigException");
    }

    // Create broker tenant on 3 Brokers
    Tenant brokerTenant = new Tenant.TenantBuilder(brokerTag).setRole(TenantRole.BROKER).setTotalInstances(3).build();
    _helixResourceManager.createBrokerTenant(brokerTenant);

    // empty server instances list
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e1) {
      // expected
    } catch (Exception e2) {
      Assert.fail("Expected InvalidTableConfigException");
    }

    // valid tenant config, null tagOverrideConfig
    serverTag = SERVER_TENANT_NAME;
    tenantConfig.setServer(serverTag);
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
    } catch (Exception e2) {
      Assert.fail("No exceptions expected");
    }

    // valid tagOverride config
    TagOverrideConfig tagOverrideConfig = new TagOverrideConfig();
    tenantConfig.setTagOverrideConfig(tagOverrideConfig);
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
    } catch (Exception e2) {
      Assert.fail("No exceptions expected");
    }

    // incorrect realtime consuming tag suffix
    tagOverrideConfig.setRealtimeConsuming("incorrectTag_XXX");
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e1) {
      // expected
    } catch (Exception e2) {
      Assert.fail("Expected InvalidTableConfigException");
    }

    // incorrect realtime consuming tag suffix
    tagOverrideConfig.setRealtimeConsuming("correctTagEmptyList_OFFLINE");
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e1) {
      // expected
    } catch (Exception e2) {
      Assert.fail("Expected InvalidTableConfigException");
    }

    // incorrect realtime completed tag suffix
    tagOverrideConfig.setRealtimeConsuming(serverTag + "_OFFLINE");
    tagOverrideConfig.setRealtimeCompleted("incorrectTag_XXX");
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e1) {
      // expected
    } catch (Exception e2) {
      Assert.fail("Expected InvalidTableConfigException");
    }

    // empty list in realtime completed
    tagOverrideConfig.setRealtimeCompleted("correctTagEmptyList_OFFLINE");
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e1) {
      // expected
    } catch (Exception e2) {
      Assert.fail("Expected InvalidTableConfigException");
    }

    // all good
    tagOverrideConfig.setRealtimeCompleted(serverTag + "_OFFLINE");
    try {
      _helixResourceManager.validateTableTenantConfig(tableConfig, tableNameWithType, tableType);
    } catch (Exception e2) {
      Assert.fail("No exceptions expected");
    }

    for (String brokerInstance : _helixResourceManager.getAllInstancesForBrokerTenant(brokerTag)) {
      _helixAdmin.removeInstanceTag(_helixClusterName, brokerInstance, TagNameUtils.getBrokerTagForTenant(brokerTag));
      _helixAdmin.addInstanceTag(_helixClusterName, brokerInstance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }
  }

  @Test
  public void testLeadControllerResource() {
    IdealState leadControllerResourceIdealState = _helixResourceManager.getHelixAdmin()
        .getResourceIdealState(getHelixClusterName(), CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    Assert.assertTrue(leadControllerResourceIdealState.isValid());
    Assert.assertTrue(leadControllerResourceIdealState.isEnabled());
    Assert.assertEquals(leadControllerResourceIdealState.getInstanceGroupTag(),
        CommonConstants.Helix.CONTROLLER_INSTANCE_TYPE);
    Assert.assertEquals(leadControllerResourceIdealState.getNumPartitions(),
        CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
    Assert.assertEquals(leadControllerResourceIdealState.getReplicas(),
        Integer.toString(LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT));
    Assert.assertEquals(leadControllerResourceIdealState.getRebalanceMode(), IdealState.RebalanceMode.FULL_AUTO);
    Assert.assertTrue(leadControllerResourceIdealState.getInstanceSet(
        leadControllerResourceIdealState.getPartitionSet().iterator().next()).isEmpty());

    ExternalView leadControllerResourceExternalView = _helixResourceManager.getHelixAdmin()
        .getResourceExternalView(getHelixClusterName(), CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    for (String partition : leadControllerResourceExternalView.getPartitionSet()) {
      Map<String, String> stateMap = leadControllerResourceExternalView.getStateMap(partition);
      Assert.assertEquals(stateMap.size(), 1);
      Map.Entry<String, String> entry = stateMap.entrySet().iterator().next();
      Assert.assertEquals(entry.getKey(), PREFIX_OF_CONTROLLER_INSTANCE + LOCAL_HOST + "_" + _controllerPort);
      Assert.assertEquals(entry.getValue(), "MASTER");
    }
  }

  @Test
  public void testLeadControllerAssignment() {
    // Given a number of instances (from 1 to 10), make sure all the instances got assigned to lead controller resource.
    for (int nInstances = 1; nInstances <= MAXIMUM_NUMBER_OF_CONTROLLER_INSTANCES; nInstances++) {
      List<String> instanceNames = new ArrayList<>(nInstances);
      List<Integer> ports = new ArrayList<>(nInstances);
      for (int i = 0; i < nInstances; i++) {
        instanceNames.add(PREFIX_OF_CONTROLLER_INSTANCE + LOCAL_HOST + "_" + i);
        ports.add(i);
      }

      List<String> partitions = new ArrayList<>(NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
      for (int i = 0; i < NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE; i++) {
        partitions.add(LEAD_CONTROLLER_RESOURCE_NAME + "_" + i);
      }

      LinkedHashMap<String, Integer> states = new LinkedHashMap<>(2);
      states.put("OFFLINE", 0);
      states.put("SLAVE", LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT - 1);
      states.put("MASTER", 1);

      CrushEdRebalanceStrategy crushEdRebalanceStrategy = new CrushEdRebalanceStrategy();
      crushEdRebalanceStrategy.init(LEAD_CONTROLLER_RESOURCE_NAME, partitions, states, Integer.MAX_VALUE);

      ClusterDataCache clusterDataCache = new ClusterDataCache();
      PropertyKey.Builder keyBuilder = new PropertyKey.Builder(getHelixClusterName());
      HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
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

  @AfterMethod
  public void cleanUpBrokerTags() {
    // Untag all Brokers for other tests
    for (String brokerInstance : _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      _helixAdmin.removeInstanceTag(_helixClusterName, brokerInstance,
          TagNameUtils.getBrokerTagForTenant(BROKER_TENANT_NAME));
      _helixAdmin.addInstanceTag(_helixClusterName, brokerInstance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
