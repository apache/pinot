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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.EndReplaceSegmentsRequest;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.InvalidTableConfigException;
import org.apache.pinot.controller.api.resources.InstanceInfo;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Segment;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


@Test(groups = "stateless")
public class PinotHelixResourceManagerStatelessTest extends ControllerTest {
  private static final int NUM_BROKER_INSTANCES = 3;
  private static final int NUM_OFFLINE_SERVER_INSTANCES = 2;
  private static final int NUM_REALTIME_SERVER_INSTANCES = 2;
  private static final int NUM_SERVER_INSTANCES = NUM_OFFLINE_SERVER_INSTANCES + NUM_REALTIME_SERVER_INSTANCES;
  private static final String BROKER_TENANT_NAME = "brokerTenant";
  private static final String SERVER_TENANT_NAME = "serverTenant";

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
    startController(properties);

    addFakeBrokerInstancesToAutoJoinHelixCluster(NUM_BROKER_INSTANCES, false);
    addFakeServerInstancesToAutoJoinHelixCluster(NUM_SERVER_INSTANCES, false);

    resetBrokerTags();
    resetServerTags();
    addDummySchema(RAW_TABLE_NAME);
  }

  private void untagBrokers() {
    for (String brokerInstance : _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      _helixResourceManager.updateInstanceTags(brokerInstance, Helix.UNTAGGED_BROKER_INSTANCE, false);
    }
  }

  private void resetBrokerTags() {
    untagBrokers();
    assertEquals(_helixResourceManager.getOnlineUnTaggedBrokerInstanceList().size(), NUM_BROKER_INSTANCES);
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, NUM_BROKER_INSTANCES, 0, 0);
    _helixResourceManager.createBrokerTenant(brokerTenant);
    assertEquals(_helixResourceManager.getOnlineUnTaggedBrokerInstanceList().size(), 0);
  }

  private void untagServers() {
    for (String serverInstance : _helixResourceManager.getAllInstancesForServerTenant(SERVER_TENANT_NAME)) {
      _helixResourceManager.updateInstanceTags(serverInstance, Helix.UNTAGGED_SERVER_INSTANCE, false);
    }
  }

  private void resetServerTags() {
    untagServers();
    assertEquals(_helixResourceManager.getOnlineUnTaggedServerInstanceList().size(), NUM_SERVER_INSTANCES);
    Tenant serverTenant =
        new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, NUM_SERVER_INSTANCES, NUM_OFFLINE_SERVER_INSTANCES,
            NUM_REALTIME_SERVER_INSTANCES);
    _helixResourceManager.createServerTenant(serverTenant);
    assertEquals(_helixResourceManager.getOnlineUnTaggedServerInstanceList().size(), 0);
  }

  public void testGetInstancesByTag() {
    List<String> controllersByTag = _helixResourceManager.getAllInstancesWithTag("controller");
    List<InstanceConfig> controllerConfigs = _helixResourceManager.getAllControllerInstanceConfigs();

    assertEquals(controllersByTag.size(), controllerConfigs.size());
    for (InstanceConfig c: controllerConfigs) {
      assertTrue(controllersByTag.contains(c.getInstanceName()));
    }
  }

  @Test
  public void testGetDataInstanceAdminEndpoints()
      throws Exception {
    Set<String> servers = _helixResourceManager.getAllInstancesForServerTenant(SERVER_TENANT_NAME);

    BiMap<String, String> adminEndpoints = _helixResourceManager.getDataInstanceAdminEndpoints(servers);
    assertEquals(adminEndpoints.size(), NUM_SERVER_INSTANCES);
    for (Map.Entry<String, String> entry : adminEndpoints.entrySet()) {
      String key = entry.getKey();
      int port = Server.DEFAULT_ADMIN_API_PORT + Integer.parseInt(key.substring("Server_localhost_".length()));
      // ports are random generated
      assertTrue(port > 0);
    }

    // Add a new server
    String serverName = "Server_localhost_" + NUM_SERVER_INSTANCES;
    Instance instance = new Instance("localhost", NUM_SERVER_INSTANCES, InstanceType.SERVER,
        Collections.singletonList(Helix.UNTAGGED_SERVER_INSTANCE), null, 0, 12345, 0, 0, false);
    _helixResourceManager.addInstance(instance, false);
    adminEndpoints = _helixResourceManager.getDataInstanceAdminEndpoints(Collections.singleton(serverName));
    assertEquals(adminEndpoints.size(), 1);
    assertEquals(adminEndpoints.get(serverName), "http://localhost:12345");

    // Modify the admin port for the new added server
    instance = new Instance("localhost", NUM_SERVER_INSTANCES, InstanceType.SERVER,
        Collections.singletonList(Helix.UNTAGGED_SERVER_INSTANCE), null, 0, 23456, 0, 0, false);
    _helixResourceManager.updateInstance(serverName, instance, false);
    // Admin endpoint is updated through the instance config change callback, which happens asynchronously
    TestUtils.waitForCondition(aVoid -> {
      try {
        BiMap<String, String> endpoints =
            _helixResourceManager.getDataInstanceAdminEndpoints(Collections.singleton(serverName));
        assertEquals(endpoints.size(), 1);
        return endpoints.get(serverName).equals("http://localhost:23456");
      } catch (InvalidConfigException e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to update the admin port");

    // Remove the new added server
    assertTrue(_helixResourceManager.dropInstance(serverName).isSuccessful());
    TestUtils.waitForCondition(aVoid -> {
      try {
        _helixResourceManager.getDataInstanceAdminEndpoints(Collections.singleton(serverName));
        return false;
      } catch (InvalidConfigException e) {
        return true;
      }
    }, 60_000L, "Failed to remove the admin endpoint");
  }

  @Test
  public void testAddRemoveInstance() {
    Set<String> serverInstances = _helixResourceManager.getAllInstancesForServerTenant(SERVER_TENANT_NAME);
    for (String instanceName : serverInstances) {
      InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(instanceName);
      assertNotNull(instanceConfig);
      assertEquals(instanceConfig.getHostName(), "Server_localhost");
      assertEquals(instanceConfig.getPort(), instanceName.substring("Server_localhost_".length()));
    }

    // Add a new instance
    String instanceName = "Server_localhost_" + NUM_SERVER_INSTANCES;
    List<String> allInstances = _helixResourceManager.getAllInstances();
    assertFalse(allInstances.contains(instanceName));
    List<String> allLiveInstances = _helixResourceManager.getAllLiveInstances();
    assertFalse(allLiveInstances.contains(instanceName));

    Instance instance = new Instance("localhost", NUM_SERVER_INSTANCES, InstanceType.SERVER,
        Collections.singletonList(Helix.UNTAGGED_SERVER_INSTANCE), null, 0, 0, 0, 0, false);
    _helixResourceManager.addInstance(instance, false);
    allInstances = _helixResourceManager.getAllInstances();
    assertTrue(allInstances.contains(instanceName));

    // Remove the added instance
    assertTrue(_helixResourceManager.dropInstance(instanceName).isSuccessful());
    allInstances = _helixResourceManager.getAllInstances();
    assertFalse(allInstances.contains(instanceName));
    allLiveInstances = _helixResourceManager.getAllLiveInstances();
    assertFalse(allLiveInstances.contains(instanceName));
  }

  @Test
  public void testUpdateBrokerResource()
      throws Exception {
    // Create broker tenant on 2 brokers
    untagBrokers();
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 2, 0, 0);
    _helixResourceManager.createBrokerTenant(brokerTenant);

    String brokerTag = TagNameUtils.getBrokerTagForTenant(BROKER_TENANT_NAME);
    List<InstanceConfig> instanceConfigs = HelixHelper.getInstanceConfigs(_helixManager);
    List<String> taggedBrokers = HelixHelper.getInstancesWithTag(instanceConfigs, brokerTag);
    assertEquals(taggedBrokers.size(), 2);
    List<String> untaggedBrokers = HelixHelper.getInstancesWithTag(instanceConfigs, Helix.UNTAGGED_BROKER_INSTANCE);
    assertEquals(untaggedBrokers.size(), 1);

    // Add a table
    addDummySchema(RAW_TABLE_NAME);
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).build();
    waitForEVToDisappear(offlineTableConfig.getTableName());
    _helixResourceManager.addTable(offlineTableConfig);
    checkBrokerResource(taggedBrokers);

    // Untag a tagged broker with instance update
    String brokerToUntag = taggedBrokers.remove(ThreadLocalRandom.current().nextInt(taggedBrokers.size()));
    Instance instance =
        new Instance("localhost", brokerToUntag.charAt(brokerToUntag.length() - 1) - '0', InstanceType.BROKER,
            Collections.singletonList(Helix.UNTAGGED_BROKER_INSTANCE), null, 0, 0, 0, 0, false);
    assertTrue(_helixResourceManager.updateInstance(brokerToUntag, instance, true).isSuccessful());
    untaggedBrokers.add(brokerToUntag);
    checkBrokerResource(taggedBrokers);

    // Tag an untagged broker with tags update
    String brokerToTag = untaggedBrokers.remove(ThreadLocalRandom.current().nextInt(untaggedBrokers.size()));
    assertTrue(_helixResourceManager.updateInstanceTags(brokerToTag, brokerTag, true).isSuccessful());
    taggedBrokers.add(brokerToTag);
    checkBrokerResource(taggedBrokers);

    // Add a new broker instance
    Instance newBrokerInstance =
        new Instance("localhost", 3, InstanceType.BROKER, Collections.singletonList(brokerTag), null, 0, 0, 0, 0,
            false);
    assertTrue(_helixResourceManager.addInstance(newBrokerInstance, true).isSuccessful());
    String newBrokerId = InstanceUtils.getHelixInstanceId(newBrokerInstance);
    taggedBrokers.add(newBrokerId);
    checkBrokerResource(taggedBrokers);

    // Untag the new broker and update the broker resource
    assertTrue(
        _helixResourceManager.updateInstanceTags(newBrokerId, Helix.UNTAGGED_BROKER_INSTANCE, false).isSuccessful());
    assertTrue(_helixResourceManager.updateBrokerResource(newBrokerId).isSuccessful());
    taggedBrokers.remove(taggedBrokers.size() - 1);
    checkBrokerResource(taggedBrokers);

    // Drop the new broker and delete the table
    assertTrue(_helixResourceManager.dropInstance(newBrokerId).isSuccessful());
    _helixResourceManager.deleteOfflineTable(OFFLINE_TABLE_NAME);
    IdealState brokerResource = HelixHelper.getBrokerIdealStates(_helixAdmin, _clusterName);
    assertTrue(brokerResource.getPartitionSet().isEmpty());

    resetBrokerTags();
  }

  private void checkBrokerResource(List<String> expectedBrokers) {
    IdealState brokerResource = HelixHelper.getBrokerIdealStates(_helixAdmin, _clusterName);
    assertEquals(brokerResource.getPartitionSet().size(), 1);
    Map<String, String> instanceStateMap = brokerResource.getInstanceStateMap(OFFLINE_TABLE_NAME);
    assertEquals(instanceStateMap.keySet(), new HashSet<>(expectedBrokers));
  }

  @Test
  public void testRebuildBrokerResourceFromHelixTags()
      throws Exception {
    // Create the table
    addDummySchema(RAW_TABLE_NAME);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).build();
    waitForEVToDisappear(tableConfig.getTableName());
    _helixResourceManager.addTable(tableConfig);

    // Untag all Brokers assigned to broker tenant
    untagBrokers();
    assertEquals(_helixResourceManager.getOnlineUnTaggedBrokerInstanceList().size(), NUM_BROKER_INSTANCES);

    // Rebuilding the broker tenant should update the ideal state size
    PinotResourceManagerResponse response =
        _helixResourceManager.rebuildBrokerResourceFromHelixTags(OFFLINE_TABLE_NAME);
    assertTrue(response.isSuccessful());
    IdealState idealState = _helixAdmin.getResourceIdealState(_clusterName, Helix.BROKER_RESOURCE_INSTANCE);
    assertTrue(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).isEmpty());

    // Create broker tenant on Brokers
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, NUM_BROKER_INSTANCES, 0, 0);
    response = _helixResourceManager.createBrokerTenant(brokerTenant);
    assertTrue(response.isSuccessful());

    // Rebuilding the broker tenant should update the ideal state size
    response = _helixResourceManager.rebuildBrokerResourceFromHelixTags(OFFLINE_TABLE_NAME);
    assertTrue(response.isSuccessful());
    idealState = _helixAdmin.getResourceIdealState(_clusterName, Helix.BROKER_RESOURCE_INSTANCE);
    assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), NUM_BROKER_INSTANCES);

    // Delete the table
    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME);

    // Reset the brokers
    resetBrokerTags();
  }

  @Test
  public void testGetLiveBrokers()
      throws Exception {
    // Create the OFFLINE table
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).build();
    waitForEVToDisappear(tableConfig.getTableName());
    _helixResourceManager.addTable(tableConfig);
    waitForTableOnlineInBrokerResourceEV(OFFLINE_TABLE_NAME);

    // Test retrieving the live brokers for table
    List<String> liveBrokersForTable = _helixResourceManager.getLiveBrokersForTable(OFFLINE_TABLE_NAME);
    assertEquals(liveBrokersForTable.size(), 3);
    for (String broker : liveBrokersForTable) {
      assertTrue(broker.startsWith("Broker_localhost"));
    }

    // Test retrieving the live brokers for table without table-type suffix
    liveBrokersForTable = _helixResourceManager.getLiveBrokersForTable(RAW_TABLE_NAME);
    assertEquals(liveBrokersForTable.size(), 3);

    // Test retrieving the live brokers for table with non-existent table-type
    assertThrows(TableNotFoundException.class, () -> _helixResourceManager.getLiveBrokersForTable(REALTIME_TABLE_NAME));

    // Test retrieving table to live brokers mapping
    Map<String, List<InstanceInfo>> tableToLiveBrokersMapping = _helixResourceManager.getTableToLiveBrokersMapping();
    assertEquals(tableToLiveBrokersMapping.size(), 1);
    assertEquals(tableToLiveBrokersMapping.get(OFFLINE_TABLE_NAME).size(), NUM_BROKER_INSTANCES);

    // Create the REALTIME table
    addDummySchema(RAW_TABLE_NAME);
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME)
            .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()).build();
    waitForEVToDisappear(tableConfig.getTableName());
    _helixResourceManager.addTable(tableConfig);
    waitForTableOnlineInBrokerResourceEV(REALTIME_TABLE_NAME);

    // Test retrieving the live brokers for table without table-type suffix
    liveBrokersForTable = _helixResourceManager.getLiveBrokersForTable(RAW_TABLE_NAME);
    assertEquals(liveBrokersForTable.size(), 3);

    // Test retrieving the live brokers for table with table-type suffix
    liveBrokersForTable = _helixResourceManager.getLiveBrokersForTable(REALTIME_TABLE_NAME);
    assertEquals(liveBrokersForTable.size(), 3);

    // Test retrieving table to live brokers mapping
    tableToLiveBrokersMapping = _helixResourceManager.getTableToLiveBrokersMapping();
    assertEquals(tableToLiveBrokersMapping.size(), 2);
    assertEquals(tableToLiveBrokersMapping.get(OFFLINE_TABLE_NAME).size(), NUM_BROKER_INSTANCES);
    assertEquals(tableToLiveBrokersMapping.get(REALTIME_TABLE_NAME).size(), NUM_BROKER_INSTANCES);

    // Test retrieving the live brokers with non-existent table
    assertThrows(TableNotFoundException.class, () -> _helixResourceManager.getLiveBrokersForTable("fake"));
    assertThrows(TableNotFoundException.class, () -> _helixResourceManager.getLiveBrokersForTable("fake_OFFLINE"));
    assertThrows(TableNotFoundException.class, () -> _helixResourceManager.getLiveBrokersForTable("fake_REALTIME"));

    // Delete the tables
    _helixResourceManager.deleteRealtimeTable(RAW_TABLE_NAME);
    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME);
    deleteSchema(RAW_TABLE_NAME);

    // Wait for tables being dropped from the broker resource external view
    TestUtils.waitForCondition(aVoid -> {
      ExternalView externalView = _helixAdmin.getResourceExternalView(_clusterName, Helix.BROKER_RESOURCE_INSTANCE);
      return externalView.getStateMap(OFFLINE_TABLE_NAME) == null
          && externalView.getStateMap(REALTIME_TABLE_NAME) == null;
    }, 60_000L, "Failed to get all brokers DROPPED");
  }

  private void waitForTableOnlineInBrokerResourceEV(String tableNameWithType) {
    TestUtils.waitForCondition(aVoid -> {
      ExternalView externalView = _helixAdmin.getResourceExternalView(_clusterName, Helix.BROKER_RESOURCE_INSTANCE);
      Map<String, String> stateMap = externalView.getStateMap(tableNameWithType);
      if (stateMap == null) {
        return false;
      }
      int numOnlineBrokers = 0;
      for (String state : stateMap.values()) {
        if (state.equals(Helix.StateModel.BrokerResourceStateModel.ONLINE)) {
          numOnlineBrokers++;
        }
      }
      return numOnlineBrokers == NUM_BROKER_INSTANCES;
    }, 60_000L, "Failed to get all brokers ONLINE");
  }

  @Test
  public void testRetrieveSegmentZKMetadata() {
    // Test retrieving OFFLINE segment ZK metadata
    {
      String segmentName = "testSegment";
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME, segmentZKMetadata);
      List<SegmentZKMetadata> retrievedSegmentsZKMetadata =
          _helixResourceManager.getSegmentsZKMetadata(OFFLINE_TABLE_NAME);
      SegmentZKMetadata retrievedSegmentZKMetadata = retrievedSegmentsZKMetadata.get(0);
      assertEquals(retrievedSegmentZKMetadata.getSegmentName(), segmentName);
      assertEquals(retrievedSegmentsZKMetadata.size(), 1);
      ZKMetadataProvider.removeSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME, segmentName);
      assertTrue(_helixResourceManager.getSegmentsZKMetadata(OFFLINE_TABLE_NAME).isEmpty());
    }

    // Test retrieving REALTIME segment ZK metadata
    {
      String segmentName = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, System.currentTimeMillis()).getSegmentName();
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(Segment.Realtime.Status.DONE);
      ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, REALTIME_TABLE_NAME, segmentZKMetadata);
      List<SegmentZKMetadata> retrievedSegmentsZKMetadata =
          _helixResourceManager.getSegmentsZKMetadata(REALTIME_TABLE_NAME);
      SegmentZKMetadata retrievedSegmentZKMetadata = retrievedSegmentsZKMetadata.get(0);
      assertEquals(retrievedSegmentZKMetadata.getSegmentName(), segmentName);
      assertEquals(segmentZKMetadata.getStatus(), Segment.Realtime.Status.DONE);
      assertEquals(retrievedSegmentsZKMetadata.size(), 1);
      ZKMetadataProvider.removeSegmentZKMetadata(_propertyStore, REALTIME_TABLE_NAME, segmentName);
      assertTrue(_helixResourceManager.getSegmentsZKMetadata(REALTIME_TABLE_NAME).isEmpty());
    }
  }

  @Test
  public void testUpdateSchemaDateTime() {
    String segmentName = "testSegment";
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);

    long currentTimeMs = System.currentTimeMillis();
    DateTimeFormatter dateTimeFormatter =
        new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").toFormatter();
    segmentZKMetadata.setRawStartTime(dateTimeFormatter.withZone(DateTimeZone.UTC).print(currentTimeMs));
    segmentZKMetadata.setRawEndTime(dateTimeFormatter.withZone(DateTimeZone.UTC).print(currentTimeMs));
    segmentZKMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME, segmentZKMetadata);
    List<SegmentZKMetadata> retrievedSegmentsZKMetadata =
        _helixResourceManager.getSegmentsZKMetadata(OFFLINE_TABLE_NAME);
    SegmentZKMetadata retrievedSegmentZKMetadata = retrievedSegmentsZKMetadata.get(0);
    assertEquals(retrievedSegmentZKMetadata.getSegmentName(), segmentName);
    assertEquals(retrievedSegmentZKMetadata.getStartTimeMs(), -1);
    assertEquals(retrievedSegmentZKMetadata.getEndTimeMs(), -1);
    assertEquals(retrievedSegmentsZKMetadata.size(), 1);

    DateTimeFieldSpec timeColumnFieldSpec =
        new DateTimeFieldSpec("timestamp", FieldSpec.DataType.STRING, "SIMPLE_DATE_FORMAT|yyyy-MM-dd'T'HH:mm:ss.SSS",
            "1:MILLISECONDS");
    _helixResourceManager.updateSegmentsZKTimeInterval(OFFLINE_TABLE_NAME, timeColumnFieldSpec);
    retrievedSegmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(OFFLINE_TABLE_NAME);
    retrievedSegmentZKMetadata = retrievedSegmentsZKMetadata.get(0);
    assertEquals(retrievedSegmentZKMetadata.getSegmentName(), segmentName);
    assertEquals(retrievedSegmentZKMetadata.getStartTimeMs(), currentTimeMs);
    assertEquals(retrievedSegmentZKMetadata.getEndTimeMs(), currentTimeMs);
    assertEquals(retrievedSegmentsZKMetadata.size(), 1);
  }

  @Test
  void testRetrieveTenantNames() {
    // Create broker tenant on 1 Broker
    untagBrokers();
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 1, 0, 0);
    PinotResourceManagerResponse response = _helixResourceManager.createBrokerTenant(brokerTenant);
    assertTrue(response.isSuccessful());

    // One broker tenant expected
    assertEquals(_helixResourceManager.getAllBrokerTenantNames(), Collections.singleton(BROKER_TENANT_NAME));

    // Add an invalid tag
    String brokerInstanceName =
        _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME).iterator().next();
    _helixAdmin.addInstanceTag(_clusterName, brokerInstanceName, "invalid");
    assertEquals(_helixResourceManager.getAllBrokerTenantNames(), Collections.singleton(BROKER_TENANT_NAME));

    resetBrokerTags();
  }

  @Test
  public void testValidateTenantConfig() {
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

    // Missing broker tenant
    offlineTableConfig.setTenantConfig(new TenantConfig(null, SERVER_TENANT_NAME, null));
    assertThrows(InvalidTableConfigException.class,
        () -> _helixResourceManager.validateTableTenantConfig(offlineTableConfig));

    // Missing server tenant
    offlineTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, null, null));
    assertThrows(InvalidTableConfigException.class,
        () -> _helixResourceManager.validateTableTenantConfig(offlineTableConfig));

    // Empty broker tag (brokerTenant_BROKER)
    untagBrokers();
    offlineTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, null));
    assertThrows(InvalidTableConfigException.class,
        () -> _helixResourceManager.validateTableTenantConfig(offlineTableConfig));
    resetBrokerTags();

    // Empty server tag (serverTenant_OFFLINE)
    untagServers();
    assertThrows(InvalidTableConfigException.class,
        () -> _helixResourceManager.validateTableTenantConfig(offlineTableConfig));
    // Re-create server tenant with only OFFLINE servers
    Tenant serverTenant =
        new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, NUM_OFFLINE_SERVER_INSTANCES, NUM_OFFLINE_SERVER_INSTANCES,
            0);
    _helixResourceManager.createServerTenant(serverTenant);

    // Valid tenant config without tagOverrideConfig
    _helixResourceManager.validateTableTenantConfig(offlineTableConfig);

    // Invalid serverTag in tierConfigs
    TierConfig tierConfig = new TierConfig("myTier", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "10d", null,
        TierFactory.PINOT_SERVER_STORAGE_TYPE, "Unknown_OFFLINE", null, null);
    offlineTableConfig.setTierConfigsList(Collections.singletonList(tierConfig));
    assertThrows(InvalidTableConfigException.class,
        () -> _helixResourceManager.validateTableTenantConfig(offlineTableConfig));

    // A null serverTag has no instances associated with it, so it's invalid.
    tierConfig = new TierConfig("myTier", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "10d", null,
        TierFactory.PINOT_SERVER_STORAGE_TYPE, null, null, null);
    offlineTableConfig.setTierConfigsList(Collections.singletonList(tierConfig));
    assertThrows(InvalidTableConfigException.class,
        () -> _helixResourceManager.validateTableTenantConfig(offlineTableConfig));

    // Valid serverTag in tierConfigs
    tierConfig = new TierConfig("myTier", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "10d", null,
        TierFactory.PINOT_SERVER_STORAGE_TYPE, SERVER_TENANT_NAME + "_OFFLINE", null, null);
    offlineTableConfig.setTierConfigsList(Collections.singletonList(tierConfig));
    _helixResourceManager.validateTableTenantConfig(offlineTableConfig);

    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).build();

    // Empty server tag (serverTenant_REALTIME)
    assertThrows(InvalidTableConfigException.class,
        () -> _helixResourceManager.validateTableTenantConfig(realtimeTableConfig));

    // Incorrect CONSUMING server tag (serverTenant_BROKER)
    TagOverrideConfig tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getBrokerTagForTenant(SERVER_TENANT_NAME),
        TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME));
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    assertThrows(InvalidTableConfigException.class,
        () -> _helixResourceManager.validateTableTenantConfig(realtimeTableConfig));

    // Empty CONSUMING server tag (serverTenant_REALTIME)
    tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getRealtimeTagForTenant(SERVER_TENANT_NAME), null);
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    assertThrows(InvalidTableConfigException.class,
        () -> _helixResourceManager.validateTableTenantConfig(realtimeTableConfig));

    // Incorrect COMPLETED server tag (serverTenant_BROKER)
    tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME),
        TagNameUtils.getBrokerTagForTenant(SERVER_TENANT_NAME));
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    assertThrows(InvalidTableConfigException.class,
        () -> _helixResourceManager.validateTableTenantConfig(realtimeTableConfig));

    // Empty COMPLETED server tag (serverTenant_REALTIME)
    tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME),
        TagNameUtils.getRealtimeTagForTenant(SERVER_TENANT_NAME));
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    assertThrows(InvalidTableConfigException.class,
        () -> _helixResourceManager.validateTableTenantConfig(realtimeTableConfig));

    // Valid tenant config with tagOverrideConfig
    tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME),
        TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME));
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);

    TableConfig dimTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).setIsDimTable(true).build();

    // Dimension table can be created when only REALTIME tenant exists
    untagServers();
    serverTenant = new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, NUM_REALTIME_SERVER_INSTANCES, 0,
        NUM_REALTIME_SERVER_INSTANCES);
    _helixResourceManager.createServerTenant(serverTenant);
    _helixResourceManager.validateTableTenantConfig(dimTableConfig);

    resetServerTags();
  }

  @Test
  public void testCreateColocatedTenant() {
    untagServers();
    Tenant serverTenant = new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, NUM_SERVER_INSTANCES, NUM_SERVER_INSTANCES,
        NUM_SERVER_INSTANCES);
    assertTrue(_helixResourceManager.createServerTenant(serverTenant).isSuccessful());
    assertEquals(
        _helixResourceManager.getInstancesWithTag(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME)).size(),
        NUM_SERVER_INSTANCES);
    assertEquals(
        _helixResourceManager.getInstancesWithTag(TagNameUtils.getRealtimeTagForTenant(SERVER_TENANT_NAME)).size(),
        NUM_SERVER_INSTANCES);
    assertTrue(_helixResourceManager.getOnlineUnTaggedServerInstanceList().isEmpty());

    untagServers();
    serverTenant = new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, NUM_SERVER_INSTANCES, NUM_SERVER_INSTANCES - 1,
        NUM_SERVER_INSTANCES - 1);
    assertTrue(_helixResourceManager.createServerTenant(serverTenant).isSuccessful());
    assertEquals(
        _helixResourceManager.getInstancesWithTag(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME)).size(),
        NUM_SERVER_INSTANCES - 1);
    assertEquals(
        _helixResourceManager.getInstancesWithTag(TagNameUtils.getRealtimeTagForTenant(SERVER_TENANT_NAME)).size(),
        NUM_SERVER_INSTANCES - 1);
    assertTrue(_helixResourceManager.getOnlineUnTaggedServerInstanceList().isEmpty());

    untagServers();
    serverTenant = new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, NUM_SERVER_INSTANCES - 1, NUM_SERVER_INSTANCES - 1,
        NUM_SERVER_INSTANCES - 1);
    assertTrue(_helixResourceManager.createServerTenant(serverTenant).isSuccessful());
    assertEquals(
        _helixResourceManager.getInstancesWithTag(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME)).size(),
        NUM_SERVER_INSTANCES - 1);
    assertEquals(
        _helixResourceManager.getInstancesWithTag(TagNameUtils.getRealtimeTagForTenant(SERVER_TENANT_NAME)).size(),
        NUM_SERVER_INSTANCES - 1);
    assertEquals(_helixResourceManager.getOnlineUnTaggedServerInstanceList().size(), 1);

    resetServerTags();
  }

  @Test
  public void testLeadControllerResource() {
    IdealState leadControllerResourceIdealState =
        _helixAdmin.getResourceIdealState(_clusterName, Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    assertTrue(leadControllerResourceIdealState.isValid());
    assertTrue(leadControllerResourceIdealState.isEnabled());
    assertEquals(leadControllerResourceIdealState.getInstanceGroupTag(), Helix.CONTROLLER_INSTANCE);
    assertEquals(leadControllerResourceIdealState.getNumPartitions(),
        Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
    assertEquals(leadControllerResourceIdealState.getReplicas(),
        Integer.toString(Helix.LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT));
    assertEquals(leadControllerResourceIdealState.getRebalanceMode(), IdealState.RebalanceMode.FULL_AUTO);
    assertTrue(leadControllerResourceIdealState.getInstanceSet(
        leadControllerResourceIdealState.getPartitionSet().iterator().next()).isEmpty());

    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          _helixAdmin.getResourceExternalView(_clusterName, Helix.LEAD_CONTROLLER_RESOURCE_NAME);
      for (String partition : leadControllerResourceExternalView.getPartitionSet()) {
        Map<String, String> stateMap = leadControllerResourceExternalView.getStateMap(partition);
        if (stateMap.size() != 1) {
          return false;
        }
        String instanceId = LeadControllerUtils.generateParticipantInstanceId(LOCAL_HOST, getControllerPort());
        return MasterSlaveSMD.States.MASTER.name().equals(stateMap.get(instanceId));
      }
      return true;
    }, 60_000L, "Failed to assign master controller");
  }

  @Test
  public void testLeadControllerAssignment() {
    // Given a number of instances (from 1 to 10), make sure all the instances got assigned to lead controller resource
    for (int numControllers = 1; numControllers <= 10; numControllers++) {
      List<String> instanceNames = new ArrayList<>(numControllers);
      List<Integer> ports = new ArrayList<>(numControllers);
      for (int i = 0; i < numControllers; i++) {
        instanceNames.add(LeadControllerUtils.generateParticipantInstanceId(LOCAL_HOST, i));
        ports.add(i);
      }

      List<String> partitions = new ArrayList<>(Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
      for (int i = 0; i < Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE; i++) {
        partitions.add(LeadControllerUtils.generatePartitionName(i));
      }

      LinkedHashMap<String, Integer> states = new LinkedHashMap<>(2);
      states.put(MasterSlaveSMD.States.OFFLINE.name(), 0);
      states.put(MasterSlaveSMD.States.SLAVE.name(), Helix.LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT - 1);
      states.put(MasterSlaveSMD.States.MASTER.name(), 1);

      CrushEdRebalanceStrategy crushEdRebalanceStrategy = new CrushEdRebalanceStrategy();
      crushEdRebalanceStrategy.init(Helix.LEAD_CONTROLLER_RESOURCE_NAME, partitions, states, Integer.MAX_VALUE);

      ResourceControllerDataProvider dataProvider = new ResourceControllerDataProvider();
      ClusterConfig clusterConfig = _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().clusterConfig());
      dataProvider.setClusterConfig(clusterConfig);

      Map<String, InstanceConfig> instanceConfigMap = new HashMap<>(numControllers);
      for (int i = 0; i < numControllers; i++) {
        String instanceName = instanceNames.get(i);
        int port = ports.get(i);
        instanceConfigMap.put(instanceName, new InstanceConfig(instanceName
            + ", {HELIX_ENABLED=true, HELIX_ENABLED_TIMESTAMP=1559546216610, HELIX_HOST=Controller_localhost, "
            + "HELIX_PORT=" + port + "}{}{TAG_LIST=[controller]}"));
      }
      dataProvider.setInstanceConfigMap(instanceConfigMap);
      ZNRecord znRecord =
          crushEdRebalanceStrategy.computePartitionAssignment(instanceNames, instanceNames, new HashMap<>(0),
              dataProvider);

      assertNotNull(znRecord);
      Map<String, List<String>> listFields = znRecord.getListFields();
      assertEquals(listFields.size(), Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);

      Map<String, Integer> instanceToMasterAssignmentCountMap = new HashMap<>();
      int maxCount = 0;
      for (List<String> assignments : listFields.values()) {
        assertEquals(assignments.size(), Helix.LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT);
        if (!instanceToMasterAssignmentCountMap.containsKey(assignments.get(0))) {
          instanceToMasterAssignmentCountMap.put(assignments.get(0), 1);
        } else {
          instanceToMasterAssignmentCountMap.put(assignments.get(0),
              instanceToMasterAssignmentCountMap.get(assignments.get(0)) + 1);
        }
        maxCount = Math.max(instanceToMasterAssignmentCountMap.get(assignments.get(0)), maxCount);
      }
      assertEquals(instanceToMasterAssignmentCountMap.size(), numControllers,
          "Not all the instances got assigned to the resource");
      for (Integer count : instanceToMasterAssignmentCountMap.values()) {
        assertTrue((maxCount - count == 0 || maxCount - count == 1), "Instance assignment isn't distributed");
      }
    }
  }

  @Test
  public void testUpdateTargetTier()
      throws Exception {
    TierConfig tierConfig =
        new TierConfig("tier1", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null, Collections.singletonList("testSegment"),
            TierFactory.PINOT_SERVER_STORAGE_TYPE, SERVER_TENANT_NAME + "_OFFLINE", null, null);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setTierConfigList(Collections.singletonList(tierConfig)).setServerTenant(SERVER_TENANT_NAME).build();
    waitForEVToDisappear(tableConfig.getTableName());
    addDummySchema(RAW_TABLE_NAME);
    _helixResourceManager.addTable(tableConfig);

    String segmentName = "testSegment";
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME, segmentZKMetadata);
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "testSegment"), "downloadUrl");
    assertNull(segmentZKMetadata.getTier());

    // Move on to new tier
    _helixResourceManager.updateTargetTier("j1", tableConfig.getTableName(), tableConfig);
    List<SegmentZKMetadata> retrievedSegmentsZKMetadata =
        _helixResourceManager.getSegmentsZKMetadata(OFFLINE_TABLE_NAME);
    SegmentZKMetadata retrievedSegmentZKMetadata = retrievedSegmentsZKMetadata.get(0);
    assertEquals(retrievedSegmentZKMetadata.getTier(), "tier1");

    // Move back to default tier
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).build();
    _helixResourceManager.updateTableConfig(tableConfig);
    _helixResourceManager.updateTargetTier("j2", tableConfig.getTableName(), tableConfig);
    retrievedSegmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(OFFLINE_TABLE_NAME);
    retrievedSegmentZKMetadata = retrievedSegmentsZKMetadata.get(0);
    assertNull(retrievedSegmentZKMetadata.getTier());
  }

  /**
   * Tests the code path where a subset of merged segments (from the original segmentsTo list)
   * is passed to the endReplace API.
   * @throws Exception
   */
  @Test
  public void testSegmentReplacementWithCustomToSegments() throws Exception {
    // Create the table
    addDummySchema(RAW_TABLE_NAME);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).build();
    waitForEVToDisappear(tableConfig.getTableName());
    _helixResourceManager.addTable(tableConfig);

    List<String> segmentsFrom = Collections.emptyList();
    List<String> segmentsTo = Arrays.asList("s20", "s21");
    String lineageEntryId =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom, segmentsTo, false, null);
    assertThrows(RuntimeException.class,
        () -> _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId,
            new EndReplaceSegmentsRequest(Arrays.asList("s9", "s6"), null)));
    // Try after new segments added to the table
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s20"), "downloadUrl");
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s21"), "downloadUrl");
    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId,
        new EndReplaceSegmentsRequest(Arrays.asList("s21"), null));
    SegmentLineage segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds(), Collections.singleton(lineageEntryId));
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getSegmentsFrom(), segmentsFrom);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId).getState(), LineageEntryState.COMPLETED);
    // Delete the table
    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertNull(segmentLineage);
  }

  @Test
  public void testSegmentReplacementRegular()
      throws Exception {
    // Create the table
    addDummySchema(RAW_TABLE_NAME);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).build();
    waitForEVToDisappear(tableConfig.getTableName());
    _helixResourceManager.addTable(tableConfig);

    // Add 5 segments
    for (int i = 0; i < 5; i++) {
      _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s" + i), "downloadUrl");
    }
    assertEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false).size(), 5);

    // Add 2 segments in batch
    List<String> segmentsFrom1 = Collections.emptyList();
    List<String> segmentsTo1 = Arrays.asList("s5", "s6");
    String lineageEntryId1 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom1, segmentsTo1, false, null);
    SegmentLineage segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds(), Collections.singleton(lineageEntryId1));
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId1).getSegmentsFrom(), segmentsFrom1);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId1).getSegmentsTo(), segmentsTo1);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId1).getState(), LineageEntryState.IN_PROGRESS);

    // Check invalid segmentsTo
    assertThrows(IllegalStateException.class,
        () -> _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, Arrays.asList("s1", "s2"),
            Arrays.asList("s3", "s4"), false, null));
    assertThrows(IllegalStateException.class,
        () -> _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, Arrays.asList("s1", "s2"),
            Collections.singletonList("s2"), false, null));

    // Check invalid segmentsFrom
    assertThrows(IllegalStateException.class,
        () -> _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, Arrays.asList("s1", "s6"),
            Collections.singletonList("s7"), false, null));

    // Invalid table
    assertThrows(RuntimeException.class,
        () -> _helixResourceManager.endReplaceSegments(REALTIME_TABLE_NAME, lineageEntryId1, null));

    // Invalid table
    assertThrows(RuntimeException.class,
        () -> _helixResourceManager.endReplaceSegments(REALTIME_TABLE_NAME, lineageEntryId1, null));
    // Invalid lineage entry id
    assertThrows(RuntimeException.class, () -> _helixResourceManager.
        endReplaceSegments(OFFLINE_TABLE_NAME, "invalid", null));

    // New segments not available in the table
    assertThrows(RuntimeException.class,
        () -> _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId1, null));

    // Try after new segments added to the table
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s5"), "downloadUrl");
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s6"), "downloadUrl");
    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId1, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds(), Collections.singleton(lineageEntryId1));
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId1).getSegmentsFrom(), segmentsFrom1);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId1).getSegmentsTo(), segmentsTo1);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId1).getState(), LineageEntryState.COMPLETED);

    // Replace 2 segments
    List<String> segmentsFrom2 = Arrays.asList("s1", "s2");
    List<String> segmentsTo2 = Arrays.asList("merged_t1_0", "merged_t1_1");
    String lineageEntryId2 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom2, segmentsTo2, false, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertSetEquals(segmentLineage.getLineageEntryIds(), lineageEntryId1, lineageEntryId2);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getSegmentsFrom(), segmentsFrom2);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getSegmentsTo(), segmentsTo2);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getState(), LineageEntryState.IN_PROGRESS);

    // Upload partial data
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "merged_t1_0"), "downloadUrl");
    IdealState idealState = _helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME);
    assertNotNull(idealState);
    assertTrue(idealState.getPartitionSet().contains("merged_t1_0"));

    // Revert the entry with partial data uploaded without forceRevert
    assertThrows(RuntimeException.class,
        () -> _helixResourceManager.revertReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId2, false, null));

    // Revert the entry with partial data uploaded with forceRevert
    _helixResourceManager.revertReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId2, true, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getState(), LineageEntryState.REVERTED);

    // 'merged_t1_0' segment should be cleaned up
    idealState = _helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME);
    assertNotNull(idealState);
    assertFalse(idealState.getPartitionSet().contains("merged_t1_0"));

    // Start a new segment replacement since the above entry is reverted
    List<String> segmentsFrom3 = Arrays.asList("s1", "s2");
    List<String> segmentsTo3 = Arrays.asList("merged_t2_0", "merged_t2_1");
    String lineageEntryId3 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom3, segmentsTo3, false, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertSetEquals(segmentLineage.getLineageEntryIds(), lineageEntryId1, lineageEntryId2, lineageEntryId3);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getSegmentsFrom(), segmentsFrom3);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getSegmentsTo(), segmentsTo3);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getState(), LineageEntryState.IN_PROGRESS);

    // Upload partial data
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "merged_t2_0"), "downloadUrl");

    // Without force cleanup, 'startReplaceSegments' again should fail because of duplicate segments on 'segmentFrom'
    List<String> segmentsFrom4 = Arrays.asList("s1", "s2");
    List<String> segmentsTo4 = Arrays.asList("merged_t3_0", "merged_t3_1");
    assertThrows(RuntimeException.class,
        () -> _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom4, segmentsTo4, false, null));

    // Test force clean up case
    String lineageEntryId4 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom4, segmentsTo4, true, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertSetEquals(segmentLineage.getLineageEntryIds(), lineageEntryId1, lineageEntryId2, lineageEntryId3,
        lineageEntryId4);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getSegmentsFrom(), segmentsFrom3);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getSegmentsTo(), segmentsTo3);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getState(), LineageEntryState.REVERTED);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId4).getSegmentsFrom(), segmentsFrom4);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId4).getSegmentsTo(), segmentsTo4);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId4).getState(), LineageEntryState.IN_PROGRESS);

    // 'merged_t2_0' segment should be cleaned up
    idealState = _helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME);
    assertNotNull(idealState);
    assertFalse(idealState.getPartitionSet().contains("merged_t2_0"));

    // Upload segments again
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "merged_t3_0"), "downloadUrl");
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "merged_t3_1"), "downloadUrl");

    // Finish the replacement
    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId4, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertSetEquals(segmentLineage.getLineageEntryIds(), lineageEntryId1, lineageEntryId2, lineageEntryId3,
        lineageEntryId4);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId4).getSegmentsFrom(), segmentsFrom4);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId4).getSegmentsTo(), segmentsTo4);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId4).getState(), LineageEntryState.COMPLETED);

    // Check empty segmentsFrom won't revert previous lineage with empty segmentsFrom
    // Start a new segment replacement with empty segmentsFrom
    List<String> segmentsFrom5 = Collections.emptyList();
    List<String> segmentsTo5 = Arrays.asList("s7", "s8");
    String lineageEntryId5 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom5, segmentsTo5, false, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertSetEquals(segmentLineage.getLineageEntryIds(), lineageEntryId1, lineageEntryId2, lineageEntryId3,
        lineageEntryId4, lineageEntryId5);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getSegmentsFrom(), segmentsFrom5);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getSegmentsTo(), segmentsTo5);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getState(), LineageEntryState.IN_PROGRESS);

    // Assuming the replacement fails in the middle, rerunning the protocol with the same segmentsTo will go through,
    // and remove the previous lineage entry
    List<String> segmentsFrom6 = Collections.emptyList();
    List<String> segmentsTo6 = Arrays.asList("s7", "s8");
    String lineageEntryId6 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom6, segmentsTo6, true, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertSetEquals(segmentLineage.getLineageEntryIds(), lineageEntryId1, lineageEntryId2, lineageEntryId3,
        lineageEntryId4, lineageEntryId6);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getSegmentsFrom(), segmentsFrom6);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getSegmentsTo(), segmentsTo6);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getState(), LineageEntryState.IN_PROGRESS);

    // Upload partial data
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s7"), "downloadUrl");

    // Start another new segment replacement with empty segmentsFrom, and check that previous lineages with empty
    // segmentsFrom are not reverted
    List<String> segmentsFrom7 = Collections.emptyList();
    List<String> segmentsTo7 = Arrays.asList("s9", "s10");
    String lineageEntryId7 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom7, segmentsTo7, true, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds().size(), 6);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getState(), LineageEntryState.IN_PROGRESS);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getState(), LineageEntryState.IN_PROGRESS);

    // Finish the replacement
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s9"), "downloadUrl");
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s10"), "downloadUrl");
    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId7, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds().size(), 6);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getState(), LineageEntryState.IN_PROGRESS);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getSegmentsFrom(), segmentsFrom7);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getSegmentsTo(), segmentsTo7);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getState(), LineageEntryState.COMPLETED);

    // Check partial overlap reverts previous lineage
    // Start a new segment replacement with non-empty segmentsFrom
    List<String> segmentsFrom8 = Arrays.asList("s9", "s10");
    List<String> segmentsTo8 = Arrays.asList("s11", "s12");
    String lineageEntryId8 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom8, segmentsTo8, false, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds().size(), 7);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getSegmentsFrom(), segmentsFrom8);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getSegmentsTo(), segmentsTo8);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getState(), LineageEntryState.IN_PROGRESS);

    // Upload partial data
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s11"), "downloadUrl");

    // Start another new segment replacement with segmentsFrom overlapping with previous lineage, and check that
    // previous lineages with overlapped segmentsFrom are reverted
    List<String> segmentsFrom9 = Arrays.asList("s0", "s9");
    List<String> segmentsTo9 = Arrays.asList("s13", "s14");
    String lineageEntryId9 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom9, segmentsTo9, true, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds().size(), 8);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getState(), LineageEntryState.REVERTED);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getState(), LineageEntryState.IN_PROGRESS);

    // Finish the replacement
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s13"), "downloadUrl");
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s14"), "downloadUrl");
    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId9, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds().size(), 8);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getSegmentsFrom(), segmentsFrom9);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getSegmentsTo(), segmentsTo9);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getState(), LineageEntryState.COMPLETED);

    // Check endReplaceSegments is idempotent
    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId9, null);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getState(), LineageEntryState.COMPLETED);

    // Test empty segmentsTo. This is a special case where we are atomically removing segments.
    List<String> segmentsFrom10 = Arrays.asList("s13", "s14");
    List<String> segmentsTo10 = Collections.emptyList();
    String lineageEntryId10 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom10, segmentsTo10, true, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds().size(), 9);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getState(), LineageEntryState.IN_PROGRESS);
    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId10, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getSegmentsFrom(), segmentsFrom10);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getSegmentsTo(), segmentsTo10);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getState(), LineageEntryState.COMPLETED);

    // Delete the table
    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertNull(segmentLineage);
  }

  @Test
  public void testSegmentReplacementForRefresh()
      throws Exception {
    // Create the table
    addDummySchema(RAW_TABLE_NAME);
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", "DAILY"));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).setIngestionConfig(ingestionConfig).build();
    waitForEVToDisappear(tableConfig.getTableName());
    _helixResourceManager.addTable(tableConfig);

    // Add 3 segments
    for (int i = 0; i < 3; i++) {
      _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s" + i), "downloadUrl");
    }
    List<String> segmentsForTable = _helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false);
    assertEquals(segmentsForTable.size(), 3);

    // Start segment replacement protocol with (s0, s1, s2) -> (s3, s4, s5)
    List<String> segmentsFrom1 = Arrays.asList("s0", "s1", "s2");
    List<String> segmentsTo1 = Arrays.asList("s3", "s4", "s5");
    String lineageEntryId1 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom1, segmentsTo1, false, null);
    SegmentLineage segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds(), Collections.singleton(lineageEntryId1));
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId1).getSegmentsFrom(), segmentsFrom1);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId1).getSegmentsTo(), segmentsTo1);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId1).getState(), LineageEntryState.IN_PROGRESS);
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false), "s0", "s1", "s2");

    // Add new segments
    for (int i = 3; i < 6; i++) {
      _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s" + i), "downloadUrl");
    }
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false), "s0", "s1", "s2", "s3", "s4",
        "s5");
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true), "s0", "s1", "s2");

    // Call end segment replacements
    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId1, null);
    assertEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false).size(), 6);
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true), "s3", "s4", "s5");
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds(), Collections.singleton(lineageEntryId1));
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId1).getSegmentsFrom(), segmentsFrom1);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId1).getSegmentsTo(), segmentsTo1);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId1).getState(), LineageEntryState.COMPLETED);

    // Start the new protocol with "forceCleanup = false" so there will be no proactive clean-up happening
    // (s3, s4, s5) -> (s6, s7, s8)
    List<String> segmentsFrom2 = Arrays.asList("s3", "s4", "s5");
    List<String> segmentsTo2 = Arrays.asList("s6", "s7", "s8");
    String lineageEntryId2 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom2, segmentsTo2, false, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds().size(), 2);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getSegmentsFrom(), segmentsFrom2);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getSegmentsTo(), segmentsTo2);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getState(), LineageEntryState.IN_PROGRESS);
    assertEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false).size(), 6);
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true), "s3", "s4", "s5");

    // Reverting the first entry should fail
    assertThrows(RuntimeException.class,
        () -> _helixResourceManager.revertReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId1, false, null));

    // Add partial segments to indicate incomplete protocol
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s6"), "downloadUrl");
    assertEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false).size(), 7);
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true), "s3", "s4", "s5");

    // Start the new protocol (s3, s4, s5) -> (s9, s10, s11) with "forceCleanup = true" to check if 2 different
    // proactive clean-up mechanism works:
    //
    // 1. the previous lineage entry (s3, s4, s5) -> (s6, s7, s8) should be "REVERTED"
    // 2. the older segments (s0, s1, s2) need to be cleaned up because we are about to upload the 3rd data snapshot
    List<String> segmentsFrom3 = Arrays.asList("s3", "s4", "s5");
    List<String> segmentsTo3 = Arrays.asList("s9", "s10", "s11");
    String lineageEntryId3 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom3, segmentsTo3, true, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntryIds().size(), 3);

    // Check that the previous entry gets reverted
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId2).getState(), LineageEntryState.REVERTED);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getSegmentsFrom(), segmentsFrom3);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getSegmentsTo(), segmentsTo3);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId3).getState(), LineageEntryState.IN_PROGRESS);

    // Check that the segments from the older lineage gets deleted
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false), "s3", "s4", "s5");
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true), "s3", "s4", "s5");

    // Invoking end segment replacement for the reverted entry should fail
    assertThrows(RuntimeException.class,
        () -> _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId2, null));

    // Add new segments
    for (int i = 9; i < 12; i++) {
      _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s" + i), "downloadUrl");
    }

    // Call end segment replacements
    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId3, null);
    assertEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false).size(), 6);
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true), "s9", "s10", "s11");

    // We clean up "segmentsTo" for the lineage entry with "REVERTED" state in 2 places:
    // 1. revertReplaceSegments API will delete segmentsTo
    // 2. startReplaceSegments API will also try to clean up segmentsTo for REVERTED lineage

    // Call revert segment replacements (s3, s4, s5) <- (s9, s10, s11) to check if the revertReplaceSegments correctly
    // deleted (s9, s10, s11).
    _helixResourceManager.revertReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId3, false, null);
    assertEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false).size(), 3);
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true), "s3", "s4", "s5");

    // Re-upload (s9, s10, s11) to test the segment clean up from startReplaceSegments
    for (int i = 9; i < 12; i++) {
      _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s" + i), "downloadUrl");
    }
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false), "s3", "s4", "s5", "s9", "s10",
        "s11");

    // Call startReplaceSegments with (s3, s4, s5) -> (s12, s13, s14), which should clean up (s9, s10, s11)
    List<String> segmentsFrom4 = Arrays.asList("s3", "s4", "s5");
    List<String> segmentsTo4 = Arrays.asList("s12", "s13", "s14");
    String lineageEntryId4 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom4, segmentsTo4, true, null);
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false), "s3", "s4", "s5");

    // Upload the new segments (s12, s13, s14)
    for (int i = 12; i < 15; i++) {
      _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s" + i), "downloadUrl");
    }

    // Call endReplaceSegments to start to use (s12, s13, s14)
    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId4, null);
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, false), "s3", "s4", "s5", "s12", "s13",
        "s14");
    assertSetEquals(_helixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true), "s12", "s13", "s14");

    // Check empty segmentsFrom won't revert previous lineage with empty segmentsFrom
    // Start a new segment replacement with empty segmentsFrom
    List<String> segmentsFrom5 = Collections.emptyList();
    List<String> segmentsTo5 = Arrays.asList("s15", "s16");
    String lineageEntryId5 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom5, segmentsTo5, false, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getSegmentsFrom(), segmentsFrom5);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getSegmentsTo(), segmentsTo5);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getState(), LineageEntryState.IN_PROGRESS);

    // Upload partial data
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s15"), "downloadUrl");

    // Start another new segment replacement with empty segmentsFrom, and check that previous lineages with empty
    // segmentsFrom are not reverted
    List<String> segmentsFrom6 = Collections.emptyList();
    List<String> segmentsTo6 = Arrays.asList("s17", "s18");
    String lineageEntryId6 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom6, segmentsTo6, true, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId5).getState(), LineageEntryState.IN_PROGRESS);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getState(), LineageEntryState.IN_PROGRESS);

    // Finish the replacement
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s17"), "downloadUrl");
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s18"), "downloadUrl");
    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId6, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getSegmentsFrom(), segmentsFrom6);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getSegmentsTo(), segmentsTo6);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId6).getState(), LineageEntryState.COMPLETED);

    // Check partial overlap of segmentsFrom reverts previous lineage
    // Start a new segment replacement with non-empty segmentsFrom
    List<String> segmentsFrom7 = Arrays.asList("s17", "s18");
    List<String> segmentsTo7 = Arrays.asList("s19", "s20");
    String lineageEntryId7 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom7, segmentsTo7, false, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getSegmentsFrom(), segmentsFrom7);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getSegmentsTo(), segmentsTo7);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getState(), LineageEntryState.IN_PROGRESS);

    // Upload partial data
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s19"), "downloadUrl");

    // Start another new segment replacement with segmentsFrom overlapping with previous lineage, and check that
    // previous lineages with overlapped segmentsFrom are reverted
    List<String> segmentsFrom8 = Arrays.asList("s14", "s17");
    List<String> segmentsTo8 = Arrays.asList("s21", "s22");
    String lineageEntryId8 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom8, segmentsTo8, true, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId7).getState(), LineageEntryState.REVERTED);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getState(), LineageEntryState.IN_PROGRESS);

    // Finish the replacement
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s21"), "downloadUrl");
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s22"), "downloadUrl");

    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId8, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getSegmentsFrom(), segmentsFrom8);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getSegmentsTo(), segmentsTo8);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId8).getState(), LineageEntryState.COMPLETED);

    // Check partial overlap of segmentsTo reverts previous lineage
    // Start a new segment replacement with non-empty segmentsFrom
    List<String> segmentsFrom9 = Arrays.asList("s21", "s22");
    List<String> segmentsTo9 = Arrays.asList("s23", "s24");
    String lineageEntryId9 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom9, segmentsTo9, false, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getSegmentsFrom(), segmentsFrom9);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getSegmentsTo(), segmentsTo9);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getState(), LineageEntryState.IN_PROGRESS);

    // Upload data
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s23"), "downloadUrl");
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s24"), "downloadUrl");

    // Start another new segment replacement with segmentsTo overlapping with previous lineage, and check that previous
    // lineages with overlapped segmentsTo are reverted
    List<String> segmentsFrom10 = Arrays.asList("s21", "s22");
    List<String> segmentsTo10 = Arrays.asList("s24", "s25");
    String lineageEntryId10 =
        _helixResourceManager.startReplaceSegments(OFFLINE_TABLE_NAME, segmentsFrom10, segmentsTo10, true, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getSegmentsFrom(), segmentsFrom9);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getSegmentsTo(), Collections.singletonList("s23"));
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId9).getState(), LineageEntryState.REVERTED);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getSegmentsFrom(), segmentsFrom10);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getSegmentsTo(), segmentsTo10);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getState(), LineageEntryState.IN_PROGRESS);

    // Finish the replacement
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s24"), "downloadUrl");
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "s25"), "downloadUrl");
    _helixResourceManager.endReplaceSegments(OFFLINE_TABLE_NAME, lineageEntryId10, null);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getSegmentsFrom(), segmentsFrom10);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getSegmentsTo(), segmentsTo10);
    assertEquals(segmentLineage.getLineageEntry(lineageEntryId10).getState(), LineageEntryState.COMPLETED);

    // Delete the table
    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME);
    segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, OFFLINE_TABLE_NAME);
    assertNull(segmentLineage);
  }

  private static void assertSetEquals(Collection<String> actual, String... expected) {
    Set<String> actualSet = actual instanceof Set ? (Set<String>) actual : new HashSet<>(actual);
    assertEquals(actualSet, new HashSet<>(Arrays.asList(expected)));
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
