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
package org.apache.pinot.controller.workload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.workload.scheme.TablePropagationScheme;
import org.apache.pinot.controller.workload.scheme.TenantPropagationScheme;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationEntity;
import org.apache.pinot.spi.config.workload.PropagationEntityOverrides;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PropagationSchemeTest {

  private PinotHelixResourceManager _pinotHelixResourceManager;
  private TablePropagationScheme _tablePropagationScheme;
  private TenantPropagationScheme _tenantPropagationScheme;

  @BeforeClass
  public void setUp() {
    _pinotHelixResourceManager = Mockito.mock(PinotHelixResourceManager.class);
    _tablePropagationScheme = new TablePropagationScheme(_pinotHelixResourceManager);
    _tenantPropagationScheme = new TenantPropagationScheme(_pinotHelixResourceManager);
  }

  @Test
  public void testTablePropagationScheme() {
    String offlineTable = "table1";
    String realtimeTable = "table2";
    EnforcementProfile profile = new EnforcementProfile(100, 100);
    PropagationEntity entity1 = new PropagationEntity(offlineTable, 50L, 50L, null);
    PropagationEntity entity2 = new PropagationEntity(realtimeTable, 50L, 50L, null);
    PropagationScheme propagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE,
        List.of(entity1, entity2));

    // Create a instancePartitionMap
    List<InstancePartitions> instancePartitions = new ArrayList<>();
    instancePartitions.add(createInstancePartitions(offlineTable, InstancePartitionsType.OFFLINE, 2));
    instancePartitions.add(createInstancePartitions(realtimeTable, InstancePartitionsType.CONSUMING, 2));
    instancePartitions.add(createInstancePartitions(realtimeTable, InstancePartitionsType.COMPLETED, 2));
    // Mock the behavior of getAllInstancePartitions to return the list of instance partitions
    ZkHelixPropertyStore<ZNRecord> propertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    for (InstancePartitions partitions : instancePartitions) {
      String partitionsPath = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(
          partitions.getInstancePartitionsName());
      ZNRecord partitionsZNRecord = partitions.toZNRecord();
      Mockito.when(propertyStore.get(partitionsPath, null, AccessOption.PERSISTENT)).thenReturn(partitionsZNRecord);
    }
    Mockito.when(_pinotHelixResourceManager.getPropertyStore()).thenReturn(propertyStore);

    // Create Broker Resource ExternalView
    ExternalView externalView = new ExternalView("brokerResource");
    Map<String, String> table1OfflineStateMap = createBrokerResourceStateMap(offlineTable, 2);
    externalView.setStateMap(offlineTable + "_OFFLINE", table1OfflineStateMap);
    Map<String, String> table2RealtimeStateMap = createBrokerResourceStateMap(realtimeTable, 2);
    externalView.setStateMap(realtimeTable + "_REALTIME", table2RealtimeStateMap);
    // Mock the behavior of getResourceExternalView to return the broker resource
    mockBrokerResource(externalView);

    // Case 1 : Test for Server instance resolution for table1 (offline) and table2 (realtime)
    NodeConfig nodeConfigServer = new NodeConfig(NodeConfig.Type.SERVER_NODE, profile, propagationScheme);
    List<PropagationEntity> entities = propagationScheme.getPropagationEntities();
    for (PropagationEntity entity : entities) {
      Set<String> expectedInstances = _tablePropagationScheme.resolveInstances(entity, nodeConfigServer.getNodeType(),
          null);
      assertTableSchemeResponse(entity, expectedInstances, NodeConfig.Type.SERVER_NODE, null, instancePartitions,
          null);
    }

    // Case 2 : Test for Broker instance resolution for table1 (offline) and table2 (realtime)
    NodeConfig nodeConfigBroker = new NodeConfig(NodeConfig.Type.BROKER_NODE, profile, propagationScheme);
    entities = propagationScheme.getPropagationEntities();
    for (PropagationEntity entity : entities) {
      Set<String> expectedInstances = _tablePropagationScheme.resolveInstances(entity, nodeConfigBroker.getNodeType(),
          null);
      assertTableSchemeResponse(entity, expectedInstances, NodeConfig.Type.BROKER_NODE, null, null,
          externalView);
    }

    // Case 3 : Test for instance resolution with overrides for consuming and completed
    PropagationEntityOverrides overrides1 = new PropagationEntityOverrides("CONSUMING", 25L, 25L);
    PropagationEntityOverrides overrides2 = new PropagationEntityOverrides("COMPLETED", 25L, 25L);
    entity2.setOverrides(List.of(overrides1, overrides2));
    for (PropagationEntityOverrides override : entity2.getOverrides()) {
      Set<String> expectedInstances = _tablePropagationScheme.resolveInstances(entity2, nodeConfigServer.getNodeType(),
          override);
      assertTableSchemeResponse(entity2, expectedInstances, NodeConfig.Type.SERVER_NODE, override, instancePartitions,
          null);
    }

    // Case 4 : Test for exception when table does not exist
    PropagationEntity entity3 = new PropagationEntity("table3", 50L, 50L, null);
    propagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, List.of(entity1, entity3));
    nodeConfigServer = new NodeConfig(NodeConfig.Type.SERVER_NODE, profile, propagationScheme);
    try {
      _tablePropagationScheme.resolveInstances(entity3, nodeConfigServer.getNodeType(), null);
      assert false;
    } catch (Exception e) {
      // Expected exception
      assert e.getMessage().contains("No instances found for entity: ");
    }

    // Case 5 : Test for exception when overrides are present for offline table
    PropagationEntityOverrides overrides3 = new PropagationEntityOverrides("CONSUMING", 50L, 50L);
    entity1.setOverrides(List.of(overrides3));
    try {
      _tablePropagationScheme.resolveInstances(entity1, nodeConfigServer.getNodeType(), overrides3);
      assert false;
    } catch (Exception e) {
      // Expected exception
      assert true;
    }
  }

  private void assertTableSchemeResponse(PropagationEntity entity, Set<String> expectedInstances,
                                         NodeConfig.Type nodeType,
                                         @Nullable PropagationEntityOverrides override,
                                         @Nullable List<InstancePartitions> instancePartitions,
                                         @Nullable ExternalView externalView) {
    int totalNumInstancesExpected = 0;
    Set<String> actualInstances = new HashSet<>();
    // For servers, get expected instances from instance partitions
    List<InstancePartitions> tableInstancePartitions = null;
    if (nodeType == NodeConfig.Type.SERVER_NODE) {
      if (override != null) {
        String partitionKey = entity.getEntity() + "_" + override.getEntity();
         tableInstancePartitions = instancePartitions.stream()
             .filter(partitions -> partitions.getInstancePartitionsName().equals(partitionKey))
             .collect(Collectors.toList());
      } else {
        tableInstancePartitions = instancePartitions.stream()
            .filter(partitions -> partitions.getInstancePartitionsName().startsWith(entity.getEntity() + "_"))
            .collect(Collectors.toList());
      }
      for (InstancePartitions partitions : tableInstancePartitions) {
        List<String> instances = partitions.getInstances(0, 0);
        actualInstances.addAll(instances);
        totalNumInstancesExpected += instances.size();
      }
    } else { // For brokers, get expected instances from broker resource external view
      Set<String> instances = externalView.getPartitionSet().stream()
          .filter(partition -> partition.startsWith(entity.getEntity() + "_"))
          .map(partition -> externalView.getStateMap(partition).keySet())
          .flatMap(Set::stream)
          .collect(Collectors.toSet());
      actualInstances.addAll(instances);
      totalNumInstancesExpected += instances.size();
    }
    assert actualInstances.equals(expectedInstances) : "Expected instances: " + expectedInstances + ", but found: "
        + actualInstances;
    assert actualInstances.size() == totalNumInstancesExpected : "Expected number of instances: "
        + totalNumInstancesExpected + ", but found: " + actualInstances.size();
  }

  private void mockBrokerResource(ExternalView externalView) {
    HelixManager helixManager = Mockito.mock(HelixManager.class);
    HelixAdmin helixAdmin = Mockito.mock(HelixAdmin.class);
    Mockito.when(_pinotHelixResourceManager.getHelixZkManager()).thenReturn(helixManager);
    Mockito.when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    Mockito.when(helixAdmin.getResourceExternalView(Mockito.any(), Mockito.eq("brokerResource")))
        .thenReturn(externalView);
  }

  private InstancePartitions createInstancePartitions(String tableName, InstancePartitionsType type, int numInstances) {
    InstancePartitions instancePartitions = new InstancePartitions(tableName + "_" + type);
    List<String> instances = new ArrayList<>();
    for (int i = 1; i <= numInstances; i++) {
      instances.add(tableName + type.name() + "_instance" + i + "_9000");
    }
    instancePartitions.setInstances(0, 0, instances);
    return instancePartitions;
  }

  private Map<String, String> createBrokerResourceStateMap(String tableName, int numInstances) {
    Map<String, String> stateMap = new HashMap<>();
    for (int i = 1; i <= numInstances; i++) {
      stateMap.put(tableName + "_BROKER_instance" + i + "_9000", "ONLINE");
    }
    return stateMap;
  }

  @Test
  public void testTenantPropagationScheme() {
    EnforcementProfile profile = new EnforcementProfile(100, 100);
    PropagationEntity entity1 = new PropagationEntity("tenant1", 50L, 50L, null);
    PropagationEntity entity2 = new PropagationEntity("tenant2", 50L, 50L, null);
    PropagationScheme propagationScheme = new PropagationScheme(PropagationScheme.Type.TENANT,
        List.of(entity1, entity2));
    // Mock the behavior of getAllHelixInstanceConfigs to return the list of instance configurations
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    instanceConfigs.add(createInstanceConfig("instance1", List.of("tenant1_REALTIME")));
    instanceConfigs.add(createInstanceConfig("instance2", List.of("tenant2_OFFLINE")));
    instanceConfigs.add(createInstanceConfig("instance3", List.of("tenant1_BROKER")));
    instanceConfigs.add(createInstanceConfig("instance4", List.of("tenant2_BROKER")));
    Mockito.when(_pinotHelixResourceManager.getAllHelixInstanceConfigs()).thenReturn(instanceConfigs);

    Map<String, Map<NodeConfig.Type, Set<String>>> actualTenantsToInstances = new HashMap<>();
    actualTenantsToInstances.put("tenant1", Map.of(NodeConfig.Type.SERVER_NODE, Set.of("instance1"),
        NodeConfig.Type.BROKER_NODE, Set.of("instance3")));
    actualTenantsToInstances.put("tenant2", Map.of(NodeConfig.Type.SERVER_NODE, Set.of("instance2"),
        NodeConfig.Type.BROKER_NODE, Set.of("instance4")));

    // Case 1 : Test for Server instance resolution for tenant1 and tenant2
    NodeConfig nodeConfigServer = new NodeConfig(NodeConfig.Type.SERVER_NODE, profile, propagationScheme);
    List<PropagationEntity> entities = nodeConfigServer.getPropagationScheme().getPropagationEntities();
    for (PropagationEntity entity : entities) {
      NodeConfig.Type nodeType = nodeConfigServer.getNodeType();
      Set<String> expectedServerInstances = _tenantPropagationScheme.resolveInstances(entity, nodeType, null);
      assertTenantSchemeResponse(entity, nodeType, expectedServerInstances, actualTenantsToInstances);
    }

    // Case 2 : Test for Broker instance resolution for tenant1 and tenant2
    NodeConfig nodeConfigBroker = new NodeConfig(NodeConfig.Type.BROKER_NODE, profile, propagationScheme);
    entities = nodeConfigBroker.getPropagationScheme().getPropagationEntities();
    for (PropagationEntity entity : entities) {
      NodeConfig.Type nodeType = nodeConfigBroker.getNodeType();
      Set<String> expectedBrokerInstances = _tenantPropagationScheme.resolveInstances(entity, nodeType, null);
      assertTenantSchemeResponse(entity, nodeType, expectedBrokerInstances, actualTenantsToInstances);
    }
  }

  private void assertTenantSchemeResponse(PropagationEntity entity, NodeConfig.Type nodeType,
                                          Set<String> expectedInstances,
                                          Map<String, Map<NodeConfig.Type, Set<String>>> actualTenantsToInstances) {
    for (String tenant : actualTenantsToInstances.keySet()) {
      if (tenant.equals(entity.getEntity())) {
        Set<String> actualInstances = actualTenantsToInstances.get(tenant).get(nodeType);
        assert actualInstances.equals(expectedInstances) : "Expected instances: " + expectedInstances + ", but found: "
            + actualInstances;
        assert actualInstances.size() == expectedInstances.size() : "Expected number of instances: "
            + expectedInstances.size() + ", but found: " + actualInstances.size();
      }
    }
  }

  private InstanceConfig createInstanceConfig(String instanceName, List<String> helixTags) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    for (String helixTag : helixTags) {
      instanceConfig.addTag(helixTag);
    }
    return instanceConfig;
  }
}
