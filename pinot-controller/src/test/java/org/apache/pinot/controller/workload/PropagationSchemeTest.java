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
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.workload.scheme.TablePropagationScheme;
import org.apache.pinot.controller.workload.scheme.TenantPropagationScheme;
import org.apache.pinot.controller.workload.splitter.CostSplitter;
import org.apache.pinot.controller.workload.splitter.DefaultCostSplitter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.workload.CostSplit;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PropagationSchemeTest {

  private PinotHelixResourceManager _pinotHelixResourceManager;
  private TablePropagationScheme _tablePropagationScheme;
  private TenantPropagationScheme _tenantPropagationScheme;
  private CostSplitter _costSplitter;

  @BeforeClass
  public void setUp() {
    _pinotHelixResourceManager = Mockito.mock(PinotHelixResourceManager.class);
    _tablePropagationScheme = new TablePropagationScheme(_pinotHelixResourceManager);
    _tenantPropagationScheme = new TenantPropagationScheme(_pinotHelixResourceManager);
    _costSplitter = new DefaultCostSplitter();
  }

  @Test
  public void testTablePropagationScheme() {
    EnforcementProfile profile = new EnforcementProfile(100, 100);
    CostSplit split1 = new CostSplit("table1", 50L, 50L, null);
    CostSplit split2 = new CostSplit("table2", 50L, 50L, null);
    PropagationScheme propagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, List.of(split1, split2));

    // Mock the behavior of getAllTableConfigs to return the list of table configurations
    List<TableConfig> tableConfigs = new ArrayList<>();
    tableConfigs.add(createTableConfig("table1", "serverTag1", "brokerTenant1", TableType.OFFLINE, null));
    TagOverrideConfig tagOverrideConfig = new TagOverrideConfig(null, "serverTag2CompletedOffline");
    tableConfigs.add(createTableConfig("table2", "serverTag2", "brokerTenant2", TableType.REALTIME, tagOverrideConfig));
    Mockito.when(_pinotHelixResourceManager.getAllTableConfigs()).thenReturn(tableConfigs);
    // Mock the behavior of getAllHelixInstanceConfigs to return the list of instance configurations
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    instanceConfigs.add(createInstanceConfig("instance1", List.of("serverTag1_OFFLINE")));
    instanceConfigs.add(createInstanceConfig("instance2", List.of("serverTag2_REALTIME")));
    Mockito.when(_pinotHelixResourceManager.getAllHelixInstanceConfigs()).thenReturn(instanceConfigs);

    // Case 1 : Test for cost split for table1 (offline) and table2 (realtime) for servers
    NodeConfig nodeConfigServer = new NodeConfig(NodeConfig.Type.SERVER_NODE, profile, propagationScheme);
    Map<String, InstanceCost> instanceCostMap = _tablePropagationScheme.resolveInstanceCostMap(nodeConfigServer,
        _costSplitter);
    // Verify the instance cost map for server node
    assert instanceCostMap.size() == 2;
    assert instanceCostMap.get("instance1").getCpuCostNs() == 50
        && instanceCostMap.get("instance1").getMemoryCostBytes() == 50;
    assert instanceCostMap.get("instance2").getCpuCostNs() == 50
        && instanceCostMap.get("instance2").getMemoryCostBytes() == 50;

    // Case 2 : Test for cost split for table1 (offline) and table2 (realtime) for brokers
    instanceConfigs.add(createInstanceConfig("instance3", List.of("brokerTenant1_BROKER")));
    instanceConfigs.add(createInstanceConfig("instance4", List.of("brokerTenant2_BROKER")));
    NodeConfig nodeConfigBroker = new NodeConfig(NodeConfig.Type.BROKER_NODE, profile, propagationScheme);
    instanceCostMap = _tablePropagationScheme.resolveInstanceCostMap(nodeConfigBroker, _costSplitter);
    // Verify the instance cost map for broker node
    assert instanceCostMap.size() == 2;
    assert instanceCostMap.get("instance3").getCpuCostNs() == 50
        && instanceCostMap.get("instance3").getMemoryCostBytes() == 50;
    assert instanceCostMap.get("instance4").getCpuCostNs() == 50
        && instanceCostMap.get("instance4").getMemoryCostBytes() == 50;

    // Case 3 : Test for cost split for table1 and table2 with sub-allocations for servers in table2
    // Add SubAllocation for table2 (realtime)
    CostSplit subSplit1 = new CostSplit("serverTag2_REALTIME", 25L, 25L, null); // Consuming
    CostSplit subSplit2 = new CostSplit("serverTag2CompletedOffline", 25L, 25L, null); // Completed
    split2.setSubAllocations(List.of(subSplit1, subSplit2));
    // Add instance for completed tenant
    instanceConfigs.add(createInstanceConfig("instance5", List.of("serverTag2CompletedOffline")));
    instanceCostMap = _tablePropagationScheme.resolveInstanceCostMap(nodeConfigServer, _costSplitter);
    // Verify the instance cost map for server node with sub-allocations
    assert instanceCostMap.size() == 3;
    assert instanceCostMap.get("instance1").getCpuCostNs() == 50
        && instanceCostMap.get("instance1").getMemoryCostBytes() == 50;
    assert instanceCostMap.get("instance2").getCpuCostNs() == 25
        && instanceCostMap.get("instance2").getMemoryCostBytes() == 25;
    assert instanceCostMap.get("instance5").getCpuCostNs() == 25
        && instanceCostMap.get("instance5").getMemoryCostBytes() == 25;

    // Case 4 : Test resolveInstances for a node config with multiple cost splits
    Set<String> instances = _tablePropagationScheme.resolveInstances(nodeConfigServer);
    // Verify the instances resolved for server node
    assert instances.size() == 3;
    instances = _tablePropagationScheme.resolveInstances(nodeConfigBroker);
    // Verify the instances resolved for broker node
    assert instances.size() == 2;
  }

  @Test
  public void testTenantPropagationScheme() {
    EnforcementProfile profile = new EnforcementProfile(100, 100);
    CostSplit split1 = new CostSplit("tenant1", 50L, 50L, null);
    CostSplit split2 = new CostSplit("tenant2", 50L, 50L, null);
    PropagationScheme propagationScheme = new PropagationScheme(PropagationScheme.Type.TENANT, List.of(split1, split2));
    // Mock the behavior of getAllHelixInstanceConfigs to return the list of instance configurations
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    instanceConfigs.add(createInstanceConfig("instance1", List.of("tenant1_REALTIME")));
    instanceConfigs.add(createInstanceConfig("instance2", List.of("tenant2_OFFLINE")));
    Mockito.when(_pinotHelixResourceManager.getAllHelixInstanceConfigs()).thenReturn(instanceConfigs);
    // Case 1 : Test for cost split for tenant1 and tenant2 for servers
    NodeConfig nodeConfigServer = new NodeConfig(NodeConfig.Type.SERVER_NODE, profile, propagationScheme);
    Map<String, InstanceCost> instanceCostMap = _tenantPropagationScheme.resolveInstanceCostMap(nodeConfigServer,
        _costSplitter);
    // Verify the instance cost map for server node
    assert instanceCostMap.size() == 2;
    assert instanceCostMap.get("instance1").getCpuCostNs() == 50
        && instanceCostMap.get("instance1").getMemoryCostBytes() == 50;
    assert instanceCostMap.get("instance2").getCpuCostNs() == 50
        && instanceCostMap.get("instance2").getMemoryCostBytes() == 50;

    // Case 2 : Test for cost split for tenant1 and tenant2 for brokers
    instanceConfigs.add(createInstanceConfig("instance3", List.of("tenant1_BROKER")));
    instanceConfigs.add(createInstanceConfig("instance4", List.of("tenant2_BROKER")));
    NodeConfig nodeConfigBroker = new NodeConfig(NodeConfig.Type.BROKER_NODE, profile, propagationScheme);
    instanceCostMap = _tenantPropagationScheme.resolveInstanceCostMap(nodeConfigBroker, _costSplitter);
    // Verify the instance cost map for broker node
    assert instanceCostMap.size() == 2;
    assert instanceCostMap.get("instance3").getCpuCostNs() == 50
        && instanceCostMap.get("instance3").getMemoryCostBytes() == 50;
    assert instanceCostMap.get("instance4").getCpuCostNs() == 50
        && instanceCostMap.get("instance4").getMemoryCostBytes() == 50;

    // Case 3 : Test resolveInstances for a node config with multiple cost splits
    Set<String> instances = _tenantPropagationScheme.resolveInstances(nodeConfigServer);
    // Verify the instances resolved for server node
    assert instances.size() == 2;
    instances = _tenantPropagationScheme.resolveInstances(nodeConfigBroker);
    // Verify the instances resolved for broker node
    assert instances.size() == 2;

    // Case 4 : Test for exception when sub-allocations are present in tenant propagation scheme,
    // since it's not supported
    CostSplit subSplit1 = new CostSplit("table1", 25L, 25L, null);
    CostSplit subSplit2 = new CostSplit("table2", 25L, 25L, null);
    split1.setSubAllocations(List.of(subSplit1, subSplit2));
    try {
      _tenantPropagationScheme.resolveInstanceCostMap(nodeConfigServer, _costSplitter);
      assert false : "Expected IllegalArgumentException for sub-allocations in TenantPropagationScheme";
    } catch (IllegalArgumentException e) {
      // Expected exception
      assert true;
    }
  }

  public TableConfig createTableConfig(String tableName, String serverTag, String brokerTenant, TableType type,
                                       @Nullable TagOverrideConfig tagOverrideConfig) {
    return new TableConfigBuilder(type)
      .setTableName(tableName)
      .setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy")
      .setNumReplicas(1)
      .setBrokerTenant(brokerTenant)
      .setServerTenant(serverTag)
      .setTagOverrideConfig(tagOverrideConfig)
      .setLoadMode("HEAP")
      .setSegmentVersion(null)
      .build();
  }

  private InstanceConfig createInstanceConfig(String instanceName, List<String> helixTags) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    for (String helixTag : helixTags) {
      instanceConfig.addTag(helixTag);
    }
    return instanceConfig;
  }
}
