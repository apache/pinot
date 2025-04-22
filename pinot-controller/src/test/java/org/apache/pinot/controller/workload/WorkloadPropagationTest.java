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

import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.workload.scheme.WorkloadPropagationUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class WorkloadPropagationTest {

    private PinotHelixResourceManager _pinotHelixResourceManager;

    @BeforeClass
    public void setUp() {
        _pinotHelixResourceManager = Mockito.mock(PinotHelixResourceManager.class);
    }

    @Test
    public void getTableToHelixTagsTest() {
        // Create a list of table configurations
        List<TableConfig> tableConfigs = new ArrayList<>();
        tableConfigs.add(createTableConfig("table1", "serverTag1", "brokerTenant1", TableType.OFFLINE));
        tableConfigs.add(createTableConfig("table2", "serverTag2", "brokerTenant2", TableType.REALTIME));
        // Mock the behavior of getAllTableConfigs to return the list of table configurations
        Mockito.when(_pinotHelixResourceManager.getAllTableConfigs()).thenReturn(tableConfigs);
        // Call the method to get table to Helix tags
        Map<String, Map<NodeConfig.Type, Set<String>>> tableToHelixTags
                = WorkloadPropagationUtils.getTableToHelixTags(_pinotHelixResourceManager);
        // Verify the results
        Map<String, Map<NodeConfig.Type, Set<String>>> expectedTags = new HashMap<>();
        expectedTags.put("table1_OFFLINE", new HashMap<>() {{
            put(NodeConfig.Type.LEAF_NODE, Set.of("serverTag1_OFFLINE"));
            put(NodeConfig.Type.NON_LEAF_NODE, Set.of("brokerTenant1_BROKER"));
        }});
        expectedTags.put("table2_REALTIME", new HashMap<>() {{
            put(NodeConfig.Type.LEAF_NODE, Set.of("serverTag2_REALTIME"));
            put(NodeConfig.Type.NON_LEAF_NODE, Set.of("brokerTenant2_BROKER"));
        }});

        Assert.assertEquals(tableToHelixTags.size(), expectedTags.size(),
                "Expected size of table to helix tags mapping does not match");
        for (Map.Entry<String, Map<NodeConfig.Type, Set<String>>> tableEntry : expectedTags.entrySet()) {
            String tableName = tableEntry.getKey();
            Map<NodeConfig.Type, Set<String>> expectedNodeTags = tableEntry.getValue();
            // For each node type in the expected map, assert the tag exists
            for (Map.Entry<NodeConfig.Type, Set<String>> entry : expectedNodeTags.entrySet()) {
                NodeConfig.Type nodeType = entry.getKey();
                Set<String> expectedTag = entry.getValue();
                Assert.assertEquals(tableToHelixTags.get(tableName).get(nodeType), expectedTag,
                        "Expected " + expectedTag + " for " + tableName + " with node type " + nodeType
                                + " but found " + tableToHelixTags.get(tableName).get(nodeType));
            }
        }
    }

    @Test
    public void getHelixTagsForTableTest() {
        // Mock the behavior of getHelixTagsForTable to return a set of helix tags
        TableConfig tableConfig = createTableConfig("table1", "serverTag1", "brokerTenant1", TableType.OFFLINE);
        TableConfig tableConfig2 = createTableConfig("table1", "serverTag2", "brokerTenant2", TableType.REALTIME);
        Mockito.when(_pinotHelixResourceManager.getTableConfig("table1_OFFLINE")).thenReturn(tableConfig);
        Mockito.when(_pinotHelixResourceManager.getTableConfig("table1_REALTIME")).thenReturn(tableConfig2);

        // Define the expected helix tags for the table
        Map<String, Set<String>> expected = new HashMap<>();
        expected.put("table1_OFFLINE", Set.of("serverTag1_OFFLINE", "brokerTenant1_BROKER"));
        expected.put("table1_REALTIME", Set.of("serverTag2_REALTIME", "brokerTenant2_BROKER"));

        for (Map.Entry<String, Set<String>> entry : expected.entrySet()) {
            String tableName = entry.getKey();
            Set<String> expectedHelixTags = entry.getValue();
            // Call the method to get helix tags for the table
            Set<String> helixTags = WorkloadPropagationUtils.getHelixTagsForTable(_pinotHelixResourceManager,
                    tableName);
            // Verify the results
            for (String helixTag : expectedHelixTags) {
                Assert.assertTrue(helixTags.contains(helixTag),
                        "Expected helix tag " + helixTag + " for table " + tableName + " but found " + helixTags);
            }
        }
    }

    @Test
    public void getHelixTagToInstancesTest() {
        // Create a list of instance configurations
        List<InstanceConfig> instanceConfigs = List.of(
                createInstanceConfig("instance1", List.of("serverTag1_OFFLINE")),
                createInstanceConfig("instance2", List.of("serverTag1_OFFLINE")),
                createInstanceConfig("instance3", List.of("brokerTenant1_BROKER")),
                createInstanceConfig("instance4", List.of("brokerTenant1_BROKER"))
        );
        Mockito.when(_pinotHelixResourceManager.getAllHelixInstanceConfigs()).thenReturn(instanceConfigs);
        // Call the method to get Helix tag to instances mapping
        Map<String, Set<String>> helixTagToInstances
                = WorkloadPropagationUtils.getHelixTagToInstances(_pinotHelixResourceManager);
        // Verify the results
        Map<String, Set<String>> expected = new HashMap<>();
        expected.put("serverTag1_OFFLINE", Set.of("instance1", "instance2"));
        expected.put("brokerTenant1_BROKER", Set.of("instance3", "instance4"));

        Assert.assertEquals(helixTagToInstances.size(), expected.size(),
                "Expected size of helix tag to instances mapping does not match");
        for (Map.Entry<String, Set<String>> entry : expected.entrySet()) {
            String helixTag = entry.getKey();
            Set<String> expectedInstances = entry.getValue();
            Assert.assertTrue(helixTagToInstances.containsKey(helixTag),
                    "Expected helix tag " + helixTag + " but not found in the mapping");
            for (String instance : expectedInstances) {
                Assert.assertTrue(helixTagToInstances.get(helixTag).contains(instance),
                        "Expected instance " + instance + " for helix tag " + helixTag + " but found "
                                + helixTagToInstances.get(helixTag));
            }
        }
    }

    @Test
    public void getInstanceToHelixTagsTest() {
        // Create a list of instance configurations
        List<InstanceConfig> instanceConfigs = List.of(
                createInstanceConfig("instance1", List.of("serverTag1_OFFLINE", "serverTag2_REALTIME")),
                createInstanceConfig("instance2", List.of("brokerTenant1_BROKER", "brokerTenant2_BROKER"))
        );
        Mockito.when(_pinotHelixResourceManager.getAllHelixInstanceConfigs()).thenReturn(instanceConfigs);
        // Call the method to get instance to Helix tags mapping
        Map<String, Set<String>> instanceToHelixTags
                = WorkloadPropagationUtils.getInstanceToHelixTags(_pinotHelixResourceManager);
        // Verify the results
        Map<String, Set<String>> expected = new HashMap<>();
        expected.put("instance1", Set.of("serverTag1_OFFLINE", "serverTag2_REALTIME"));
        expected.put("instance2", Set.of("brokerTenant1_BROKER", "brokerTenant2_BROKER"));

        Assert.assertEquals(instanceToHelixTags.size(), expected.size(),
                "Expected size of instance to helix tags mapping does not match");
        for (Map.Entry<String, Set<String>> entry : expected.entrySet()) {
            String instanceName = entry.getKey();
            Set<String> expectedHelixTags = entry.getValue();
            Assert.assertTrue(instanceToHelixTags.containsKey(instanceName),
                    "Expected instance " + instanceName + " but not found in the mapping");
            for (String helixTag : expectedHelixTags) {
                Assert.assertTrue(instanceToHelixTags.get(instanceName).contains(helixTag),
                        "Expected helix tag " + helixTag + " for instance " + instanceName + " but found "
                                + instanceToHelixTags.get(instanceName));
            }
        }
    }

    @Test
    public void getQueryWorkloadConfigsForTagsTest() {
        // Create a list of query workload configurations
        QueryWorkloadConfig workloadConfig1 = createQueryWorkloadConfig("workload1",
            new PropagationScheme(PropagationScheme.Type.TABLE, List.of("table1", "table2")),
            new PropagationScheme(PropagationScheme.Type.TABLE, List.of("table1", "table2")));
        QueryWorkloadConfig workloadConfig2 = createQueryWorkloadConfig("workload2",
            new PropagationScheme(PropagationScheme.Type.TENANT, List.of("serverTag1")),
            new PropagationScheme(PropagationScheme.Type.TENANT, List.of("brokerTenant1_BROKER")));
        QueryWorkloadConfig workloadConfig3 = createQueryWorkloadConfig("workload3",
            new PropagationScheme(PropagationScheme.Type.TENANT, List.of("serverTag2_REALTIME")),
            new PropagationScheme(PropagationScheme.Type.TENANT, List.of("brokerTenant2")));
        List<QueryWorkloadConfig> queryWorkloadConfigs = List.of(workloadConfig1, workloadConfig2, workloadConfig3);
        // Create TableConfig for the workload
        List<TableConfig> tableConfigs = List.of(
                createTableConfig("table1", "serverTag1", "brokerTenant1", TableType.OFFLINE),
                createTableConfig("table2", "serverTag2", "brokerTenant2", TableType.REALTIME)
        );
        // Mock the behavior of getAllQueryWorkloadConfigs to return the list of query workload configurations
        Mockito.when(_pinotHelixResourceManager.getAllQueryWorkloadConfigs()).thenReturn(queryWorkloadConfigs);
        // Mock the behavior of getAllTableConfigs to return the list of table configurations
        Mockito.when(_pinotHelixResourceManager.getAllTableConfigs()).thenReturn(tableConfigs);

        Set<String> helixTags = Set.of("serverTag1_OFFLINE", "brokerTenant1_BROKER",
                "serverTag2_REALTIME", "brokerTenant2_BROKER");
        Set<QueryWorkloadConfig> workloadConfigsForTags
                = WorkloadPropagationUtils.getQueryWorkloadConfigsForTags(_pinotHelixResourceManager, helixTags);
        // Verify the results
        Set<QueryWorkloadConfig> expectedWorkloadConfigs = Set.of(workloadConfig1, workloadConfig2, workloadConfig3);
        Assert.assertEquals(workloadConfigsForTags.size(), expectedWorkloadConfigs.size(),
                "Expected size of workload configs for tags does not match");
        for (QueryWorkloadConfig workloadConfig : workloadConfigsForTags) {
            Assert.assertTrue(expectedWorkloadConfigs.contains(workloadConfig),
                    "Expected workload config " + workloadConfig.getQueryWorkloadName()
                            + " but not found in the expected set");
        }
    }

    private TableConfig createTableConfig(String tableName, String serverTag, String brokerTenant, TableType type) {
        return new TableConfigBuilder(type)
            .setTableName(tableName)
            .setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy")
            .setNumReplicas(1)
            .setBrokerTenant(brokerTenant)
            .setServerTenant(serverTag)
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

    private QueryWorkloadConfig createQueryWorkloadConfig(String name, PropagationScheme leafScheme,
                                                          PropagationScheme nonLeafScheme) {
        EnforcementProfile enforcementProfile = new EnforcementProfile(10, 10, 600L);
        return new QueryWorkloadConfig(name, Map.of(
          NodeConfig.Type.LEAF_NODE, new NodeConfig(enforcementProfile, leafScheme),
          NodeConfig.Type.NON_LEAF_NODE, new NodeConfig(enforcementProfile, nonLeafScheme)
      ));
    }
}
