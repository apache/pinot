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
package com.linkedin.pinot.controller.helix.core.realtime.partition;

import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.StreamConsumptionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TenantConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class StreamPartitionAssignmentStrategyTest {

  private static final String DEFAULT_SERVER_TENANT = "defaultTenant";
  private String[] serverNames;

  @BeforeTest
  public void setUp() {
    final int maxInstances = 20;
    serverNames = new String[maxInstances];
    for (int i = 0; i < maxInstances; i++) {
      serverNames[i] = "Server_" + i;
    }
  }

  /**
   * Tests for {@link UniformStreamPartitionAssignmentStrategy}
   * Validates partition assignment and checks that instances were assigned only the right amount of partitions
   */
  @Test
  public void testUniformPartitionAssignmentStrategy() {

    List<TableConfig> allTableConfigsInSameTenant = new ArrayList<>(1);
    Map<String, Integer> nPartitionsExpected = new HashMap<>(1);
    String table1 = "rtPartitionAwareTable1_REALTIME";
    String table2 = "rtPartitionAwareTable2_REALTIME";
    int nReplicasExpected = 2;
    List<String> instanceNames = null;
    Map<String, List<RealtimePartition>> currentAssignment = new HashMap<>(1);

    // single new table
    TableConfig tableConfig1 = makeTableConfig(table1, nReplicasExpected, DEFAULT_SERVER_TENANT,
        StreamPartitionAssignmentStrategyEnum.UniformStreamPartitionAssignment);
    int nKafkaPartitions1 = 2;
    allTableConfigsInSameTenant.add(tableConfig1);
    nPartitionsExpected.put(table1, nKafkaPartitions1);
    instanceNames = getInstanceList(8);
    currentAssignment =
        uniformPartitionAssignmentStrategyTest(tableConfig1, nKafkaPartitions1, instanceNames, currentAssignment,
            allTableConfigsInSameTenant, 1, nPartitionsExpected, nReplicasExpected);

    // no change
    currentAssignment =
        uniformPartitionAssignmentStrategyTest(tableConfig1, nKafkaPartitions1, instanceNames, currentAssignment,
            allTableConfigsInSameTenant, 1, nPartitionsExpected, nReplicasExpected);

    // instances change
    instanceNames = getInstanceList(10);
    currentAssignment =
        uniformPartitionAssignmentStrategyTest(tableConfig1, nKafkaPartitions1, instanceNames, currentAssignment,
            allTableConfigsInSameTenant, 1, nPartitionsExpected, nReplicasExpected);

    // partitions change
    nKafkaPartitions1 = 10;
    nPartitionsExpected.put(table1, nKafkaPartitions1);
    currentAssignment =
        uniformPartitionAssignmentStrategyTest(tableConfig1, nKafkaPartitions1, instanceNames, currentAssignment,
            allTableConfigsInSameTenant, 1, nPartitionsExpected, nReplicasExpected);

    // multiple tables
    TableConfig tableConfig2 = makeTableConfig(table2, nReplicasExpected, DEFAULT_SERVER_TENANT,
        StreamPartitionAssignmentStrategyEnum.UniformStreamPartitionAssignment);
    int nKafkaPartitions2 = 8;
    allTableConfigsInSameTenant.add(tableConfig2);
    nPartitionsExpected.put(table2, nKafkaPartitions2);
    currentAssignment =
        uniformPartitionAssignmentStrategyTest(tableConfig2, nKafkaPartitions2, instanceNames, currentAssignment,
            allTableConfigsInSameTenant, 1, nPartitionsExpected, nReplicasExpected);

    // no change
    currentAssignment =
        uniformPartitionAssignmentStrategyTest(tableConfig2, nKafkaPartitions2, instanceNames, currentAssignment,
            allTableConfigsInSameTenant, 1, nPartitionsExpected, nReplicasExpected);

    // change in 2 - partitions
    nKafkaPartitions2 = 12;
    nPartitionsExpected.put(table2, nKafkaPartitions2);
    currentAssignment =
        uniformPartitionAssignmentStrategyTest(tableConfig2, nKafkaPartitions2, instanceNames, currentAssignment,
            allTableConfigsInSameTenant, 1, nPartitionsExpected, nReplicasExpected);

    // change in instances
    instanceNames = getInstanceList(8);
    currentAssignment =
        uniformPartitionAssignmentStrategyTest(tableConfig2, nKafkaPartitions2, instanceNames, currentAssignment,
            allTableConfigsInSameTenant, 1, nPartitionsExpected, nReplicasExpected);
  }

  private Map<String, List<RealtimePartition>> uniformPartitionAssignmentStrategyTest(TableConfig tableConfig,
      int nKafkaPartitions, List<String> instanceNames, Map<String, List<RealtimePartition>> currentPartitionAssignment,
      List<TableConfig> allTableConfigsInTenant, int numTablesExpected, Map<String, Integer> nPartitionsExpected,
      int nReplicasExpected) {

    UniformStreamPartitionAssignmentStrategy strategy = new UniformStreamPartitionAssignmentStrategy();
    strategy.init(allTableConfigsInTenant, instanceNames, currentPartitionAssignment);
    Map<String, List<RealtimePartition>> uniformPartitionAssignment =
        strategy.generatePartitionAssignment(tableConfig, nKafkaPartitions);

    Assert.assertEquals(uniformPartitionAssignment.size(), numTablesExpected);

    // validate assignment for number of partitions for each table, number replicas for each table, and correct instances used
    validatePartitionAssignment(uniformPartitionAssignment, nPartitionsExpected, nReplicasExpected, instanceNames);

    // partition aware assignment will simply spray partition replicas to list of servers uniformly.
    // check that servers got expected number of assignments
    for (Map.Entry<String, List<RealtimePartition>> entry : uniformPartitionAssignment.entrySet()) {
      List<RealtimePartition> assignment = entry.getValue();
      Map<String, Integer> instanceToPartitionCount = new HashMap<>(instanceNames.size());
      for (String instance : instanceNames) {
        instanceToPartitionCount.put(instance, 0);
      }
      for (RealtimePartition partition : assignment) {
        for (String instance : partition.getInstanceNames()) {
          int partitionCount = instanceToPartitionCount.get(instance);
          instanceToPartitionCount.put(instance, partitionCount + 1);
        }
      }
      String tableName = entry.getKey();
      int expectedPartitionCountMin = (nReplicasExpected * nPartitionsExpected.get(tableName)) / instanceNames.size();
      int expectedPartitionCountMax = expectedPartitionCountMin + 1;
      for (Integer partitionCount : instanceToPartitionCount.values()) {
        Assert.assertTrue(partitionCount == expectedPartitionCountMin || partitionCount == expectedPartitionCountMax);
      }
    }
    return uniformPartitionAssignment;
  }

  /**
   * Tests for autoRebalancePartitionAssignment
   * Validates partition assignment and checks that assignment was changed only when necessary
   */
  @Test
  public void testGenerateAutoRebalanceAssignment() {

    List<TableConfig> allTableConfigsInSameTenant = new ArrayList<>(1);
    Map<String, Integer> nPartitionsExpected = new HashMap<>(1);
    String table1 = "rtTable1_REALTIME";
    String table2 = "rtTable2_REALTIME";
    int nReplicasExpected = 2;
    List<String> instanceNames = null;
    Map<String, List<RealtimePartition>> currentAssignment = new HashMap<>(1);
    Map<String, Boolean> assignmentChanged = new HashMap<>(1);

    // single new table
    TableConfig tableConfig1 = makeTableConfig(table1, nReplicasExpected, DEFAULT_SERVER_TENANT,
        StreamPartitionAssignmentStrategyEnum.BalancedStreamPartitionAssignment);
    int nKafkaPartitions1 = 8;
    allTableConfigsInSameTenant.add(tableConfig1);
    nPartitionsExpected.put(table1, nKafkaPartitions1);
    instanceNames = getInstanceList(4);
    assignmentChanged.put(table1, true);
    currentAssignment = autoRebalanceTest(tableConfig1, nKafkaPartitions1, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 1, nPartitionsExpected, nReplicasExpected);

    // no change
    assignmentChanged.put(table1, false);
    currentAssignment = autoRebalanceTest(tableConfig1, nKafkaPartitions1, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 1, nPartitionsExpected, nReplicasExpected);

    // instances change
    instanceNames = getInstanceList(10);
    assignmentChanged.put(table1, true);
    currentAssignment = autoRebalanceTest(tableConfig1, nKafkaPartitions1, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 1, nPartitionsExpected, nReplicasExpected);

    // partitions change
    nKafkaPartitions1 = 10;
    nPartitionsExpected.put(table1, nKafkaPartitions1);
    currentAssignment = autoRebalanceTest(tableConfig1, nKafkaPartitions1, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 1, nPartitionsExpected, nReplicasExpected);

    // multiple tables
    TableConfig tableConfig2 = makeTableConfig(table2, nReplicasExpected, DEFAULT_SERVER_TENANT,
        StreamPartitionAssignmentStrategyEnum.BalancedStreamPartitionAssignment);
    int nKafkaPartitions2 = 8;
    allTableConfigsInSameTenant.add(tableConfig2);
    assignmentChanged.put(table1, false);
    assignmentChanged.put(table2, true);
    nPartitionsExpected.put(table2, nKafkaPartitions2);
    currentAssignment = autoRebalanceTest(tableConfig2, nKafkaPartitions2, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 2, nPartitionsExpected, nReplicasExpected);

    // no change
    assignmentChanged.put(table2, false);
    currentAssignment = autoRebalanceTest(tableConfig2, nKafkaPartitions2, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 2, nPartitionsExpected, nReplicasExpected);

    // change in 2 - partitions
    nKafkaPartitions2 = 12;
    nPartitionsExpected.put(table2, nKafkaPartitions2);
    assignmentChanged.put(table2, true);
    currentAssignment = autoRebalanceTest(tableConfig2, nKafkaPartitions2, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 2, nPartitionsExpected, nReplicasExpected);

    // change in instances
    instanceNames = getInstanceList(8);
    assignmentChanged.put(table1, true);
    assignmentChanged.put(table2, true);
    currentAssignment = autoRebalanceTest(tableConfig2, nKafkaPartitions2, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 2, nPartitionsExpected, nReplicasExpected);
  }

  private Map<String, List<RealtimePartition>> autoRebalanceTest(TableConfig tableConfig, int nKafkaPartitions,
      List<String> instanceNames, Map<String, List<RealtimePartition>> currentPartitionAssignment,
      List<TableConfig> allTableConfigsInTenant, Map<String, Boolean> assignmentChanged, int numTablesExpected,
      Map<String, Integer> nPartitionsExpected, int nReplicasExpected) {

    BalancedStreamPartitionAssignmentStrategy strategy = new BalancedStreamPartitionAssignmentStrategy();
    strategy.init(allTableConfigsInTenant, instanceNames, currentPartitionAssignment);

    Map<String, List<RealtimePartition>> autoRebalancePartitionAssignment =
        strategy.generatePartitionAssignment(tableConfig, nKafkaPartitions);

    // check that we got a new assignment for each table in our list
    Assert.assertEquals(autoRebalancePartitionAssignment.size(), numTablesExpected);

    // validate assignment for number of partitions for each table, number replicas for each table, and correct instances used
    validatePartitionAssignment(autoRebalancePartitionAssignment, nPartitionsExpected, nReplicasExpected,
        instanceNames);

    // validate if assignment changed when it should have
    for (Map.Entry<String, List<RealtimePartition>> entry : autoRebalancePartitionAssignment.entrySet()) {
      String tableName = entry.getKey();
      if (assignmentChanged.get(tableName)) {
        Assert.assertFalse(entry.getValue().equals(currentPartitionAssignment.get(tableName)));
      } else {
        for (int i = 0; i < entry.getValue().size(); i++) {
          Assert.assertEquals(entry.getValue().get(i), currentPartitionAssignment.get(tableName).get(i));
        }
      }
    }
    return autoRebalancePartitionAssignment;
  }

  // Make a tableconfig that returns the table name and nReplicas per partition as we need it.
  private TableConfig makeTableConfig(String tableName, int nReplicas, String serverTenant,
      StreamPartitionAssignmentStrategyEnum strategy) {
    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.getTableName()).thenReturn(tableName);
    SegmentsValidationAndRetentionConfig mockValidationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(mockValidationConfig.getReplicasPerPartition()).thenReturn(Integer.toString(nReplicas));
    when(mockValidationConfig.getReplicasPerPartitionNumber()).thenReturn(nReplicas);
    when(mockTableConfig.getValidationConfig()).thenReturn(mockValidationConfig);

    StreamConsumptionConfig mockStreamConsumptionConfig = mock(StreamConsumptionConfig.class);
    when(mockStreamConsumptionConfig.getStreamPartitionAssignmentStrategy()).thenReturn(strategy.toString());
    IndexingConfig mockIndexConfig = mock(IndexingConfig.class);
    when(mockIndexConfig.getStreamConsumptionConfig()).thenReturn(mockStreamConsumptionConfig);
    when(mockTableConfig.getIndexingConfig()).thenReturn(mockIndexConfig);
    TenantConfig mockTenantConfig = mock(TenantConfig.class);
    when(mockTenantConfig.getServer()).thenReturn(serverTenant);
    when(mockTableConfig.getTenantConfig()).thenReturn(mockTenantConfig);

    return mockTableConfig;
  }

  private List<String> getInstanceList(final int nServers) {
    Assert.assertTrue(nServers <= serverNames.length);
    String[] instanceArray = Arrays.copyOf(serverNames, nServers);
    return Arrays.asList(instanceArray);
  }

  private void validatePartitionAssignment(Map<String, List<RealtimePartition>> tableNameToPartitions,
      Map<String, Integer> nKafkaPartitions, int nReplicas, List<String> instances) {
    List<RealtimePartition> partitionsList;
    for (Map.Entry<String, List<RealtimePartition>> entry : tableNameToPartitions.entrySet()) {
      String tableName = entry.getKey();
      partitionsList = entry.getValue();
      Assert.assertEquals(partitionsList.size(), nKafkaPartitions.get(tableName).intValue());
      for (RealtimePartition partition : partitionsList) {
        Assert.assertEquals(partition.getInstanceNames().size(), nReplicas);
        Assert.assertTrue(instances.containsAll(partition.getInstanceNames()));
      }
    }
  }
}

