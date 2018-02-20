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
import com.linkedin.pinot.controller.helix.PartitionAssignment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class StreamPartitionAssignmentGenerationTest {

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
    Map<String, PartitionAssignment> currentAssignment = new HashMap<>(1);

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

  private Map<String, PartitionAssignment> uniformPartitionAssignmentStrategyTest(TableConfig tableConfig,
      int nKafkaPartitions, List<String> instanceNames, Map<String, PartitionAssignment> currentPartitionAssignment,
      List<TableConfig> allTableConfigsInTenant, int numTablesExpected, Map<String, Integer> nPartitionsExpected,
      int nReplicasExpected) {

    UniformStreamPartitionAssignmentStrategy strategy = new UniformStreamPartitionAssignmentStrategy();
    strategy.init(allTableConfigsInTenant, instanceNames, currentPartitionAssignment);
    Map<String, PartitionAssignment> uniformPartitionAssignment =
        strategy.generatePartitionAssignment(tableConfig, nKafkaPartitions);

    Assert.assertEquals(uniformPartitionAssignment.size(), numTablesExpected);

    // validate assignment for number of partitions for each table, number replicas for each table, and correct instances used
    validatePartitionAssignment(uniformPartitionAssignment, nPartitionsExpected, nReplicasExpected, instanceNames);

    // uniform assignment will simply spray partition replicas to list of servers uniformly.
    // check that servers got expected number of assignments
    for (Map.Entry<String, PartitionAssignment> entry : uniformPartitionAssignment.entrySet()) {
      Map<String, Integer> instanceToPartitionCount = new HashMap<>(instanceNames.size());
      for (String instance : instanceNames) {
        instanceToPartitionCount.put(instance, 0);
      }
      PartitionAssignment partitionAssignment = entry.getValue();
      for (Map.Entry<String, List<String>> partitionToInstances : partitionAssignment.getPartitionToInstances()
          .entrySet()) {
        for (String instance : partitionToInstances.getValue()) {
          int partitionCount = instanceToPartitionCount.get(instance);
          instanceToPartitionCount.put(instance, partitionCount + 1);
        }
      }
      String tableName = partitionAssignment.getTableName();
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
    Map<String, PartitionAssignment> currentAssignment = new HashMap<>(1);
    Map<String, Boolean> assignmentChanged = new HashMap<>(1);

    // single new table
    TableConfig tableConfig1 = makeTableConfig(table1, nReplicasExpected, DEFAULT_SERVER_TENANT,
        StreamPartitionAssignmentStrategyEnum.BalancedStreamPartitionAssignment);
    int nKafkaPartitions1 = 8;
    allTableConfigsInSameTenant.add(tableConfig1);
    nPartitionsExpected.put(table1, nKafkaPartitions1);
    instanceNames = getInstanceList(4);
    assignmentChanged.put(table1, true);
    currentAssignment = autoRebalanceStrategyTest(tableConfig1, nKafkaPartitions1, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 1, nPartitionsExpected, nReplicasExpected);

    // no change
    assignmentChanged.put(table1, false);
    currentAssignment = autoRebalanceStrategyTest(tableConfig1, nKafkaPartitions1, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 1, nPartitionsExpected, nReplicasExpected);

    // instances change
    instanceNames = getInstanceList(10);
    assignmentChanged.put(table1, true);
    currentAssignment = autoRebalanceStrategyTest(tableConfig1, nKafkaPartitions1, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 1, nPartitionsExpected, nReplicasExpected);

    // partitions change
    nKafkaPartitions1 = 10;
    nPartitionsExpected.put(table1, nKafkaPartitions1);
    currentAssignment = autoRebalanceStrategyTest(tableConfig1, nKafkaPartitions1, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 1, nPartitionsExpected, nReplicasExpected);

    // multiple tables
    TableConfig tableConfig2 = makeTableConfig(table2, nReplicasExpected, DEFAULT_SERVER_TENANT,
        StreamPartitionAssignmentStrategyEnum.BalancedStreamPartitionAssignment);
    int nKafkaPartitions2 = 8;
    allTableConfigsInSameTenant.add(tableConfig2);
    assignmentChanged.put(table1, false);
    assignmentChanged.put(table2, true);
    nPartitionsExpected.put(table2, nKafkaPartitions2);
    currentAssignment = autoRebalanceStrategyTest(tableConfig2, nKafkaPartitions2, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 2, nPartitionsExpected, nReplicasExpected);

    // no change
    assignmentChanged.put(table2, false);
    currentAssignment = autoRebalanceStrategyTest(tableConfig2, nKafkaPartitions2, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 2, nPartitionsExpected, nReplicasExpected);

    // change in 2 - partitions
    nKafkaPartitions2 = 12;
    nPartitionsExpected.put(table2, nKafkaPartitions2);
    assignmentChanged.put(table2, true);
    currentAssignment = autoRebalanceStrategyTest(tableConfig2, nKafkaPartitions2, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 2, nPartitionsExpected, nReplicasExpected);

    // change in instances
    instanceNames = getInstanceList(8);
    assignmentChanged.put(table1, true);
    assignmentChanged.put(table2, true);
    currentAssignment = autoRebalanceStrategyTest(tableConfig2, nKafkaPartitions2, instanceNames, currentAssignment,
        allTableConfigsInSameTenant, assignmentChanged, 2, nPartitionsExpected, nReplicasExpected);
  }

  private Map<String, PartitionAssignment> autoRebalanceStrategyTest(TableConfig tableConfig, int nKafkaPartitions,
      List<String> instanceNames, Map<String, PartitionAssignment> currentPartitionAssignment,
      List<TableConfig> allTableConfigsInTenant, Map<String, Boolean> assignmentChanged, int numTablesExpected,
      Map<String, Integer> nPartitionsExpected, int nReplicasExpected) {

    BalancedStreamPartitionAssignmentStrategy strategy = new BalancedStreamPartitionAssignmentStrategy();
    strategy.init(allTableConfigsInTenant, instanceNames, currentPartitionAssignment);

    Map<String, PartitionAssignment> autoRebalancePartitionAssignment =
        strategy.generatePartitionAssignment(tableConfig, nKafkaPartitions);

    // check that we got a new assignment for each table in our list
    Assert.assertEquals(autoRebalancePartitionAssignment.size(), numTablesExpected);

    // validate assignment for number of partitions for each table, number replicas for each table, and correct instances used
    validatePartitionAssignment(autoRebalancePartitionAssignment, nPartitionsExpected, nReplicasExpected,
        instanceNames);

    // validate if assignment changed when it should have
    for (Map.Entry<String, PartitionAssignment> entry : autoRebalancePartitionAssignment.entrySet()) {
      String tableName = entry.getKey();
      PartitionAssignment newAssignment = entry.getValue();
      PartitionAssignment currentAssignment = currentPartitionAssignment.get(tableName);
      if (assignmentChanged.get(tableName)) {
        Assert.assertFalse(newAssignment.equals(currentAssignment));
      } else {
        for (int i = 0; i < newAssignment.getNumPartitions(); i++) {
          Assert.assertEquals(newAssignment.getInstancesListForPartition(String.valueOf(i)),
              currentAssignment.getInstancesListForPartition(String.valueOf(i)));
        }
      }
    }
    return autoRebalancePartitionAssignment;
  }

  /**
   * Tests generatePartitionAssignment, as initiated by different tables, in scenarios:
   * 1. new table with BalancedAssignmentStrategy
   * 2. change in instances/partitions/replicas
   * 3. another table with BalancedAssignmentStrategy in the same tenant
   * 4. another table with BalancedAssignmentStrategy in new tenant
   * 5. table with UniformAssignmentStrategy in same tenant
   * 6. table with UniformAssignmentStrategy in new tenant
   *
   * For each of the scenarios, verifies that only the required tables undergo a partition assignment
   */
  @Test
  public void testGeneratePartitionAssignment() {
    List<String> instances = getInstanceList(4);
    String table1 = "rtTable1_REALTIME";
    String table2 = "rtTable2_REALTIME";
    String uniformStrategyTable1 = "rtUniformAssignmentTable1_REALTIME";
    String table4 = "rtTable4_REALTIME";
    String uniformStrategyTable2 = "rtUniformStrategyTable2_REALTIME";
    String uniformStrategyTable3 = "rtUniformStrategyTable3_REALTIME";
    String aTenant = "aTenant";
    String anotherTenant = "anotherTenant";
    String yetAnotherTenant = "yetAnotherTenant";
    int nReplicas = 2;
    Map<String, Integer> kafkaPartitionsMap = new HashMap<>(1);
    List<String> allTables = new ArrayList<>(1);

    MockStreamPartitionAssignmentGenerator streamPartitionAssignmentGenerator =
        new MockStreamPartitionAssignmentGenerator(null);

    // new table: 1st table in server tenant, with BalancedStreamPartitionAssignment
    int nKafkaPartitions1 = 8;

    TableConfig tableConfig1 = makeTableConfig(table1, nReplicas, aTenant,
        StreamPartitionAssignmentStrategyEnum.BalancedStreamPartitionAssignment);
    streamPartitionAssignmentGenerator.addToTableConfigsStore(table1, tableConfig1);
    kafkaPartitionsMap.put(table1, nKafkaPartitions1);
    allTables.add(table1);

    Map<String, PartitionAssignment> newPartitionAssignment =
        streamPartitionAssignmentGenerator.generatePartitionAssignment(tableConfig1, nKafkaPartitions1, instances,
            allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    // verify that only the required tables got a new partition assignment
    Assert.assertEquals(newPartitionAssignment.size(), 1);
    Assert.assertTrue(newPartitionAssignment.containsKey(table1));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);

    // change partitions
    nKafkaPartitions1 = 10;
    kafkaPartitionsMap.put(table1, nKafkaPartitions1);
    newPartitionAssignment =
        streamPartitionAssignmentGenerator.generatePartitionAssignment(tableConfig1, nKafkaPartitions1, instances,
            allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    Assert.assertEquals(newPartitionAssignment.size(), 1);
    Assert.assertTrue(newPartitionAssignment.containsKey(table1));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);

    // change instances
    instances = getInstanceList(6);
    newPartitionAssignment =
        streamPartitionAssignmentGenerator.generatePartitionAssignment(tableConfig1, nKafkaPartitions1, instances,
            allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    Assert.assertEquals(newPartitionAssignment.size(), 1);
    Assert.assertTrue(newPartitionAssignment.containsKey(table1));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);

    // new table: 2nd table in same tenant with BalancedStreamPartitionAssignment
    int nKafkaPartitions2 = 6;
    TableConfig tableConfig2 = makeTableConfig(table2, nReplicas, aTenant,
        StreamPartitionAssignmentStrategyEnum.BalancedStreamPartitionAssignment);
    streamPartitionAssignmentGenerator.addToTableConfigsStore(table2, tableConfig2);
    allTables.add(table2);
    kafkaPartitionsMap.put(table2, nKafkaPartitions2);
    newPartitionAssignment =
        streamPartitionAssignmentGenerator.generatePartitionAssignment(tableConfig2, nKafkaPartitions2, instances,
            allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    Assert.assertEquals(newPartitionAssignment.size(), 2);
    Assert.assertTrue(newPartitionAssignment.containsKey(table1));
    Assert.assertTrue(newPartitionAssignment.containsKey(table2));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);

    // change partitions
    nKafkaPartitions2 = 12;
    kafkaPartitionsMap.put(table2, nKafkaPartitions2);
    newPartitionAssignment =
        streamPartitionAssignmentGenerator.generatePartitionAssignment(tableConfig2, nKafkaPartitions2, instances,
            allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    Assert.assertEquals(newPartitionAssignment.size(), 2);
    Assert.assertTrue(newPartitionAssignment.containsKey(table1));
    Assert.assertTrue(newPartitionAssignment.containsKey(table2));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);

    // change instances
    instances = getInstanceList(5);
    newPartitionAssignment =
        streamPartitionAssignmentGenerator.generatePartitionAssignment(tableConfig2, nKafkaPartitions2, instances,
            allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    Assert.assertEquals(newPartitionAssignment.size(), 2);
    Assert.assertTrue(newPartitionAssignment.containsKey(table1));
    Assert.assertTrue(newPartitionAssignment.containsKey(table2));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);

    // new table: 3rd table in same tenant, but with UniformStreamPartitionAssignment strategy
    int uniformStrategyNKafkaPartitions1 = 4;
    TableConfig uniformStartegyTableConfig1 = makeTableConfig(uniformStrategyTable1, nReplicas, aTenant,
        StreamPartitionAssignmentStrategyEnum.UniformStreamPartitionAssignment);
    streamPartitionAssignmentGenerator.addToTableConfigsStore(uniformStrategyTable1, uniformStartegyTableConfig1);
    allTables.add(uniformStrategyTable1);
    kafkaPartitionsMap.put(uniformStrategyTable1, uniformStrategyNKafkaPartitions1);
    newPartitionAssignment = streamPartitionAssignmentGenerator.generatePartitionAssignment(uniformStartegyTableConfig1,
        uniformStrategyNKafkaPartitions1, instances, allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    Assert.assertEquals(newPartitionAssignment.size(), 1);
    Assert.assertTrue(newPartitionAssignment.containsKey(uniformStrategyTable1));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);

    // change partitions of uniform table 1
    uniformStrategyNKafkaPartitions1 = 6;
    kafkaPartitionsMap.put(uniformStrategyTable1, uniformStrategyNKafkaPartitions1);
    newPartitionAssignment = streamPartitionAssignmentGenerator.generatePartitionAssignment(uniformStartegyTableConfig1,
        uniformStrategyNKafkaPartitions1, instances, allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    Assert.assertEquals(newPartitionAssignment.size(), 1);
    Assert.assertTrue(newPartitionAssignment.containsKey(uniformStrategyTable1));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);

    // change partitions of table2
    nKafkaPartitions2 = 14;
    kafkaPartitionsMap.put(table2, nKafkaPartitions2);
    newPartitionAssignment =
        streamPartitionAssignmentGenerator.generatePartitionAssignment(tableConfig2, nKafkaPartitions2, instances,
            allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    Assert.assertEquals(newPartitionAssignment.size(), 2);
    Assert.assertTrue(newPartitionAssignment.containsKey(table1));
    Assert.assertTrue(newPartitionAssignment.containsKey(table2));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);

    // change instances, detected by uniform table 1
    instances = getInstanceList(8);
    newPartitionAssignment = streamPartitionAssignmentGenerator.generatePartitionAssignment(uniformStartegyTableConfig1,
        uniformStrategyNKafkaPartitions1, instances, allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    Assert.assertEquals(newPartitionAssignment.size(), 1);
    Assert.assertTrue(newPartitionAssignment.containsKey(uniformStrategyTable1));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);

    // new table in another tenant with BalancedStreamPartitionAssignment
    int nKafkaPartitions4 = 8;
    TableConfig tableConfig4 = makeTableConfig(table4, nReplicas, anotherTenant,
        StreamPartitionAssignmentStrategyEnum.BalancedStreamPartitionAssignment);
    streamPartitionAssignmentGenerator.addToTableConfigsStore(table4, tableConfig4);
    allTables = new ArrayList<>(1);
    allTables.add(table4);
    kafkaPartitionsMap.put(table4, nKafkaPartitions4);
    newPartitionAssignment =
        streamPartitionAssignmentGenerator.generatePartitionAssignment(tableConfig4, nKafkaPartitions4, instances,
            allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    Assert.assertEquals(newPartitionAssignment.size(), 1);
    Assert.assertTrue(newPartitionAssignment.containsKey(table4));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);

    // new table with uniform strategy in yetAnotherTenant
    int uniformStrategyNKafkaPartitions2 = 8;
    TableConfig uniformStrategyTableConfig2 = makeTableConfig(uniformStrategyTable2, nReplicas, yetAnotherTenant,
        StreamPartitionAssignmentStrategyEnum.UniformStreamPartitionAssignment);
    streamPartitionAssignmentGenerator.addToTableConfigsStore(uniformStrategyTable2, uniformStrategyTableConfig2);
    allTables = new ArrayList<>(1);
    allTables.add(uniformStrategyTable2);
    kafkaPartitionsMap.put(uniformStrategyTable2, uniformStrategyNKafkaPartitions2);
    newPartitionAssignment = streamPartitionAssignmentGenerator.generatePartitionAssignment(uniformStrategyTableConfig2,
        uniformStrategyNKafkaPartitions2, instances, allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    Assert.assertEquals(newPartitionAssignment.size(), 1);
    Assert.assertTrue(newPartitionAssignment.containsKey(uniformStrategyTable2));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);

    // new table with uniform strategy in yetAnotherTenant
    int uniformStartegyNKafkaPartitions3 = 8;
    TableConfig uniformStrategyTableConfig3 = makeTableConfig(uniformStrategyTable3, nReplicas, yetAnotherTenant,
        StreamPartitionAssignmentStrategyEnum.UniformStreamPartitionAssignment);
    streamPartitionAssignmentGenerator.addToTableConfigsStore(uniformStrategyTable3, uniformStrategyTableConfig3);
    allTables.add(uniformStrategyTable3);
    kafkaPartitionsMap.put(uniformStrategyTable3, uniformStartegyNKafkaPartitions3);
    newPartitionAssignment = streamPartitionAssignmentGenerator.generatePartitionAssignment(uniformStrategyTableConfig3,
        uniformStartegyNKafkaPartitions3, instances, allTables);
    streamPartitionAssignmentGenerator.addToPartitionsListStore(newPartitionAssignment);
    Assert.assertEquals(newPartitionAssignment.size(), 1);
    Assert.assertTrue(newPartitionAssignment.containsKey(uniformStrategyTable3));
    validatePartitionAssignment(newPartitionAssignment, kafkaPartitionsMap, nReplicas, instances);
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

  private void validatePartitionAssignment(Map<String, PartitionAssignment> tableNameToPartitions,
      Map<String, Integer> nKafkaPartitions, int nReplicas, List<String> instances) {
    for (Map.Entry<String, PartitionAssignment> entry : tableNameToPartitions.entrySet()) {
      String tableName = entry.getKey();
      PartitionAssignment partitionsAssignment = entry.getValue();
      Map<String, List<String>> partitionToInstances = partitionsAssignment.getPartitionToInstances();
      Assert.assertEquals(partitionsAssignment.getNumPartitions(), nKafkaPartitions.get(tableName).intValue());
      for (Map.Entry<String, List<String>> partition : partitionToInstances.entrySet()) {
        Assert.assertEquals(partition.getValue().size(), nReplicas);
        Assert.assertTrue(instances.containsAll(partition.getValue()));
      }
    }
  }



  static class MockStreamPartitionAssignmentGenerator extends StreamPartitionAssignmentGenerator {

    private Map<String, TableConfig> _tableConfigsStore;
    private Map<String, PartitionAssignment> _tableNameToPartitionsListMap;

    public MockStreamPartitionAssignmentGenerator(ZkHelixPropertyStore<ZNRecord> propertyStore) {
      super(propertyStore);
      _tableConfigsStore = new HashMap<>(1);
      _tableNameToPartitionsListMap = new HashMap<>(1);
    }

    public void addToTableConfigsStore(String tableNameWithType, TableConfig tableConfig) {
      _tableConfigsStore.put(tableNameWithType, tableConfig);
    }

    public void addToPartitionsListStore(Map<String, PartitionAssignment> tableNameToPartitionAssignment) {
      for (Map.Entry<String, PartitionAssignment> entry : tableNameToPartitionAssignment.entrySet()) {
        _tableNameToPartitionsListMap.put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    protected TableConfig getRealtimeTableConfig(String tableNameWithType) {
      return _tableConfigsStore.get(tableNameWithType);
    }

    @Override
    protected Map<String, List<String>> getPartitionsToInstances(String realtimeTableName) {
      Map<String, List<String>> partitionToInstances = null;
      PartitionAssignment partitionAssignment = _tableNameToPartitionsListMap.get(realtimeTableName);
      if (partitionAssignment != null) {
        partitionToInstances = partitionAssignment.getPartitionToInstances();
      }
      return  partitionToInstances;
    }
  }
}

