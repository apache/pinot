/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.partition;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TenantConfig;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class StreamPartitionAssignmentGeneratorTest {

  private String aServerTenant = "aTenant";
  private HelixManager _mockHelixManager;
  private String[] consumingServerNames;

  private List<String> getConsumingInstanceList(final int nServers) {
    Assert.assertTrue(nServers <= consumingServerNames.length);
    String[] instanceArray = Arrays.copyOf(consumingServerNames, nServers);
    return Lists.newArrayList(instanceArray);
  }

  @BeforeMethod
  public void setUp() throws Exception {
    _mockHelixManager = mock(HelixManager.class);

    final int maxInstances = 20;
    consumingServerNames = new String[maxInstances];
    for (int i = 0; i < maxInstances; i++) {
      consumingServerNames[i] = "ConsumingServer_" + i;
    }
  }

  /**
   * Given an ideal state, constructs the partition assignment for the table
   */
  @Test
  public void testGetPartitionAssignmentFromIdealState() {

    String tableName = "aTableName_REALTIME";
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(tableName);

    IdealStateBuilderUtil idealStateBuilder = new IdealStateBuilderUtil(tableName);
    IdealState idealState;

    int numPartitions = 0;
    int numReplicas = 2;

    // empty ideal state
    idealState = idealStateBuilder.build();
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions);

    // 3 partitions 2 replicas, all on 0th seq number (6 segments)
    numPartitions = 3;
    List<String> instances = getConsumingInstanceList(4);
    idealState = idealStateBuilder.addConsumingSegments(numPartitions, 0, numReplicas, instances).build();
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions);

    // 3 partitions 2 replicas, 2 on 0th seq number 1 on 1st seq number (8 segments)
    instances = idealStateBuilder.getInstances(0, 0);
    idealState =
        idealStateBuilder.setSegmentState(0, 0, "ONLINE").addConsumingSegment(0, 1, numReplicas, instances).build();
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions);

    // 3 partitions 2 replicas, all on 1st seg num (12 segments)
    instances = idealStateBuilder.getInstances(1, 0);
    idealStateBuilder.setSegmentState(1, 0, "ONLINE").addConsumingSegment(1, 1, numReplicas, instances);
    instances = idealStateBuilder.getInstances(2, 0);
    idealState =
        idealStateBuilder.setSegmentState(2, 0, "ONLINE").addConsumingSegment(2, 1, numReplicas, instances).build();
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions);

    // 3 partitions 2 replicas, seq 0 has moved to COMPLETED servers, seq 1 on CONSUMING instances
    instances = Lists.newArrayList("s1_completed", "s2_completed");
    idealState = idealStateBuilder.moveToServers(0, 0, instances).build();
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions);

    // status of latest segments OFFLINE/ERROR
    idealState = idealStateBuilder.setSegmentState(1, 1, "OFFLINE").setSegmentState(2, 1, "OFFLINE").build();
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions);

    // all non llc segments
    String aNonLLCSegment = "randomName";
    Map<String, String> instanceStateMap = new HashMap<>(1);
    instanceStateMap.put("s1", "CONSUMING");
    instanceStateMap.put("s2", "CONSUMING");
    idealState = idealStateBuilder.clear().addSegment(aNonLLCSegment, instanceStateMap).build();
    numPartitions = 0;
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions);

    // some non llc segments, some llc segments
    numPartitions = 1;
    instances = getConsumingInstanceList(6);
    idealState = idealStateBuilder.addConsumingSegments(numPartitions, 0, numReplicas, instances).build();
    verifyPartitionAssignmentFromIdealState(tableConfig, idealState, numPartitions);
  }

  private void verifyPartitionAssignmentFromIdealState(TableConfig tableConfig, IdealState idealState,
      int numPartitions) {
    TestStreamPartitionAssignmentGenerator partitionAssignmentGenerator =
        new TestStreamPartitionAssignmentGenerator(_mockHelixManager);
    PartitionAssignment partitionAssignmentFromIdealState =
        partitionAssignmentGenerator.getStreamPartitionAssignmentFromIdealState(tableConfig, idealState);
    Assert.assertEquals(tableConfig.getTableName(), partitionAssignmentFromIdealState.getTableName());
    Assert.assertEquals(partitionAssignmentFromIdealState.getNumPartitions(), numPartitions);
    // check that latest segments are honoring partition assignment
    Map<String, LLCSegmentName> partitionIdToLatestLLCSegment =
        partitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
    for (Map.Entry<String, LLCSegmentName> entry : partitionIdToLatestLLCSegment.entrySet()) {
      Set<String> idealStateInstances = idealState.getInstanceStateMap(entry.getValue().getSegmentName()).keySet();
      List<String> partitionAssignmentInstances =
          partitionAssignmentFromIdealState.getInstancesListForPartition(entry.getKey());
      Assert.assertEquals(idealStateInstances.size(), partitionAssignmentInstances.size());
      Assert.assertTrue(idealStateInstances.containsAll(partitionAssignmentInstances));
    }
  }

  @Test
  public void testGeneratePartitionAssignment() {
    RandomStringUtils.randomAlphabetic(10);
    for (int i = 0; i < 20; i++) {
      String tableName = RandomStringUtils.randomAlphabetic(10) + "_REALTIME";
      testGeneratePartitionAssignmentForTable(tableName);
    }
  }

  private void testGeneratePartitionAssignmentForTable(String tableName) {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(tableName);
    TenantConfig mockTenantConfig = mock((TenantConfig.class));
    when(mockTenantConfig.getServer()).thenReturn(aServerTenant);
    when(tableConfig.getTenantConfig()).thenReturn(mockTenantConfig);
    SegmentsValidationAndRetentionConfig mockValidationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(mockValidationConfig.getReplicasPerPartitionNumber()).thenReturn(2);
    when(tableConfig.getValidationConfig()).thenReturn(mockValidationConfig);
    int numPartitions = 0;
    List<String> consumingInstanceList = getConsumingInstanceList(0);
    PartitionAssignment previousPartitionAssignment = new PartitionAssignment(tableName);
    boolean exceptionExpected;
    boolean unchanged = false;

    // 0 consuming instances - error not enough instances
    exceptionExpected = true;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exceptionExpected, unchanged);

    // 1 consuming instance - error not enough instances
    consumingInstanceList = getConsumingInstanceList(1);
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exceptionExpected, unchanged);

    // 0 partitions - 3 consuming instances - empty partition assignment
    consumingInstanceList = getConsumingInstanceList(3);
    exceptionExpected = false;
    unchanged = true;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exceptionExpected, unchanged);

    // 3 partitions - 3 consuming instances
    numPartitions = 3;
    unchanged = false;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exceptionExpected, unchanged);

    // same - shouldn't change
    unchanged = true;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exceptionExpected, unchanged);

    // 3 partitions - 12 consuming instances
    consumingInstanceList = getConsumingInstanceList(12);
    unchanged = false;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exceptionExpected, unchanged);

    // same - shouldn't change
    unchanged = true;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exceptionExpected, unchanged);

    // 3 partitions - 6 consuming instances
    consumingInstanceList = getConsumingInstanceList(6);
    unchanged = false;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exceptionExpected, unchanged);

    // same - shouldn't change
    unchanged = true;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exceptionExpected, unchanged);

    String server0 = consumingInstanceList.get(0);
    consumingInstanceList.set(0, server0 + "_replaced");
    unchanged = false;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exceptionExpected, unchanged);

    // increase in partitions - 4
    numPartitions = 4;
    unchanged = false;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exceptionExpected, unchanged);

    // same - shouldn't change
    unchanged = true;
    previousPartitionAssignment = verifyGeneratePartitionAssignment(tableConfig, numPartitions, consumingInstanceList,
        previousPartitionAssignment, exceptionExpected, unchanged);
  }

  private PartitionAssignment verifyGeneratePartitionAssignment(TableConfig tableConfig, int numPartitions,
      List<String> consumingInstanceList, PartitionAssignment previousPartitionAssignment, boolean exceptionExpected,
      boolean unchanged) {
    String tableName = tableConfig.getTableName();
    TestStreamPartitionAssignmentGenerator partitionAssignmentGenerator =
        new TestStreamPartitionAssignmentGenerator(_mockHelixManager);
    partitionAssignmentGenerator.setConsumingInstances(consumingInstanceList);
    PartitionAssignment partitionAssignment;
    try {
      partitionAssignment = partitionAssignmentGenerator.generateStreamPartitionAssignment(tableConfig, numPartitions);
      Assert.assertFalse(exceptionExpected, "Unexpected exception for table " + tableName);
      verify(tableName, partitionAssignment, numPartitions, consumingInstanceList, unchanged,
          previousPartitionAssignment);
    } catch (Exception e) {
      Assert.assertTrue(exceptionExpected, "Expected exception for table " + tableName);
      partitionAssignment = previousPartitionAssignment;
    }

    return partitionAssignment;
  }

  private void verify(String tableName, PartitionAssignment partitionAssignment, int numPartitions,
      List<String> consumingInstanceList, boolean unchanged, PartitionAssignment previousPartitionAssignment) {
    Assert.assertEquals(partitionAssignment.getTableName(), tableName);

    // check num partitions equal
    Assert.assertEquals(partitionAssignment.getNumPartitions(), numPartitions,
        "NumPartitions do not match for table " + tableName);

    List<String> instancesUsed = new ArrayList<>();
    for (int p = 0; p < partitionAssignment.getNumPartitions(); p++) {
      for (String instance : partitionAssignment.getInstancesListForPartition(String.valueOf(p))) {
        if (!instancesUsed.contains(instance)) {
          instancesUsed.add(instance);
        }
      }
    }

    // check all instances belong to the super set
    Assert.assertTrue(consumingInstanceList.containsAll(instancesUsed), "Instances test failed for table " + tableName);

    // verify strategy is uniform
    int serverId = 0;
    for (int p = 0; p < partitionAssignment.getNumPartitions(); p++) {
      for (String instance : partitionAssignment.getInstancesListForPartition(String.valueOf(p))) {
        Assert.assertTrue(instance.equals(instancesUsed.get(serverId++)),
            "Uniform strategy test failed for table " + tableName);
        if (serverId == instancesUsed.size()) {
          serverId = 0;
        }
      }
    }

    // if nothing changed, should be same as before
    if (unchanged) {
      Assert.assertEquals(partitionAssignment, previousPartitionAssignment,
          "Partition assignment should have been unchanged for table " + tableName);
    }
  }

  private class TestStreamPartitionAssignmentGenerator extends StreamPartitionAssignmentGenerator {

    private List<String> _consumingInstances;

    public TestStreamPartitionAssignmentGenerator(HelixManager helixManager) {
      super(helixManager);
      _consumingInstances = new ArrayList<>();
    }

    @Override
    protected List<String> getConsumingTaggedInstances(TableConfig tableConfig) {
      return _consumingInstances;
    }

    void setConsumingInstances(List<String> consumingInstances) {
      _consumingInstances = consumingInstances;
    }
  }
}