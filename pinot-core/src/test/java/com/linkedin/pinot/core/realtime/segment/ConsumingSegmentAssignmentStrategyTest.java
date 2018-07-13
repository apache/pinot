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
package com.linkedin.pinot.core.realtime.segment;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.partition.IdealStateBuilderUtil;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.partition.StreamPartitionAssignmentGenerator;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Test for verification of correct assignment of segments given a partition assignment
 */
public class ConsumingSegmentAssignmentStrategyTest {

  private String[] consumingServerNames;
  private static final int MAX_CONSUMING_INSTANCES = 20;

  private List<String> getConsumingInstanceList(final int nServers) {
    Assert.assertTrue(nServers <= consumingServerNames.length);
    String[] instanceArray = Arrays.copyOf(consumingServerNames, nServers);
    return Lists.newArrayList(instanceArray);
  }

  @BeforeMethod
  public void setUp() throws Exception {

    consumingServerNames = new String[MAX_CONSUMING_INSTANCES];
    for (int i = 0; i < MAX_CONSUMING_INSTANCES; i++) {
      consumingServerNames[i] = "ConsumingServer_" + i;
    }
  }

  /**
   * Verifies that segments in segment assignment matches input list
   * Verifies that segment assignment is as expected given the partition assignment
   * @param newSegments
   * @param partitionAssignment
   * @param expectException
   */
  private void verifyAssignment(List<String> newSegments, PartitionAssignment partitionAssignment,
      boolean expectException, List<String> expectedSegmentsInSegmentAssignment) {

    ConsumingSegmentAssignmentStrategy strategy = new ConsumingSegmentAssignmentStrategy();
    try {
      Map<String, List<String>> segmentAssignment = strategy.assign(newSegments, partitionAssignment);
      Assert.assertFalse(expectException);
      Assert.assertEquals(segmentAssignment.keySet().size(), expectedSegmentsInSegmentAssignment.size());
      Assert.assertTrue(segmentAssignment.keySet().containsAll(expectedSegmentsInSegmentAssignment));
      for (String segmentName : expectedSegmentsInSegmentAssignment) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        int partitionId = llcSegmentName.getPartitionId();
        List<String> expectedInstances = partitionAssignment.getInstancesListForPartition(String.valueOf(partitionId));
        List<String> assignedInstances = segmentAssignment.get(segmentName);
        Assert.assertEquals(expectedInstances.size(), assignedInstances.size());
        Assert.assertTrue(expectedInstances.containsAll(assignedInstances));
      }
    } catch (InvalidConfigException e) {
      Assert.assertTrue(expectException);
    }
  }

  /**
   * Tests various scenarios of how segment assignment will be invoked
   */
  @Test
  public void testAssign() {
    String tableName = "aTableToTest_REALTIME";

    List<String> newSegments;
    PartitionAssignment partitionAssignment;

    // empty new segments list
    newSegments = new ArrayList<>();
    partitionAssignment = new PartitionAssignment(tableName);
    verifyAssignment(newSegments, partitionAssignment, false, newSegments);

    // non empty new segments list, empty partition assignment
    LLCSegmentName llcSegmentName0 = new LLCSegmentName(tableName, 0, 0, System.currentTimeMillis());
    LLCSegmentName llcSegmentName1 = new LLCSegmentName(tableName, 1, 0, System.currentTimeMillis());
    LLCSegmentName llcSegmentName2 = new LLCSegmentName(tableName, 2, 0, System.currentTimeMillis());
    newSegments.add(llcSegmentName0.getSegmentName());
    newSegments.add(llcSegmentName1.getSegmentName());
    newSegments.add(llcSegmentName2.getSegmentName());
    verifyAssignment(newSegments, partitionAssignment, true, newSegments);

    // non empty new segments list, non empty partition assignment, partitions match
    partitionAssignment.addPartition("0", Lists.newArrayList("s1", "s2"));
    partitionAssignment.addPartition("1", Lists.newArrayList("s3", "s1"));
    partitionAssignment.addPartition("2", Lists.newArrayList("s2", "s3"));
    verifyAssignment(newSegments, partitionAssignment, false, newSegments);

    // partition for a segment missing in partition assignment
    LLCSegmentName llcSegmentName3 = new LLCSegmentName(tableName, 3, 0, System.currentTimeMillis());
    newSegments.add(llcSegmentName3.getSegmentName());
    verifyAssignment(newSegments, partitionAssignment, true, newSegments);

    // extra partitions in partition assignment than needed
    partitionAssignment.addPartition("3", Lists.newArrayList("s1", "s2"));
    partitionAssignment.addPartition("4", Lists.newArrayList("s3", "s1"));
    verifyAssignment(newSegments, partitionAssignment, false, newSegments);

    // non llc segment name
    List<String> goodSegments = Lists.newArrayList(newSegments);
    newSegments.add("nonLLCSegmentName");
    verifyAssignment(newSegments, partitionAssignment, false, goodSegments);
  }

  /**
   * Tests a segment lifecycle
   * 1) new CONSUMING segments in a newly created table, residing on consuming servers
   * 2) consuming segments become ONLINE on the consuming servers and a new sequence of CONSUMING segments is added
   * 3) ONLINE segments move to completed servers, cCONSUMING segments still on consuming servers
   * 4) latest CONSUMING segments become OFFLINE
   * In all these scenarios, we want to check that the final assignment received from the strategy, adheres strictly to what the latest segments say
   *
   */
  @Test
  public void testSegmentLifecycle() throws Exception {
    String tableName = "tableName_REALTIME";
    int numReplicas = 2;
    List<String> completedInstances = Lists.newArrayList("CompletedServer_0", "CompletedServer_1");
    long seed = new Random().nextLong();
    System.out.println("Random seed " + seed);
    Random rand = new Random(seed);
    for (int i = 0; i < 20; i++) {
      int numPartitions = Math.max(2, rand.nextInt(25)); // use at least 2 partitions
      int numConsumingInstances =
          Math.max(numReplicas, rand.nextInt(MAX_CONSUMING_INSTANCES)); // use at least numReplicas num servers
      List<String> consumingInstances = getConsumingInstanceList(numConsumingInstances);
      testSegmentCompletionScenario(tableName, numPartitions, numReplicas, consumingInstances, completedInstances);
    }
  }

  private void testSegmentCompletionScenario(String tableName, int numPartitions, int numReplicas,
      List<String> consumingInstances, List<String> completedInstances) throws InvalidConfigException {

    IdealState idealState;
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(tableName);
    IdealStateBuilderUtil idealStateBuilder = new IdealStateBuilderUtil(tableName);

    HelixManager _mockHelixManager = mock(HelixManager.class);
    TestStreamPartitionAssignmentGenerator partitionAssignmentGenerator =
        new TestStreamPartitionAssignmentGenerator(_mockHelixManager);
    partitionAssignmentGenerator.setConsumingInstances(consumingInstances);
    PartitionAssignment partitionAssignmentFromIdealState;
    ConsumingSegmentAssignmentStrategy consumingSegmentAssignmentStrategy = new ConsumingSegmentAssignmentStrategy();

    // 1) new table - all partitions have only consuming segments

    // create ideal state, seq 0 in ONLINE on completed servers, seq 1 in CONSUMING on consuming servers
    idealState = idealStateBuilder.addConsumingSegments(numPartitions, 0, numReplicas, consumingInstances).build();

    // getPartitionsAssignmentFrom
    partitionAssignmentFromIdealState =
        partitionAssignmentGenerator.getStreamPartitionAssignmentFromIdealState(tableConfig, idealState);

    // assign
    List<String> segmentNames = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      LLCSegmentName segmentName = new LLCSegmentName(tableName, i, 1, System.currentTimeMillis());
      segmentNames.add(segmentName.getSegmentName());
    }
    Map<String, List<String>> assignment = consumingSegmentAssignmentStrategy.assign(segmentNames, partitionAssignmentFromIdealState);

    // verify
    verifyAssignmentIsFromLatest(partitionAssignmentGenerator, idealState, assignment);

    // 2) consuming segments moved to ONLINE, new set of consuming segments generated

    idealState = idealStateBuilder.setSegmentState(0, 0, "ONLINE")
        .setSegmentState(1, 0, "ONLINE")
        .setSegmentState(2, 0, "ONLINE")
        .addConsumingSegments(numPartitions, 1, numReplicas, consumingInstances)
        .build();

    partitionAssignmentFromIdealState =
        partitionAssignmentGenerator.getStreamPartitionAssignmentFromIdealState(tableConfig, idealState);

    segmentNames = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      LLCSegmentName segmentName = new LLCSegmentName(tableName, i, 2, System.currentTimeMillis());
      segmentNames.add(segmentName.getSegmentName());
    }
    assignment = consumingSegmentAssignmentStrategy.assign(segmentNames, partitionAssignmentFromIdealState);

    // verify
    verifyAssignmentIsFromLatest(partitionAssignmentGenerator, idealState, assignment);

    // 3) ONLINE segments moved to completed servers - latest consuming segments still on consuming

    idealState = idealStateBuilder.moveToServers(0, 0, completedInstances)
        .moveToServers(1, 0, completedInstances)
        .moveToServers(2, 0, completedInstances)
        .build();
    partitionAssignmentFromIdealState =
        partitionAssignmentGenerator.getStreamPartitionAssignmentFromIdealState(tableConfig, idealState);

    assignment = consumingSegmentAssignmentStrategy.assign(segmentNames, partitionAssignmentFromIdealState);

    // verify
    verifyAssignmentIsFromLatest(partitionAssignmentGenerator, idealState, assignment);

    // 4) latest consuming segments became OFFLINE

    idealState = idealStateBuilder.setSegmentState(0, 1, "OFFLINE")
        .setSegmentState(1, 1, "OFFLINE")
        .setSegmentState(2, 1, "OFFLINE")
        .build();
    partitionAssignmentFromIdealState =
        partitionAssignmentGenerator.getStreamPartitionAssignmentFromIdealState(tableConfig, idealState);

    assignment = consumingSegmentAssignmentStrategy.assign(segmentNames, partitionAssignmentFromIdealState);

    // verify
    verifyAssignmentIsFromLatest(partitionAssignmentGenerator, idealState, assignment);
  }

  private void verifyAssignmentIsFromLatest(StreamPartitionAssignmentGenerator streamPartitionAssignmentGenerator,
      IdealState idealState, Map<String, List<String>> assignment) {
    Map<String, LLCSegmentName> partitionToLatestSegments =
        streamPartitionAssignmentGenerator.getPartitionToLatestSegments(idealState);
    for (Map.Entry<String, List<String>> entry : assignment.entrySet()) {
      String segmentName = entry.getKey();
      List<String> assignedInstances = entry.getValue();
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partitionId = llcSegmentName.getPartitionId();
      LLCSegmentName latestSegment = partitionToLatestSegments.get(String.valueOf(partitionId));
      Set<String> instancesInIdealState = idealState.getInstanceStateMap(latestSegment.getSegmentName()).keySet();
      Assert.assertEquals(assignedInstances.size(), instancesInIdealState.size());
      Assert.assertTrue(assignedInstances.containsAll(instancesInIdealState));
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