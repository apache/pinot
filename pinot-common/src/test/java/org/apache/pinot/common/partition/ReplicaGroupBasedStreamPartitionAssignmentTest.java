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
package org.apache.pinot.common.partition;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link ReplicaGroupBasedStreamPartitionAssignmentStrategy}
 */
public class ReplicaGroupBasedStreamPartitionAssignmentTest {

  @Test
  public void testReplicaGroupBasedStreamPartitionAssignmentStrategy() throws InvalidConfigException {
    MockReplicaGroupBasedStreamPartitionAssignmentStrategy mockStreamPartitionAssignmentStrategy =
        new MockReplicaGroupBasedStreamPartitionAssignmentStrategy();

    // Realtime table with 2 replicas, 6 partitions, 4 numInstancesPerReplicaGroup.
    // 8 servers distributed across 2 replica groups into 2 sets of 4.
    String tableNameWithType = "tableName_REALTIME";
    List<String> partitions = Lists.newArrayList("0", "1", "2", "3", "4", "5");
    int numReplicas = 2;
    List<String> allTaggedInstances =
        Lists.newArrayList("server_1", "server_2", "server_3", "server_4", "server_5", "server_6", "server_7",
            "server_8");

    ReplicaGroupPartitionAssignment replicaGroupPartitionAssignment =
        new ReplicaGroupPartitionAssignment(tableNameWithType);
    replicaGroupPartitionAssignment.setInstancesToReplicaGroup(0, 0,
        Lists.newArrayList("server_1", "server_2", "server_3", "server_4"));
    replicaGroupPartitionAssignment.setInstancesToReplicaGroup(0, 1,
        Lists.newArrayList("server_5", "server_6", "server_7", "server_8"));

    // null replica group partition assignment
    mockStreamPartitionAssignmentStrategy.setReplicaGroupPartitionAssignment(null);
    boolean exception = false;
    try {
      mockStreamPartitionAssignmentStrategy.getStreamPartitionAssignment(null, tableNameWithType, partitions,
          numReplicas, allTaggedInstances);
    } catch (InvalidConfigException e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    // mismatch between numReplicas and numReplicaGroups - follow the replica group assignment
    mockStreamPartitionAssignmentStrategy.setReplicaGroupPartitionAssignment(replicaGroupPartitionAssignment);
    PartitionAssignment streamPartitionAssignment =
        mockStreamPartitionAssignmentStrategy.getStreamPartitionAssignment(null, tableNameWithType, partitions, 5,
            allTaggedInstances);
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("0"),
        Lists.newArrayList("server_1", "server_5"));
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("1"),
        Lists.newArrayList("server_2", "server_6"));
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("2"),
        Lists.newArrayList("server_3", "server_7"));
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("3"),
        Lists.newArrayList("server_4", "server_8"));
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("4"),
        Lists.newArrayList("server_1", "server_5"));
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("5"),
        Lists.newArrayList("server_2", "server_6"));

    // happy path - correctly generated partition assignment
    mockStreamPartitionAssignmentStrategy.setReplicaGroupPartitionAssignment(replicaGroupPartitionAssignment);
    streamPartitionAssignment =
        mockStreamPartitionAssignmentStrategy.getStreamPartitionAssignment(null, tableNameWithType, partitions,
            numReplicas, allTaggedInstances);
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("0"),
        Lists.newArrayList("server_1", "server_5"));
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("1"),
        Lists.newArrayList("server_2", "server_6"));
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("2"),
        Lists.newArrayList("server_3", "server_7"));
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("3"),
        Lists.newArrayList("server_4", "server_8"));
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("4"),
        Lists.newArrayList("server_1", "server_5"));
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("5"),
        Lists.newArrayList("server_2", "server_6"));

    // 0 partitions
    streamPartitionAssignment =
        mockStreamPartitionAssignmentStrategy.getStreamPartitionAssignment(null, tableNameWithType,
            Collections.emptyList(), numReplicas, allTaggedInstances);
    Assert.assertEquals(streamPartitionAssignment.getNumPartitions(), 0);

    // only 1 instance per replica group
    replicaGroupPartitionAssignment.setInstancesToReplicaGroup(0, 0, Lists.newArrayList("server_1"));
    replicaGroupPartitionAssignment.setInstancesToReplicaGroup(0, 1, Lists.newArrayList("server_2"));
    mockStreamPartitionAssignmentStrategy.setReplicaGroupPartitionAssignment(replicaGroupPartitionAssignment);
    streamPartitionAssignment =
        mockStreamPartitionAssignmentStrategy.getStreamPartitionAssignment(null, tableNameWithType, partitions,
            numReplicas, allTaggedInstances);
    ArrayList<String> verticalSlice = Lists.newArrayList("server_1", "server_2");
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("0"), verticalSlice);
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("1"), verticalSlice);
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("2"), verticalSlice);
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("3"), verticalSlice);
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("4"), verticalSlice);
    Assert.assertEquals(streamPartitionAssignment.getInstancesListForPartition("5"), verticalSlice);
  }

  private class MockReplicaGroupBasedStreamPartitionAssignmentStrategy
      extends ReplicaGroupBasedStreamPartitionAssignmentStrategy {
    private ReplicaGroupPartitionAssignment _replicaGroupPartitionAssignment;

    @Override
    protected ReplicaGroupPartitionAssignment getReplicaGroupPartitionAssignment(HelixManager helixManager,
        String tableNameWithType) {
      return _replicaGroupPartitionAssignment;
    }

    void setReplicaGroupPartitionAssignment(ReplicaGroupPartitionAssignment replicaGroupPartitionAssignment) {
      _replicaGroupPartitionAssignment = replicaGroupPartitionAssignment;
    }
  }
}
