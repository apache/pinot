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
package org.apache.pinot.controller.helix.core.assignment.segment.strategy;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;


/**
 * Segment assignment strategy class where segments are assigned in a round-robin fashion across available instances.
 * It can be used in the same scenarios as {@link ReplicaGroupSegmentAssignmentStrategy}, except the new segment will
 * not be assigned to the instance with the least number of segments, but instead assigned to the next instance in a
 * round-robin fashion within each replica-group. Rebalance with this strategy is identical to
 * {@link ReplicaGroupSegmentAssignmentStrategy}.
 *
 * <p>When the number of instances in each replica group increases, this is useful when we don't care about the
 * existing segments as they might be removed later by retention, or there are significantly unbalanced demands on
 * newer segments. This strategy works better than {@link ReplicaGroupSegmentAssignmentStrategy} since that strategy
 * may require a bootstrap rebalance in this case.
 *
 * <p>The round-robin counter is maintained per table and partition in memory and does not sync across controller
 * instances, which means segment assignment may not be strictly round-robin after controller restarts or when
 * multiple controllers assign segments concurrently.
 *
 */
public class RoundRobinReplicaGroupSegmentAssignmentStrategy extends ReplicaGroupSegmentAssignmentStrategy {
  private static final Map<Pair<String, Integer>, Integer> TABLE_ROUND_ROBIN_COUNTER = new ConcurrentHashMap<>();
  private static final Random RANDOM = new Random();

  /**
   * Assign the segment to the replica groups of its partition. The instance in each replica group is picked in a
   * round-robin manner, and the counter increases regardless whether the segment is actually added to the ideal
   * state or not.
   */
  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions) {
    checkReplication(instancePartitions, _replication, _tableName);
    int numPartitions = instancePartitions.getNumPartitions();
    int partitionId;
    if (numPartitions == 1) {
      partitionId = 0;
    } else {
      partitionId =
          SegmentUtils.getSegmentPartitionIdOrDefault(segmentName, _tableName, _helixManager, _partitionColumn)
              % numPartitions;
    }
    int numInstancesPerReplicaGroup = instancePartitions.getInstances(partitionId, 0).size();

    Pair<String, Integer> counterKey = Pair.of(_tableName, partitionId);
    int instanceId = TABLE_ROUND_ROBIN_COUNTER.compute(counterKey,
        (key, value) -> value == null ? RANDOM.nextInt(numInstancesPerReplicaGroup)
            : (value + 1) % numInstancesPerReplicaGroup);
    return SegmentAssignmentUtils.getOneInstanceFromEachReplicaGroup(instancePartitions, partitionId, instanceId);
  }
}
