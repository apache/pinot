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
package org.apache.pinot.controller.helix.core.assignment.segment;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.Pairs;


/**
 * Utility class for segment assignment.
 */
public class SegmentAssignmentUtils {
  private SegmentAssignmentUtils() {
  }

  /**
   * Returns the number of segments assigned to each instance.
   */
  static int[] getNumSegmentsAssignedPerInstance(Map<String, Map<String, String>> segmentAssignment,
      List<String> instances) {
    int[] numSegmentsPerInstance = new int[instances.size()];
    Map<String, Integer> instanceNameToIdMap = getInstanceNameToIdMap(instances);
    for (Map<String, String> instanceStateMep : segmentAssignment.values()) {
      for (String instanceName : instanceStateMep.keySet()) {
        Integer instanceId = instanceNameToIdMap.get(instanceName);
        if (instanceId != null) {
          numSegmentsPerInstance[instanceId]++;
        }
      }
    }
    return numSegmentsPerInstance;
  }

  private static Map<String, Integer> getInstanceNameToIdMap(List<String> instances) {
    int numInstances = instances.size();
    Map<String, Integer> instanceNameToIdMap = new HashMap<>();
    for (int i = 0; i < numInstances; i++) {
      instanceNameToIdMap.put(instances.get(i), i);
    }
    return instanceNameToIdMap;
  }

  /**
   * Returns instances for non-replica-group based assignment.
   */
  static List<String> getInstancesForNonReplicaGroupBasedAssignment(InstancePartitions instancePartitions,
      int replication) {
    Preconditions
        .checkState(instancePartitions.getNumReplicaGroups() == 1 && instancePartitions.getNumPartitions() == 1,
            "Instance partitions: %s should contain 1 replica and 1 partition for non-replica-group based assignment",
            instancePartitions.getInstancePartitionsName());
    List<String> instances = instancePartitions.getInstances(0, 0);
    int numInstances = instances.size();
    Preconditions.checkState(numInstances >= replication,
        "There are less instances: %s in instance partitions: %s than the table replication: %s", numInstances,
        instancePartitions.getInstancePartitionsName(), replication);
    return instances;
  }

  /**
   * Rebalances the table with Helix AutoRebalanceStrategy.
   */
  static Map<String, Map<String, String>> rebalanceTableWithHelixAutoRebalanceStrategy(
      Map<String, Map<String, String>> currentAssignment, List<String> instances, int replication) {
    // Use Helix AutoRebalanceStrategy to rebalance the table
    LinkedHashMap<String, Integer> states = new LinkedHashMap<>();
    states.put(SegmentOnlineOfflineStateModel.ONLINE, replication);
    AutoRebalanceStrategy autoRebalanceStrategy =
        new AutoRebalanceStrategy(null, new ArrayList<>(currentAssignment.keySet()), states);
    // Make a copy of the current assignment because this step might change the passed in assignment
    Map<String, Map<String, String>> currentAssignmentCopy = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      currentAssignmentCopy.put(segmentName, new TreeMap<>(instanceStateMap));
    }
    return autoRebalanceStrategy.computePartitionAssignment(instances, instances, currentAssignmentCopy, null)
        .getMapFields();
  }

  /**
   * Rebalances the table for the replica-group based segment assignment strategy.
   * <p>The number of partitions for the segments can be different from the number of partitions in the instance
   * partitions. Uniformly spray the segment partitions over the instance partitions.
   */
  static Map<String, Map<String, String>> rebalanceReplicaGroupBasedTable(
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions,
      Map<Integer, Set<String>> partitionIdToSegmentsMap) {
    Map<String, Map<String, String>> newAssignment = new TreeMap<>();
    int numPartitions = instancePartitions.getNumPartitions();
    for (Map.Entry<Integer, Set<String>> entry : partitionIdToSegmentsMap.entrySet()) {
      // Uniformly spray the segment partitions over the instance partitions
      int partitionId = entry.getKey() % numPartitions;
      SegmentAssignmentUtils
          .rebalanceReplicaGroupBasedPartition(currentAssignment, instancePartitions, partitionId, entry.getValue(),
              newAssignment);
    }
    return newAssignment;
  }

  /**
   * Rebalances one partition of the table for the replica-group based segment assignment strategy.
   * <ul>
   *   <li>
   *     1. Calculate the target number of segments on each instance
   *   </li>
   *   <li>
   *     2. Loop over all the segments and keep the assignment if target number of segments for the instance has not
   *     been reached and track the not assigned segments
   *   </li>
   *   <li>
   *     3. Assign the left-over segments to the instances with the least segments, or the smallest index if there is a
   *     tie
   *   </li>
   *   <li>
   *     4. Mirror the assignment to other replica-groups
   *   </li>
   * </ul>
   */
  static void rebalanceReplicaGroupBasedPartition(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, int partitionId, Set<String> segments,
      Map<String, Map<String, String>> newAssignment) {
    // Fetch instances in replica-group 0
    List<String> instances = instancePartitions.getInstances(partitionId, 0);
    Map<String, Integer> instanceNameToIdMap = SegmentAssignmentUtils.getInstanceNameToIdMap(instances);

    // Calculate target number of segments per instance
    // NOTE: in order to minimize the segment movements, use the ceiling of the quotient
    int numInstances = instances.size();
    int numSegments = segments.size();
    int targetNumSegmentsPerInstance = (numSegments + numInstances - 1) / numInstances;

    // Do not move segment if target number of segments is not reached, track the segments need to be moved
    int[] numSegmentsAssignedPerInstance = new int[numInstances];
    List<String> segmentsNotAssigned = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      // Skip segments not in the partition
      if (!segments.contains(segmentName)) {
        continue;
      }
      boolean segmentAssigned = false;
      for (String instanceName : entry.getValue().keySet()) {
        Integer instanceId = instanceNameToIdMap.get(instanceName);
        if (instanceId != null && numSegmentsAssignedPerInstance[instanceId] < targetNumSegmentsPerInstance) {
          newAssignment
              .put(segmentName, getReplicaGroupBasedInstanceStateMap(instancePartitions, partitionId, instanceId));
          numSegmentsAssignedPerInstance[instanceId]++;
          segmentAssigned = true;
          break;
        }
      }
      if (!segmentAssigned) {
        segmentsNotAssigned.add(segmentName);
      }
    }

    // Assign each not assigned segment to the instance with the least segments, or the smallest id if there is a tie
    PriorityQueue<Pairs.IntPair> heap = new PriorityQueue<>(numInstances, Pairs.intPairComparator());
    for (int instanceId = 0; instanceId < numInstances; instanceId++) {
      heap.add(new Pairs.IntPair(numSegmentsAssignedPerInstance[instanceId], instanceId));
    }
    for (String segmentName : segmentsNotAssigned) {
      Pairs.IntPair intPair = heap.remove();
      int instanceId = intPair.getRight();
      newAssignment.put(segmentName, getReplicaGroupBasedInstanceStateMap(instancePartitions, partitionId, instanceId));
      intPair.setLeft(intPair.getLeft() + 1);
      heap.add(intPair);
    }
  }

  /**
   * Returns the map from instance name to Helix partition state for the replica-group based segment assignment
   * strategy, which can be put into the segment assignment. The instances are picked from the instance partitions by
   * the given partition id and instance id.
   */
  private static Map<String, String> getReplicaGroupBasedInstanceStateMap(InstancePartitions instancePartitions,
      int partitionId, int instanceId) {
    Map<String, String> instanceStateMap = new TreeMap<>();
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
      instanceStateMap.put(instancePartitions.getInstances(partitionId, replicaGroupId).get(instanceId),
          SegmentOnlineOfflineStateModel.ONLINE);
    }
    return instanceStateMap;
  }

  /**
   * Returns the map from instance name to Helix partition state, which can be put into the segment assignment.
   */
  public static Map<String, String> getInstanceStateMap(Collection<String> instances, String state) {
    Map<String, String> instanceStateMap = new TreeMap<>();
    for (String instanceName : instances) {
      instanceStateMap.put(instanceName, state);
    }
    return instanceStateMap;
  }

  /**
   * Returns a map from instance name to number of segments to be moved to it.
   */
  public static Map<String, Integer> getNumSegmentsToBeMovedPerInstance(Map<String, Map<String, String>> oldAssignment,
      Map<String, Map<String, String>> newAssignment) {
    Map<String, Integer> numSegmentsToBeMovedPerInstance = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> entry : newAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Set<String> newInstancesAssigned = entry.getValue().keySet();
      Set<String> oldInstancesAssigned = oldAssignment.get(segmentName).keySet();
      // For each new assigned instance, check if the segment needs to be moved to it
      for (String instanceName : newInstancesAssigned) {
        if (!oldInstancesAssigned.contains(instanceName)) {
          numSegmentsToBeMovedPerInstance.merge(instanceName, 1, Integer::sum);
        }
      }
    }
    return numSegmentsToBeMovedPerInstance;
  }

  /**
   * Class that splits segment assignment into COMPLETED, CONSUMING and OFFLINE segments.
   */
  static class CompletedConsumingOfflineSegmentAssignment {
    private final Map<String, Map<String, String>> _completedSegmentAssignment = new TreeMap<>();
    private final Map<String, Map<String, String>> _consumingSegmentAssignment = new TreeMap<>();
    private final Map<String, Map<String, String>> _offlineSegmentAssignment = new TreeMap<>();

    // NOTE: split the segments based on the following criteria:
    //       1. At least one instance ONLINE -> COMPLETED segment
    //       2. At least one instance CONSUMING -> CONSUMING segment
    //       3. All instances OFFLINE (all instances encountered error while consuming) -> OFFLINE segment
    CompletedConsumingOfflineSegmentAssignment(Map<String, Map<String, String>> segmentAssignment) {
      for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
        String segmentName = entry.getKey();
        Map<String, String> instanceStateMap = entry.getValue();
        if (instanceStateMap.values().contains(RealtimeSegmentOnlineOfflineStateModel.ONLINE)) {
          _completedSegmentAssignment.put(segmentName, instanceStateMap);
        } else if (instanceStateMap.values().contains(RealtimeSegmentOnlineOfflineStateModel.CONSUMING)) {
          _consumingSegmentAssignment.put(segmentName, instanceStateMap);
        } else {
          _offlineSegmentAssignment.put(segmentName, instanceStateMap);
        }
      }
    }

    Map<String, Map<String, String>> getCompletedSegmentAssignment() {
      return _completedSegmentAssignment;
    }

    Map<String, Map<String, String>> getConsumingSegmentAssignment() {
      return _consumingSegmentAssignment;
    }

    Map<String, Map<String, String>> getOfflineSegmentAssignment() {
      return _offlineSegmentAssignment;
    }
  }
}
