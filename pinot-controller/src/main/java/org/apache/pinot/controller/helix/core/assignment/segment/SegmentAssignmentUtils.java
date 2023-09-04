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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.Pairs;


/**
 * Utility class for segment assignment.
 */
public class SegmentAssignmentUtils {
  private SegmentAssignmentUtils() {
  }

  /**
   * Returns the number of segments assigned to each instance.
   */
  public static int[] getNumSegmentsAssignedPerInstance(Map<String, Map<String, String>> segmentAssignment,
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
  public static List<String> getInstancesForNonReplicaGroupBasedAssignment(InstancePartitions instancePartitions,
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
   * Assigns the segment for the non-replica-group based segment assignment strategy and returns the assigned instances.
   */
  public static List<String> assignSegmentWithoutReplicaGroup(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, int replication) {
    List<String> instances = getInstancesForNonReplicaGroupBasedAssignment(instancePartitions, replication);
    int[] numSegmentsAssignedPerInstance = getNumSegmentsAssignedPerInstance(currentAssignment, instances);
    int numInstances = numSegmentsAssignedPerInstance.length;
    PriorityQueue<Pairs.IntPair> heap = new PriorityQueue<>(numInstances, Pairs.intPairComparator());
    for (int instanceId = 0; instanceId < numInstances; instanceId++) {
      heap.add(new Pairs.IntPair(numSegmentsAssignedPerInstance[instanceId], instanceId));
    }
    List<String> instancesAssigned = new ArrayList<>(replication);
    for (int i = 0; i < replication; i++) {
      instancesAssigned.add(instances.get(heap.remove().getRight()));
    }
    return instancesAssigned;
  }

  /**
   * Assigns the segment for the replica-group based segment assignment strategy and returns the assigned instances.
   */
  public static List<String> assignSegmentWithReplicaGroup(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, int partitionId) {
    // First assign the segment to replica-group 0
    List<String> instances = instancePartitions.getInstances(partitionId, 0);
    int[] numSegmentsAssignedPerInstance = getNumSegmentsAssignedPerInstance(currentAssignment, instances);
    int minNumSegmentsAssigned = numSegmentsAssignedPerInstance[0];
    int instanceIdWithLeastSegmentsAssigned = 0;
    int numInstances = numSegmentsAssignedPerInstance.length;
    for (int instanceId = 1; instanceId < numInstances; instanceId++) {
      if (numSegmentsAssignedPerInstance[instanceId] < minNumSegmentsAssigned) {
        minNumSegmentsAssigned = numSegmentsAssignedPerInstance[instanceId];
        instanceIdWithLeastSegmentsAssigned = instanceId;
      }
    }

    // Mirror the assignment to all replica-groups
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    List<String> instancesAssigned = new ArrayList<>(numReplicaGroups);
    for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
      instancesAssigned
          .add(instancePartitions.getInstances(partitionId, replicaGroupId).get(instanceIdWithLeastSegmentsAssigned));
    }

    return instancesAssigned;
  }

  /**
   * Rebalances the table with Helix AutoRebalanceStrategy.
   */
  public static Map<String, Map<String, String>> rebalanceTableWithHelixAutoRebalanceStrategy(
      Map<String, Map<String, String>> currentAssignment, List<String> instances, int replication) {
    // Use Helix AutoRebalanceStrategy to rebalance the table
    LinkedHashMap<String, Integer> states = new LinkedHashMap<>();
    states.put(SegmentStateModel.ONLINE, replication);
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
   * Rebalances the table for the replica-group based segment assignment strategy by uniformly spraying group of
   * segments belonging to each instancePartitionId to the instances of that instance partition.
   */
  public static Map<String, Map<String, String>> rebalanceReplicaGroupBasedTable(
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions,
      Map<Integer, List<String>> instancePartitionIdToSegmentsMap) {
    Map<String, Map<String, String>> newAssignment = new TreeMap<>();
    for (Map.Entry<Integer, List<String>> entry : instancePartitionIdToSegmentsMap.entrySet()) {
      // Uniformly spray the segment partitions over the instance partitions
      int instancePartitionId = entry.getKey();
      List<String> segments = entry.getValue();
      rebalanceReplicaGroupBasedPartition(currentAssignment, instancePartitions, instancePartitionId, segments,
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
  public static void rebalanceReplicaGroupBasedPartition(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, int partitionId, List<String> segments,
      Map<String, Map<String, String>> newAssignment) {
    // Fetch instances in replica-group 0
    List<String> instances = instancePartitions.getInstances(partitionId, 0);
    Map<String, Integer> instanceNameToIdMap = getInstanceNameToIdMap(instances);

    // Calculate target number of segments per instance
    // NOTE: in order to minimize the segment movements, use the ceiling of the quotient
    int numInstances = instances.size();
    int numSegments = segments.size();
    int targetNumSegmentsPerInstance = (numSegments + numInstances - 1) / numInstances;

    // Do not move segment if target number of segments is not reached, track the segments need to be moved
    int[] numSegmentsAssignedPerInstance = new int[numInstances];
    List<String> segmentsNotAssigned = new ArrayList<>();
    for (String segmentName : segments) {
      boolean segmentAssigned = false;
      for (String instanceName : currentAssignment.get(segmentName).keySet()) {
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
      numSegmentsAssignedPerInstance[instanceId]++;
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
      instanceStateMap
          .put(instancePartitions.getInstances(partitionId, replicaGroupId).get(instanceId), SegmentStateModel.ONLINE);
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

  public static Set<String> getSegmentsToMove(Map<String, Map<String, String>> oldAssignment,
      Map<String, Map<String, String>> newAssignment) {
    Set<String> result = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : newAssignment.entrySet()) {
      String segmentName = entry.getKey();
      if (!entry.getValue().equals(oldAssignment.get(segmentName))) {
        result.add(segmentName);
      }
    }
    return result;
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
        if (instanceStateMap.containsValue(SegmentStateModel.ONLINE)) {
          _completedSegmentAssignment.put(segmentName, instanceStateMap);
        } else if (instanceStateMap.containsValue(SegmentStateModel.CONSUMING)) {
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

  /**
   * Takes a segment assignment and splits them up based on which tiers the segments are eligible for. Only considers
   * ONLINE segments.
   * Tiers are selected according to the order provided in the tiers list.
   */
  static class TierSegmentAssignment {

    private final Map<String, Map<String, Map<String, String>>> _tierNameToSegmentAssignmentMap = new TreeMap<>();
    private final Map<String, Map<String, String>> _nonTierSegmentAssignment = new TreeMap<>();

    /**
     * Creates a TierSegmentAssignment from the given segmentAssignment
     * @param tableNameWithType table to which the segment assignment belongs
     * @param sortedTiers list of tiers, pre-sorted as per desired order by caller
     * @param segmentAssignment segment assignment of the table
     */
    TierSegmentAssignment(String tableNameWithType, List<Tier> sortedTiers,
        Map<String, Map<String, String>> segmentAssignment) {

      // initialize tier to segmentAssignment map
      sortedTiers.forEach(t -> _tierNameToSegmentAssignmentMap.put(t.getName(), new TreeMap<>()));

      // iterate over all segments
      for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
        String segmentName = entry.getKey();
        Map<String, String> instanceStateMap = entry.getValue();
        boolean selected = false;

        // only consider ONLINE segments for tiers
        if (instanceStateMap.containsValue(SegmentStateModel.ONLINE)) {
          // find an eligible tier for the segment, from the ordered list of tiers
          for (Tier tier : sortedTiers) {
            if (tier.getSegmentSelector().selectSegment(tableNameWithType, segmentName)) {
              _tierNameToSegmentAssignmentMap.get(tier.getName()).put(segmentName, instanceStateMap);
              selected = true;
              break;
            }
          }
        }

        // if segment not eligible for any tier, put in ordinary segments map
        if (!selected) {
          _nonTierSegmentAssignment.put(segmentName, instanceStateMap);
        }
      }

      // remove tiers with no eligible segments
      _tierNameToSegmentAssignmentMap.entrySet().removeIf(e -> e.getValue().isEmpty());
    }

    /**
     * Returns a map from tier name to segment assignments for segments which are eligible for that tier
     */
    public Map<String, Map<String, Map<String, String>>> getTierNameToSegmentAssignmentMap() {
      return _tierNameToSegmentAssignmentMap;
    }

    /**
     * Returns segment assignments of segments which were not eligible for any tier
     */
    public Map<String, Map<String, String>> getNonTierSegmentAssignment() {
      return _nonTierSegmentAssignment;
    }
  }

  /**
   * Returns a partition id for offline table
   */
  public static int getOfflineSegmentPartitionId(String segmentName, String offlineTableName, HelixManager helixManager,
      @Nullable String partitionColumn) {
    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(helixManager.getHelixPropertyStore(), offlineTableName, segmentName);
    Preconditions
        .checkState(segmentZKMetadata != null, "Failed to find segment ZK metadata for segment: %s of table: %s",
            segmentName, offlineTableName);
    return getPartitionId(segmentZKMetadata, offlineTableName, partitionColumn);
  }

  private static int getPartitionId(SegmentZKMetadata segmentZKMetadata, String offlineTableName,
      @Nullable String partitionColumn) {
    String segmentName = segmentZKMetadata.getSegmentName();
    ColumnPartitionMetadata partitionMetadata =
        segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().get(partitionColumn);
    Preconditions.checkState(partitionMetadata != null,
        "Segment ZK metadata for segment: %s of table: %s does not contain partition metadata for column: %s",
        segmentName, offlineTableName, partitionColumn);
    Set<Integer> partitions = partitionMetadata.getPartitions();
    Preconditions.checkState(partitions.size() == 1,
        "Segment ZK metadata for segment: %s of table: %s contains multiple partitions for column: %s", segmentName,
        offlineTableName, partitionColumn);
    return partitions.iterator().next();
  }

  /**
   * Returns map of instance partition id to segments for offline tables
   */
  public static Map<Integer, List<String>> getOfflineInstancePartitionIdToSegmentsMap(Set<String> segments,
      int numInstancePartitions, String offlineTableName, HelixManager helixManager, @Nullable String partitionColumn) {
    // Fetch partition id from segment ZK metadata
    List<SegmentZKMetadata> segmentsZKMetadata =
        ZKMetadataProvider.getSegmentsZKMetadata(helixManager.getHelixPropertyStore(), offlineTableName);

    Map<Integer, List<String>> instancePartitionIdToSegmentsMap = new HashMap<>();
    Set<String> segmentsWithoutZKMetadata = new HashSet<>(segments);
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      String segmentName = segmentZKMetadata.getSegmentName();
      if (segmentsWithoutZKMetadata.remove(segmentName)) {
        int partitionId = getPartitionId(segmentZKMetadata, offlineTableName, partitionColumn);
        int instancePartitionId = partitionId % numInstancePartitions;
        instancePartitionIdToSegmentsMap.computeIfAbsent(instancePartitionId, k -> new ArrayList<>()).add(segmentName);
      }
    }
    Preconditions.checkState(segmentsWithoutZKMetadata.isEmpty(), "Failed to find ZK metadata for segments: %s",
        segmentsWithoutZKMetadata);

    return instancePartitionIdToSegmentsMap;
  }

  /**
   * Returns a partition id for realtime table
   */
  public static int getRealtimeSegmentPartitionId(String segmentName, String realtimeTableName,
      HelixManager helixManager, @Nullable String partitionColumn) {
    Integer segmentPartitionId =
        SegmentUtils.getRealtimeSegmentPartitionId(segmentName, realtimeTableName, helixManager, partitionColumn);
    if (segmentPartitionId == null) {
      // This case is for the uploaded segments for which there's no partition information.
      // A random, but consistent, partition id is calculated based on the hash code of the segment name.
      // Note that '% 10K' is used to prevent having partition ids with large value which will be problematic later in
      // instance assignment formula.
      segmentPartitionId = Math.abs(segmentName.hashCode() % 10_000);
    }
    return segmentPartitionId;
  }

  /**
   * Returns map of instance partition id to segments for realtime tables
   */
  public static Map<Integer, List<String>> getRealtimeInstancePartitionIdToSegmentsMap(Set<String> segments,
      int numInstancePartitions, String realtimeTableName, HelixManager helixManager,
      @Nullable String partitionColumn) {
    Map<Integer, List<String>> instancePartitionIdToSegmentsMap = new HashMap<>();
    for (String segmentName : segments) {
      int instancePartitionId =
          getRealtimeSegmentPartitionId(segmentName, realtimeTableName, helixManager, partitionColumn)
              % numInstancePartitions;
      instancePartitionIdToSegmentsMap.computeIfAbsent(instancePartitionId, k -> new ArrayList<>()).add(segmentName);
    }
    return instancePartitionIdToSegmentsMap;
  }
}
