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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;


/// Assignment of a table: the instance state map of every segment, the partition assignment (the instances of every
/// partition, see [org.apache.pinot.common.assignment.PartitionAssignmentUtils]) for a partition-pinned table, empty
/// for other tables, and the partition ids of the segments (see [BaseSegmentAssignment#getPartitionIds]), for the
/// strict replica-group batching of the rebalance, which moves the segments of a partition together.
public class TableAssignment {
  private final Map<String, Map<String, String>> _segmentAssignment;
  private final Map<Integer, List<String>> _partitionAssignment;
  private final Object2IntMap<String> _segmentPartitionIds;
  private final Map<Integer, ReplicateModePartitionAssignment> _replicateModePartitionAssignment;
  private final Set<String> _replicateModeSegments;

  /// Creates an assignment without partition ids of the segments.
  public TableAssignment(Map<String, Map<String, String>> segmentAssignment,
      Map<Integer, List<String>> partitionAssignment) {
    this(segmentAssignment, partitionAssignment, BaseSegmentAssignment.EMPTY_SEGMENT_PARTITION_IDS);
  }

  public TableAssignment(Map<String, Map<String, String>> segmentAssignment,
      Map<Integer, List<String>> partitionAssignment, Object2IntMap<String> segmentPartitionIds) {
    this(segmentAssignment, partitionAssignment, segmentPartitionIds, Map.of(), Set.of());
  }

  public TableAssignment(Map<String, Map<String, String>> segmentAssignment,
      Map<Integer, List<String>> partitionAssignment, Object2IntMap<String> segmentPartitionIds,
      Map<Integer, ReplicateModePartitionAssignment> replicateModePartitionAssignment,
      Set<String> replicateModeSegments) {
    _segmentAssignment = segmentAssignment;
    _partitionAssignment = partitionAssignment;
    _segmentPartitionIds = segmentPartitionIds;
    _replicateModePartitionAssignment = Map.copyOf(replicateModePartitionAssignment);
    _replicateModeSegments = Set.copyOf(replicateModeSegments);
  }

  public Map<String, Map<String, String>> getSegmentAssignment() {
    return _segmentAssignment;
  }

  public Map<Integer, List<String>> getPartitionAssignment() {
    return _partitionAssignment;
  }

  public Object2IntMap<String> getSegmentPartitionIds() {
    return _segmentPartitionIds;
  }

  public Map<Integer, ReplicateModePartitionAssignment> getReplicateModePartitionAssignment() {
    return _replicateModePartitionAssignment;
  }

  /// Snapshot-true unfinished segments, including pauseless COMMITTING segments that are ONLINE in IdealState.
  public Set<String> getReplicateModeSegments() {
    return _replicateModeSegments;
  }

  /// Complete replica candidates and the single consumption leader; neither implies current Server readiness.
  public record ReplicateModePartitionAssignment(String leader, List<String> replicas) {
    public ReplicateModePartitionAssignment {
      // Persisted role candidates use this sorted form. Native cyclic placement can return a different order.
      replicas = List.copyOf(PartitionAssignmentUtils.toInstanceList(replicas));
    }
  }
}
