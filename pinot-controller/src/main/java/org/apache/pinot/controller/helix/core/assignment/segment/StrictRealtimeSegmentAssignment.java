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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;


/**
 * Segment assignment for LLC real-time table using upsert. The assignSegment() of RealtimeSegmentAssignment is
 * overridden to add new segment for a table partition in a way that's consistent with the assignment in idealState to
 * make sure that at any time the segments from the same table partition is hosted by the same server.
 * <ul>
 *   <li>
 *     For the CONSUMING segments, in addition to what's done in RealtimeSegmentAssignment, the assignment has to be
 *     checked if it's consistent with the current idealState. If the assignment calculated according to the
 *     InstancePartition and the one in idealState are different, the one in idealState must be used so that segments
 *     from the same table partition are always hosted on the same server as set in current idealState. If the
 *     idealState is not honored, segments from the same table partition may be assigned to different servers,
 *     breaking the key assumption for queries to be correct for the table using upsert.
 *   </li>
 *   <li>
 *     There is no need to handle COMPLETED segments for tables using upsert, because their completed segments should
 *     not be relocated to servers tagged to host COMPLETED segments. Basically, upsert-enabled tables can only use
 *     servers tagged for CONSUMING segments to host both consuming and completed segments from a table partition.
 *   </li>
 * </ul>
 */
public class StrictRealtimeSegmentAssignment extends RealtimeSegmentAssignment {

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    Preconditions.checkState(instancePartitionsMap.size() == 1, "One instance partition type should be provided");
    Map.Entry<InstancePartitionsType, InstancePartitions> typeToInstancePartitions =
        instancePartitionsMap.entrySet().iterator().next();
    InstancePartitionsType instancePartitionsType = typeToInstancePartitions.getKey();
    InstancePartitions instancePartitions = typeToInstancePartitions.getValue();
    Preconditions.checkState(instancePartitionsType == InstancePartitionsType.CONSUMING,
        "Only CONSUMING instance partition type is allowed for table using upsert but got: " + instancePartitionsType);
    _logger.info("Assigning segment: {} with instance partitions: {} for table: {}", segmentName, instancePartitions,
        _tableNameWithType);
    int segmentPartitionId =
        SegmentAssignmentUtils.getRealtimeSegmentPartitionId(segmentName, _tableNameWithType, _helixManager,
            _partitionColumn);
    List<String> instancesAssigned = assignConsumingSegment(segmentPartitionId, instancePartitions);
    List<String> idealAssignment = new ArrayList<>(instancesAssigned.size());
    // Iterate the idealState to find the first segment that's in the same table partition with the new segment, and
    // check if their assignments are same. We try to derive the partition id from segment name to avoid ZK reads.
    Set<String> nonStandardSegments = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      LLCSegmentName llcSegmentName = LLCSegmentName.of(entry.getKey());
      if (llcSegmentName == null) {
        nonStandardSegments.add(entry.getKey());
        continue;
      }
      if (llcSegmentName.getPartitionGroupId() == segmentPartitionId) {
        idealAssignment.addAll(entry.getValue().keySet());
        break;
      }
    }
    if (idealAssignment.isEmpty()) {
      _logger.debug("Check ZK metadata of segments: {} for any one from partition: {}", nonStandardSegments.size(),
          segmentPartitionId);
      // Check ZK metadata for segments with non-standard LLC segment names.
      for (String nonStandardSegment : nonStandardSegments) {
        if (SegmentAssignmentUtils.getRealtimeSegmentPartitionId(nonStandardSegment, _tableNameWithType, _helixManager,
            _partitionColumn) == segmentPartitionId) {
          idealAssignment.addAll(currentAssignment.get(nonStandardSegment).keySet());
          break;
        }
      }
    }
    if (idealAssignment.isEmpty()) {
      _logger.info("Found no existing assignment from idealState, using the one decided by instancePartitions");
    } else if (!idealAssignment.equals(instancesAssigned)) {
      _logger.info("Assignment: {} is inconsistent with idealState: {}, using the one as from idealState",
          instancesAssigned, idealAssignment);
      instancesAssigned = idealAssignment;
    }
    _logger.info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned,
        _tableNameWithType);
    return instancesAssigned;
  }
}
