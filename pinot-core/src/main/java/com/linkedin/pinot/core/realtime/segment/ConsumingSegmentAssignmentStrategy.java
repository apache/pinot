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

import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Assigns the given list of consuming segments onto instances based on given partition assignment
 */
public class ConsumingSegmentAssignmentStrategy implements RealtimeSegmentAssignmentStrategy {

  /**
   * Assigns new segments to instances by referring to the partition assignment
   * @param newSegments segments to assign
   * @param partitionAssignment partition assignment for the table to which the segments belong
   * @return map of segment name to instances list
   */
  public Map<String, List<String>> assign(Collection<String> newSegments, PartitionAssignment partitionAssignment)
      throws InvalidConfigException {

    Map<String, List<String>> segmentAssignment = new HashMap<>(newSegments.size());

    for (String segmentName : newSegments) {
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        int partitionId = llcSegmentName.getPartitionId();
        List<String> instancesListForPartition =
            partitionAssignment.getInstancesListForPartition(String.valueOf(partitionId));
        if (instancesListForPartition == null) {
          throw new InvalidConfigException(
              "No partition assignment " + partitionId + " found for segment " + segmentName);
        }
        segmentAssignment.put(segmentName, instancesListForPartition);
      }
    }
    return segmentAssignment;
  }
}
