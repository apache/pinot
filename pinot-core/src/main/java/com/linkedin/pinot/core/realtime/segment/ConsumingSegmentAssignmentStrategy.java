package com.linkedin.pinot.core.realtime.segment;

import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.utils.LLCSegmentName;
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
  public Map<String, List<String>> assign(List<String> newSegments, PartitionAssignment partitionAssignment) {

    Map<String, List<String>> segmentAssignment = new HashMap<>(newSegments.size());

    for (String segmentName : newSegments) {
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        int partitionId = llcSegmentName.getPartitionId();
        List<String> instancesListForPartition = partitionAssignment.getInstancesListForPartition(String.valueOf(partitionId));
        if (instancesListForPartition == null) {
          throw new IllegalStateException("No partition assignment " + partitionId + " found for segment " + segmentName);
        }
        segmentAssignment.put(segmentName, instancesListForPartition);
      }
    }
    return segmentAssignment;
  }

}
