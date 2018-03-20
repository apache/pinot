package com.linkedin.pinot.core.realtime.segment;

import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Assigns the given list of consuming segments based on partition assignment
 */
public class ConsumingSegmentAssignmentStrategy implements RealtimeSegmentAssignmentStrategy {

  /**
   * Assigns new segments to instances by looking at the partition assignment
   * @param newSegments
   * @param partitionAssignment
   * @return
   */
  public Map<String, List<String>> assign(List<String> newSegments, PartitionAssignment partitionAssignment) {
    Map<String, List<String>> segmentAssignment = new HashMap<>(newSegments.size());
    for (String segmentName : newSegments) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partitionId = llcSegmentName.getPartitionId();
      List<String> instancesListForPartition =
          partitionAssignment.getInstancesListForPartition(String.valueOf(partitionId));
      segmentAssignment.put(segmentName, instancesListForPartition);
    }
    return segmentAssignment;
  }

}
