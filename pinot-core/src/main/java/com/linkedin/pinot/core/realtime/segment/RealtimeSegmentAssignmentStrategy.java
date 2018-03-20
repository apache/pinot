package com.linkedin.pinot.core.realtime.segment;

import com.linkedin.pinot.common.partition.PartitionAssignment;
import java.util.List;
import java.util.Map;


/**
 * An interface for segment assignment of realtime segments
 */
public interface RealtimeSegmentAssignmentStrategy {

  Map<String, List<String>> assign(List<String> newSegments, PartitionAssignment partitionAssignment);

}