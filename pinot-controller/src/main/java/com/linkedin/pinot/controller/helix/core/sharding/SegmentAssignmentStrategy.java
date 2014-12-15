package com.linkedin.pinot.controller.helix.core.sharding;

import java.util.List;

import org.apache.helix.HelixAdmin;

import com.linkedin.pinot.common.segment.SegmentMetadata;


/**
 * Given a segmentMetadata, each strategy has to implement its own method to compute the assigned instances.
 * 
 * @author xiafu
 *
 */
public interface SegmentAssignmentStrategy {
  public List<String> getAssignedInstances(HelixAdmin helixAdmin, String helixClusterName,
      SegmentMetadata segmentMetadata, int numReplicas);
}
