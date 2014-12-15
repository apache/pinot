package com.linkedin.pinot.controller.helix.core.sharding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.helix.HelixAdmin;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.helix.core.HelixHelper;


/**
 * Assigns a segment to the instance that has same sharding key.
 * 
 * @author xiafu
 *
 */
public class BucketizedSegmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = Logger.getLogger(BucketizedSegmentStrategy.class);

  @Override
  public List<String> getAssignedInstances(HelixAdmin helixAdmin, String helixClusterName,
      SegmentMetadata segmentMetadata, int numReplicas) {
    List<String> allInstances =
        helixAdmin.getInstancesInClusterWithTag(helixClusterName, segmentMetadata.getResourceName());
    List<String> selectedInstanceList = new ArrayList<String>();
    if (segmentMetadata.getShardingKey() != null) {
      for (String instance : allInstances) {
        if (HelixHelper.getInstanceConfigsMapFor(instance, helixClusterName, helixAdmin).get("shardingKey")
            .equalsIgnoreCase(segmentMetadata.getShardingKey())) {
          selectedInstanceList.add(instance);
        }
      }
      LOGGER.info("Segment assignment result for : " + segmentMetadata.getName() + ", in resource : "
          + segmentMetadata.getResourceName() + ", selected instances: "
          + Arrays.toString(selectedInstanceList.toArray()));
      return selectedInstanceList;
    } else {
      throw new RuntimeException("Segment missing sharding key!");
    }
  }

}
