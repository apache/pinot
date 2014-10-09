package com.linkedin.pinot.controller.helix.core.sharding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.helix.HelixAdmin;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.segment.SegmentMetadata;


/**
 * Random assign segment to instances.
 * 
 * @author xiafu
 *
 */
public class RandomAssignmentStrategy implements SegmentAssignmentStrategy {

  private static final Logger LOGGER = Logger.getLogger(RandomAssignmentStrategy.class);

  @Override
  public List<String> getAssignedInstances(HelixAdmin helixAdmin, String helixClusterName,
      SegmentMetadata segmentMetadata, int numReplicas) {
    final Random random = new Random(System.currentTimeMillis());
    List<String> allInstanceList =
        helixAdmin.getInstancesInClusterWithTag(helixClusterName, segmentMetadata.getResourceName());
    List<String> selectedInstanceList = new ArrayList<String>();
    for (int i = 0; i < numReplicas; ++i) {
      final int idx = random.nextInt(allInstanceList.size());
      selectedInstanceList.add(allInstanceList.get(idx));
      allInstanceList.remove(idx);
    }
    LOGGER.info("Segment assignment result for : " + segmentMetadata.getName() + ", in resource : "
        + segmentMetadata.getResourceName() + ", selected instances: "
        + Arrays.toString(selectedInstanceList.toArray()));

    return selectedInstanceList;
  }
}
