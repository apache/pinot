package com.linkedin.pinot.controller.helix.core.sharding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.common.utils.Pairs.Number2ObjectPair;


/**
 * Assigns a segment to the instance that has least number of segments.
 * 
 * @author xiafu
 *
 */
public class BalanceNumSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = Logger.getLogger(BalanceNumSegmentAssignmentStrategy.class);

  @Override
  public List<String> getAssignedInstances(HelixAdmin helixAdmin, String helixClusterName,
      SegmentMetadata segmentMetadata, int numReplicas) {
    String resourceName = segmentMetadata.getResourceName();
    List<String> selectedInstances = new ArrayList<String>();
    Map<String, Integer> currentNumSegmentsPerInstanceMap = new HashMap<String, Integer>();
    List<String> allTaggedInstances = helixAdmin.getInstancesInClusterWithTag(helixClusterName, resourceName);
    for (String instance : allTaggedInstances) {
      currentNumSegmentsPerInstanceMap.put(instance, 0);
    }
    ExternalView externalView = helixAdmin.getResourceExternalView(helixClusterName, resourceName);
    if (externalView != null) {
      for (String partitionName : externalView.getPartitionSet()) {
        Map<String, String> instanceToStateMap = externalView.getStateMap(partitionName);
        for (String instanceName : instanceToStateMap.keySet()) {
          if (currentNumSegmentsPerInstanceMap.containsKey(instanceName)) {
            currentNumSegmentsPerInstanceMap.put(instanceName, currentNumSegmentsPerInstanceMap.get(instanceName) + 1);
          } else {
            currentNumSegmentsPerInstanceMap.put(instanceName, 1);
          }
        }
      }

    }
    PriorityQueue<Number2ObjectPair<String>> priorityQueue =
        new PriorityQueue<Number2ObjectPair<String>>(numReplicas, Pairs.getDescendingnumber2ObjectPairComparator());
    for (String key : currentNumSegmentsPerInstanceMap.keySet()) {
      priorityQueue.add(new Number2ObjectPair<String>(currentNumSegmentsPerInstanceMap.get(key), key));
      if (priorityQueue.size() > numReplicas) {
        priorityQueue.poll();
      }
    }

    while (!priorityQueue.isEmpty()) {
      selectedInstances.add(priorityQueue.poll().getB());
    }
    LOGGER.info("Segment assignment result for : " + segmentMetadata.getName() + ", in resource : "
        + segmentMetadata.getResourceName() + ", selected instances: " + Arrays.toString(selectedInstances.toArray()));
    return selectedInstances;
  }
}
