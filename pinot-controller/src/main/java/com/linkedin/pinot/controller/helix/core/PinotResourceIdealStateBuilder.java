package com.linkedin.pinot.controller.helix.core;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;

import com.linkedin.pinot.controller.api.pojos.Resource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public class PinotResourceIdealStateBuilder {
  public static final String ONLINE = "ONLINE";
  public static final String OFFLINE = "OFFLINE";
  public static final String PINOT_RESOURCE_NUM_REPLICAS = "pinot.resource.numReplicas";

  public static IdealState buildEmptyIdealStateFor(Resource resource, HelixAdmin helixAdmin,
      String helixClusterName) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(resource.getResourceName());
    final int replicas = resource.getNumReplicas();
    customModeIdealStateBuilder.setStateModel(PinotHelixStateModelGenerator.PINOT_HELIX_STATE_MODEL)
    .setNumPartitions(0).setNumReplica(replicas).setMaxPartitionsPerNode(1);
    final IdealState idealState = customModeIdealStateBuilder.build();
    return idealState;
  }

  public synchronized static IdealState addNewSegmentToIdealStateFor(IndexSegment indexSegment, HelixAdmin helixAdmin,
      String helixClusterName) {

    final String resourceName = indexSegment.getSegmentMetadata().getResourceName();
    final String segmentName = indexSegment.getSegmentName();

    final IdealState currentIdealState = helixAdmin.getResourceIdealState(helixClusterName, resourceName);
    final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(segmentName);
    if (currentInstanceSet.isEmpty()) {
      // Adding new Segments
      final List<String> instances = helixAdmin.getInstancesInClusterWithTag(helixClusterName, resourceName);
      final int replicas =
          Integer.parseInt(HelixHelper.getResourceConfigsFor(helixClusterName, resourceName, helixAdmin).get(
              PINOT_RESOURCE_NUM_REPLICAS));
      final Set<String> selectedInstances = getRandomAssignedInstances(instances, replicas);
      for (final String instance : selectedInstances) {
        currentIdealState.setPartitionState(segmentName, instance, ONLINE);
      }
      currentIdealState.setNumPartitions(currentIdealState.getNumPartitions() + 1);
    } else {
      // Update new Segments
      for (final String instance : currentInstanceSet) {
        currentIdealState.setPartitionState(segmentName, instance, OFFLINE);
        currentIdealState.setPartitionState(segmentName, instance, ONLINE);
      }
    }
    return currentIdealState;
  }

  private static Set<String> getRandomAssignedInstances(List<String> instances, int replicas) {
    final Set<String> assignedInstances = new HashSet<String>();
    final Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < replicas; ++i) {
      final int idx = random.nextInt(instances.size());
      assignedInstances.add(instances.get(idx));
      instances.remove(idx);
    }
    return assignedInstances;
  }
}
