package com.linkedin.pinot.controller.helix.core;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;

import com.linkedin.pinot.controller.helix.api.PinotStandaloneResource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public class PinotResourceIdealStateBuilder {
  public static final String ONLINE = "ONLINE";
  public static final String OFFLINE = "OFFLINE";
  public static final String PINOT_RESOURCE_NUM_REPLICAS = "pinot.resource.numReplicas";

  public static IdealState buildEmptyIdealStateFor(PinotStandaloneResource resource, HelixAdmin helixAdmin,
      String helixClusterName) {
    CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(resource.getResourceName());
    int replicas =
        Integer.parseInt(HelixHelper.getResourceConfigsFor(helixClusterName, resource.getResourceName(), helixAdmin)
            .get(PINOT_RESOURCE_NUM_REPLICAS));
    customModeIdealStateBuilder.setStateModel(PinotHelixStateModelGenerator.PINOT_HELIX_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(replicas).setMaxPartitionsPerNode(1);
    IdealState idealState = customModeIdealStateBuilder.build();
    return idealState;
  }

  public synchronized static IdealState addNewSegmentToIdealStateFor(IndexSegment indexSegment, HelixAdmin helixAdmin,
      String helixClusterName) {

    String resourceName = indexSegment.getSegmentMetadata().getResourceName();
    String segmentName = indexSegment.getSegmentName();

    IdealState currentIdealState = helixAdmin.getResourceIdealState(helixClusterName, resourceName);
    Set<String> currentInstanceSet = currentIdealState.getInstanceSet(segmentName);
    if (currentInstanceSet.isEmpty()) {
      // Adding new Segments
      List<String> instances = helixAdmin.getInstancesInClusterWithTag(helixClusterName, resourceName);
      int replicas =
          Integer.parseInt(HelixHelper.getResourceConfigsFor(helixClusterName, resourceName, helixAdmin).get(
              PINOT_RESOURCE_NUM_REPLICAS));
      Set<String> selectedInstances = getRandomAssignedInstances(instances, replicas);
      for (String instance : selectedInstances) {
        currentIdealState.setPartitionState(segmentName, instance, ONLINE);
      }
      currentIdealState.setNumPartitions(currentIdealState.getNumPartitions() + 1);
    } else {
      // Update new Segments
      for (String instance : currentInstanceSet) {
        currentIdealState.setPartitionState(segmentName, instance, OFFLINE);
        currentIdealState.setPartitionState(segmentName, instance, ONLINE);
      }
    }
    return currentIdealState;
  }

  private static Set<String> getRandomAssignedInstances(List<String> instances, int replicas) {
    Set<String> assignedInstances = new HashSet<String>();
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < replicas; ++i) {
      int idx = random.nextInt(instances.size());
      assignedInstances.add(instances.get(idx));
      instances.remove(idx);
    }
    return assignedInstances;
  }
}
