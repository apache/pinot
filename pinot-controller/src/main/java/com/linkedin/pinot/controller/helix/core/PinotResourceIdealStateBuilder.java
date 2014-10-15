package com.linkedin.pinot.controller.helix.core;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.api.pojos.BrokerDataResource;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.core.sharding.BrokerResourceAssignmentStrategy;
import com.linkedin.pinot.controller.helix.core.sharding.SegmentAssignmentStrategy;
import com.linkedin.pinot.controller.helix.core.sharding.SegmentAssignmentStrategyFactory;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;


/**
 * Pinot data server layer IdealState builder.
 *
 * @author xiafu
 *
 */
public class PinotResourceIdealStateBuilder {
  public static final String ONLINE = "ONLINE";
  public static final String OFFLINE = "OFFLINE";
  public static final String DROPPED = "DROPPED";

  public static final Map<String, SegmentAssignmentStrategy> SEGMENT_ASSIGNMENT_STRATEGY_MAP =
      new HashMap<String, SegmentAssignmentStrategy>();

  /**
   *
   * Building an empty idealState for a given resource.
   * Used when creating a new resource.
   *
   * @param resource
   * @param helixAdmin
   * @param helixClusterName
   * @return
   */
  public static IdealState buildEmptyIdealStateFor(DataResource resource, HelixAdmin helixAdmin, String helixClusterName) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(resource.getResourceName());
    final int replicas = resource.getNumberOfCopies();
    customModeIdealStateBuilder
        .setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(replicas).setMaxPartitionsPerNode(1);
    final IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(resource.getResourceName());
    return idealState;
  }

  /**
   *
   * Building an empty idealState for a given resource.
   * Used when creating a new resource.
   *
   * @param resource
   * @param helixAdmin
   * @param helixClusterName
   * @return
   */
  public static IdealState buildEmptyIdealStateForBrokerResource(HelixAdmin helixAdmin, String helixClusterName) {
    final CustomModeISBuilder customModeIdealStateBuilder =
        new CustomModeISBuilder(V1Constants.Helix.BROKER_RESOURCE_INSTANCE);
    customModeIdealStateBuilder
        .setStateModel(
            PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL)
        .setMaxPartitionsPerNode(Integer.MAX_VALUE).setNumReplica(Integer.MAX_VALUE)
        .setNumPartitions(Integer.MAX_VALUE);
    final IdealState idealState = customModeIdealStateBuilder.build();
    return idealState;
  }

  /**
   * For adding a new segment, we have to recompute the ideal states.
   *
   * @param segmentMetadata
   * @param helixAdmin
   * @param helixClusterName
   * @return
   */
  public static IdealState addNewSegmentToIdealStateFor(SegmentMetadata segmentMetadata, HelixAdmin helixAdmin,
      String helixClusterName) {

    final String resourceName = segmentMetadata.getResourceName();
    final String segmentName = segmentMetadata.getName();
    if (!SEGMENT_ASSIGNMENT_STRATEGY_MAP.containsKey(resourceName)) {
      if (HelixHelper.getResourceConfigsFor(helixClusterName, resourceName, helixAdmin).containsKey(
          V1Constants.Helix.KEY_OF_SEGMENT_ASSIGNMENT_STRATEGY)) {
        SEGMENT_ASSIGNMENT_STRATEGY_MAP.put(
            resourceName,
            SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(HelixHelper.getResourceConfigsFor(
                helixClusterName, resourceName, helixAdmin).get(V1Constants.Helix.KEY_OF_SEGMENT_ASSIGNMENT_STRATEGY)));

      } else {
        SEGMENT_ASSIGNMENT_STRATEGY_MAP.put(resourceName, SegmentAssignmentStrategyFactory
            .getSegmentAssignmentStrategy(V1Constants.Helix.DEFAULT_SEGMENT_ASSIGNMENT_STRATEGY));
      }
    }
    final SegmentAssignmentStrategy segmentAssignmentStrategy = SEGMENT_ASSIGNMENT_STRATEGY_MAP.get(resourceName);

    final IdealState currentIdealState = helixAdmin.getResourceIdealState(helixClusterName, resourceName);
    final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(segmentName);
    if (currentInstanceSet.isEmpty()) {
      // Adding new Segments
      final int replicas =
          Integer.parseInt(HelixHelper.getResourceConfigsFor(helixClusterName, resourceName, helixAdmin).get(
              V1Constants.Helix.DataSource.NUMBER_OF_COPIES));
      final List<String> selectedInstances =
          segmentAssignmentStrategy.getAssignedInstances(helixAdmin, helixClusterName, segmentMetadata, replicas);
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

  /**
   * Remove a segment is also required to recompute the ideal state.
   *
   * @param resourceName
   * @param segmentId
   * @param helixAdmin
   * @param helixClusterName
   * @return
   */
  public synchronized static IdealState removeSegmentFromIdealStateFor(String resourceName, String segmentId,
      HelixAdmin helixAdmin, String helixClusterName) {

    final IdealState currentIdealState = helixAdmin.getResourceIdealState(helixClusterName, resourceName);
    final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(segmentId);
    if (!currentInstanceSet.isEmpty() && currentIdealState.getPartitionSet().contains(segmentId)) {
      currentIdealState.getPartitionSet().remove(segmentId);
      currentIdealState.setNumPartitions(currentIdealState.getNumPartitions() - 1);
    } else {
      throw new RuntimeException("Cannot found segmentId - " + segmentId + " in resource - " + resourceName);
    }
    return currentIdealState;
  }

  /**
   *
   * @param brokerResourceName
   * @param helixAdmin
   * @param helixClusterName
   * @return
   */
  public static IdealState removeBrokerResourceFromIdealStateFor(String brokerResourceName, HelixAdmin helixAdmin,
      String helixClusterName) {
    final IdealState currentIdealState =
        helixAdmin.getResourceIdealState(helixClusterName, V1Constants.Helix.BROKER_RESOURCE_INSTANCE);
    final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(brokerResourceName);
    if (!currentInstanceSet.isEmpty() && currentIdealState.getPartitionSet().contains(brokerResourceName)) {
      currentIdealState.getPartitionSet().remove(brokerResourceName);
    } else {
      throw new RuntimeException("Cannot found broker resource - " + brokerResourceName + " in broker resource ");
    }
    return currentIdealState;
  }

  /**
   * @param brokerResource
   * @param helixAdmin
   * @param helixClusterName
   * @return
   * @throws Exception
   */
  public static IdealState addBrokerResourceToIdealStateFor(BrokerDataResource brokerResource, HelixAdmin helixAdmin,
      String helixClusterName) throws Exception {
    final String resourceName = brokerResource.getResourceName();
    try {

      final IdealState currentIdealState =
          helixAdmin.getResourceIdealState(helixClusterName, V1Constants.Helix.BROKER_RESOURCE_INSTANCE);
      final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(resourceName);

      if (currentInstanceSet.isEmpty()) {
        // Adding new broker resources
        final List<String> selectedInstances =
            BrokerResourceAssignmentStrategy.getRandomAssignedInstances(helixAdmin, helixClusterName, brokerResource);
        for (final String instance : selectedInstances) {
          currentIdealState.setPartitionState(resourceName, instance, ONLINE);
        }
      } else {
        return updateBrokerResourceToIdealStateFor(currentIdealState, brokerResource, helixAdmin, helixClusterName);
      }
      return currentIdealState;
    } catch (final Exception ex) {
      throw ex;
    }
  }

  /**
   * @param currentIdealState
   * @param brokerDataResource
   * @param helixAdmin
   * @param helixClusterName
   * @return
   */
  private static IdealState updateBrokerResourceToIdealStateFor(IdealState currentIdealState,
      BrokerDataResource brokerDataResource, HelixAdmin helixAdmin, String helixClusterName) {

    final String resourceName = brokerDataResource.getResourceName();
    final int numBrokerInstancesToServeDataResource = brokerDataResource.getNumBrokerInstances();
    final Set<String> currentOnlineInstanceSet = new HashSet<String>();

    final Map<String, String> currentInstanceStateMap = currentIdealState.getInstanceStateMap(resourceName);

    // Update online/offline instances staus.
    for (final String instance : currentInstanceStateMap.keySet()) {
      if (currentInstanceStateMap.get(instance).equalsIgnoreCase(ONLINE)) {
        currentOnlineInstanceSet.add(instance);
      }
    }

    // Update broker resources
    if (currentOnlineInstanceSet.size() > numBrokerInstancesToServeDataResource) {
      // remove instances
      int numInstancesToRemove = currentOnlineInstanceSet.size() - numBrokerInstancesToServeDataResource;
      final Set<String> removedInstanceNames = new HashSet<String>();

      currentIdealState.getPartitionSet().remove(resourceName);
      for (final String instance : currentOnlineInstanceSet) {
        if (numInstancesToRemove > 0) {
          removedInstanceNames.add(instance);
          helixAdmin.enablePartition(false, helixClusterName, instance, V1Constants.Helix.BROKER_RESOURCE_INSTANCE,
              Arrays.asList(resourceName));
          numInstancesToRemove--;
        } else {
          currentIdealState.setPartitionState(resourceName, instance, ONLINE);
        }
      }

      helixAdmin.setResourceIdealState(helixClusterName, V1Constants.Helix.BROKER_RESOURCE_INSTANCE, currentIdealState);
      for (final String instance : removedInstanceNames) {
        helixAdmin.enablePartition(true, helixClusterName, instance, V1Constants.Helix.BROKER_RESOURCE_INSTANCE,
            Arrays.asList(resourceName));
      }
      return null;
    }

    if (currentOnlineInstanceSet.size() < numBrokerInstancesToServeDataResource) {
      // Adding new instances
      int numInstancesToAdd = numBrokerInstancesToServeDataResource - currentOnlineInstanceSet.size();

      // Adding new instances.
      final List<String> selectedInstances =
          BrokerResourceAssignmentStrategy.getRandomAssignedInstances(helixAdmin, helixClusterName, brokerDataResource);

      for (final String instance : selectedInstances) {
        if (currentOnlineInstanceSet.contains(instance)) {
          continue;
        }
        currentIdealState.setPartitionState(resourceName, instance, ONLINE);
        numInstancesToAdd--;
        if (numInstancesToAdd == 0) {
          break;
        }
      }
    }
    return currentIdealState;
  }
}
