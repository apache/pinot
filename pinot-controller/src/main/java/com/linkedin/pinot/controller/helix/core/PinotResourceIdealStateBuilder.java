/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.json.JSONException;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.controller.helix.core.sharding.SegmentAssignmentStrategy;
import com.linkedin.pinot.controller.helix.core.sharding.SegmentAssignmentStrategyFactory;


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
  public static IdealState buildEmptyIdealStateFor(String resourceName, int numCopies, HelixAdmin helixAdmin,
      String helixClusterName) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(resourceName);
    final int replicas = numCopies;
    customModeIdealStateBuilder
        .setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(replicas).setMaxPartitionsPerNode(1);
    final IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(resourceName);
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
        new CustomModeISBuilder(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    customModeIdealStateBuilder
        .setStateModel(
            PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL)
        .setMaxPartitionsPerNode(Integer.MAX_VALUE).setNumReplica(Integer.MAX_VALUE)
        .setNumPartitions(Integer.MAX_VALUE);
    final IdealState idealState = customModeIdealStateBuilder.build();
    return idealState;
  }

  public static IdealState addNewRealtimeSegmentToIdealState(String segmentId, IdealState state, String instanceName) {
    state.setPartitionState(segmentId, instanceName, ONLINE);
    state.setNumPartitions(state.getNumPartitions() + 1);
    return state;
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
  public synchronized static IdealState dropSegmentFromIdealStateFor(String resourceName, String segmentId,
      HelixAdmin helixAdmin, String helixClusterName) {

    final IdealState currentIdealState = helixAdmin.getResourceIdealState(helixClusterName, resourceName);
    final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(segmentId);
    if (!currentInstanceSet.isEmpty() && currentIdealState.getPartitionSet().contains(segmentId)) {
      for (String instanceName : currentIdealState.getInstanceSet(segmentId)) {
        currentIdealState.setPartitionState(segmentId, instanceName, "DROPPED");
      }
    } else {
      throw new RuntimeException("Cannot found segmentId - " + segmentId + " in resource - " + resourceName);
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
    if (currentIdealState != null && currentIdealState.getPartitionSet() != null
        && currentIdealState.getPartitionSet().contains(segmentId)) {
      currentIdealState.getPartitionSet().remove(segmentId);
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
        helixAdmin.getResourceIdealState(helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(brokerResourceName);
    if (!currentInstanceSet.isEmpty() && currentIdealState.getPartitionSet().contains(brokerResourceName)) {
      currentIdealState.getPartitionSet().remove(brokerResourceName);
    } else {
      throw new RuntimeException("Cannot found broker resource - " + brokerResourceName + " in broker resource ");
    }
    return currentIdealState;
  }

  public static IdealState updateExpandedDataResourceIdealStateFor(String resourceName, int numCopies,
      HelixAdmin helixAdmin, String helixClusterName) {
    IdealState idealState = helixAdmin.getResourceIdealState(helixClusterName, resourceName);
    // Increase number of replicas
    if (Integer.parseInt(idealState.getReplicas()) < numCopies) {
      Random randomSeed = new Random(System.currentTimeMillis());
      int currentReplicas = Integer.parseInt(idealState.getReplicas());
      idealState.setReplicas(numCopies + "");
      Set<String> segmentSet = idealState.getPartitionSet();
      List<String> instanceList = helixAdmin.getInstancesInClusterWithTag(helixClusterName, resourceName);
      for (String segmentName : segmentSet) {
        // TODO(xiafu) : current just random assign one more replica.
        // In future, has to implement read segmentMeta from PropertyStore then use segmentAssignmentStrategy to assign.
        Set<String> selectedInstanceSet = idealState.getInstanceSet(segmentName);
        int numInstancesToAssign = numCopies - currentReplicas;
        int numInstancesAvailable = instanceList.size() - selectedInstanceSet.size();
        for (String instance : instanceList) {
          if (selectedInstanceSet.contains(instance)) {
            continue;
          }
          if (randomSeed.nextInt(numInstancesAvailable) < numInstancesToAssign) {
            idealState.setPartitionState(segmentName, instance,
                PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);
            numInstancesToAssign--;
          }
          if (numInstancesToAssign == 0) {
            break;
          }
          numInstancesAvailable--;
        }
      }

      return idealState;
    }
    // Decrease number of replicas
    if (Integer.parseInt(idealState.getReplicas()) > numCopies) {
      int replicas = numCopies;
      int currentReplicas = Integer.parseInt(idealState.getReplicas());
      idealState.setReplicas(replicas + "");
      Set<String> segmentSet = idealState.getPartitionSet();

      for (String segmentName : segmentSet) {
        Set<String> instanceSet = idealState.getInstanceSet(segmentName);
        int cnt = 1;
        for (final String instance : instanceSet) {
          idealState.setPartitionState(segmentName, instance,
              PinotHelixSegmentOnlineOfflineStateModelGenerator.DROPPED_STATE);
          if (cnt++ > (currentReplicas - replicas)) {
            break;
          }
        }
      }

      return idealState;
    }
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
  public static IdealState updateExistedSegmentToIdealStateFor(SegmentMetadata segmentMetadata, HelixAdmin helixAdmin,
      String helixClusterName) {

    final String resourceName = segmentMetadata.getResourceName();
    final String segmentName = segmentMetadata.getName();

    final IdealState currentIdealState = helixAdmin.getResourceIdealState(helixClusterName, resourceName);
    final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(segmentName);
    for (final String instance : currentInstanceSet) {
      currentIdealState.setPartitionState(segmentName, instance, OFFLINE);
    }
    helixAdmin.setResourceIdealState(helixClusterName, resourceName, currentIdealState);
    for (final String instance : currentInstanceSet) {
      currentIdealState.setPartitionState(segmentName, instance, ONLINE);
    }
    return currentIdealState;
  }

  public static IdealState buildInitialRealtimeIdealStateFor(String realtimeTableName,
      AbstractTableConfig realtimeTableConfig, HelixAdmin helixAdmin, String helixClusterName,
      ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore) {
    KafkaStreamMetadata kafkaStreamMetadata =
        new KafkaStreamMetadata(realtimeTableConfig.getIndexingConfig().getStreamConfigs());
    String realtimeServerTenant =
        ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(realtimeTableConfig.getTenantConfig().getServer());
    switch (kafkaStreamMetadata.getConsumerType()) {
      case highLevel:
        IdealState idealState =
            buildInitialKafkaHighLevelConsumerRealtimeIdealStateFor(realtimeTableName, helixAdmin, helixClusterName,
                zkHelixPropertyStore);
        List<String> realtimeInstances =
            helixAdmin.getInstancesInClusterWithTag(helixClusterName, realtimeServerTenant);
        setupInstanceConfigForKafkaHighLevelConsumer(realtimeTableName, realtimeInstances.size(),
            Integer.parseInt(realtimeTableConfig.getValidationConfig().getReplication()), realtimeTableConfig
                .getIndexingConfig().getStreamConfigs(), zkHelixPropertyStore, realtimeInstances);
        return idealState;
      case simple:
      default:
        throw new UnsupportedOperationException("Not support kafka consumer type: "
            + kafkaStreamMetadata.getConsumerType());
    }
  }

  public static IdealState buildInitialKafkaHighLevelConsumerRealtimeIdealStateFor(String realtimeTableName,
      HelixAdmin helixAdmin, String helixClusterName, ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(realtimeTableName);
    customModeIdealStateBuilder
        .setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(1).setMaxPartitionsPerNode(1);
    final IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(realtimeTableName);

    return idealState;
  }

  private static void setupInstanceConfigForKafkaHighLevelConsumer(String realtimeTableName, int numDataInstances,
      int numDataReplicas, Map<String, String> streamProviderConfig,
      ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore, List<String> instanceList) {
    int numInstancesPerReplica = numDataInstances / numDataReplicas;
    int partitionId = 0;
    int replicaId = 0;

    String groupId = getGroupIdFromRealtimeDataResource(realtimeTableName, streamProviderConfig);
    for (String instance : instanceList) {
      InstanceZKMetadata instanceZKMetadata = ZKMetadataProvider.getInstanceZKMetadata(zkHelixPropertyStore, instance);
      if (instanceZKMetadata == null) {
        instanceZKMetadata = new InstanceZKMetadata();
        String[] instanceConfigs = instance.split("_");
        assert (instanceConfigs.length == 3);
        instanceZKMetadata.setInstanceType(instanceConfigs[0]);
        instanceZKMetadata.setInstanceName(instanceConfigs[1]);
        instanceZKMetadata.setInstancePort(Integer.parseInt(instanceConfigs[2]));
      }
      instanceZKMetadata.setGroupId(realtimeTableName, groupId + "_" + replicaId);
      instanceZKMetadata.setPartition(realtimeTableName, Integer.toString(partitionId));
      partitionId = (partitionId + 1) % numInstancesPerReplica;
      if (partitionId == 0) {
        replicaId++;
      }
      ZKMetadataProvider.setInstanceZKMetadata(zkHelixPropertyStore, instanceZKMetadata);
    }
  }

  private static String getGroupIdFromRealtimeDataResource(String realtimeTableName,
      Map<String, String> streamProviderConfig) {
    String keyOfGroupId =
        StringUtil
            .join(".", Helix.DataSource.STREAM_PREFIX, Helix.DataSource.Realtime.Kafka.HighLevelConsumer.GROUP_ID);
    String groupId = StringUtil.join("_", realtimeTableName, System.currentTimeMillis() + "");
    if (streamProviderConfig.containsKey(keyOfGroupId) && !streamProviderConfig.get(keyOfGroupId).isEmpty()) {
      groupId = streamProviderConfig.get(keyOfGroupId);
    }
    return groupId;
  }

  public static IdealState addNewOfflineSegmentToIdealStateFor(SegmentMetadata segmentMetadata,
      HelixAdmin helixAdmin, String helixClusterName, ZkHelixPropertyStore<ZNRecord> propertyStore, String serverTenant)
      throws JsonParseException, JsonMappingException, JsonProcessingException, JSONException, IOException {

    final String offlineTableName =
        TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(segmentMetadata.getResourceName());

    final String segmentName = segmentMetadata.getName();
    AbstractTableConfig offlineTableConfig = ZKMetadataProvider.getOfflineTableConfig(propertyStore, offlineTableName);

    if (!SEGMENT_ASSIGNMENT_STRATEGY_MAP.containsKey(offlineTableName)) {
      SEGMENT_ASSIGNMENT_STRATEGY_MAP.put(offlineTableName, SegmentAssignmentStrategyFactory
          .getSegmentAssignmentStrategy(offlineTableConfig.getValidationConfig().getSegmentAssignmentStrategy()));
    }
    final SegmentAssignmentStrategy segmentAssignmentStrategy = SEGMENT_ASSIGNMENT_STRATEGY_MAP.get(offlineTableName);

    final IdealState currentIdealState = helixAdmin.getResourceIdealState(helixClusterName, offlineTableName);
    final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(segmentName);
    if (currentInstanceSet.isEmpty()) {
      // Adding new Segments
      final int replicas = Integer.parseInt(offlineTableConfig.getValidationConfig().getReplication());
      final List<String> selectedInstances =
          segmentAssignmentStrategy.getAssignedInstances(helixAdmin, helixClusterName, segmentMetadata, replicas,
              serverTenant);
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
}
