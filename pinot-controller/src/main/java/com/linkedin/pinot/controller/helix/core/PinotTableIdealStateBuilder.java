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
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.json.JSONException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Pinot data server layer IdealState builder.
 *
 *
 */
public class PinotTableIdealStateBuilder {
  public static final String ONLINE = "ONLINE";
  public static final String OFFLINE = "OFFLINE";
  public static final String DROPPED = "DROPPED";
    private static Logger logger = Logger.getLogger(PinotTableIdealStateBuilder.class);

  public static final Map<String, SegmentAssignmentStrategy> SEGMENT_ASSIGNMENT_STRATEGY_MAP =
      new HashMap<String, SegmentAssignmentStrategy>();

  /**
   *
   * Building an empty idealState for a given table.
   * Used when creating a new table.
   *
   * @param tableName
   * @param helixAdmin
   * @param helixClusterName
   * @return
   */
  public static IdealState buildEmptyIdealStateFor(String tableName, int numCopies, HelixAdmin helixAdmin,
      String helixClusterName) {
//    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(tableName);
//    final int replicas = numCopies;
//    customModeIdealStateBuilder
//        .setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
//        .setNumPartitions(0).setNumReplica(replicas).setMaxPartitionsPerNode(1);
//    final IdealState idealState = customModeIdealStateBuilder.build();
//    idealState.setInstanceGroupTag(tableName);
      final UAutoModeISBuilder uAutoModeISBuilder = new UAutoModeISBuilder(tableName);
      final int replicas = numCopies;
      uAutoModeISBuilder.setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
              .setNumPartitions(0).setNumReplica(replicas).setMaxPartitionsPerNode(1);

      final IdealState idealState = uAutoModeISBuilder.build();
      idealState.setRebalancerClassName(UAutoRebalancer.class.getName());
    return idealState;
  }

  /**
   *
   * Building an empty idealState for a given table.
   * Used when creating a new table.
   *
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
   * @param tableName
   * @param segmentId
   * @param helixAdmin
   * @param helixClusterName
   * @return
   */
  public synchronized static IdealState dropSegmentFromIdealStateFor(String tableName, String segmentId,
      HelixAdmin helixAdmin, String helixClusterName) {

    final IdealState currentIdealState = helixAdmin.getResourceIdealState(helixClusterName, tableName);
    final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(segmentId);
    if (!currentInstanceSet.isEmpty() && currentIdealState.getPartitionSet().contains(segmentId)) {
      for (String instanceName : currentIdealState.getInstanceSet(segmentId)) {
        currentIdealState.setPartitionState(segmentId, instanceName, "DROPPED");
      }
    } else {
      throw new RuntimeException("Cannot found segmentId - " + segmentId + " in table - " + tableName);
    }
    return currentIdealState;
  }

  /**
   * Remove a segment is also required to recompute the ideal state.
   *
   * @param tableName
   * @param segmentId
   * @param helixAdmin
   * @param helixClusterName
   * @return
   */
  public synchronized static IdealState removeSegmentFromIdealStateFor(String tableName, String segmentId,
      HelixAdmin helixAdmin, String helixClusterName) {

    final IdealState currentIdealState = helixAdmin.getResourceIdealState(helixClusterName, tableName);
    if (currentIdealState != null && currentIdealState.getPartitionSet() != null
        && currentIdealState.getPartitionSet().contains(segmentId)) {
      currentIdealState.getPartitionSet().remove(segmentId);
    } else {
      throw new RuntimeException("Cannot found segmentId - " + segmentId + " in table - " + tableName);
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
        List<String> realtimeInstances = helixAdmin.getInstancesInClusterWithTag(helixClusterName, realtimeServerTenant);
        if (realtimeInstances.size() % Integer.parseInt(realtimeTableConfig.getValidationConfig().getReplication()) != 0) {
          throw new RuntimeException("Number of instance in current tenant should be an integer multiples of the number of replications");
        }
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

    String groupId = getGroupIdFromRealtimeDataTable(realtimeTableName, streamProviderConfig);
    for (int i = 0; i < numInstancesPerReplica * numDataReplicas; ++i) {
      String instance = instanceList.get(i);
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

  private static String getGroupIdFromRealtimeDataTable(String realtimeTableName,
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
        TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(segmentMetadata.getTableName());

    final String segmentName = segmentMetadata.getName();
    AbstractTableConfig offlineTableConfig = ZKMetadataProvider.getOfflineTableConfig(propertyStore, offlineTableName);

    if (!SEGMENT_ASSIGNMENT_STRATEGY_MAP.containsKey(offlineTableName)) {
      SEGMENT_ASSIGNMENT_STRATEGY_MAP.put(offlineTableName, SegmentAssignmentStrategyFactory
              .getSegmentAssignmentStrategy(offlineTableConfig.getValidationConfig().getSegmentAssignmentStrategy()));
    }
    final SegmentAssignmentStrategy segmentAssignmentStrategy = SEGMENT_ASSIGNMENT_STRATEGY_MAP.get(offlineTableName);
    logger.info("get current ideal state");
    //fixme
    final IdealState currentIdealState = helixAdmin.getResourceIdealState(helixClusterName, offlineTableName);
    final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(segmentName);
    if (currentInstanceSet.isEmpty()) {
      // Adding new Segments
        logger.info("current instance set is empty");
      final int replicas = Integer.parseInt(offlineTableConfig.getValidationConfig().getReplication());
      final List<String> selectedInstances =
          segmentAssignmentStrategy.getAssignedInstances(helixAdmin, helixClusterName, segmentMetadata, replicas,
              serverTenant);
        logger.info("addNewOfflineSegmentToIdealStateFor:"+selectedInstances.toString());
      for (final String instance : selectedInstances) {
        currentIdealState.setPartitionState(segmentName, instance, ONLINE);
      }
        currentIdealState.getRecord().setListField(segmentName,selectedInstances);
      currentIdealState.setNumPartitions(currentIdealState.getNumPartitions() + 1);
    } else {
      // Update new Segments
        logger.info("current instance set is not empty");
      for (final String instance : currentInstanceSet) {
        currentIdealState.setPartitionState(segmentName, instance, OFFLINE);
        currentIdealState.setPartitionState(segmentName, instance, ONLINE);
      }
    }
      logger.info("addNewOfflineSegmentToIdealStateFor :"+segmentName+"; Current ideal state is :"+currentIdealState.toString());
        return currentIdealState;
  }
}
