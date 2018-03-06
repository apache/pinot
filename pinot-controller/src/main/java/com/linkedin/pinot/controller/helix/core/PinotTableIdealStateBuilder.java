/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.config.RealtimeTagConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import com.linkedin.pinot.core.realtime.stream.StreamMetadata;
import com.linkedin.pinot.core.realtime.stream.StreamMetadataFactory;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Pinot data server layer IdealState builder.
 *
 *
 */
public class PinotTableIdealStateBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableIdealStateBuilder.class);

  /**
   *
   * Building an empty idealState for a given table.
   * Used when creating a new table.
   * @return ideal state
   */
  public static IdealState buildEmptyIdealStateForBrokerResource() {
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

  /**
   * Build an empty ideal state for a table
   * @param realtimeTableName name of the table for which to build an empty ideal state
   * @param numReplicas number of replicas
   * @return
   */
  public static IdealState buildEmptyIdealStateFor(String realtimeTableName, int numReplicas) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(realtimeTableName);
    customModeIdealStateBuilder
        .setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(numReplicas).setMaxPartitionsPerNode(1);
    final IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(realtimeTableName);
    return idealState;
  }

  /**
   * Add a segment to an ideal state
   * @param segmentId
   * @param state
   * @param instanceName
   * @return ideal state with new segment
   */
  public static IdealState addNewRealtimeSegmentToIdealState(String segmentId, IdealState state, String instanceName) {
    state.setPartitionState(segmentId, instanceName, PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);
    state.setNumPartitions(state.getNumPartitions() + 1);
    return state;
  }


  public static IdealState buildInitialHighLevelRealtimeIdealStateFor(String realtimeTableName,
      TableConfig realtimeTableConfig, HelixAdmin helixAdmin, String helixClusterName,
      ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore) {
    String realtimeServerTenant =
        ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(realtimeTableConfig.getTenantConfig().getServer());
    final List<String> realtimeInstances = helixAdmin.getInstancesInClusterWithTag(helixClusterName,
        realtimeServerTenant);
    IdealState idealState = buildEmptyIdealStateFor(realtimeTableName, 1);
    if (realtimeInstances.size() % Integer.parseInt(realtimeTableConfig.getValidationConfig().getReplication()) != 0) {
      throw new RuntimeException(
          "Number of instance in current tenant should be an integer multiples of the number of replications");
    }
    setupInstanceConfigForKafkaHighLevelConsumer(realtimeTableName, realtimeInstances.size(),
        Integer.parseInt(realtimeTableConfig.getValidationConfig().getReplication()),
        realtimeTableConfig.getIndexingConfig().getStreamConfigs(), zkHelixPropertyStore, realtimeInstances);
    return idealState;
  }

  public static void buildLowLevelRealtimeIdealStateFor(String realtimeTableName, TableConfig realtimeTableConfig,
      HelixAdmin helixAdmin, String helixClusterName, HelixManager helixManager, IdealState idealState) {

    boolean create = false;
    // Validate replicasPerPartition here.
    final String replicasPerPartitionStr = realtimeTableConfig.getValidationConfig().getReplicasPerPartition();
    if (replicasPerPartitionStr == null || replicasPerPartitionStr.isEmpty()) {
      throw new RuntimeException("Null or empty value for replicasPerPartition, expected a number");
    }
    final int nReplicas;
    try {
      nReplicas = Integer.valueOf(replicasPerPartitionStr);
    } catch (NumberFormatException e) {
      throw new PinotHelixResourceManager.InvalidTableConfigException(
          "Invalid value for replicasPerPartition, expected a number: " + replicasPerPartitionStr, e);
    }
    if (idealState == null) {
      idealState = buildEmptyIdealStateFor(realtimeTableName, nReplicas);
      create = true;
    }
    LOGGER.info("Assigning partitions to instances for simple consumer for table {}", realtimeTableName);
    final StreamMetadata streamMetadata = StreamMetadataFactory.getStreamMetadata(realtimeTableConfig);
    final PinotLLCRealtimeSegmentManager segmentManager = PinotLLCRealtimeSegmentManager.getInstance();
    final int nPartitions = streamMetadata.getPartitionCount();
    LOGGER.info("Assigning {} partitions to instances for simple consumer for table {}", nPartitions, realtimeTableName);

    RealtimeTagConfig realtimeTagConfig = new RealtimeTagConfig(realtimeTableConfig, helixManager);
    final List<String> realtimeInstances = helixAdmin.getInstancesInClusterWithTag(helixClusterName,
        realtimeTagConfig.getConsumingServerTag());
    segmentManager.setupHelixEntries(realtimeTagConfig, streamMetadata, nPartitions, realtimeInstances, idealState, create);
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


}
