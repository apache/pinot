/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import com.linkedin.pinot.core.realtime.stream.PartitionCountFetcher;
import com.linkedin.pinot.core.realtime.stream.StreamConfig;
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
  public static final String ONLINE = "ONLINE";
  public static final String OFFLINE = "OFFLINE";

  /**
   *
   * Building an empty idealState for a given table.
   * Used when creating a new table.
   *
   * @param tableName resource name
   * @param numCopies is the number of replicas
   * @return
   */
  public static IdealState buildEmptyIdealStateFor(String tableName, int numCopies) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(tableName);
    final int replicas = numCopies;
    customModeIdealStateBuilder
        .setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(replicas).setMaxPartitionsPerNode(1);
    final IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(tableName);
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

  public static IdealState buildInitialHighLevelRealtimeIdealStateFor(String realtimeTableName,
      TableConfig realtimeTableConfig, HelixManager helixManager, ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore) {
    RealtimeTagConfig realtimeTagConfig = new RealtimeTagConfig(realtimeTableConfig);
    final List<String> realtimeInstances =
        HelixHelper.getInstancesWithTag(helixManager, realtimeTagConfig.getConsumingServerTag());
    IdealState idealState = buildEmptyRealtimeIdealStateFor(realtimeTableName, 1);
    if (realtimeInstances.size() % Integer.parseInt(realtimeTableConfig.getValidationConfig().getReplication()) != 0) {
      throw new RuntimeException(
          "Number of instance in current tenant should be an integer multiples of the number of replications");
    }
    setupInstanceConfigForHighLevelConsumer(realtimeTableName, realtimeInstances.size(),
        Integer.parseInt(realtimeTableConfig.getValidationConfig().getReplication()),
        realtimeTableConfig.getIndexingConfig().getStreamConfigs(), zkHelixPropertyStore, realtimeInstances);
    return idealState;
  }

  public static void buildLowLevelRealtimeIdealStateFor(String realtimeTableName, TableConfig realtimeTableConfig,
      IdealState idealState) {

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
      idealState = buildEmptyRealtimeIdealStateFor(realtimeTableName, nReplicas);
    }
    final PinotLLCRealtimeSegmentManager segmentManager = PinotLLCRealtimeSegmentManager.getInstance();
    try {
      segmentManager.setupNewTable(realtimeTableConfig, idealState);
    } catch (InvalidConfigException e) {
      throw new IllegalStateException("Caught exception when creating table " + realtimeTableName, e);
    }
  }

  public static int getPartitionCount(StreamConfig streamConfig) {
    PartitionCountFetcher partitionCountFetcher = new PartitionCountFetcher(streamConfig);
    try {
      RetryPolicies.noDelayRetryPolicy(3).attempt(partitionCountFetcher);
      return partitionCountFetcher.getPartitionCount();
    } catch (Exception e) {
      Exception fetcherException = partitionCountFetcher.getException();
      LOGGER.error("Could not get partition count for {}", streamConfig.getTopicName(), fetcherException);
      throw new RuntimeException(fetcherException);
    }
  }

  public static IdealState buildEmptyRealtimeIdealStateFor(String realtimeTableName, int replicaCount) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(realtimeTableName);
    customModeIdealStateBuilder
        .setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(replicaCount).setMaxPartitionsPerNode(1);
    final IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(realtimeTableName);

    return idealState;
  }

  private static void setupInstanceConfigForHighLevelConsumer(String realtimeTableName, int numDataInstances,
      int numDataReplicas, Map<String, String> streamConfig,
      ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore, List<String> instanceList) {
    int numInstancesPerReplica = numDataInstances / numDataReplicas;
    int partitionId = 0;
    int replicaId = 0;

    String groupId = getGroupIdFromRealtimeDataTable(realtimeTableName, streamConfig);
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
      Map<String, String> streamConfigMap) {
    String groupId = StringUtil.join("_", realtimeTableName, System.currentTimeMillis() + "");
    StreamConfig streamConfig = new StreamConfig(streamConfigMap);
    String streamConfigGroupId = streamConfig.getGroupId();
    if (streamConfigGroupId != null && !streamConfigGroupId.isEmpty()) {
      groupId = streamConfigGroupId;
    }
    return groupId;
  }
}
