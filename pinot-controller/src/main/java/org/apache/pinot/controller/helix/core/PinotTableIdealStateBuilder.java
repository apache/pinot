/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core;

import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.PartitionCountFetcher;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
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
  public static IdealState buildEmptyIdealStateFor(String tableName, int numCopies, boolean enableBatchMessageMode) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(tableName);
    final int replicas = numCopies;
    customModeIdealStateBuilder
        .setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(replicas).setMaxPartitionsPerNode(1);
    final IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(tableName);
    idealState.setBatchMessageMode(enableBatchMessageMode);
    return idealState;
  }

  public static IdealState addNewRealtimeSegmentToIdealState(String segmentId, IdealState state, String instanceName) {
    state.setPartitionState(segmentId, instanceName, ONLINE);
    state.setNumPartitions(state.getNumPartitions() + 1);
    return state;
  }

  public static IdealState buildInitialHighLevelRealtimeIdealStateFor(String realtimeTableName,
      TableConfig realtimeTableConfig, HelixManager helixManager, ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore,
      boolean enableBatchMessageMode) {
    List<String> realtimeInstances = HelixHelper.getInstancesWithTag(helixManager,
        TagNameUtils.extractConsumingServerTag(realtimeTableConfig.getTenantConfig()));
    IdealState idealState = buildEmptyRealtimeIdealStateFor(realtimeTableName, 1, enableBatchMessageMode);
    if (realtimeInstances.size() % Integer.parseInt(realtimeTableConfig.getValidationConfig().getReplication()) != 0) {
      throw new RuntimeException(
          "Number of instance in current tenant should be an integer multiples of the number of replications");
    }
    setupInstanceConfigForHighLevelConsumer(realtimeTableName, realtimeInstances.size(),
        Integer.parseInt(realtimeTableConfig.getValidationConfig().getReplication()),
        realtimeTableConfig.getIndexingConfig().getStreamConfigs(), zkHelixPropertyStore, realtimeInstances);
    return idealState;
  }

  public static void buildLowLevelRealtimeIdealStateFor(PinotLLCRealtimeSegmentManager pinotLLCRealtimeSegmentManager,
      String realtimeTableName, TableConfig realtimeTableConfig, IdealState idealState,
      boolean enableBatchMessageMode) {

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
      idealState = buildEmptyRealtimeIdealStateFor(realtimeTableName, nReplicas, enableBatchMessageMode);
    }
    pinotLLCRealtimeSegmentManager.setUpNewTable(realtimeTableConfig, idealState);
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

  public static IdealState buildEmptyRealtimeIdealStateFor(String realtimeTableName, int replicaCount,
      boolean enableBatchMessageMode) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(realtimeTableName);
    customModeIdealStateBuilder
        .setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(replicaCount).setMaxPartitionsPerNode(1);
    final IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(realtimeTableName);
    idealState.setBatchMessageMode(enableBatchMessageMode);

    return idealState;
  }

  private static void setupInstanceConfigForHighLevelConsumer(String realtimeTableName, int numDataInstances,
      int numDataReplicas, Map<String, String> streamConfig, ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore,
      List<String> instanceList) {
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

  private static String getGroupIdFromRealtimeDataTable(String realtimeTableName, Map<String, String> streamConfigMap) {
    String groupId = StringUtil.join("_", realtimeTableName, System.currentTimeMillis() + "");
    StreamConfig streamConfig = new StreamConfig(realtimeTableName, streamConfigMap);
    String streamConfigGroupId = streamConfig.getGroupId();
    if (streamConfigGroupId != null && !streamConfigGroupId.isEmpty()) {
      groupId = streamConfigGroupId;
    }
    return groupId;
  }
}
