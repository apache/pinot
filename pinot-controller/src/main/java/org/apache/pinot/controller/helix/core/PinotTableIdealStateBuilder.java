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
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.PartitionGroupMetadataFetcher;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotTableIdealStateBuilder {
  private PinotTableIdealStateBuilder() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableIdealStateBuilder.class);
  private static final RetryPolicy DEFAULT_IDEALSTATE_UPDATE_RETRY_POLICY =
      RetryPolicies.randomDelayRetryPolicy(3, 100L, 200L);

  public static IdealState buildEmptyIdealStateFor(String tableNameWithType, int numReplicas,
      boolean enableBatchMessageMode) {
    CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(tableNameWithType);
    customModeIdealStateBuilder
        .setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(numReplicas).setMaxPartitionsPerNode(1);
    IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(tableNameWithType);
    idealState.setBatchMessageMode(enableBatchMessageMode);
    return idealState;
  }

  /**
   * Fetches the list of {@link PartitionGroupMetadata} for the new partition groups for the stream,
   * with the help of the {@link PartitionGroupConsumptionStatus} of the current partitionGroups.
   *
   * Reasons why <code>partitionGroupConsumptionStatusList</code> is needed:
   *
   * 1)
   * The current {@link PartitionGroupConsumptionStatus} is used to determine the offsets that have been consumed for
   * a partition group.
   * An example of where the offsets would be used:
   * e.g. If partition group 1 contains shardId 1, with status DONE and endOffset 150. There's 2 possibilities:
   * 1) the stream indicates that shardId's last offset is 200.
   * This tells Pinot that partition group 1 still has messages which haven't been consumed, and must be included in
   * the response.
   * 2) the stream indicates that shardId's last offset is 150,
   * This tells Pinot that all messages of partition group 1 have been consumed, and it need not be included in the
   * response.
   * Thus, this call will skip a partition group when it has reached end of life and all messages from that partition
   * group have been consumed.
   *
   * The current {@link PartitionGroupConsumptionStatus} is also used to know about existing groupings of partitions,
   * and accordingly make the new partition groups.
   * e.g. Assume that partition group 1 has status IN_PROGRESS and contains shards 0,1,2
   * and partition group 2 has status DONE and contains shards 3,4.
   * In the above example, the <code>partitionGroupConsumptionStatusList</code> indicates that
   * the collection of shards in partition group 1, should remain unchanged in the response,
   * whereas shards 3,4 can be added to new partition groups if needed.
   *
   * @param streamConfig the streamConfig from the tableConfig
   * @param partitionGroupConsumptionStatusList List of {@link PartitionGroupConsumptionStatus} for the current
   *                                            partition groups.
   *                                          The size of this list is equal to the number of partition groups,
   *                                          and is created using the latest segment zk metadata.
   */
  public static List<PartitionGroupMetadata> getPartitionGroupMetadataList(StreamConfig streamConfig,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatusList) {
    PartitionGroupMetadataFetcher partitionGroupMetadataFetcher =
        new PartitionGroupMetadataFetcher(streamConfig, partitionGroupConsumptionStatusList);
    try {
      DEFAULT_IDEALSTATE_UPDATE_RETRY_POLICY.attempt(partitionGroupMetadataFetcher);
      return partitionGroupMetadataFetcher.getPartitionGroupMetadataList();
    } catch (Exception e) {
      Exception fetcherException = partitionGroupMetadataFetcher.getException();
      LOGGER.error("Could not get PartitionGroupMetadata for topic: {} of table: {}", streamConfig.getTopicName(),
          streamConfig.getTableNameWithType(), fetcherException);
      throw new RuntimeException(fetcherException);
    }
  }
}
