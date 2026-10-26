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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorCategory;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassification;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassifier;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorReporter;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadataFetcher;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadata;
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
  // Source failure WARN lines already logged in the last minute, by table and category
  private static final Cache<String, Boolean> RECENT_SOURCE_FAILURE_LOGS =
      CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();

  public static IdealState buildEmptyIdealStateFor(String tableNameWithType, int numReplicas) {
    CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(tableNameWithType);
    customModeIdealStateBuilder
        .setStateModel(PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(numReplicas).setMaxPartitionsPerNode(1);
    IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(tableNameWithType);
    return idealState;
  }

  /// Fetches the list of [StreamMetadata] for all streams of the table,
  /// with the help of the [PartitionGroupConsumptionStatus] of the current partitionGroups.
  /// In particular, this method can also be used to fetch from multiple stream topics.
  ///
  /// Reasons why `partitionGroupConsumptionStatusList` is needed:
  ///
  /// 1)
  /// The current [PartitionGroupConsumptionStatus] is used to determine the offsets that have been consumed for
  /// a partition group.
  /// An example of where the offsets would be used:
  /// e.g. If partition group 1 contains shardId 1, with status DONE and endOffset 150. There's 2 possibilities:
  /// 1) the stream indicates that shardId's last offset is 200.
  /// This tells Pinot that partition group 1 still has messages which haven't been consumed, and must be included in
  /// the response.
  /// 2) the stream indicates that shardId's last offset is 150,
  /// This tells Pinot that all messages of partition group 1 have been consumed, and it need not be included in the
  /// response.
  /// Thus, this call will skip a partition group when it has reached end of life and all messages from that partition
  /// group have been consumed.
  ///
  /// The current [PartitionGroupConsumptionStatus] is also used to know about existing groupings of partitions,
  /// and accordingly make the new partition groups.
  /// e.g. Assume that partition group 1 has status IN_PROGRESS and contains shards 0,1,2
  /// and partition group 2 has status DONE and contains shards 3,4.
  /// In the above example, the `partitionGroupConsumptionStatusList` indicates that
  /// the collection of shards in partition group 1, should remain unchanged in the response,
  /// whereas shards 3,4 can be added to new partition groups if needed.
  ///
  /// @param streamConfigs the List of streamConfig from the tableConfig
  /// @param partitionGroupConsumptionStatusList List of [PartitionGroupConsumptionStatus] for the current
  ///                                            partition groups.
  ///                                          The size of this list is equal to the number of partition groups,
  ///                                          and is created using the latest segment zk metadata.
  /// @param pausedTopicIndices List of inactive topic indices. Index is the index of the topic in the streamConfigMaps.
  /// @param forceGetOffsetFromStream - details in PinotLLCRealtimeSegmentManager.fetchPartitionGroupIdToSmallestOffset
  public static List<StreamMetadata> getStreamMetadataList(List<StreamConfig> streamConfigs,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatusList, List<Integer> pausedTopicIndices,
      boolean forceGetOffsetFromStream) {
    PartitionGroupMetadataFetcher partitionGroupMetadataFetcher = new PartitionGroupMetadataFetcher(
        streamConfigs, partitionGroupConsumptionStatusList, pausedTopicIndices, forceGetOffsetFromStream);
    String tableNameWithType = streamConfigs.get(0).getTableNameWithType();
    // A plugin that answers with a partial result reports the source failure it swallowed
    try (IngestionSourceErrorReporter.Scope ignored = IngestionSourceErrorReporter.listen(
        failure -> recordSourceFailure(tableNameWithType, failure))) {
      DEFAULT_IDEALSTATE_UPDATE_RETRY_POLICY.attempt(partitionGroupMetadataFetcher);
      for (Exception skipped : partitionGroupMetadataFetcher.getSkippedTopicExceptions()) {
        recordSourceFailure(tableNameWithType, skipped);
      }
      return partitionGroupMetadataFetcher.getStreamMetadataList();
    } catch (Exception e) {
      Exception fetcherException = partitionGroupMetadataFetcher.getException();
      LOGGER.error("Could not get StreamMetadata for topic: {} of table: {}",
          streamConfigs.stream().map(StreamConfig::getTopicName).collect(Collectors.joining(",")),
          tableNameWithType, fetcherException);
      ControllerMetrics controllerMetrics = ControllerMetrics.get();
      controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.PARTITION_GROUP_METADATA_FETCH_ERROR,
          1L);
      recordSourceFailure(tableNameWithType, fetcherException);
      throw new RuntimeException(fetcherException);
    }
  }

  /// Records a stream source failure the metadata fetch gave up on (it exhausted its retries, a topic was skipped as
  /// permanently failed, or the plugin reported a failure it swallows): the table's timestamp gauge for its category,
  /// and a WARN line rate-limited per table and category. The gauges are never cleared on success;
  /// [org.apache.pinot.controller.helix.SegmentStatusChecker] and table deletion remove them. Never throws, and the
  /// line carries no exception message.
  private static void recordSourceFailure(String tableNameWithType, @Nullable Throwable failure) {
    if (failure == null) {
      return;
    }
    try {
      IngestionSourceErrorClassification classification =
          IngestionSourceErrorClassifier.classify(failure).orElse(null);
      if (classification == null) {
        return;
      }
      IngestionSourceErrorCategory category = classification.getCategory();
      ControllerGauge gauge = category == IngestionSourceErrorCategory.AUTH
          ? ControllerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS
          : ControllerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS;
      ControllerMetrics.get().setValueOfTableGauge(tableNameWithType, gauge,
          TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
      if (RECENT_SOURCE_FAILURE_LOGS.asMap().putIfAbsent(tableNameWithType + "/" + category, Boolean.TRUE) == null) {
        LOGGER.warn("Ingestion source {} failure for table: {}, at: metadata_fetch: {}", category, tableNameWithType,
            classification);
      }
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to record an ingestion source failure for table: {}", tableNameWithType, e);
    }
  }
}
