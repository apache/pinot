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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
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
  // A failure in recording itself, such as a metrics registry that throws: logged from any thread, at most once a
  // minute for the process
  private static final long RECORDING_FAILURE_LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
  private static final AtomicLong LAST_RECORDING_FAILURE_LOG_MS = new AtomicLong();
  private static final RetryPolicy DEFAULT_IDEALSTATE_UPDATE_RETRY_POLICY =
      RetryPolicies.randomDelayRetryPolicy(3, 100L, 200L);
  // Source failure WARN lines already logged in the last minute, by table and category
  private static final Cache<String, Boolean> RECENT_SOURCE_FAILURE_LOGS =
      CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();
  // Tables this controller has written a source failure gauge for, so that one written after its table was deleted
  // can be found and removed. A table's write and its removal are atomic with each other, through this map.
  private static final Map<String, Boolean> TABLES_WITH_SOURCE_FAILURES = new ConcurrentHashMap<>();
  private static final List<ControllerGauge> SOURCE_FAILURE_GAUGES =
      List.of(ControllerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS,
          ControllerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS);

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
    // Failures swallowed so that the fetch can proceed (a topic skipped as permanently failed, a plugin answering with
    // a partial result) are reported as they happen
    try (IngestionSourceErrorReporter.Scope ignored = IngestionSourceErrorReporter.listen(
        failure -> recordSourceFailure(tableNameWithType, failure))) {
      DEFAULT_IDEALSTATE_UPDATE_RETRY_POLICY.attempt(partitionGroupMetadataFetcher);
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

  /// Removes a table's source failure gauges.
  public static void removeSourceFailureGauges(String tableNameWithType, ControllerMetrics controllerMetrics) {
    for (ControllerGauge gauge : SOURCE_FAILURE_GAUGES) {
      controllerMetrics.removeTableGauge(tableNameWithType, gauge);
    }
  }

  /// Removes the source failure gauges this controller wrote for tables that no longer exist. A metadata fetch still
  /// running when its table is deleted, on this controller or another, writes its gauge after the deletion removed it.
  public static void removeSourceFailureGaugesOfDeletedTables(Predicate<String> tableExists,
      ControllerMetrics controllerMetrics) {
    for (String tableNameWithType : TABLES_WITH_SOURCE_FAILURES.keySet()) {
      // Checked and removed atomically with the table's writes, so a table created again meanwhile keeps its gauge
      TABLES_WITH_SOURCE_FAILURES.computeIfPresent(tableNameWithType, (table, remembered) -> {
        if (tableExists.test(table)) {
          return remembered;
        }
        removeSourceFailureGauges(table, controllerMetrics);
        return null;
      });
    }
  }

  /// Records a stream source failure the metadata fetch gave up on (it exhausted its retries, a topic was skipped as
  /// permanently failed, or the plugin reported a failure it swallows): the table's timestamp gauge for its category,
  /// and a WARN line rate-limited per table and category. The gauges are never cleared on success;
  /// [org.apache.pinot.controller.helix.SegmentStatusChecker] and table deletion remove them. Never throws, and the
  /// line carries no exception message.
  @VisibleForTesting
  static void recordSourceFailure(String tableNameWithType, @Nullable Throwable failure) {
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
      TABLES_WITH_SOURCE_FAILURES.compute(tableNameWithType, (table, remembered) -> {
        ControllerMetrics.get().setValueOfTableGauge(table, gauge,
            TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
        return Boolean.TRUE;
      });
      if (RECENT_SOURCE_FAILURE_LOGS.asMap().putIfAbsent(tableNameWithType + "/" + category, Boolean.TRUE) == null) {
        LOGGER.warn("Ingestion source {} failure for table: {}, at: metadata_fetch: {}", category, tableNameWithType,
            classification);
      }
    } catch (RuntimeException e) {
      long nowMs = System.currentTimeMillis();
      long lastLoggedMs = LAST_RECORDING_FAILURE_LOG_MS.get();
      if (nowMs - lastLoggedMs >= RECORDING_FAILURE_LOG_INTERVAL_MS
          && LAST_RECORDING_FAILURE_LOG_MS.compareAndSet(lastLoggedMs, nowMs)) {
        LOGGER.warn("Failed to record an ingestion source failure for table: {}", tableNameWithType, e);
      }
    }
  }
}
