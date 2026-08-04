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
package org.apache.pinot.plugin.stream.kafka30;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.pinot.plugin.stream.kafka.KafkaConsumerPartitionLag;
import org.apache.pinot.plugin.stream.kafka.KafkaPartitionSubsetUtils;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.stream.PermanentConsumerException;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.TransientConsumerException;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaStreamMetadataProvider extends KafkaPartitionLevelConnectionHandler
    implements StreamMetadataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamMetadataProvider.class);
  /// Immutable partition ID subset from table config. Read once at construction; does not change during the
  /// provider's lifetime. Empty when no subset is configured (consume all partitions).
  private final List<Integer> _partitionIdSubset;

  public KafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    this(clientId, streamConfig, Integer.MIN_VALUE);
  }

  public KafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition);
    List<Integer> subset =
        KafkaPartitionSubsetUtils.getPartitionIdsFromConfig(_config.getStreamConfigMap());
    if (subset != null) {
      _partitionIdSubset = Collections.unmodifiableList(subset);
      validatePartitionIds(_partitionIdSubset);
    } else {
      _partitionIdSubset = List.of();
    }
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    try {
      List<PartitionInfo> partitionInfos = fetchPartitionInfos(timeoutMillis);
      if (CollectionUtils.isNotEmpty(partitionInfos)) {
        return partitionInfos.size();
      }
      throw new TransientConsumerException(new RuntimeException(
          "Failed to fetch partition information for topic: " + _topic));
    } catch (TimeoutException e) {
      throw new TransientConsumerException(e);
    }
  }

  @Override
  public Set<Integer> fetchPartitionIds(long timeoutMillis) {
    try {
      List<PartitionInfo> partitionInfos = fetchPartitionInfos(timeoutMillis);
      if (CollectionUtils.isEmpty(partitionInfos)) {
        throw new TransientConsumerException(new RuntimeException(
            "Failed to fetch partition information for topic: " + _topic));
      }
      Set<Integer> partitionIds = Sets.newHashSetWithExpectedSize(partitionInfos.size());
      for (PartitionInfo partitionInfo : partitionInfos) {
        partitionIds.add(partitionInfo.partition());
      }
      return partitionIds;
    } catch (TimeoutException e) {
      throw new TransientConsumerException(e);
    }
  }

  @Override
  public List<PartitionGroupMetadata> computePartitionGroupMetadata(String clientId, StreamConfig streamConfig,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatuses, int timeoutMillis)
      throws IOException, java.util.concurrent.TimeoutException {
    Map<Integer, StreamPartitionMsgOffset> partitionIdToEndOffset =
        new HashMap<>(partitionGroupConsumptionStatuses.size());
    for (PartitionGroupConsumptionStatus s : partitionGroupConsumptionStatuses) {
      partitionIdToEndOffset.put(s.getStreamPartitionGroupId(), s.getEndOffset());
    }

    List<Integer> partitionIds;
    if (_partitionIdSubset.isEmpty()) {
      int partitionCount = fetchPartitionCount(timeoutMillis);
      partitionIds = new ArrayList<>(partitionCount);
      for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
        partitionIds.add(partitionId);
      }
    } else {
      partitionIds = _partitionIdSubset;
    }

    // Partitions already covered by a consumption status reuse its offset; the remaining partitions have their
    // offsets fetched from the stream in a single batched call. Kafka's beginningOffsets/endOffsets/offsetsForTimes
    // accept a collection of partitions and resolve them in one broker round-trip, so we avoid creating a fresh
    // consumer per partition (previously hundreds of serial ~1s consumer creations on high-partition tables, all
    // executed inside the controller's ideal-state update lock).
    List<Integer> partitionIdsToFetch = new ArrayList<>(partitionIds.size());
    for (Integer partitionId : partitionIds) {
      if (!partitionIdToEndOffset.containsKey(partitionId)) {
        partitionIdsToFetch.add(partitionId);
      }
    }
    Map<Integer, StreamPartitionMsgOffset> fetchedOffsets =
        fetchOffsetsForPartitions(partitionIdsToFetch, streamConfig.getOffsetCriteria(), timeoutMillis);

    List<PartitionGroupMetadata> result = new ArrayList<>(partitionIds.size());
    for (Integer partitionId : partitionIds) {
      // fetchOffsetsForPartitions returns an offset for every requested partition (or throws), so the lookup below
      // is non-null for the partitions that were fetched.
      StreamPartitionMsgOffset startOffset = partitionIdToEndOffset.containsKey(partitionId)
          ? partitionIdToEndOffset.get(partitionId) : fetchedOffsets.get(partitionId);
      result.add(new PartitionGroupMetadata(partitionId, startOffset));
    }
    return result;
  }

  @Override
  public Map<Integer, StreamPartitionMsgOffset> fetchLatestStreamOffset(Set<Integer> partitionIds, long timeoutMillis) {
    List<TopicPartition> topicPartitions = new ArrayList<>(partitionIds.size());
    for (Integer streamPartition: partitionIds) {
      topicPartitions.add(new TopicPartition(_topic, streamPartition));
    }
    try {
      Map<TopicPartition, Long> topicPartitionToLatestOffsetMap =
          _consumer.endOffsets(topicPartitions, Duration.ofMillis(timeoutMillis));

      Map<Integer, StreamPartitionMsgOffset> partitionIdToLatestOffset =
          new HashMap<>(topicPartitionToLatestOffsetMap.size());
      for (Map.Entry<TopicPartition, Long> entry : topicPartitionToLatestOffsetMap.entrySet()) {
        partitionIdToLatestOffset.put(entry.getKey().partition(), new LongMsgOffset(entry.getValue()));
      }

      return partitionIdToLatestOffset;
    } catch (TimeoutException e) {
      throw new TransientConsumerException(e);
    }
  }

  @Override
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis) {
    Preconditions.checkNotNull(offsetCriteria);
    // fetchOffsetsForPartitions throws if the stream returns no offset for _partition, so the result is non-null.
    return fetchOffsetsForPartitions(List.of(_partition), offsetCriteria, timeoutMillis).get(_partition);
  }

  /// Fetches the offset matching `offsetCriteria` for the given partitions in a single batched call to the stream.
  /// Kafka's `beginningOffsets`/`endOffsets`/`offsetsForTimes` all accept a collection of partitions, so this issues
  /// one broker round-trip regardless of the number of partitions (these calls do not require the consumer to be
  /// assigned to the partitions).
  ///
  /// @return an offset for every requested partition
  /// @throws TransientConsumerException if the stream returns no offset for a requested partition (so the caller
  ///         retries rather than treating the partition as absent)
  private Map<Integer, StreamPartitionMsgOffset> fetchOffsetsForPartitions(Collection<Integer> partitionIds,
      OffsetCriteria offsetCriteria, long timeoutMillis) {
    Preconditions.checkNotNull(offsetCriteria);
    if (partitionIds.isEmpty()) {
      return Map.of();
    }
    List<TopicPartition> topicPartitions = new ArrayList<>(partitionIds.size());
    for (Integer partitionId : partitionIds) {
      topicPartitions.add(new TopicPartition(_topic, partitionId));
    }
    Duration timeout = Duration.ofMillis(timeoutMillis);
    try {
      Map<TopicPartition, Long> topicPartitionToOffset;
      if (offsetCriteria.isLargest()) {
        topicPartitionToOffset = _consumer.endOffsets(topicPartitions, timeout);
      } else if (offsetCriteria.isSmallest()) {
        topicPartitionToOffset = _consumer.beginningOffsets(topicPartitions, timeout);
      } else if (offsetCriteria.isPeriod() || offsetCriteria.isTimestamp()) {
        long timestampMillis = offsetCriteria.isPeriod()
            ? Clock.systemUTC().millis() - TimeUtils.convertPeriodToMillis(offsetCriteria.getOffsetString())
            : TimeUtils.convertTimestampToMillis(offsetCriteria.getOffsetString());
        Map<TopicPartition, Long> timestampToSearch = new HashMap<>(topicPartitions.size());
        for (TopicPartition topicPartition : topicPartitions) {
          timestampToSearch.put(topicPartition, timestampMillis);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = _consumer.offsetsForTimes(timestampToSearch, timeout);
        topicPartitionToOffset = new HashMap<>(topicPartitions.size());
        // Partitions with no message at/after the requested time return a null offset; fall back to their end
        // offset in a single batched call, preserving the per-partition fallback behavior.
        List<TopicPartition> fallbackPartitions = new ArrayList<>();
        for (TopicPartition topicPartition : topicPartitions) {
          OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(topicPartition);
          if (offsetAndTimestamp != null) {
            topicPartitionToOffset.put(topicPartition, offsetAndTimestamp.offset());
          } else {
            fallbackPartitions.add(topicPartition);
          }
        }
        if (!fallbackPartitions.isEmpty()) {
          topicPartitionToOffset.putAll(_consumer.endOffsets(fallbackPartitions, timeout));
          LOGGER.warn("Initial offset type is {} and evaluated to null for topic: {} partitions: {}; proceeding "
              + "with their end offsets", offsetCriteria, _topic, fallbackPartitions);
        }
      } else {
        throw new IllegalArgumentException("Unknown initial offset value " + offsetCriteria);
      }
      Map<Integer, StreamPartitionMsgOffset> result = new HashMap<>(topicPartitionToOffset.size());
      for (Map.Entry<TopicPartition, Long> entry : topicPartitionToOffset.entrySet()) {
        if (entry.getValue() != null) {
          result.put(entry.getKey().partition(), new LongMsgOffset(entry.getValue()));
        }
      }
      // Every requested partition must resolve to an offset. If the stream returned none for a partition, fail the
      // whole fetch with a transient error so PartitionGroupMetadataFetcher retries it on the next run, rather than
      // returning a partial map: a missing partition would be misread downstream as having reached end of life (its
      // CONSUMING segment marked ONLINE with no successor) and would shrink the derived partition count.
      for (Integer partitionId : partitionIds) {
        if (!result.containsKey(partitionId)) {
          throw new TransientConsumerException(new RuntimeException(
              "No offset returned for topic: " + _topic + " partition: " + partitionId));
        }
      }
      return result;
    } catch (TimeoutException e) {
      throw new TransientConsumerException(e);
    }
  }

  @Override
  public Map<String, PartitionLagState> getCurrentPartitionLagState(
      Map<String, ConsumerPartitionState> currentPartitionStateMap) {
    Map<String, PartitionLagState> perPartitionLag = new HashMap<>();
    for (Map.Entry<String, ConsumerPartitionState> entry : currentPartitionStateMap.entrySet()) {
      ConsumerPartitionState partitionState = entry.getValue();
      // Compute records-lag
      StreamPartitionMsgOffset currentOffset = partitionState.getCurrentOffset();
      StreamPartitionMsgOffset upstreamLatest = partitionState.getUpstreamLatestOffset();
      String offsetLagString = PartitionLagState.NOT_CALCULATED;

      if (currentOffset instanceof LongMsgOffset && upstreamLatest instanceof LongMsgOffset) {
        long offsetLag = ((LongMsgOffset) upstreamLatest).getOffset() - ((LongMsgOffset) currentOffset).getOffset();
        offsetLagString = String.valueOf(offsetLag);
      }

      // Compute record-availability. Only when both the last-processed wall-clock time and the record's upstream
      // ingestion time are valid; otherwise a missing/invalid ingestion time (e.g. records without a Kafka
      // timestamp) would turn the subtraction into an epoch-sized value that leaks to the metric and UI. Keep the
      // NOT_CALCULATED sentinel in that case so the lag is reported as not-calculated rather than a bogus number.
      String availabilityLagMs = PartitionLagState.NOT_CALCULATED;
      StreamMessageMetadata lastProcessedMessageMetadata = partitionState.getLastProcessedRowMetadata();
      if (lastProcessedMessageMetadata != null && partitionState.getLastProcessedTimeMs() > 0
          && lastProcessedMessageMetadata.getRecordIngestionTimeMs() > 0) {
        long availabilityLag =
            partitionState.getLastProcessedTimeMs() - lastProcessedMessageMetadata.getRecordIngestionTimeMs();
        availabilityLagMs = String.valueOf(availabilityLag);
      }

      perPartitionLag.put(entry.getKey(), new KafkaConsumerPartitionLag(offsetLagString, availabilityLagMs));
    }
    return perPartitionLag;
  }

  @Override
  public List<TopicMetadata> getTopics() {
    try {
      AdminClient adminClient = getOrCreateSharedAdminClient();
      ListTopicsResult result = adminClient.listTopics();
      if (result == null) {
        return List.of();
      }
      return result.names()
          .get()
          .stream()
          .map(topic -> new KafkaTopicMetadata().setName(topic))
          .collect(Collectors.toList());
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean supportsOffsetLag() {
    return true;
  }

  public static class KafkaTopicMetadata implements TopicMetadata {
    private String _name;

    public String getName() {
      return _name;
    }

    public KafkaTopicMetadata setName(String name) {
      _name = name;
      return this;
    }
  }



  @Override
  public StreamPartitionMsgOffset getOffsetAtTimestamp(int partitionId, long timestampMillis, long timeoutMillis) {
    try {
      OffsetAndTimestamp offsetAndTimestamp = _consumer.offsetsForTimes(Map.of(_topicPartition, timestampMillis),
          Duration.ofMillis(timeoutMillis)).get(_topicPartition);
      if (offsetAndTimestamp == null) {
        return null;
      }
      return new LongMsgOffset(offsetAndTimestamp.offset());
    } catch (Exception e) {
      LOGGER.warn("Failed to get offset at timestamp {} for partition {}", timestampMillis, partitionId, e);
      return null;
    }
  }

  @Override
  public Map<String, StreamPartitionMsgOffset> getStreamStartOffsets() {
    List<PartitionInfo> partitionInfos = _consumer.partitionsFor(_topic);
    Map<TopicPartition, Long> startOffsets = _consumer.beginningOffsets(
        partitionInfos.stream()
            .filter(info -> info != null)
            .map(info -> new TopicPartition(_topic, info.partition()))
            .collect(Collectors.toList()));
    return startOffsets.entrySet().stream().collect(
        Collectors.toMap(
            entry -> String.valueOf(entry.getKey().partition()),
            entry -> new LongMsgOffset(entry.getValue()),
            (existingValue, newValue) -> newValue
        ));
  }

  @Override
  public Map<String, StreamPartitionMsgOffset> getStreamEndOffsets() {
    List<PartitionInfo> partitionInfos = _consumer.partitionsFor(_topic);
    Map<TopicPartition, Long> startOffsets = _consumer.endOffsets(
        partitionInfos.stream()
            .filter(info -> info != null)
            .map(info -> new TopicPartition(_topic, info.partition()))
            .collect(Collectors.toList()));
    return startOffsets.entrySet().stream().collect(
        Collectors.toMap(
            entry -> String.valueOf(entry.getKey().partition()),
            entry -> new LongMsgOffset(entry.getValue()),
            (existingValue, newValue) -> newValue
        ));
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }

  private List<PartitionInfo> fetchPartitionInfos(long timeoutMillis) {
    long deadlineMs = System.currentTimeMillis() + timeoutMillis;
    List<PartitionInfo> partitionInfos = null;
    Exception lastError = null;
    boolean topicMissing = false;
    while (System.currentTimeMillis() < deadlineMs) {
      long remainingMs = deadlineMs - System.currentTimeMillis();
      long requestTimeoutMs = Math.min(500L, Math.max(1L, remainingMs));
      try {
        partitionInfos = _consumer.partitionsFor(_topic, Duration.ofMillis(requestTimeoutMs));
      } catch (TimeoutException e) {
        lastError = e;
      }

      if (CollectionUtils.isNotEmpty(partitionInfos)) {
        return partitionInfos;
      }

      try {
        if (!topicExists(requestTimeoutMs)) {
          topicMissing = true;
          lastError = new RuntimeException("Topic does not exist: " + _topic);
        } else {
          topicMissing = false;
        }
      } catch (TransientConsumerException e) {
        lastError = e;
      } catch (RuntimeException e) {
        lastError = e;
      }

      if (System.currentTimeMillis() >= deadlineMs) {
        break;
      }
      try {
        Thread.sleep(100L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    if (lastError != null) {
      if (topicMissing) {
        throw new PermanentConsumerException(new RuntimeException("Topic does not exist: " + _topic));
      }
      if (lastError instanceof TransientConsumerException) {
        throw (TransientConsumerException) lastError;
      }
      if (lastError instanceof TimeoutException) {
        throw new TransientConsumerException(lastError);
      }
    }

    throw new TransientConsumerException(
        new RuntimeException("Failed to fetch partition information for topic: " + _topic));
  }

  private void validatePartitionIds(List<Integer> subset) {
    Set<Integer> topicPartitionIds = new HashSet<>();
    List<PartitionInfo> partitionInfos = fetchPartitionInfos(10_000L);
    if (partitionInfos == null || partitionInfos.isEmpty()) {
      throw new IllegalStateException(
          "Cannot validate partition IDs: topic " + _topic + " metadata not available. "
              + "Ensure the topic exists and is accessible.");
    }
    for (PartitionInfo partitionInfo : partitionInfos) {
      topicPartitionIds.add(partitionInfo.partition());
    }
    List<Integer> missingPartitionIds = new ArrayList<>();
    for (Integer partitionId : subset) {
      if (!topicPartitionIds.contains(partitionId)) {
        missingPartitionIds.add(partitionId);
      }
    }
    Preconditions.checkArgument(
        missingPartitionIds.isEmpty(),
        "Invalid partition ids %s for table stream config. Available partitions on topic %s are: %s",
        missingPartitionIds, _topic, topicPartitionIds);
  }

  private boolean topicExists(long timeoutMillis) {
    try {
      AdminClient adminClient = getOrCreateSharedAdminClient();
      ListTopicsResult result = adminClient.listTopics();
      return result.names().get(timeoutMillis, TimeUnit.MILLISECONDS).contains(_topic);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TransientConsumerException(e);
    } catch (ExecutionException e) {
      throw new TransientConsumerException(e);
    } catch (java.util.concurrent.TimeoutException e) {
      throw new TransientConsumerException(e);
    }
  }
}
