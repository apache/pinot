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
package org.apache.pinot.plugin.stream.kafka40;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
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
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
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
  /**
   * Immutable partition ID subset from table config. Read once at construction; does not change during the
   * provider's lifetime. Empty when no subset is configured (consume all partitions).
   */
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
      _partitionIdSubset = Collections.emptyList();
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
    if (_partitionIdSubset.isEmpty()) {
      return StreamMetadataProvider.super.computePartitionGroupMetadata(clientId, streamConfig,
          partitionGroupConsumptionStatuses, timeoutMillis);
    }
    List<Integer> subset = _partitionIdSubset;
    Map<Integer, StreamPartitionMsgOffset> partitionIdToEndOffset =
        new HashMap<>(partitionGroupConsumptionStatuses.size());
    for (PartitionGroupConsumptionStatus s : partitionGroupConsumptionStatuses) {
      partitionIdToEndOffset.put(s.getStreamPartitionGroupId(), s.getEndOffset());
    }
    StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    List<PartitionGroupMetadata> result = new ArrayList<>(subset.size());
    for (Integer partitionId : subset) {
      StreamPartitionMsgOffset endOffset = partitionIdToEndOffset.get(partitionId);
      StreamPartitionMsgOffset startOffset;
      if (endOffset == null) {
        try (StreamMetadataProvider partitionMetadataProvider =
            streamConsumerFactory.createPartitionMetadataProvider(
                StreamConsumerFactory.getUniqueClientId(clientId), partitionId)) {
          startOffset = partitionMetadataProvider.fetchStreamPartitionOffset(
              streamConfig.getOffsetCriteria(), timeoutMillis);
        }
      } else {
        startOffset = endOffset;
      }
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
    long offset;
    try {
      if (offsetCriteria.isLargest()) {
        offset = _consumer.endOffsets(Collections.singletonList(_topicPartition), Duration.ofMillis(timeoutMillis))
            .get(_topicPartition);
      } else if (offsetCriteria.isSmallest()) {
        offset =
            _consumer.beginningOffsets(Collections.singletonList(_topicPartition), Duration.ofMillis(timeoutMillis))
                .get(_topicPartition);
      } else if (offsetCriteria.isPeriod()) {
        OffsetAndTimestamp offsetAndTimestamp = _consumer.offsetsForTimes(Collections.singletonMap(_topicPartition,
                Clock.systemUTC().millis() - TimeUtils.convertPeriodToMillis(offsetCriteria.getOffsetString())))
            .get(_topicPartition);
        if (offsetAndTimestamp == null) {
          offset = _consumer.endOffsets(Collections.singletonList(_topicPartition), Duration.ofMillis(timeoutMillis))
              .get(_topicPartition);
          LOGGER.warn(
              "initial offset type is period and its value evaluates to null hence proceeding with offset {} for "
                  + "topic {} partition {}", offset, _topicPartition.topic(), _topicPartition.partition());
        } else {
          offset = offsetAndTimestamp.offset();
        }
      } else if (offsetCriteria.isTimestamp()) {
        OffsetAndTimestamp offsetAndTimestamp = _consumer.offsetsForTimes(Collections.singletonMap(_topicPartition,
            TimeUtils.convertTimestampToMillis(offsetCriteria.getOffsetString()))).get(_topicPartition);
        if (offsetAndTimestamp == null) {
          offset = _consumer.endOffsets(Collections.singletonList(_topicPartition), Duration.ofMillis(timeoutMillis))
              .get(_topicPartition);
          LOGGER.warn(
              "initial offset type is timestamp and its value evaluates to null hence proceeding with offset {} for "
                  + "topic {} partition {}", offset, _topicPartition.topic(), _topicPartition.partition());
        } else {
          offset = offsetAndTimestamp.offset();
        }
      } else {
        throw new IllegalArgumentException("Unknown initial offset value " + offsetCriteria);
      }
      return new LongMsgOffset(offset);
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
      String offsetLagString = "UNKNOWN";

      if (currentOffset instanceof LongMsgOffset && upstreamLatest instanceof LongMsgOffset) {
        long offsetLag = ((LongMsgOffset) upstreamLatest).getOffset() - ((LongMsgOffset) currentOffset).getOffset();
        offsetLagString = String.valueOf(offsetLag);
      }

      // Compute record-availability
      String availabilityLagMs = "UNKNOWN";
      StreamMessageMetadata lastProcessedMessageMetadata = partitionState.getLastProcessedRowMetadata();
      if (lastProcessedMessageMetadata != null && partitionState.getLastProcessedTimeMs() > 0) {
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
        return Collections.emptyList();
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
    Map<TopicPartition, Long> endOffsets = _consumer.endOffsets(
        partitionInfos.stream()
            .map(info -> new TopicPartition(_topic, info.partition()))
            .collect(Collectors.toList()));
    return endOffsets.entrySet().stream().collect(
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
        throw new RuntimeException("Topic does not exist: " + _topic);
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
