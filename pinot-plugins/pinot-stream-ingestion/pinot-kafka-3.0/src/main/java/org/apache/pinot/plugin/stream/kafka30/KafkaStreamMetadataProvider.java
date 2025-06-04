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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.pinot.plugin.stream.kafka.KafkaConsumerPartitionLag;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.TransientConsumerException;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaStreamMetadataProvider extends KafkaPartitionLevelConnectionHandler
    implements StreamMetadataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamMetadataProvider.class);

  public KafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    this(clientId, streamConfig, Integer.MIN_VALUE);
  }

  public KafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition);
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    try {
      List<PartitionInfo> partitionInfos = _consumer.partitionsFor(_topic, Duration.ofMillis(timeoutMillis));
      if (CollectionUtils.isNotEmpty(partitionInfos)) {
        return partitionInfos.size();
      }
      throw new RuntimeException(String.format("Failed to fetch partition information for topic: %s", _topic));
    } catch (TimeoutException e) {
      throw new TransientConsumerException(e);
    }
  }

  @Override
  public Set<Integer> fetchPartitionIds(long timeoutMillis) {
    try {
      List<PartitionInfo> partitionInfos = _consumer.partitionsFor(_topic, Duration.ofMillis(timeoutMillis));
      if (CollectionUtils.isEmpty(partitionInfos)) {
        throw new RuntimeException(String.format("Failed to fetch partition information for topic: %s", _topic));
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
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis) {
    Preconditions.checkNotNull(offsetCriteria);
    try (AdminClient adminClient = createAdminClient()) {
      // Build the offset spec request for this partition
      Map<TopicPartition, OffsetSpec> request = new HashMap<>();
      if (offsetCriteria.isLargest()) {
        request.put(_topicPartition, OffsetSpec.latest());
      } else if (offsetCriteria.isSmallest()) {
        request.put(_topicPartition, OffsetSpec.earliest());
      } else if (offsetCriteria.isPeriod()) {
        long ts = Clock.systemUTC().millis() - TimeUtils.convertPeriodToMillis(offsetCriteria.getOffsetString());
        request.put(_topicPartition, OffsetSpec.forTimestamp(ts));
      } else if (offsetCriteria.isTimestamp()) {
        long ts = TimeUtils.convertTimestampToMillis(offsetCriteria.getOffsetString());
        request.put(_topicPartition, OffsetSpec.forTimestamp(ts));
      } else {
        throw new IllegalArgumentException("Unknown offset criteria: " + offsetCriteria);
      }
      // Query via AdminClient (thread-safe)
      ListOffsetsResult result = adminClient.listOffsets(request);
      Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
          result.all().get(timeoutMillis, TimeUnit.MILLISECONDS);
      if (!isValidOffsetInfo(offsets) && (offsetCriteria.isTimestamp() || offsetCriteria.isPeriod())) {
        // fetch endOffsets as fallback
        request.put(_topicPartition, OffsetSpec.latest());
        result = adminClient.listOffsets(request);
        offsets = result.all().get(timeoutMillis, TimeUnit.MILLISECONDS);
        LOGGER.warn(
            "initial offset type is {} and its value evaluates to null hence proceeding with offset {} " + "for "
                + "topic {} partition {}", offsetCriteria, offsets.get(_topicPartition).offset(),
            _topicPartition.topic(), _topicPartition.partition());
      }
      ListOffsetsResult.ListOffsetsResultInfo info = offsets.get(_topicPartition);
      if (info == null) {
        throw new TransientConsumerException(new RuntimeException(
            String.format("Failed to fetch offset for topic: %s partition: %d", _topic, _topicPartition.partition())));
      }
      return new LongMsgOffset(info.offset());
    } catch (InterruptedException | ExecutionException | java.util.concurrent.TimeoutException e) {
      throw new TransientConsumerException(e);
    }
  }

  private boolean isValidOffsetInfo(Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets) {
    return offsets != null && offsets.containsKey(_topicPartition) && offsets.get(_topicPartition).offset() >= 0;
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
      RowMetadata lastProcessedMessageMetadata = partitionState.getLastProcessedRowMetadata();
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
    try (AdminClient adminClient = createAdminClient()) {
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
  public void close()
      throws IOException {
    super.close();
  }
}
