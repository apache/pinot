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
package org.apache.pinot.plugin.stream.kafka20;

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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
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
          LOGGER.warn("initial offset type is period and its value evaluates "
              + "to null hence proceeding with offset " + offset + "for topic " + _topicPartition.topic()
              + " partition " + _topicPartition.partition());
        } else {
          offset = offsetAndTimestamp.offset();
        }
      } else if (offsetCriteria.isTimestamp()) {
        OffsetAndTimestamp offsetAndTimestamp = _consumer.offsetsForTimes(Collections.singletonMap(_topicPartition,
            TimeUtils.convertTimestampToMillis(offsetCriteria.getOffsetString()))).get(_topicPartition);
        if (offsetAndTimestamp == null) {
          offset = _consumer.endOffsets(Collections.singletonList(_topicPartition), Duration.ofMillis(timeoutMillis))
              .get(_topicPartition);
          LOGGER.warn("initial offset type is timestamp and its value evaluates "
              + "to null hence proceeding with offset " + offset + "for topic " + _topicPartition.topic()
              + " partition " + _topicPartition.partition());
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
    for (Map.Entry<String, ConsumerPartitionState> entry: currentPartitionStateMap.entrySet()) {
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
        long availabilityLag = partitionState.getLastProcessedTimeMs()
            - lastProcessedMessageMetadata.getRecordIngestionTimeMs();
        availabilityLagMs = String.valueOf(availabilityLag);
      }

      perPartitionLag.put(entry.getKey(), new KafkaConsumerPartitionLag(offsetLagString, availabilityLagMs));
    }
    return perPartitionLag;
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
