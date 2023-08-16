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
package org.apache.pinot.plugin.stream.kinesis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Shard;


/**
 * A {@link StreamMetadataProvider} implementation for the Kinesis stream
 */
public class KinesisStreamMetadataProvider implements StreamMetadataProvider {
  private static final String SHARD_ID_PREFIX = "shardId-";
  private final KinesisConnectionHandler _kinesisConnectionHandler;
  private final StreamConsumerFactory _kinesisStreamConsumerFactory;
  private final String _clientId;
  private final int _fetchTimeoutMs;
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisStreamMetadataProvider.class);

  public KinesisStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    KinesisConfig kinesisConfig = new KinesisConfig(streamConfig);
    _kinesisConnectionHandler = new KinesisConnectionHandler(kinesisConfig);
    _kinesisStreamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    _clientId = clientId;
    _fetchTimeoutMs = streamConfig.getFetchTimeoutMillis();
  }

  public KinesisStreamMetadataProvider(String clientId, StreamConfig streamConfig,
      KinesisConnectionHandler kinesisConnectionHandler, StreamConsumerFactory streamConsumerFactory) {
    _kinesisConnectionHandler = kinesisConnectionHandler;
    _kinesisStreamConsumerFactory = streamConsumerFactory;
    _clientId = clientId;
    _fetchTimeoutMs = streamConfig.getFetchTimeoutMillis();
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis) {
    throw new UnsupportedOperationException();
  }

  /**
   * This call returns all active shards, taking into account the consumption status for those shards.
   * {@link PartitionGroupMetadata} is returned for a shard if:
   * 1. It is a branch new shard AND its parent has been consumed completely
   * 2. It is still being actively consumed from i.e. the consuming partition has not reached the end of the shard
   */
  @Override
  public List<PartitionGroupMetadata> computePartitionGroupMetadata(String clientId, StreamConfig streamConfig,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatuses, int timeoutMillis)
      throws IOException, TimeoutException {

    List<PartitionGroupMetadata> newPartitionGroupMetadataList = new ArrayList<>();

    Map<String, Shard> shardIdToShardMap = _kinesisConnectionHandler.getShards().stream()
        .collect(Collectors.toMap(Shard::shardId, s -> s, (s1, s2) -> s1));
    Set<String> shardsInCurrent = new HashSet<>();
    Set<String> shardsEnded = new HashSet<>();

    // TODO: Once we start supporting multiple shards in a PartitionGroup,
    //  we need to iterate over all shards to check if any of them have reached end

    // Process existing shards. Add them to new list if still consuming from them
    for (PartitionGroupConsumptionStatus currentPartitionGroupConsumptionStatus : partitionGroupConsumptionStatuses) {
      KinesisPartitionGroupOffset kinesisStartCheckpoint =
          (KinesisPartitionGroupOffset) currentPartitionGroupConsumptionStatus.getStartOffset();
      String shardId = kinesisStartCheckpoint.getShardToStartSequenceMap().keySet().iterator().next();
      shardsInCurrent.add(shardId);
      Shard shard = shardIdToShardMap.get(shardId);
      if (shard == null) { // Shard has expired
        shardsEnded.add(shardId);
        String lastConsumedSequenceID = kinesisStartCheckpoint.getShardToStartSequenceMap().get(shardId);
        LOGGER.warn("Kinesis shard with id: " + shardId
            + " has expired. Data has been consumed from the shard till sequence number: " + lastConsumedSequenceID
            + ". There can be potential data loss.");
        continue;
      }

      StreamPartitionMsgOffset newStartOffset;
      StreamPartitionMsgOffset currentEndOffset = currentPartitionGroupConsumptionStatus.getEndOffset();
      if (currentEndOffset != null) { // Segment DONE (committing/committed)
        String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
        if (endingSequenceNumber != null) { // Shard has ended, check if we're also done consuming it
          if (consumedEndOfShard(currentEndOffset, currentPartitionGroupConsumptionStatus)) {
            shardsEnded.add(shardId);
            continue; // Shard ended and we're done consuming it. Skip
          }
        }
        newStartOffset = currentEndOffset;
      } else { // Segment IN_PROGRESS
        newStartOffset = currentPartitionGroupConsumptionStatus.getStartOffset();
      }
      newPartitionGroupMetadataList.add(
          new PartitionGroupMetadata(currentPartitionGroupConsumptionStatus.getPartitionGroupId(), newStartOffset));
    }

    // Add brand new shards
    for (Map.Entry<String, Shard> entry : shardIdToShardMap.entrySet()) {
      // If shard was already in current list, skip
      String newShardId = entry.getKey();
      if (shardsInCurrent.contains(newShardId)) {
        continue;
      }
      StreamPartitionMsgOffset newStartOffset;
      Shard newShard = entry.getValue();
      String parentShardId = newShard.parentShardId();

      // Add the new shard in the following 3 cases:
      // 1. Root shards - Parent shardId will be null. Will find this case when creating new table.
      // 2. Parent expired - Parent shardId will not be part of shardIdToShard map
      // 3. Parent reached EOL and completely consumed.
      if (parentShardId == null || !shardIdToShardMap.containsKey(parentShardId) || shardsEnded.contains(
          parentShardId)) {
        Map<String, String> shardToSequenceNumberMap = new HashMap<>();
        shardToSequenceNumberMap.put(newShardId, newShard.sequenceNumberRange().startingSequenceNumber());
        newStartOffset = new KinesisPartitionGroupOffset(shardToSequenceNumberMap);
        int partitionGroupId = getPartitionGroupIdFromShardId(newShardId);
        newPartitionGroupMetadataList.add(new PartitionGroupMetadata(partitionGroupId, newStartOffset));
      }
    }
    return newPartitionGroupMetadataList;
  }

  /**
   * Converts a shardId string to a partitionGroupId integer by parsing the digits of the shardId
   * e.g. "shardId-000000000001" becomes 1
   * FIXME: Although practically the shard values follow this format, the Kinesis docs don't guarantee it.
   *  Re-evaluate if this convention needs to be changed.
   */
  private int getPartitionGroupIdFromShardId(String shardId) {
    String shardIdNum = StringUtils.stripStart(StringUtils.removeStart(shardId, SHARD_ID_PREFIX), "0");
    return shardIdNum.isEmpty() ? 0 : Integer.parseInt(shardIdNum);
  }

  private boolean consumedEndOfShard(StreamPartitionMsgOffset startCheckpoint,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus)
      throws IOException, TimeoutException {
    PartitionGroupConsumer partitionGroupConsumer =
        _kinesisStreamConsumerFactory.createPartitionGroupConsumer(_clientId, partitionGroupConsumptionStatus);

    MessageBatch messageBatch;
    try {
      messageBatch = partitionGroupConsumer.fetchMessages(startCheckpoint, null, _fetchTimeoutMs);
    } finally {
      partitionGroupConsumer.close();
    }
    return messageBatch.isEndOfPartitionGroup();
  }

  @Override
  public Map<String, PartitionLagState> getCurrentPartitionLagState(
      Map<String, ConsumerPartitionState> currentPartitionStateMap) {
    Map<String, PartitionLagState> perPartitionLag = new HashMap<>();
    for (Map.Entry<String, ConsumerPartitionState> entry: currentPartitionStateMap.entrySet()) {
      ConsumerPartitionState partitionState = entry.getValue();
      // Compute record-availability
      String recordAvailabilityLag = "UNKNOWN";
      RowMetadata lastProcessedMessageMetadata = partitionState.getLastProcessedRowMetadata();
      if (lastProcessedMessageMetadata != null && partitionState.getLastProcessedTimeMs() > 0) {
        long availabilityLag = partitionState.getLastProcessedTimeMs()
            - lastProcessedMessageMetadata.getRecordIngestionTimeMs();
        recordAvailabilityLag = String.valueOf(availabilityLag);
      }
      perPartitionLag.put(entry.getKey(), new KinesisConsumerPartitionLag(recordAvailabilityLag));
    }
    return perPartitionLag;
  }

  @Override
  public void close() { }
}
