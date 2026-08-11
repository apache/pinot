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
import java.math.BigInteger;
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
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;

/**
 * A {@link StreamMetadataProvider} implementation for the Kinesis stream
 */
public class KinesisStreamMetadataProvider implements StreamMetadataProvider {
  public static final String SHARD_ID_PREFIX = "shardId-";
  /// Max empty non-EOP GetRecords probes when checking whether a closed shard is fully consumed.
  /// AWS may return several empty batches with a non-null iterator before the iterator goes null.
  static final int MAX_END_OF_SHARD_EMPTY_PROBES = 5;
  private final KinesisConnectionHandler _kinesisConnectionHandler;
  private final StreamConsumerFactory _kinesisStreamConsumerFactory;
  private final String _clientId;
  private final int _fetchTimeoutMs;
  private final String _partitionId;
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisStreamMetadataProvider.class);

  public KinesisStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    this(clientId, streamConfig, String.valueOf(Integer.MIN_VALUE));
  }

  public KinesisStreamMetadataProvider(String clientId, StreamConfig streamConfig, String partitionId) {
    KinesisConfig kinesisConfig = new KinesisConfig(streamConfig);
    _kinesisConnectionHandler = new KinesisConnectionHandler(kinesisConfig);
    _kinesisStreamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    _clientId = clientId;
    _partitionId = partitionId;
    _fetchTimeoutMs = streamConfig.getFetchTimeoutMillis();
  }

  public KinesisStreamMetadataProvider(String clientId, StreamConfig streamConfig,
      KinesisConnectionHandler kinesisConnectionHandler, StreamConsumerFactory streamConsumerFactory) {
    this(clientId, streamConfig, String.valueOf(Integer.MIN_VALUE), kinesisConnectionHandler, streamConsumerFactory);
  }

  public KinesisStreamMetadataProvider(String clientId, StreamConfig streamConfig, String partitionId,
      KinesisConnectionHandler kinesisConnectionHandler, StreamConsumerFactory streamConsumerFactory) {
    _kinesisConnectionHandler = kinesisConnectionHandler;
    _kinesisStreamConsumerFactory = streamConsumerFactory;
    _clientId = clientId;
    _partitionId = partitionId;
    _fetchTimeoutMs = streamConfig.getFetchTimeoutMillis();
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    try {
      List<Shard> shards = _kinesisConnectionHandler.getShards();
      return shards.size();
    } catch (Exception e) {
      LOGGER.error("Failed to fetch partition count", e);
      throw new RuntimeException("Failed to fetch partition count", e);
    }
  }

  @Override
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis) {
    // fetch offset for _partitionId
    Shard foundShard = _kinesisConnectionHandler.getShards().stream()
        .filter(shard -> {
          String shardId = shard.shardId();
          int partitionGroupId = getPartitionGroupIdFromShardId(shardId);
          return partitionGroupId == Integer.parseInt(_partitionId);
        })
        .findFirst().orElseThrow(() -> new RuntimeException("Failed to find shard for partitionId: " + _partitionId));
    SequenceNumberRange sequenceNumberRange = foundShard.sequenceNumberRange();
    if (offsetCriteria.isSmallest()) {
      return new KinesisPartitionGroupOffset(foundShard.shardId(), sequenceNumberRange.startingSequenceNumber());
    } else if (offsetCriteria.isLargest()) {
      return new KinesisPartitionGroupOffset(foundShard.shardId(), sequenceNumberRange.endingSequenceNumber());
    }
    throw new IllegalArgumentException("Unsupported offset criteria: " + offsetCriteria);
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
      String shardId = kinesisStartCheckpoint.getShardId();
      shardsInCurrent.add(shardId);
      Shard shard = shardIdToShardMap.get(shardId);
      if (shard == null) { // Shard has expired
        shardsEnded.add(shardId);
        String lastConsumedSequenceID = kinesisStartCheckpoint.getSequenceNumber();
        LOGGER.warn(
            "Kinesis shard with id: {} has expired. Data has been consumed from the shard till sequence number: {}. "
                + "There can be potential data loss.", shardId, lastConsumedSequenceID);
        continue;
      }

      StreamPartitionMsgOffset newStartOffset;
      StreamPartitionMsgOffset currentEndOffset = currentPartitionGroupConsumptionStatus.getEndOffset();
      if (currentEndOffset != null) { // Segment DONE (committing/committed)
        String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
        if (endingSequenceNumber != null) { // Shard has ended, check if we're also done consuming it
          if (consumedEndOfShard(currentEndOffset, currentPartitionGroupConsumptionStatus, endingSequenceNumber)) {
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
      Shard newShard = entry.getValue();
      String parentShardId = newShard.parentShardId();

      // Add the new shard in the following 3 cases:
      // 1. Root shards - Parent shardId will be null. Will find this case when creating new table.
      // 2. Parent expired - Parent shardId will not be part of shardIdToShard map
      // 3. Parent reached EOL and completely consumed.
      if (parentShardId == null || !shardIdToShardMap.containsKey(parentShardId) || shardsEnded.contains(
          parentShardId)) {
        // TODO: Revisit this. Kinesis starts consuming AFTER the start sequence number, and we might miss the first
        //       message.
        StreamPartitionMsgOffset newStartOffset =
            new KinesisPartitionGroupOffset(newShardId, newShard.sequenceNumberRange().startingSequenceNumber());
        int partitionGroupId = getPartitionGroupIdFromShardId(newShardId);
        newPartitionGroupMetadataList.add(new PartitionGroupMetadata(partitionGroupId, newStartOffset));
      }
    }
    return newPartitionGroupMetadataList;
  }

  /**
   * Refer documentation for {@link #computePartitionGroupMetadata(String, StreamConfig, List, int)}
   * @param forceGetOffsetFromStream - the flag is not required for Kinesis stream. Kinesis implementation
   *                                 takes care of returning non-null offsets for all old and new partitions.
   *                                 The flag is primarily required for Kafka stream which requires refactoring
   *                                 to avoid this flag. More details in {@link
   *                                 StreamMetadataProvider#computePartitionGroupMetadata(
   *                                 String, StreamConfig, List, int, boolean)}
   */
  @Override
  public List<PartitionGroupMetadata> computePartitionGroupMetadata(String clientId, StreamConfig streamConfig,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatuses, int timeoutMillis,
      boolean forceGetOffsetFromStream)
      throws IOException, TimeoutException {
    return computePartitionGroupMetadata(clientId, streamConfig, partitionGroupConsumptionStatuses, timeoutMillis);
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

  /// Returns true when a closed Kinesis shard has been fully consumed at/after `startCheckpoint`.
  /// Only invoked when the shard's `endingSequenceNumber` is non-null (shard closed by split/merge).
  ///
  /// Order of checks:
  /// 1. Sequence short-circuit: checkpoint SN >= ending SN (BigInteger) means fully consumed, no GetRecords.
  /// 2. Poll GetRecords until EOP, messages found, empty-probe budget exhausted, or time budget exhausted.
  ///
  /// On a closed shard, empty-only probes (no messages ever seen) mean no backlog after the checkpoint, so we
  /// assume ended (fail-open for EOL). Returning false here re-opens the parent forever and blocks child shards.
  /// Transient empties from rate-limit/timeout are soft-failed: they do not burn the hard empty-probe budget the
  /// same way; a wall-clock time budget still bounds the loop.
  private boolean consumedEndOfShard(StreamPartitionMsgOffset startCheckpoint,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus, String endingSequenceNumber)
      throws IOException, TimeoutException {
    if (hasConsumedThroughEndingSequence(startCheckpoint, endingSequenceNumber)) {
      return true;
    }

    try (PartitionGroupConsumer partitionGroupConsumer = _kinesisStreamConsumerFactory.createPartitionGroupConsumer(
        _clientId, partitionGroupConsumptionStatus)) {
      // Allow multiple full fetch timeouts so throttled empties can be retried without failing closed.
      long timeBudgetMs = Math.max((long) _fetchTimeoutMs, 1L) * MAX_END_OF_SHARD_EMPTY_PROBES;
      long deadlineMs = currentTimeMillis() + timeBudgetMs;
      int emptyProbes = 0;
      int totalAttempts = 0;
      // Cap total iterations so instant soft-failures cannot spin until the deadline with no progress signal.
      int maxTotalAttempts = MAX_END_OF_SHARD_EMPTY_PROBES * 4;

      while (true) {
        long remainingMs = deadlineMs - currentTimeMillis();
        if (remainingMs <= 0) {
          // Only assume ended if we observed at least one hard empty (non-throttle) probe. Throttle-only exhaustion
          // must not skip an unconsumed closed-shard tail (data loss).
          if (emptyProbes > 0) {
            LOGGER.warn("Time budget exhausted checking end of shard from checkpoint {} after {} hard empty probes. "
                    + "Assuming end of shard consumed (closed shard, no messages seen).", startCheckpoint, emptyProbes);
            return true;
          }
          LOGGER.warn("Time budget exhausted checking end of shard from checkpoint {} with only transient/throttle "
                  + "empties (totalAttempts={}). Assuming NOT fully consumed to avoid skipping tail records.",
              startCheckpoint, totalAttempts);
          return false;
        }

        int fetchTimeoutMs = (int) Math.min(Math.max(_fetchTimeoutMs, 1L), remainingMs);
        long fetchStartMs = currentTimeMillis();
        MessageBatch<?> messageBatch = partitionGroupConsumer.fetchMessages(startCheckpoint, fetchTimeoutMs);
        long fetchElapsedMs = currentTimeMillis() - fetchStartMs;
        totalAttempts++;

        if (messageBatch.getMessageCount() > 0) {
          // There are messages left to be consumed so we haven't consumed the shard fully
          return false;
        }
        if (messageBatch.isEndOfPartitionGroup()) {
          // Shard can't be iterated further. We have consumed all the messages because message count = 0
          return true;
        }

        // Empty non-EOP: AWS may need several GetRecords before nextShardIterator is null on a closed shard,
        // or the consumer returned a rate-limit/timeout empty batch (no iterator advance).
        // Soft-fail throttles/timeouts: a fetch that burned most of its timeout is treated as transient and does
        // not increment the hard empty-probe counter. Hard empties (quick empty responses) do.
        boolean likelyTransientEmpty = fetchElapsedMs >= Math.max(1L, fetchTimeoutMs * 3L / 4L);
        if (!likelyTransientEmpty) {
          emptyProbes++;
        }
        if (emptyProbes >= MAX_END_OF_SHARD_EMPTY_PROBES) {
          LOGGER.warn("Reached max empty probes checking end of shard from checkpoint {} "
                  + "(emptyProbes={}, totalAttempts={}). "
                  + "Assuming end of shard consumed (closed shard, no messages seen).",
              startCheckpoint, emptyProbes, totalAttempts);
          return true;
        }
        if (totalAttempts >= maxTotalAttempts) {
          // Exhausted attempts without enough hard empties: prefer not ending over skipping tail under throttle.
          if (emptyProbes > 0) {
            LOGGER.warn("Reached max total attempts checking end of shard from checkpoint {} "
                    + "(emptyProbes={}, totalAttempts={}). Assuming end of shard consumed.",
                startCheckpoint, emptyProbes, totalAttempts);
            return true;
          }
          LOGGER.warn("Reached max total attempts checking end of shard from checkpoint {} with only transient "
                  + "empties. Assuming NOT fully consumed.", startCheckpoint);
          return false;
        }
        // continue to fetch messages. reusing the partitionGroupConsumer ensures we use new shard iterator
      }
    }
  }

  /// True when the checkpoint sequence number is at or past the shard ending sequence number.
  /// The Kinesis consumer starts AFTER the checkpoint sequence number, so checkpoint == ending means the ending
  /// record was already consumed (nothing remains after it).
  static boolean hasConsumedThroughEndingSequence(StreamPartitionMsgOffset checkpoint, String endingSequenceNumber) {
    if (!(checkpoint instanceof KinesisPartitionGroupOffset) || StringUtils.isEmpty(endingSequenceNumber)) {
      return false;
    }
    String checkpointSequenceNumber = ((KinesisPartitionGroupOffset) checkpoint).getSequenceNumber();
    if (StringUtils.isEmpty(checkpointSequenceNumber)) {
      return false;
    }
    try {
      return new BigInteger(checkpointSequenceNumber).compareTo(new BigInteger(endingSequenceNumber)) >= 0;
    } catch (NumberFormatException e) {
      LOGGER.warn("Failed to compare Kinesis sequence numbers checkpoint={} ending={}; falling back to probe",
          checkpointSequenceNumber, endingSequenceNumber);
      return false;
    }
  }

  /// Visible for tests that need to control wall-clock behavior of the end-of-shard probe loop.
  long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  @Override
  public Map<String, PartitionLagState> getCurrentPartitionLagState(
      Map<String, ConsumerPartitionState> currentPartitionStateMap) {
    Map<String, PartitionLagState> perPartitionLag = new HashMap<>();
    for (Map.Entry<String, ConsumerPartitionState> entry : currentPartitionStateMap.entrySet()) {
      ConsumerPartitionState partitionState = entry.getValue();
      // Compute record-availability. Only when both the last-processed wall-clock time and the record's upstream
      // ingestion time are valid; otherwise a missing/invalid ingestion time (e.g. records without a valid
      // timestamp) would turn the subtraction into an epoch-sized value that leaks to the metric and UI. Keep the
      // NOT_CALCULATED sentinel in that case so the lag is reported as not-calculated rather than a bogus number.
      String recordAvailabilityLag = PartitionLagState.NOT_CALCULATED;
      StreamMessageMetadata lastProcessedMessageMetadata = partitionState.getLastProcessedRowMetadata();
      if (lastProcessedMessageMetadata != null && partitionState.getLastProcessedTimeMs() > 0
          && lastProcessedMessageMetadata.getRecordIngestionTimeMs() > 0) {
        long availabilityLag =
            partitionState.getLastProcessedTimeMs() - lastProcessedMessageMetadata.getRecordIngestionTimeMs();
        recordAvailabilityLag = String.valueOf(availabilityLag);
      }
      perPartitionLag.put(entry.getKey(), new KinesisConsumerPartitionLag(recordAvailabilityLag));
    }
    return perPartitionLag;
  }

  @Override
  public void close() {
  }

  @Override
  public List<TopicMetadata> getTopics() {
    return _kinesisConnectionHandler.getStreamNames()
        .stream()
        .map(streamName -> new KinesisTopicMetadata().setName(streamName))
        .collect(Collectors.toList());
  }

  @Override
  public boolean supportsOffsetLag() {
    return false;
  }

  public static class KinesisTopicMetadata implements TopicMetadata {
    private String _name;

    public String getName() {
      return _name;
    }

    public KinesisTopicMetadata setName(String name) {
      _name = name;
      return this;
    }
  }
}
