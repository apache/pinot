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
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.stream.Checkpoint;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupInfo;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import software.amazon.awssdk.services.kinesis.model.Shard;


/**
 * A {@link StreamMetadataProvider} implementation for the Kinesis stream
 */
public class KinesisStreamMetadataProvider implements StreamMetadataProvider {
  private final KinesisConnectionHandler _kinesisConnectionHandler;
  private final StreamConsumerFactory _kinesisStreamConsumerFactory;
  private final String _clientId;
  private final int _fetchTimeoutMs;

  public KinesisStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    KinesisConfig kinesisConfig = new KinesisConfig(streamConfig);
    _kinesisConnectionHandler = new KinesisConnectionHandler(kinesisConfig.getStream(), kinesisConfig.getAwsRegion());
    _kinesisStreamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    _clientId = clientId;
    _fetchTimeoutMs = streamConfig.getFetchTimeoutMillis();
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis) {
    throw new UnsupportedOperationException();
  }

  /**
   * This call returns all active shards, taking into account the consumption status for those shards.
   * PartitionGroupInfo is returned for a shard if:
   * 1. It is a branch new shard AND its parent has been consumed completely
   * 2. It is still being actively consumed from i.e. the consuming partition has not reached the end of the shard
   */
  @Override
  public List<PartitionGroupInfo> getPartitionGroupInfoList(String clientId, StreamConfig streamConfig,
      List<PartitionGroupMetadata> currentPartitionGroupsMetadata, int timeoutMillis)
      throws IOException, TimeoutException {

    List<PartitionGroupInfo> newPartitionGroupInfos = new ArrayList<>();

    Map<String, Shard> shardIdToShardMap =
        _kinesisConnectionHandler.getShards().stream().collect(Collectors.toMap(Shard::shardId, s -> s));
    Set<String> shardsInCurrent = new HashSet<>();
    Set<String> shardsEnded = new HashSet<>();

    // TODO: Once we start supporting multiple shards in a PartitionGroup,
    //  we need to iterate over all shards to check if any of them have reached end

    // Process existing shards. Add them to new list if still consuming from them
    for (PartitionGroupMetadata currentPartitionGroupMetadata : currentPartitionGroupsMetadata) {
      KinesisCheckpoint kinesisStartCheckpoint = (KinesisCheckpoint) currentPartitionGroupMetadata.getStartCheckpoint();
      String shardId = kinesisStartCheckpoint.getShardToStartSequenceMap().keySet().iterator().next();
      Shard shard = shardIdToShardMap.get(shardId);
      shardsInCurrent.add(shardId);

      Checkpoint newStartCheckpoint;
      Checkpoint currentEndCheckpoint = currentPartitionGroupMetadata.getEndCheckpoint();
      if (currentEndCheckpoint != null) { // Segment DONE (committing/committed)
        String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
        if (endingSequenceNumber != null) { // Shard has ended, check if we're also done consuming it
          if (consumedEndOfShard(currentEndCheckpoint, currentPartitionGroupMetadata)) {
            shardsEnded.add(shardId);
            continue; // Shard ended and we're done consuming it. Skip
          }
        }
        newStartCheckpoint = currentEndCheckpoint;
      } else { // Segment IN_PROGRESS
        newStartCheckpoint = currentPartitionGroupMetadata.getStartCheckpoint();
      }
      newPartitionGroupInfos.add(new PartitionGroupInfo(currentPartitionGroupMetadata.getPartitionGroupId(), newStartCheckpoint));
    }

    // Add new shards. Parent should be null (new table case, very first shards) OR we should be flagged as reached EOL and completely consumed.
    for (Map.Entry<String, Shard> entry : shardIdToShardMap.entrySet()) {
      String newShardId = entry.getKey();
      if (shardsInCurrent.contains(newShardId)) {
        continue;
      }
      Checkpoint newStartCheckpoint;
      Shard newShard = entry.getValue();
      String parentShardId = newShard.parentShardId();

      if (parentShardId == null || shardsEnded.contains(parentShardId)) {
        Map<String, String> shardToSequenceNumberMap = new HashMap<>();
        shardToSequenceNumberMap.put(newShardId, newShard.sequenceNumberRange().startingSequenceNumber());
        newStartCheckpoint = new KinesisCheckpoint(shardToSequenceNumberMap);
        int partitionGroupId = getPartitionGroupIdFromShardId(newShardId);
        newPartitionGroupInfos.add(new PartitionGroupInfo(partitionGroupId, newStartCheckpoint));
      }
    }
    return newPartitionGroupInfos;
  }

  /**
   * Converts a shardId string to a partitionGroupId integer by parsing the digits of the shardId
   * e.g. "shardId-000000000001" becomes 1
   */
  private int getPartitionGroupIdFromShardId(String shardId) {
    String shardIdNum = StringUtils.stripStart(StringUtils.removeStart(shardId, "shardId-"), "0");
    return shardIdNum.isEmpty() ? 0 : Integer.parseInt(shardIdNum);
  }

  private boolean consumedEndOfShard(Checkpoint startCheckpoint, PartitionGroupMetadata partitionGroupMetadata)
      throws IOException, TimeoutException {
    PartitionGroupConsumer partitionGroupConsumer =
        _kinesisStreamConsumerFactory.createPartitionGroupConsumer(_clientId, partitionGroupMetadata);

    MessageBatch messageBatch;
    try {
      messageBatch = partitionGroupConsumer.fetchMessages(startCheckpoint, null, _fetchTimeoutMs);
    } finally {
      partitionGroupConsumer.close();
    }
    return messageBatch.isEndOfPartitionGroup();
  }

  @Override
  public void close() {

  }
}
