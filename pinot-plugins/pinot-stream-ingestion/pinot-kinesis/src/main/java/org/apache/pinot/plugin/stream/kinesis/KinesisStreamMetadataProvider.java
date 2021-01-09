package org.apache.pinot.plugin.stream.kinesis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
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
   * 1. It is a branch new shard i.e. no partitionGroupMetadata was found for it in the current list
   * 2. It is still being actively consumed from i.e. the consuming partition has not reached the end of the shard
   */
  @Override
  public List<PartitionGroupInfo> getPartitionGroupInfoList(String clientId, StreamConfig streamConfig,
      List<PartitionGroupMetadata> currentPartitionGroupsMetadata, int timeoutMillis)
      throws IOException, TimeoutException {

    Map<Integer, PartitionGroupMetadata> currentPartitionGroupMap = currentPartitionGroupsMetadata.stream()
        .collect(Collectors.toMap(PartitionGroupMetadata::getPartitionGroupId, p -> p));

    List<PartitionGroupInfo> newPartitionGroupInfos = new ArrayList<>();
    List<Shard> shards = _kinesisConnectionHandler.getShards();
    for (Shard shard : shards) {
      KinesisCheckpoint newStartCheckpoint;

      String shardId = shard.shardId();
      int partitionGroupId = getPartitionGroupIdFromShardId(shardId);
      PartitionGroupMetadata currentPartitionGroupMetadata = currentPartitionGroupMap.get(partitionGroupId);

      if (currentPartitionGroupMetadata != null) { // existing shard
        KinesisCheckpoint currentEndCheckpoint = null;
        try {
          currentEndCheckpoint = new KinesisCheckpoint(currentPartitionGroupMetadata.getEndCheckpoint());
        } catch (Exception e) {
          // ignore. No end checkpoint yet for IN_PROGRESS segment
        }
        if (currentEndCheckpoint != null) { // end checkpoint available i.e. committing/committed segment
          String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
          if (endingSequenceNumber != null) { // shard has ended
            // check if segment has consumed all the messages already
            PartitionGroupConsumer partitionGroupConsumer =
                _kinesisStreamConsumerFactory.createPartitionGroupConsumer(_clientId, currentPartitionGroupMetadata);

            MessageBatch messageBatch;
            try {
              messageBatch = partitionGroupConsumer.fetchMessages(currentEndCheckpoint, null, _fetchTimeoutMs);
            } finally {
              partitionGroupConsumer.close();
            }
            if (messageBatch.isEndOfPartitionGroup()) {
              // shard has ended. Skip it from results
              continue;
            }
          }
          newStartCheckpoint = currentEndCheckpoint;
        } else {
          newStartCheckpoint = new KinesisCheckpoint(currentPartitionGroupMetadata.getStartCheckpoint());
        }
      } else { // new shard
        Map<String, String> shardToSequenceNumberMap = new HashMap<>();
        shardToSequenceNumberMap.put(shardId, shard.sequenceNumberRange().startingSequenceNumber());
        newStartCheckpoint = new KinesisCheckpoint(shardToSequenceNumberMap);
      }

      newPartitionGroupInfos.add(new PartitionGroupInfo(partitionGroupId, newStartCheckpoint.serialize()));
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

  @Override
  public void close() {

  }
}
