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
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupInfo;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import software.amazon.awssdk.services.kinesis.model.Shard;


public class KinesisStreamMetadataProvider implements StreamMetadataProvider {
  private final KinesisConnectionHandler _kinesisConnectionHandler;

  public KinesisStreamMetadataProvider(String clientId, KinesisConfig kinesisConfig) {
    _kinesisConnectionHandler = new KinesisConnectionHandler(kinesisConfig.getStream(), kinesisConfig.getAwsRegion());
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PartitionGroupInfo> getPartitionGroupInfoList(String clientId, StreamConfig streamConfig,
      List<PartitionGroupMetadata> currentPartitionGroupsMetadata, int timeoutMillis)
      throws IOException {

    Map<Integer, PartitionGroupMetadata> currentPartitionGroupMap =
        currentPartitionGroupsMetadata.stream().collect(Collectors.toMap(PartitionGroupMetadata::getPartitionGroupId, p -> p));

    List<PartitionGroupInfo> newPartitionGroupInfos = new ArrayList<>();
    List<Shard> shards = _kinesisConnectionHandler.getShards();
    for (Shard shard : shards) { // go over all shards
      String shardId = shard.shardId();
      int partitionGroupId = getPartitionGroupIdFromShardId(shardId);
      PartitionGroupMetadata currentPartitionGroupMetadata = currentPartitionGroupMap.get(partitionGroupId);
      KinesisCheckpoint newStartCheckpoint;
      if (currentPartitionGroupMetadata != null) { // existing shard
        KinesisCheckpoint currentEndCheckpoint = null;
        try {
          currentEndCheckpoint = new KinesisCheckpoint(currentPartitionGroupMetadata.getEndCheckpoint());
        } catch (Exception e) {
          // ignore. No end checkpoint yet for IN_PROGRESS segment
        }
        if (currentEndCheckpoint != null) { // end checkpoint available i.e. committing segment
          String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
          if (endingSequenceNumber != null) { // shard has ended
            // FIXME: this logic is not working
            //  was expecting sequenceNumOfLastMsgInShard == endSequenceNumOfShard.
            //  But it is much lesser than the endSeqNumOfShard
            Map<String, String> shardToSequenceNumberMap = new HashMap<>();
            shardToSequenceNumberMap.put(shardId, endingSequenceNumber);
            KinesisCheckpoint shardEndCheckpoint = new KinesisCheckpoint(shardToSequenceNumberMap);
            if (currentEndCheckpoint.compareTo(shardEndCheckpoint) >= 0) {
              // shard has ended AND we have reached the end checkpoint.
              // skip this partition group in the result
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
      newPartitionGroupInfos
          .add(new PartitionGroupInfo(partitionGroupId, newStartCheckpoint.serialize()));
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
