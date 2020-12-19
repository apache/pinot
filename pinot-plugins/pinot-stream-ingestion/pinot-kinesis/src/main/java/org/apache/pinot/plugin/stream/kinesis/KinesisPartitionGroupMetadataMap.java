package org.apache.pinot.plugin.stream.kinesis;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadataMap;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;


public class KinesisPartitionGroupMetadataMap extends KinesisConnectionHandler implements PartitionGroupMetadataMap {
  private final List<PartitionGroupMetadata> _stringPartitionGroupMetadataIndex = new ArrayList<>();

  public KinesisPartitionGroupMetadataMap(String stream, String awsRegion) {
    super(stream, awsRegion);
    List<Shard> shardList = getShards();
    for (Shard shard : shardList) {
      String startSequenceNumber = shard.sequenceNumberRange().startingSequenceNumber();
      String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
      KinesisShardMetadata shardMetadata = new KinesisShardMetadata(shard.shardId(), stream, awsRegion);
      shardMetadata.setStartCheckpoint(new KinesisCheckpoint(startSequenceNumber));
      shardMetadata.setEndCheckpoint(new KinesisCheckpoint(endingSequenceNumber));
      _stringPartitionGroupMetadataIndex.add(shardMetadata);
    }
  }

  @Override
  public List<PartitionGroupMetadata> getMetadataList() {
    return _stringPartitionGroupMetadataIndex;
  }

  @Override
  public PartitionGroupMetadata getPartitionGroupMetadata(int index) {
    return _stringPartitionGroupMetadataIndex.get(index);
  }
}
