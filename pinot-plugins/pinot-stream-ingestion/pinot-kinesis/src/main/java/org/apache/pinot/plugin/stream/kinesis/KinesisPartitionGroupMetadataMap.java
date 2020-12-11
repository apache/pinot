package org.apache.pinot.plugin.stream.kinesis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadataMap;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;


public class KinesisPartitionGroupMetadataMap extends KinesisConnectionHandler implements PartitionGroupMetadataMap {
  private Map<String, PartitionGroupMetadata> _stringPartitionGroupMetadataMap = new HashMap<>();

  public KinesisPartitionGroupMetadataMap(String stream, String awsRegion){
    super(awsRegion);
    ListShardsResponse listShardsResponse = _kinesisClient.listShards(ListShardsRequest.builder().streamName(stream).build());
    List<Shard> shardList = listShardsResponse.shards();
    for(Shard shard : shardList){
      String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
      KinesisShardMetadata shardMetadata = new KinesisShardMetadata(shard.shardId(), stream);
      shardMetadata.setEndCheckpoint(new KinesisCheckpoint(endingSequenceNumber));
      _stringPartitionGroupMetadataMap.put(shard.shardId(), shardMetadata);
    }
  }

  public Map<String, PartitionGroupMetadata> getPartitionMetadata(){
      return _stringPartitionGroupMetadataMap;
  }
}
