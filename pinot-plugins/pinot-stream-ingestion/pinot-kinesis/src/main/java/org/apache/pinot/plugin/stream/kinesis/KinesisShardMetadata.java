package org.apache.pinot.plugin.stream.kinesis;

import org.apache.pinot.spi.stream.v2.Checkpoint;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadata;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;


public class KinesisShardMetadata extends KinesisConnectionHandler implements PartitionGroupMetadata {
  Checkpoint _startCheckpoint;
  Checkpoint _endCheckpoint;

  public KinesisShardMetadata(String shardId, String streamName) {
    GetShardIteratorResponse getShardIteratorResponse = _kinesisClient.getShardIterator(GetShardIteratorRequest.builder().shardId(shardId).streamName(streamName).build());
    _startCheckpoint = new KinesisCheckpoint(getShardIteratorResponse.shardIterator());
  }

  @Override
  public Checkpoint getStartCheckpoint() {
    return _startCheckpoint;
  }

  @Override
  public Checkpoint getEndCheckpoint() {
    return _endCheckpoint;
  }

  @Override
  public void setStartCheckpoint(Checkpoint startCheckpoint) {
    _startCheckpoint = startCheckpoint;
  }

  @Override
  public void setEndCheckpoint(Checkpoint endCheckpoint) {
    _endCheckpoint = endCheckpoint;
  }

  @Override
  public byte[] serialize() {
    return new byte[0];
  }

  @Override
  public PartitionGroupMetadata deserialize(byte[] blob) {
    return null;
  }
}
