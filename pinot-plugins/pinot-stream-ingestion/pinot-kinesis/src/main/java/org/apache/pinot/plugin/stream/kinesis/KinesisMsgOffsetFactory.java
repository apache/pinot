package org.apache.pinot.plugin.stream.kinesis;

import java.io.IOException;
import org.apache.pinot.spi.stream.PartitionGroupCheckpointFactory;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;


/**
 * An implementation of the {@link PartitionGroupCheckpointFactory} for Kinesis stream
 */
public class KinesisMsgOffsetFactory implements StreamPartitionMsgOffsetFactory {

  KinesisConfig _kinesisConfig;

  @Override
  public void init(StreamConfig streamConfig) {
    _kinesisConfig = new KinesisConfig(streamConfig);
  }

  @Override
  public StreamPartitionMsgOffset create(String offsetStr) {
    try {
      return new KinesisCheckpoint(offsetStr);
    }catch (IOException e){
      return null;
    }
  }

  @Override
  public StreamPartitionMsgOffset create(StreamPartitionMsgOffset other) {
    return new KinesisCheckpoint(((KinesisCheckpoint) other).getShardToStartSequenceMap());
  }

}
