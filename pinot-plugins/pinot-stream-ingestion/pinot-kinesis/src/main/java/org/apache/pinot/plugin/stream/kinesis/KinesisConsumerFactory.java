package org.apache.pinot.plugin.stream.kinesis;

import java.util.Map;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.v2.ConsumerV2;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadataMap;
import org.apache.pinot.spi.stream.v2.SegmentNameGenerator;
import org.apache.pinot.spi.stream.v2.StreamConsumerFactoryV2;


public class KinesisConsumerFactory implements StreamConsumerFactoryV2 {
  private StreamConfig _streamConfig;
  private final String AWS_REGION = "aws-region";

  @Override
  public void init(StreamConfig streamConfig) {
    _streamConfig = streamConfig;
  }

  @Override
  public PartitionGroupMetadataMap getPartitionGroupsMetadata(
      PartitionGroupMetadataMap currentPartitionGroupsMetadata) {
    return new KinesisPartitionGroupMetadataMap(_streamConfig.getTopicName(),
        _streamConfig.getStreamConfigsMap().getOrDefault(AWS_REGION, "global"));
  }

  @Override
  public SegmentNameGenerator getSegmentNameGenerator() {
    return null;
  }

  @Override
  public ConsumerV2 createConsumer(PartitionGroupMetadata metadata) {
    return new KinesisConsumer(_streamConfig.getTopicName(), _streamConfig, metadata);
  }
}
