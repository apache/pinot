package org.apache.pinot.spi.stream.v2;

import java.util.Map;
import org.apache.pinot.spi.stream.StreamConfig;


public interface StreamConsumerFactoryV2 {
  void init(StreamConfig streamConfig);

  // takes the current state of partition groups (groupings of shards, the state of the consumption) and creates the new state
  Map<Long, PartitionGroupMetadata> getPartitionGroupsMetadata(Map<Long, PartitionGroupMetadata> currentPartitionGroupsMetadata);

  // creates a name generator which generates segment name for a partition group
  SegmentNameGenerator getSegmentNameGenerator();

  // creates a consumer which consumes from a partition group
  ConsumerV2 createConsumer(PartitionGroupMetadata metadata);

}
