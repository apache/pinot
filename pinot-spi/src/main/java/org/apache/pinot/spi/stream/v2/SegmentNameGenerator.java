package org.apache.pinot.spi.stream.v2;

public interface SegmentNameGenerator {
  // generates a unique name for a partition group based on the metadata
    String generateSegmentName(PartitionGroupMetadata metadata);

}
