package org.apache.pinot.controller.helix.core.realtime;

import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


public class PartitionGroupMetadataWrapper {
  public PartitionGroupMetadata metadata;
  int sequence;

  PartitionGroupMetadataWrapper(PartitionGroupMetadata metadata, int sequence) {
    this.metadata = metadata;
    this.sequence = sequence;
  }

  int getSequence() {
    return sequence;
  }

  int getPartitionGroupId() {
    return metadata.getPartitionGroupId();
  }

  StreamPartitionMsgOffset getStartOffset() {
    return metadata.getStartOffset();
  }
}
