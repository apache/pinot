package org.apache.pinot.spi.stream.v2;

import java.util.List;


public interface PartitionGroupMetadataMap {

  List<PartitionGroupMetadata> getMetadataList();

  PartitionGroupMetadata getPartitionGroupMetadata(int index);

}
