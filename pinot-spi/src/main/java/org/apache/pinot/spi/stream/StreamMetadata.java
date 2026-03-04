/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.stream;

import java.util.List;


/**
 * Groups partition metadata for a single stream/topic.
 *
 * <p>This replaces the flat {@code List<PartitionGroupMetadata>} pattern where partitions from all streams were mixed
 * together and required partition ID padding to identify stream membership.
 *
 * <p>The {@link PartitionGroupMetadata} items within this container use Pinot-encoded partition IDs
 * (i.e., {@code streamIndex * 10000 + streamPartitionId}) to maintain backward compatibility with segment names
 * stored in ZooKeeper.
 */
public class StreamMetadata {

  private final StreamConfig _streamConfig;
  private final int _numPartitions;
  private final List<PartitionGroupMetadata> _partitionGroupMetadataList;

  public StreamMetadata(StreamConfig streamConfig, int numPartitions,
      List<PartitionGroupMetadata> partitionGroupMetadataList) {
    _streamConfig = streamConfig;
    _numPartitions = numPartitions;
    _partitionGroupMetadataList = List.copyOf(partitionGroupMetadataList);
  }

  public StreamConfig getStreamConfig() {
    return _streamConfig;
  }

  public String getTopicName() {
    return _streamConfig.getTopicName();
  }

  public List<PartitionGroupMetadata> getPartitionGroupMetadataList() {
    return _partitionGroupMetadataList;
  }

  /**
   * Returns the total number of partitions for this stream. This may be greater than the size of
   * {@link #getPartitionGroupMetadataList()} when only a subset of partitions is assigned.
   */
  public int getNumPartitions() {
    return _numPartitions;
  }
}
