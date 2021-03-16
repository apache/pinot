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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;


/**
 * Interface for provider of stream metadata such as partition count, partition offsets
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface StreamMetadataProvider extends Closeable {
  /**
   * Fetches the number of partitions for a topic given the stream configs
   * @param timeoutMillis
   * @return
   */
  int fetchPartitionCount(long timeoutMillis);

  // Issue 5953 Retain this interface for 0.5.0, remove in 0.6.0
  @Deprecated
  long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis)
      throws java.util.concurrent.TimeoutException;
  /**
   * Fetches the offset for a given partition and offset criteria
   * @param offsetCriteria
   * @param timeoutMillis
   * @return
   * @throws java.util.concurrent.TimeoutException
   */
  default StreamPartitionMsgOffset fetchStreamPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis)
      throws java.util.concurrent.TimeoutException {
    long offset = fetchPartitionOffset(offsetCriteria, timeoutMillis);
    return new LongMsgOffset(offset);
  }

  /**
   * Fetch the list of partition group info for the latest state of the stream.
   * Default behavior is the one for the Kafka stream, where each partition group contains only one partition
   * @param currentPartitionGroupsMetadata The list of metadata for the current partition groups
   */
  default List<PartitionGroupInfo> getPartitionGroupInfoList(String clientId, StreamConfig streamConfig,
      List<PartitionGroupMetadata> currentPartitionGroupsMetadata, int timeoutMillis)
      throws TimeoutException, IOException {
    int partitionCount = fetchPartitionCount(timeoutMillis);
    List<PartitionGroupInfo> newPartitionGroupInfoList = new ArrayList<>(partitionCount);

    // Add a PartitionGroupInfo into the list foreach partition already present in current.
    for (PartitionGroupMetadata currentPartitionGroupMetadata : currentPartitionGroupsMetadata) {
      newPartitionGroupInfoList.add(new PartitionGroupInfo(currentPartitionGroupMetadata.getPartitionGroupId(),
          currentPartitionGroupMetadata.getEndOffset()));
    }
    // Add PartitionGroupInfo for new partitions
    // Use offset criteria from stream config
    StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    for (int i = currentPartitionGroupsMetadata.size(); i < partitionCount; i++) {
      StreamMetadataProvider partitionMetadataProvider =
          streamConsumerFactory.createPartitionMetadataProvider(clientId, i);
      StreamPartitionMsgOffset streamPartitionMsgOffset =
          partitionMetadataProvider.fetchStreamPartitionOffset(streamConfig.getOffsetCriteria(), timeoutMillis);
      newPartitionGroupInfoList.add(new PartitionGroupInfo(i, streamPartitionMsgOffset));
    }
    return newPartitionGroupInfoList;
  }

}
