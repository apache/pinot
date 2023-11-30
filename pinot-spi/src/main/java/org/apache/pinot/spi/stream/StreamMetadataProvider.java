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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
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
   * @param timeoutMillis Fetch timeout
   * @return number of partitions
   */
  int fetchPartitionCount(long timeoutMillis);

  /**
   * Fetches the partition ids for a topic given the stream configs.
   */
  default Set<Integer> fetchPartitionIds(long timeoutMillis) {
    throw new UnsupportedOperationException();
  }

  /**
   * Fetches the offset for a given partition and offset criteria
   * @param offsetCriteria offset criteria to fetch{@link StreamPartitionMsgOffset}.
   *                       Depends on the semantics of the stream e.g. smallest, largest for Kafka
   * @param timeoutMillis fetch timeout
   * @return {@link StreamPartitionMsgOffset} based on the offset criteria provided
   * @throws TimeoutException if timed out trying to connect and fetch from stream
   */
  StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis)
      throws TimeoutException;

  /**
   * Computes the list of {@link PartitionGroupMetadata} for the latest state of the stream, using the current
   * {@link PartitionGroupConsumptionStatus}
   *
   * Default behavior is the one for the Kafka stream, where each partition group contains only one partition
   * @param partitionGroupConsumptionStatuses list of {@link PartitionGroupConsumptionStatus} for current partition
   *                                          groups
   */
  default List<PartitionGroupMetadata> computePartitionGroupMetadata(String clientId, StreamConfig streamConfig,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatuses, int timeoutMillis)
      throws IOException, TimeoutException {
    int partitionCount = fetchPartitionCount(timeoutMillis);
    List<PartitionGroupMetadata> newPartitionGroupMetadataList = new ArrayList<>(partitionCount);

    // Add a PartitionGroupMetadata into the list, foreach partition already present in current.
    // Setting endOffset (exclusive) as the startOffset for new partition group.
    // If partition group is still in progress, this value will be null
    for (PartitionGroupConsumptionStatus currentPartitionGroupConsumptionStatus : partitionGroupConsumptionStatuses) {
      newPartitionGroupMetadataList.add(
          new PartitionGroupMetadata(currentPartitionGroupConsumptionStatus.getPartitionGroupId(),
              currentPartitionGroupConsumptionStatus.getEndOffset()));
    }
    // Add PartitionGroupMetadata for new partitions
    // Use offset criteria from stream config
    StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    for (int i = partitionGroupConsumptionStatuses.size(); i < partitionCount; i++) {
      try (StreamMetadataProvider partitionMetadataProvider = streamConsumerFactory.createPartitionMetadataProvider(
          clientId, i)) {
        StreamPartitionMsgOffset streamPartitionMsgOffset =
            partitionMetadataProvider.fetchStreamPartitionOffset(streamConfig.getOffsetCriteria(), timeoutMillis);
        newPartitionGroupMetadataList.add(new PartitionGroupMetadata(i, streamPartitionMsgOffset));
      }
    }
    return newPartitionGroupMetadataList;
  }

  default Map<String, PartitionLagState> getCurrentPartitionLagState(
      Map<String, ConsumerPartitionState> currentPartitionStateMap) {
    Map<String, PartitionLagState> result = new HashMap<>();
    PartitionLagState unknownLagState = new UnknownLagState();
    currentPartitionStateMap.forEach((k, v) -> result.put(k, unknownLagState));
    return result;
  }

  class UnknownLagState extends PartitionLagState {
  }
}
