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
package org.apache.pinot.plugin.stream.pulsar;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StreamMetadataProvider} implementation for the Pulsar stream
 */
public class PulsarStreamMetadataProvider extends PulsarPartitionLevelConnectionHandler implements StreamMetadataProvider {
  private Logger LOGGER = LoggerFactory.getLogger(PulsarStreamMetadataProvider.class);

  private StreamConfig _streamConfig;
  private int _partition;

  public PulsarStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    super(clientId, streamConfig, 0);
    _streamConfig = streamConfig;
  }

  public PulsarStreamMetadataProvider(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition);
    _streamConfig = streamConfig;
    _partition = partition;
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    try {
      return _pulsarClient.getPartitionsForTopic(_streamConfig.getTopicName()).get().size();
    } catch (Exception e) {
      throw new RuntimeException("Cannot fetch partitions for topic: " + _streamConfig.getTopicName(), e);
    }
  }

  public synchronized long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis) {
    throw new UnsupportedOperationException("The use of this method is not supported");
  }

  /**
   * Fetch the messageId and use it as offset.
   * If offset criteria is smallest, the message id of earliest record in the partition is returned.
   * If offset criteria is largest, the message id of the largest record in the partition is returned.
   * throws {@link IllegalArgumentException} if the offset criteria is invalid
   * throws {@link PulsarClientException} if there was an error from pulsar server on requesting message ids.
   * @param offsetCriteria offset criteria to fetch {@link StreamPartitionMsgOffset}.
   *                       Depends on the semantics of the stream e.g. smallest, largest for Kafka
   * @param timeoutMillis fetch timeout
   */
  @Override
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(@Nonnull OffsetCriteria offsetCriteria,
      long timeoutMillis) {
    Preconditions.checkNotNull(offsetCriteria);
    try {
      MessageId offset = null;
      if (offsetCriteria.isLargest()) {
        _reader.seek(MessageId.latest);
        if (_reader.hasMessageAvailable()) {
          offset = _reader.readNext().getMessageId();
        }
      } else if (offsetCriteria.isSmallest()) {
        _reader.seek(MessageId.earliest);
        if (_reader.hasMessageAvailable()) {
          offset = _reader.readNext().getMessageId();
        }
      } else {
        throw new IllegalArgumentException("Unknown initial offset value " + offsetCriteria.toString());
      }
      return new MessageIdStreamOffset(offset);
    } catch (PulsarClientException e) {
      LOGGER.error("Cannot fetch offsets for partition " + _partition + " and topic " + _topic + " and offsetCriteria "
          + offsetCriteria.toString(), e);
      return null;
    }
  }

  /**
   * We assume 1:1 mapping from partitionGroupId to partitionId in pulsar. This works because pulsar doesn't have
   * the concept of child partitions as well as reducing the current number of partition.
   */
  @Override
  public List<PartitionGroupMetadata> computePartitionGroupMetadata(String clientId, StreamConfig streamConfig,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatuses, int timeoutMillis)
      throws TimeoutException, IOException {
    List<PartitionGroupMetadata> newPartitionGroupMetadataList = new ArrayList<>();

    for (PartitionGroupConsumptionStatus partitionGroupConsumptionStatus : partitionGroupConsumptionStatuses) {
      newPartitionGroupMetadataList.add(
          new PartitionGroupMetadata(partitionGroupConsumptionStatus.getPartitionGroupId(),
              partitionGroupConsumptionStatus.getStartOffset()));
    }

    try {
      List<String> partitionedTopicNameList = _pulsarClient.getPartitionsForTopic(_topic).get();

      if (partitionedTopicNameList.size() > partitionGroupConsumptionStatuses.size()) {
        int newPartitionStartIndex = partitionGroupConsumptionStatuses.size();

        for (int p = newPartitionStartIndex; p < partitionedTopicNameList.size(); p++) {

          Reader reader =
              _pulsarClient.newReader().topic(getPartitionedTopicName(p)).startMessageId(_config.getInitialMessageId())
                  .create();

          if (reader.hasMessageAvailable()) {
            Message message = reader.readNext();
            newPartitionGroupMetadataList
                .add(new PartitionGroupMetadata(p, new MessageIdStreamOffset(message.getMessageId())));
          }
        }
      }
    } catch (Exception e) {
      // No partition found
    }

    return newPartitionGroupMetadataList;
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
