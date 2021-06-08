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
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarStreamMetadataProvider extends PulsarPartitionLevelConnectionHandler implements StreamMetadataProvider {
  private Logger LOGGER = LoggerFactory.getLogger(PulsarStreamMetadataProvider.class);

  private String _clientId;
  private StreamConfig _streamConfig;
  private int _partition;

  public PulsarStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    super(clientId, streamConfig, 0);
    _clientId = clientId;
    _streamConfig = streamConfig;
  }

  public PulsarStreamMetadataProvider(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition);
    _clientId = clientId;
    _streamConfig = streamConfig;
    _partition = partition;
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    try {
      return _pulsarClient.getPartitionsForTopic(_streamConfig.getTopicName()).get().size();
    } catch (Exception e) {
      //TODO: Handle error
      return 0;
    }
  }

  public synchronized long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis) {
    throw new UnsupportedOperationException("The use of this method is not supported");
  }

  @Override
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(@Nonnull OffsetCriteria offsetCriteria,
      long timeoutMillis) {
    Preconditions.checkNotNull(offsetCriteria);
    try {
      MessageId offset = null;
      if (offsetCriteria.isLargest()) {
        _reader.seek(MessageId.earliest);
        if (_reader.hasMessageAvailable()) {
          offset = _reader.readNext().getMessageId();
        }
      } else if (offsetCriteria.isSmallest()) {
        _reader.seek(MessageId.latest);
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
   * @param clientId
   * @param streamConfig
   * @param partitionGroupConsumptionStatuses list of {@link PartitionGroupConsumptionStatus} for current partition groups
   * @param timeoutMillis
   * @return
   * @throws TimeoutException
   * @throws IOException
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
      //No partition found with id p
    }

    return newPartitionGroupMetadataList;
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
