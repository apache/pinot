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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.util.ConsumerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StreamMetadataProvider} implementation for the Pulsar stream
 */
public class PulsarStreamMetadataProvider extends PulsarPartitionLevelConnectionHandler
    implements StreamMetadataProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarStreamMetadataProvider.class);

  private final int _partition;
  private final String _topic;

  public PulsarStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    this(clientId, streamConfig, 0);
  }

  public PulsarStreamMetadataProvider(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig);
    _topic = _config.getPulsarTopicName();
    _partition = partition;
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    try {
      return _pulsarClient.getPartitionsForTopic(_topic).get().size();
    } catch (Exception e) {
      throw new RuntimeException("Cannot fetch partitions for topic: " + _topic, e);
    }
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
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis) {
    Preconditions.checkNotNull(offsetCriteria);
    String subscription = "Pinot_" + UUID.randomUUID();
    MessageId offset;
    try (Consumer consumer = _pulsarClient.newConsumer().topic(_topic)
        .subscriptionInitialPosition(PulsarUtils.offsetCriteriaToSubscription(offsetCriteria))
        .subscriptionMode(SubscriptionMode.NonDurable)  // automatically deletes subscription on consumer close
        .subscriptionName(subscription).subscribe()) {
      if (offsetCriteria.isLargest()) {
        offset = consumer.getLastMessageId();
      } else if (offsetCriteria.isSmallest()) {
        offset = consumer.receive((int) timeoutMillis, TimeUnit.MILLISECONDS).getMessageId();
      } else {
        throw new IllegalArgumentException("Unknown initial offset value " + offsetCriteria);
      }
      return new MessageIdStreamOffset(offset);
    } catch (PulsarClientException e) {
      LOGGER.error("Cannot fetch offsets for partition " + _partition + " and topic " + _topic + " and offsetCriteria "
          + offsetCriteria, e);
      return null;
    }
  }

  /**
   * We assume 1:1 mapping from partitionGroupId to partitionId in pulsar. This works because pulsar doesn't have
   * the concept of child partitions as well as reducing the current number of partition.
   */
  @Override
  public List<PartitionGroupMetadata> computePartitionGroupMetadata(String clientId, StreamConfig streamConfig,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatuses, int timeoutMillis) {
    List<PartitionGroupMetadata> newPartitionGroupMetadataList = new ArrayList<>();

    for (PartitionGroupConsumptionStatus partitionGroupConsumptionStatus : partitionGroupConsumptionStatuses) {
      newPartitionGroupMetadataList.add(
          new PartitionGroupMetadata(partitionGroupConsumptionStatus.getPartitionGroupId(),
              partitionGroupConsumptionStatus.getStartOffset()));
    }

    String subscription = ConsumerName.generateRandomName();
    try {
      List<String> partitionedTopicNameList = _pulsarClient.getPartitionsForTopic(_topic).get();

      if (partitionedTopicNameList.size() > partitionGroupConsumptionStatuses.size()) {
        int newPartitionStartIndex = partitionGroupConsumptionStatuses.size();

        for (int p = newPartitionStartIndex; p < partitionedTopicNameList.size(); p++) {
          try (Consumer consumer = _pulsarClient.newConsumer().topic(partitionedTopicNameList.get(p))
              .subscriptionInitialPosition(_config.getInitialSubscriberPosition())
              .subscriptionMode(SubscriptionMode.NonDurable).subscriptionName(subscription).subscribe()) {

            Message message = consumer.receive(timeoutMillis, TimeUnit.MILLISECONDS);
            if (message != null) {
              newPartitionGroupMetadataList.add(
                  new PartitionGroupMetadata(p, new MessageIdStreamOffset(message.getMessageId())));
            } else {
              MessageId lastMessageId;
              try {
                lastMessageId = (MessageId) consumer.getLastMessageIdAsync().get(timeoutMillis, TimeUnit.MILLISECONDS);
              } catch (TimeoutException t) {
                lastMessageId = MessageId.latest;
              }
              newPartitionGroupMetadataList.add(
                  new PartitionGroupMetadata(p, new MessageIdStreamOffset(lastMessageId)));
            }
          } catch (PulsarClientException pce) {
            LOGGER.warn(
                "Error encountered while calculating partition group metadata for topic " + _config.getPulsarTopicName()
                    + " partition " + partitionedTopicNameList.get(p), pce);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Error encountered when trying to fetch partition list for pulsar topic " + _topic, e);
    }
    return newPartitionGroupMetadataList;
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
