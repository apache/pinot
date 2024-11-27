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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;


public class PulsarUtils {
  private PulsarUtils() {
  }

  public static SubscriptionInitialPosition offsetCriteriaToSubscription(OffsetCriteria offsetCriteria) {
    if (offsetCriteria.isLargest()) {
      return SubscriptionInitialPosition.Latest;
    }
    if (offsetCriteria.isSmallest()) {
      return SubscriptionInitialPosition.Earliest;
    }
    throw new IllegalArgumentException("Unsupported offset criteria: " + offsetCriteria);
  }

  /**
   * Stitch key and value bytes together using a simple format:
   * 4 bytes for key length + key bytes + 4 bytes for value length + value bytes
   */
  public static byte[] stitchKeyValue(byte[] keyBytes, byte[] valueBytes) {
    byte[] stitchedBytes = new byte[8 + keyBytes.length + valueBytes.length];
    ByteBuffer buffer = ByteBuffer.wrap(stitchedBytes);
    buffer.putInt(keyBytes.length);
    buffer.put(keyBytes);
    buffer.putInt(valueBytes.length);
    buffer.put(valueBytes);
    return stitchedBytes;
  }

  public static BytesStreamMessage buildPulsarStreamMessage(Message<byte[]> message, PulsarConfig config) {
    byte[] key = message.getKeyBytes();
    byte[] value = message.getData();
    if (config.getEnableKeyValueStitch()) {
      value = stitchKeyValue(key, value);
    }
    return new BytesStreamMessage(key, value, extractMessageMetadata(message, config));
  }

  @VisibleForTesting
  static StreamMessageMetadata extractMessageMetadata(Message<byte[]> message, PulsarConfig config) {
    long recordIngestionTimeMs = message.getBrokerPublishTime().orElse(message.getPublishTime());
    MessageId messageId = message.getMessageId();
    MessageIdStreamOffset offset = new MessageIdStreamOffset(messageId);
    MessageIdStreamOffset nextOffset = new MessageIdStreamOffset(getNextMessageId(messageId));
    StreamMessageMetadata.Builder builder =
        new StreamMessageMetadata.Builder().setRecordIngestionTimeMs(recordIngestionTimeMs)
            .setOffset(offset, nextOffset).setSerializedValueSize(message.size());
    if (config.isPopulateMetadata()) {
      Map<String, String> properties = message.getProperties();
      if (!properties.isEmpty()) {
        GenericRow header = new GenericRow();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          header.putValue(entry.getKey(), entry.getValue());
        }
        builder.setHeaders(header);
      }
      Set<PulsarStreamMessageMetadata.PulsarMessageMetadataValue> metadataFields = config.getMetadataFields();
      if (!metadataFields.isEmpty()) {
        Map<String, String> metadataMap = Maps.newHashMapWithExpectedSize(metadataFields.size());
        for (PulsarStreamMessageMetadata.PulsarMessageMetadataValue metadataField : metadataFields) {
          populateMetadataField(message, metadataField, metadataMap);
        }
        builder.setMetadata(metadataMap);
      }
    }
    return builder.build();
  }

  /**
   * Returns next message id supposed to be present in the pulsar topic partition.
   *
   * The message id is composed of 3 parts - ledgerId, entryId and partitionId.
   * The ledger id are always increasing in number but may not be sequential. e.g. for first 10 records ledger id can
   * be 12 but for next 10 it can be 18. Each entry inside a ledger is always in a sequential and increases by 1 for
   * next message.
   * The partition id is fixed for a particular partition.
   * We return entryId incremented by 1 while keeping ledgerId and partitionId as same.
   * If ledgerId has incremented, the {@link Reader} takes care of that during seek operation, and returns the first
   * record in the new ledger.
   */
  public static MessageId getNextMessageId(MessageId messageId) {
    MessageIdAdv messageIdAdv = (MessageIdAdv) messageId;
    long ledgerId = messageIdAdv.getLedgerId();
    long entryId = messageIdAdv.getEntryId();
    int partitionIndex = messageIdAdv.getPartitionIndex();
    int batchSize = messageIdAdv.getBatchSize();
    if (batchSize > 0) {
      int batchIndex = messageIdAdv.getBatchIndex();
      BitSet ackSet = messageIdAdv.getAckSet();
      if (batchIndex < batchSize - 1) {
        return new BatchMessageIdImpl(ledgerId, entryId, partitionIndex, batchIndex + 1, batchSize, ackSet);
      } else {
        return new BatchMessageIdImpl(ledgerId, entryId + 1, partitionIndex, 0, batchSize, ackSet);
      }
    } else {
      return new MessageIdImpl(ledgerId, entryId + 1, partitionIndex);
    }
  }

  private static void populateMetadataField(Message<byte[]> message,
      PulsarStreamMessageMetadata.PulsarMessageMetadataValue metadataField, Map<String, String> metadataMap) {
    switch (metadataField) {
      case PUBLISH_TIME:
        long publishTime = message.getPublishTime();
        if (publishTime > 0) {
          setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.PUBLISH_TIME,
              publishTime);
        }
        break;
      case EVENT_TIME:
        long eventTime = message.getEventTime();
        if (eventTime > 0) {
          setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.EVENT_TIME,
              eventTime);
        }
        break;
      case BROKER_PUBLISH_TIME:
        message.getBrokerPublishTime().ifPresent(brokerPublishTime -> setMetadataMapField(metadataMap,
            PulsarStreamMessageMetadata.PulsarMessageMetadataValue.BROKER_PUBLISH_TIME, brokerPublishTime));
        break;
      case MESSAGE_KEY:
        setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.MESSAGE_KEY,
            message.getKey());
        break;
      case MESSAGE_ID:
        setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.MESSAGE_ID,
            message.getMessageId().toString());
        break;
      case MESSAGE_ID_BYTES_B64:
        setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.MESSAGE_ID_BYTES_B64,
            message.getMessageId().toByteArray());
        break;
      case PRODUCER_NAME:
        setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.PRODUCER_NAME,
            message.getProducerName());
        break;
      case SCHEMA_VERSION:
        setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.SCHEMA_VERSION,
            message.getSchemaVersion());
        break;
      case SEQUENCE_ID:
        setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.SEQUENCE_ID,
            message.getSequenceId());
        break;
      case ORDERING_KEY:
        if (message.hasOrderingKey()) {
          setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.ORDERING_KEY,
              message.getOrderingKey());
        }
        break;
      case SIZE:
        setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.SIZE, message.size());
        break;
      case TOPIC_NAME:
        setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.TOPIC_NAME,
            message.getTopicName());
        break;
      case INDEX:
        message.getIndex().ifPresent(
            index -> setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.INDEX,
                index));
        break;
      case REDELIVERY_COUNT:
        setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.REDELIVERY_COUNT,
            message.getRedeliveryCount());
        break;
      default:
        throw new IllegalArgumentException("Unsupported metadata field: " + metadataField);
    }
  }

  private static void setMetadataMapField(Map<String, String> metadataMap,
      PulsarStreamMessageMetadata.PulsarMessageMetadataValue metadataValue, String value) {
    if (StringUtils.isNotEmpty(value)) {
      metadataMap.put(metadataValue.getKey(), value);
    }
  }

  private static void setMetadataMapField(Map<String, String> metadataMap,
      PulsarStreamMessageMetadata.PulsarMessageMetadataValue metadataValue, int value) {
    setMetadataMapField(metadataMap, metadataValue, Integer.toString(value));
  }

  private static void setMetadataMapField(Map<String, String> metadataMap,
      PulsarStreamMessageMetadata.PulsarMessageMetadataValue metadataValue, long value) {
    setMetadataMapField(metadataMap, metadataValue, Long.toString(value));
  }

  private static void setMetadataMapField(Map<String, String> metadataMap,
      PulsarStreamMessageMetadata.PulsarMessageMetadataValue metadataValue, byte[] value) {
    if (value != null && value.length > 0) {
      setMetadataMapField(metadataMap, metadataValue, Base64.getEncoder().encodeToString(value));
    }
  }
}
