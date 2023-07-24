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

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pulsar.client.api.Message;

public interface PulsarMetadataExtractor {
  static PulsarMetadataExtractor build(boolean populateMetadata,
      Set<PulsarStreamMessageMetadata.PulsarMessageMetadataValue> metadataValuesToExtract) {
    return message -> {
      long publishTime = message.getPublishTime();
      long brokerPublishTime = message.getBrokerPublishTime().orElse(0L);
      long recordTimestamp = brokerPublishTime != 0 ? brokerPublishTime : publishTime;

      Map<String, String> metadataMap = populateMetadataMap(populateMetadata, message, metadataValuesToExtract);

      GenericRow headerGenericRow = populateMetadata ? buildGenericRow(message) : null;
      return new PulsarStreamMessageMetadata(recordTimestamp, headerGenericRow, metadataMap);
    };
  }

  RowMetadata extract(Message<?> record);

  static GenericRow buildGenericRow(Message<?> message) {
    if (MapUtils.isEmpty(message.getProperties())) {
      return null;
    }
    GenericRow genericRow = new GenericRow();
    for (Map.Entry<String, String> entry : message.getProperties().entrySet()) {
      genericRow.putValue(entry.getKey(), entry.getValue());
    }
    return genericRow;
  }

  static Map<String, String> populateMetadataMap(boolean populateAllFields, Message<?> message,
      Set<PulsarStreamMessageMetadata.PulsarMessageMetadataValue> metadataValuesToExtract) {

    Map<String, String> metadataMap = new HashMap<>();
    populateMetadataField(PulsarStreamMessageMetadata.PulsarMessageMetadataValue.EVENT_TIME, message, metadataMap);
    populateMetadataField(PulsarStreamMessageMetadata.PulsarMessageMetadataValue.PUBLISH_TIME, message, metadataMap);
    populateMetadataField(PulsarStreamMessageMetadata.PulsarMessageMetadataValue.BROKER_PUBLISH_TIME, message,
        metadataMap);
    populateMetadataField(PulsarStreamMessageMetadata.PulsarMessageMetadataValue.MESSAGE_KEY, message, metadataMap);

    // Populate some timestamps for lag calculation even if populateMetadata is false

    if (!populateAllFields) {
      return metadataMap;
    }

    for (PulsarStreamMessageMetadata.PulsarMessageMetadataValue metadataValue : metadataValuesToExtract) {
      populateMetadataField(metadataValue, message, metadataMap);
    }

    return metadataMap;
  }

  private static void populateMetadataField(PulsarStreamMessageMetadata.PulsarMessageMetadataValue value,
      Message<?> message, Map<String, String> metadataMap) {
    switch (value) {
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
        message.getBrokerPublishTime()
            .ifPresent(brokerPublishTime -> setMetadataMapField(metadataMap,
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
        setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.SIZE,
            message.size());
        break;
      case TOPIC_NAME:
        setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.TOPIC_NAME,
            message.getTopicName());
        break;
      case INDEX:
        message.getIndex().ifPresent(index -> setMetadataMapField(metadataMap,
            PulsarStreamMessageMetadata.PulsarMessageMetadataValue.INDEX, index));
        break;
      case REDELIVERY_COUNT:
        setMetadataMapField(metadataMap, PulsarStreamMessageMetadata.PulsarMessageMetadataValue.REDELIVERY_COUNT,
            message.getRedeliveryCount());
        break;
      default:
        throw new IllegalArgumentException("Unsupported metadata value: " + value);
    }
  }

  private static void setMetadataMapField(Map<String, String> metadataMap,
      PulsarStreamMessageMetadata.PulsarMessageMetadataValue metadataValue,
      String value) {
    if (StringUtils.isNotBlank(value)) {
      metadataMap.put(metadataValue.getKey(), value);
    }
  }

  private static void setMetadataMapField(Map<String, String> metadataMap,
      PulsarStreamMessageMetadata.PulsarMessageMetadataValue metadataValue,
      int value) {
    setMetadataMapField(metadataMap, metadataValue, String.valueOf(value));
  }

  private static void setMetadataMapField(Map<String, String> metadataMap,
      PulsarStreamMessageMetadata.PulsarMessageMetadataValue metadataValue,
      long value) {
    setMetadataMapField(metadataMap, metadataValue, String.valueOf(value));
  }

  private static void setMetadataMapField(Map<String, String> metadataMap,
      PulsarStreamMessageMetadata.PulsarMessageMetadataValue metadataValue,
      byte[] value) {
    if (value != null && value.length > 0) {
      setMetadataMapField(metadataMap, metadataValue, Base64.getEncoder().encodeToString(value));
    }
  }
}
