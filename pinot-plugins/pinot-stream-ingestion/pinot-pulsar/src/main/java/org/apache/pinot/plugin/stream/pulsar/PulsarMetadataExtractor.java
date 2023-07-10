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

import java.util.HashMap;
import java.util.Map;
import java.util.Base64;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pulsar.client.api.Message;


public interface PulsarMetadataExtractor {
  static PulsarMetadataExtractor build(boolean populateMetadata) {
    return message -> {
      long publishTime = message.getPublishTime();
      long brokerPublishTime = message.getBrokerPublishTime().orElse(0L);
      long recordTimestamp = brokerPublishTime != 0 ? brokerPublishTime : publishTime;

      Map<String, String> metadataMap = populateMetadataMap(populateMetadata, message);

      if (!populateMetadata) {
        return new PulsarStreamMessageMetadata(recordTimestamp, null, metadataMap);
      }
      GenericRow headerGenericRow = buildGenericRow(message);
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

  static Map<String, String> populateMetadataMap(boolean populateAllFields, Message<?> message) {
    long eventTime = message.getEventTime();
    long publishTime = message.getPublishTime();

    Map<String, String> metadataMap = new HashMap<>();
    if (eventTime > 0) {
      metadataMap.put(PulsarStreamMessageMetadata.EVENT_TIME_KEY, String.valueOf(eventTime));
    }
    if (publishTime > 0) {
      metadataMap.put(PulsarStreamMessageMetadata.PUBLISH_TIME_KEY, String.valueOf(publishTime));
    }

    message.getBrokerPublishTime().ifPresent(
        brokerPublishTime -> metadataMap.put(PulsarStreamMessageMetadata.BROKER_PUBLISH_TIME_KEY,
            String.valueOf(brokerPublishTime)));

    String key = message.getKey();
    if (StringUtils.isNotBlank(key)) {
      metadataMap.put(PulsarStreamMessageMetadata.MESSAGE_KEY_KEY, key);
    }
    String messageIdStr = message.getMessageId().toString();
    metadataMap.put(PulsarStreamMessageMetadata.MESSAGE_ID_KEY, messageIdStr);
    byte[] messageIdBytes = message.getMessageId().toByteArray();
    metadataMap.put(PulsarStreamMessageMetadata.MESSAGE_ID_BYTES_B64_KEY, Base64.getEncoder()
        .encodeToString(messageIdBytes));

    //From Kafka and Kensis metadata extractors we seem to still populate some timestamps,
    // even if populateMetadata is false

    if (!populateAllFields) {
      return metadataMap;
    }

    String producerName = message.getProducerName();
    if (StringUtils.isNotBlank(producerName)) {
      metadataMap.put(PulsarStreamMessageMetadata.PRODUCER_NAME_KEY, producerName);
    }

    byte[] schemaVersion = message.getSchemaVersion();
    if (schemaVersion.length > 0) {
      metadataMap.put(PulsarStreamMessageMetadata.SCHEMA_VERSION_KEY, Base64.getEncoder().encodeToString(schemaVersion));
    }
    long sequenceId = message.getSequenceId();
    if (sequenceId > 0) {
      metadataMap.put(PulsarStreamMessageMetadata.SEQUENCE_ID_KEY, String.valueOf(sequenceId));
    }

    if (message.hasOrderingKey()) {
      metadataMap.put(PulsarStreamMessageMetadata.ORDERING_KEY_KEY,
          Base64.getEncoder().encodeToString(message.getOrderingKey()));
    }

    int size = message.size();
    metadataMap.put(PulsarStreamMessageMetadata.SIZE_KEY, String.valueOf(size));
    String topicName = message.getTopicName();
    if (StringUtils.isNotBlank(topicName)) {
      metadataMap.put(PulsarStreamMessageMetadata.TOPIC_NAME_KEY, topicName);
    }
    message.getIndex()
        .ifPresent(index -> metadataMap.put(PulsarStreamMessageMetadata.INDEX_KEY, String.valueOf(index)));
    int redeliveryCount = message.getRedeliveryCount();
    metadataMap.put(PulsarStreamMessageMetadata.REDELIVERY_COUNT_KEY, String.valueOf(redeliveryCount));
    return metadataMap;
  }
}
