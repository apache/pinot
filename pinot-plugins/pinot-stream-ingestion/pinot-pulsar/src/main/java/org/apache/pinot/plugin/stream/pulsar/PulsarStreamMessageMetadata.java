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

import java.util.EnumSet;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageMetadata;

/**
 * Pulsar specific implementation of {@link StreamMessageMetadata}
 * Pulsar makes many metadata values available for each message. Please see the pulsar documentation for more details.
 * @see <a href="https://pulsar.apache.org/docs/en/concepts-messaging/#message-properties">Pulsar Message Properties</a>
 */
public class PulsarStreamMessageMetadata extends StreamMessageMetadata {

    public enum PulsarMessageMetadataValue {
        PUBLISH_TIME("publishTime"),
        EVENT_TIME("eventTime"),
        BROKER_PUBLISH_TIME("brokerPublishTime"),
        MESSAGE_KEY("key"),
        MESSAGE_ID("messageId"),
        MESSAGE_ID_BYTES_B64("messageIdBytes"),
        PRODUCER_NAME("producerName"),
        SCHEMA_VERSION("schemaVersion"),
        SEQUENCE_ID("sequenceId"),
        ORDERING_KEY("orderingKey"),
        SIZE("size"),
        TOPIC_NAME("topicName"),
        INDEX("index"),
        REDELIVERY_COUNT("redeliveryCount");

        private final String key;

        PulsarMessageMetadataValue(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }

        public static PulsarMessageMetadataValue findByKey(final String key){
            EnumSet<PulsarMessageMetadataValue> values = EnumSet.allOf(PulsarMessageMetadataValue.class);
            return values.stream().filter(value -> value.getKey().equals(key)).findFirst().orElse(null);
        }
    }

    public PulsarStreamMessageMetadata(long recordIngestionTimeMs,
                                        @Nullable GenericRow headers) {
        super(recordIngestionTimeMs, headers);
    }

    public PulsarStreamMessageMetadata(long recordIngestionTimeMs, @Nullable GenericRow headers,
                                        Map<String, String> metadata) {
        super(recordIngestionTimeMs, headers, metadata);
    }
}
