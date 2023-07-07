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
    public static final String PUBLISH_TIME_KEY = "publishTime";
    public static final String EVENT_TIME_KEY = "eventTime";
    public static final String BROKER_PUBLISH_TIME_KEY = "brokerPublishTime";
    public static final String MESSAGE_KEY_KEY = "key";
    public static final String MESSAGE_ID_KEY = "messageId";
    public static final String MESSAGE_ID_BYTES_B64_KEY = "messageIdBytes";
    public static final String PRODUCER_NAME_KEY = "producerName";
    public static final String SCHEMA_VERSION_KEY = "schemaVersion";
    public static final String SEQUENCE_ID_KEY = "sequenceId";
    public static final String ORDERING_KEY_KEY = "orderingKey";
    public static final String SIZE_KEY = "size";
    public static final String TOPIC_NAME_KEY = "topicName";
    public static final String INDEX_KEY = "index";
    public static final String REDELIVERY_COUNT_KEY = "redeliveryCount";


    public PulsarStreamMessageMetadata(long recordIngestionTimeMs,
                                        @Nullable GenericRow headers) {
        super(recordIngestionTimeMs, headers);
    }

    public PulsarStreamMessageMetadata(long recordIngestionTimeMs, @Nullable GenericRow headers,
                                        Map<String, String> metadata) {
        super(recordIngestionTimeMs, headers, metadata);
    }
}
