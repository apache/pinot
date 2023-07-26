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

import javax.annotation.Nullable;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pulsar.client.api.MessageId;

public class PulsarStreamMessage extends StreamMessage<byte[]> {

    private final MessageId _messageId;
    public PulsarStreamMessage(@Nullable byte[] key, byte[] value, MessageId messageId,
                                @Nullable PulsarStreamMessageMetadata metadata, int length) {
        super(key, value, metadata, length);
        _messageId = messageId;
    }

    public MessageId getMessageId() {
        return _messageId;
    }

    int getKeyLength() {
        byte[] key = getKey();
        return key == null ? 0 : key.length;
    }

    int getValueLength() {
        byte[] value = getValue();
        return value == null ? 0 : value.length;
    }
}
