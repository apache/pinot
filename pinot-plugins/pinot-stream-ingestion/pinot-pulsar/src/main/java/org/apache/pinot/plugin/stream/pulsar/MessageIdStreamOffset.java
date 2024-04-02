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

import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.MessageId;


/**
 * {@link StreamPartitionMsgOffset} implementation for Pulsar {@link MessageId}
 */
public class MessageIdStreamOffset implements StreamPartitionMsgOffset {
  private final MessageId _messageId;

  public MessageIdStreamOffset(MessageId messageId) {
    _messageId = messageId;
  }

  public MessageIdStreamOffset(String messageId) {
    try {
      _messageId = MessageId.fromByteArray(Hex.decodeHex(messageId));
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid Pulsar message id: " + messageId);
    }
  }

  public MessageId getMessageId() {
    return _messageId;
  }

  @Override
  public int compareTo(StreamPartitionMsgOffset other) {
    return _messageId.compareTo(((MessageIdStreamOffset) other).getMessageId());
  }

  @Override
  public String toString() {
    return Hex.encodeHexString(_messageId.toByteArray());
  }
}
