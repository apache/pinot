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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link StreamPartitionMsgOffset} implementation for Pulsar {@link MessageId}
 */
public class MessageIdStreamOffset implements StreamPartitionMsgOffset {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageIdStreamOffset.class);
  private MessageId _messageId;

  public MessageIdStreamOffset(MessageId messageId) {
    _messageId = messageId;
  }

  /**
   * returns the class object from string message id in the format ledgerId:entryId:partitionId
   * throws {@link IOException} if message if format is invalid.
   * @param messageId
   */
  public MessageIdStreamOffset(String messageIdStringRepresentation) {
    String messageParts = messageIdStringRepresentation.split(":");
    if (messageParts.length == 3) {
      Long ledgerId = Long.parseLong(messageIdParts[0];
      Long entryId = Long.parseLong(messageIdParts[1];
      int partitionId = Integer.parseInt(messageIdParts[2]);
      _messageId = new MessageIdImpl(ledgerId, entryId, partitionId);
    } else if (messageParts.length == 4) {
      Long ledgerId = Long.parseLong(messageIdParts[0];
      Long entryId = Long.parseLong(messageIdParts[1];
      int partitionId = Integer.parseInt(messageIdParts[2]);
      int batchId = Integer.parseInt(messageIdParts[3]);
      _messageId = new BatchMessageIdImpl(ledgerId, entryId, partitionId, batchId);
    } else {
      LOOGER.error("Illegal Pulsar MessageId={}", messageIdStringRepresentation);
    }
  }

  public MessageId getMessageId() {
    return _messageId;
  }

  @Override
  public StreamPartitionMsgOffset fromString(String streamPartitionMsgOffsetStr) {
    return new MessageIdStreamOffset(streamPartitionMsgOffsetStr);
  }

  @Override
  public int compareTo(Object other) {
    MessageIdStreamOffset messageIdStreamOffset = (MessageIdStreamOffset) other;
    return _messageId.compareTo(messageIdStreamOffset.getMessageId());
  }

  @Override
  public String toString() {
    return _messageId.toString();
  }
}
