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

import java.util.List;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.MessageBatch;


/**
 * A {@link MessageBatch} for collecting messages from pulsar topic
 */
public class PulsarMessageBatch implements MessageBatch<byte[]> {
  private final List<BytesStreamMessage> _messages;
  private final MessageIdStreamOffset _offsetOfNextBatch;
  private final boolean _endOfTopic;

  public PulsarMessageBatch(List<BytesStreamMessage> messages, MessageIdStreamOffset offsetOfNextBatch,
      boolean endOfTopic) {
    _messages = messages;
    _offsetOfNextBatch = offsetOfNextBatch;
    _endOfTopic = endOfTopic;
  }

  @Override
  public int getMessageCount() {
    return _messages.size();
  }

  @Override
  public BytesStreamMessage getStreamMessage(int index) {
    return _messages.get(index);
  }

  @Override
  public MessageIdStreamOffset getOffsetOfNextBatch() {
    return _offsetOfNextBatch;
  }

  @Override
  public boolean isEndOfPartitionGroup() {
    return _endOfTopic;
  }
}
