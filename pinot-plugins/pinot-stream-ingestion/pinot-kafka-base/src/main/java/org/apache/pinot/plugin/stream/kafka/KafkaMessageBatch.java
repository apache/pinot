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
package org.apache.pinot.plugin.stream.kafka;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


public class KafkaMessageBatch implements MessageBatch<byte[]> {
  private final List<BytesStreamMessage> _messages;
  private final int _unfilteredMessageCount;
  private final long _offsetOfNextBatch;
  private final long _firstOffset;
  private final StreamMessageMetadata _lastMessageMetadata;
  private final boolean _hasDataLoss;

  /**
   * @param messages the messages, which may be smaller than {@see unfilteredMessageCount}
   * @param unfilteredMessageCount how many messages were received from the topic before being filtered
   * @param offsetOfNextBatch the offset of the next batch
   * @param firstOffset the offset of the first unfiltered message, -1 if no unfiltered messages
   * @param lastMessageMetadata metadata for the last unfiltered message in the batch, useful for estimating ingestion
   *                            delay when a batch has all messages filtered.
   */
  public KafkaMessageBatch(List<BytesStreamMessage> messages, int unfilteredMessageCount, long offsetOfNextBatch,
      long firstOffset, @Nullable StreamMessageMetadata lastMessageMetadata, boolean hasDataLoss) {
    _messages = messages;
    _unfilteredMessageCount = unfilteredMessageCount;
    _offsetOfNextBatch = offsetOfNextBatch;
    _firstOffset = firstOffset;
    _lastMessageMetadata = lastMessageMetadata;
    _hasDataLoss = hasDataLoss;
  }

  @Override
  public int getMessageCount() {
    return _messages.size();
  }

  @Override
  public int getUnfilteredMessageCount() {
    return _unfilteredMessageCount;
  }

  @Override
  public BytesStreamMessage getStreamMessage(int index) {
    return _messages.get(index);
  }

  @Override
  public StreamPartitionMsgOffset getOffsetOfNextBatch() {
    return new LongMsgOffset(_offsetOfNextBatch);
  }

  @Nullable
  @Override
  public StreamPartitionMsgOffset getFirstMessageOffset() {
    return _firstOffset >= 0 ? new LongMsgOffset(_firstOffset) : null;
  }

  @Nullable
  @Override
  public StreamMessageMetadata getLastMessageMetadata() {
    return _lastMessageMetadata;
  }

  @Override
  public boolean hasDataLoss() {
    return _hasDataLoss;
  }
}
