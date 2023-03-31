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
package org.apache.pinot.plugin.stream.kafka20;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


public class KafkaMessageBatch implements MessageBatch<StreamMessage<byte[]>> {
  private final List<StreamMessage<byte[]>> _messageList;
  private final int _unfilteredMessageCount;
  private final long _lastOffset;
  private final StreamMessageMetadata _lastMessageMetadata;

  /**
   * @param unfilteredMessageCount how many messages were received from the topic before being filtered
   * @param lastOffset the offset of the last message in the batch
   * @param batch the messages, which may be smaller than {@see unfilteredMessageCount}
   * @param lastMessageMetadata metadata for last filtered message in the batch, useful for estimating ingestion delay
   *                            when a batch has all messages filtered.
   */
  public KafkaMessageBatch(int unfilteredMessageCount, long lastOffset, List<StreamMessage<byte[]>> batch,
      StreamMessageMetadata lastMessageMetadata) {
    _messageList = batch;
    _lastOffset = lastOffset;
    _unfilteredMessageCount = unfilteredMessageCount;
    _lastMessageMetadata = lastMessageMetadata;
  }

  @Override
  /**
   * Returns the metadata for the last filtered message if any, null otherwise.
   */
  public StreamMessageMetadata getLastMessageMetadata() {
    return _lastMessageMetadata;
  }

  @Override
  public int getMessageCount() {
    return _messageList.size();
  }

  @Override
  public int getUnfilteredMessageCount() {
    return _unfilteredMessageCount;
  }

  @Override
  public StreamMessage getMessageAtIndex(int index) {
    return _messageList.get(index);
  }

  @Override
  public int getMessageOffsetAtIndex(int index) {
    return ByteBuffer.wrap(_messageList.get(index).getValue()).arrayOffset();
  }

  @Override
  public int getMessageLengthAtIndex(int index) {
    return _messageList.get(index).getValue().length;
  }

  @Override
  public long getNextStreamMessageOffsetAtIndex(int index) {
    throw new UnsupportedOperationException("This method is deprecated");
  }

  @Override
  public StreamPartitionMsgOffset getNextStreamPartitionMsgOffsetAtIndex(int index) {
    return new LongMsgOffset(((KafkaStreamMessage) _messageList.get(index)).getNextOffset());
  }

  @Override
  public StreamPartitionMsgOffset getOffsetOfNextBatch() {
    return new LongMsgOffset(_lastOffset + 1);
  }

  @Override
  public RowMetadata getMetadataAtIndex(int index) {
    return _messageList.get(index).getMetadata();
  }

  @Override
  public byte[] getMessageBytesAtIndex(int index) {
    return _messageList.get(index).getValue();
  }

  @Override
  public StreamMessage getStreamMessage(int index) {
    return _messageList.get(index);
  }
}
