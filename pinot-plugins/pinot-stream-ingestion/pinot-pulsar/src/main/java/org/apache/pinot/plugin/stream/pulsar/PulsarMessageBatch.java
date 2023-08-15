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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.internal.DefaultImplementation;


/**
 * A {@link MessageBatch} for collecting messages from pulsar topic
 * When 'enableKeyValueStitch' flag is enabled, existing {@link org.apache.pinot.spi.stream.StreamMessageDecoder}
 * plugins will not work. A custom decoder will be needed to unpack key and value byte arrays and decode
 * them independently.
 */
public class PulsarMessageBatch implements MessageBatch<PulsarStreamMessage> {
  private final List<PulsarStreamMessage> _messageList = new ArrayList<>();
  private final boolean _enableKeyValueStitch;

  public PulsarMessageBatch(Iterable<PulsarStreamMessage> iterable, boolean enableKeyValueStitch) {
    iterable.forEach(_messageList::add);
    _enableKeyValueStitch = enableKeyValueStitch;
  }

  @Override
  public int getMessageCount() {
    return _messageList.size();
  }

  @Deprecated
  @Override
  public PulsarStreamMessage getMessageAtIndex(int index) {
    return _messageList.get(index);
  }

  @Override
  public byte[] getMessageBytesAtIndex(int index) {
    return getMessageAtIndex(index).getValue();
  }
  @Override
  public int getMessageOffsetAtIndex(int index) {
    return ByteBuffer.wrap(_messageList.get(index).getValue()).arrayOffset();
  }

  @Override
  public int getMessageLengthAtIndex(int index) {
    return _messageList.get(index).getValue().length; //if _enableKeyValueStitch is true,
    // then they are already stitched in the consumer. If false, then the value is the raw value
  }

  /**
   * Returns next message id supposed to be present in the pulsar topic partition.
   * The message id is composed of 3 parts - ledgerId, entryId and partitionId.
   * The ledger id are always increasing in number but may not be sequential. e.g. for first 10 records ledger id can
   * be 12 but for next 10 it can be 18.
   * each entry inside a ledger is always in a sequential and increases by 1 for next message.
   * the partition id is fixed for a particular partition.
   * We return entryId incremented by 1 while keeping ledgerId and partitionId as same.
   * If ledgerId has incremented, the {@link org.apache.pulsar.client.api.Reader} takes care of that during seek
   * operation
   * and returns the first record in the new ledger.
   */
  @Override
  public StreamPartitionMsgOffset getNextStreamPartitionMsgOffsetAtIndex(int index) {
    MessageIdImpl currentMessageId = MessageIdImpl.convertToMessageIdImpl(_messageList.get(index).getMessageId());
    MessageId nextMessageId;

    long currentLedgerId = currentMessageId.getLedgerId();
    long currentEntryId = currentMessageId.getEntryId();
    int currentPartitionIndex = currentMessageId.getPartitionIndex();

    if (currentMessageId instanceof BatchMessageIdImpl) {
      int currentBatchIndex = ((BatchMessageIdImpl) currentMessageId).getBatchIndex();
      int currentBatchSize = ((BatchMessageIdImpl) currentMessageId).getBatchSize();

      if (currentBatchIndex < currentBatchSize - 1) {
        nextMessageId =
            new BatchMessageIdImpl(currentLedgerId, currentEntryId, currentPartitionIndex, currentBatchIndex + 1,
                currentBatchSize, ((BatchMessageIdImpl) currentMessageId).getAcker());
      } else {
        nextMessageId =
            new BatchMessageIdImpl(currentLedgerId, currentEntryId + 1, currentPartitionIndex, 0, currentBatchSize,
                ((BatchMessageIdImpl) currentMessageId).getAcker());
      }
    } else {
      nextMessageId = DefaultImplementation.getDefaultImplementation()
          .newMessageId(currentLedgerId, currentEntryId + 1, currentPartitionIndex);
    }
    return new MessageIdStreamOffset(nextMessageId);
  }

  @Override
  public long getNextStreamMessageOffsetAtIndex(int index) {
    throw new UnsupportedOperationException("Pulsar does not support long stream offsets");
  }
}
