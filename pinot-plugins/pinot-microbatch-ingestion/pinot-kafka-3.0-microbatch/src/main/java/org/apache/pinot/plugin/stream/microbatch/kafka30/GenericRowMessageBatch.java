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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import java.util.List;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


/**
 * Implementation of {@link MessageBatch} that holds {@link GenericRow} messages.
 *
 * <p>This class is used by the MicroBatch consumer to return decoded records from batch files.
 * Each batch contains a list of stream messages with their associated metadata, including
 * composite offsets that track both the Kafka message offset and the record position within
 * the batch file.
 *
 * @see KafkaPartitionLevelMicroBatchConsumer
 * @see MessageBatchReader
 */
public class GenericRowMessageBatch implements MessageBatch<GenericRow> {
  private final List<StreamMessage<GenericRow>> _messages;
  private final int _unfilteredMessageCount;
  private final boolean _hasDataLoss;
  private final long _sizeInBytes;
  private final StreamPartitionMsgOffset _offsetOfNextBatch;

  public GenericRowMessageBatch(
      List<StreamMessage<GenericRow>> messages,
      int unfilteredMessageCount,
      boolean hasDataLoss,
      long sizeInBytes,
      StreamPartitionMsgOffset offsetOfNextBatch) {
    _messages = messages;
    _unfilteredMessageCount = unfilteredMessageCount;
    _hasDataLoss = hasDataLoss;
    _sizeInBytes = sizeInBytes;
    _offsetOfNextBatch = offsetOfNextBatch;
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
  public StreamMessage<GenericRow> getStreamMessage(int index) {
    return _messages.get(index);
  }

  @Override
  public StreamPartitionMsgOffset getOffsetOfNextBatch() {
    return _offsetOfNextBatch;
  }

  @Override
  public boolean hasDataLoss() {
    return _hasDataLoss;
  }

  @Override
  public long getSizeInBytes() {
    return _sizeInBytes;
  }
}
