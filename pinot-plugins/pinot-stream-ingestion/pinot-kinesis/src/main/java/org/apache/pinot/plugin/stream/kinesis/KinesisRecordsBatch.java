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
package org.apache.pinot.plugin.stream.kinesis;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


/**
 * A {@link MessageBatch} for collecting records from the Kinesis stream
 */
public class KinesisRecordsBatch implements MessageBatch<KinesisStreamMessage> {
  private final List<KinesisStreamMessage> _recordList;
  private final String _shardId;
  private final boolean _endOfShard;

  public KinesisRecordsBatch(List<KinesisStreamMessage> recordList, String shardId, boolean endOfShard) {
    _recordList = recordList;
    _shardId = shardId;
    _endOfShard = endOfShard;
  }

  @Override
  public int getMessageCount() {
    return _recordList.size();
  }

  @Override
  public KinesisStreamMessage getMessageAtIndex(int index) {
    return _recordList.get(index);
  }

  @Override
  public byte[] getMessageBytesAtIndex(int index) {
    return _recordList.get(index).getValue();
  }

  @Override
  public int getMessageOffsetAtIndex(int index) {
    return ByteBuffer.wrap(_recordList.get(index).getValue()).arrayOffset();
  }

  @Override
  public int getMessageLengthAtIndex(int index) {
    return _recordList.get(index).getValue().length;
  }

  @Override
  public StreamPartitionMsgOffset getNextStreamPartitionMsgOffsetAtIndex(int index) {
    Map<String, String> shardToSequenceMap = new HashMap<>();
    shardToSequenceMap.put(_shardId, _recordList.get(index).sequenceNumber());
    return new KinesisPartitionGroupOffset(shardToSequenceMap);
  }

  @Override
  public long getNextStreamMessageOffsetAtIndex(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEndOfPartitionGroup() {
    return _endOfShard;
  }

  @Override
  public RowMetadata getMetadataAtIndex(int index) {
    return _recordList.get(index).getMetadata();
  }

  @Override
  public StreamMessage getStreamMessage(int index) {
    return _recordList.get(index);
  }
}
