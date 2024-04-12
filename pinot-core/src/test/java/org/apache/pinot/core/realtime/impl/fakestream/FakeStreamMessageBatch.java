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
package org.apache.pinot.core.realtime.impl.fakestream;

import java.util.List;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


/**
 * MessageBatch implementation for the fake stream
 */
class FakeStreamMessageBatch implements MessageBatch<byte[]> {
  private final List<byte[]> _values;
  private final List<Integer> _offsets;
  private final int _offsetOfNextBatch;

  FakeStreamMessageBatch(List<byte[]> values, List<Integer> offsets, int offsetOfNextBatch) {
    _values = values;
    _offsets = offsets;
    _offsetOfNextBatch = offsetOfNextBatch;
  }

  @Override
  public int getMessageCount() {
    return _values.size();
  }

  @Override
  public BytesStreamMessage getStreamMessage(int index) {
    byte[] value = _values.get(index);
    int offset = _offsets.get(index);
    return new BytesStreamMessage(value,
        new StreamMessageMetadata.Builder().setOffset(new LongMsgOffset(offset), new LongMsgOffset(offset + 1))
            .build());
  }

  @Override
  public StreamPartitionMsgOffset getOffsetOfNextBatch() {
    return new LongMsgOffset(_offsetOfNextBatch);
  }
}
