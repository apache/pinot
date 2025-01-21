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
package org.apache.pinot.plugin.stream.push;

import java.util.List;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


/**
 * A {@link MessageBatch} for collecting records from the Kinesis stream
 */
public class PushBasedIngestionMessageBatch implements MessageBatch<byte[]> {
  private final List<BytesStreamMessage> _messages;
  private final LongMsgOffset _offsetOfNextBatch;

  public PushBasedIngestionMessageBatch(List<BytesStreamMessage> messages, LongMsgOffset offsetOfNextBatch) {
    _messages = messages;
    _offsetOfNextBatch = offsetOfNextBatch;
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
  public StreamPartitionMsgOffset getOffsetOfNextBatch() {
    return _offsetOfNextBatch;
  }
}
