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

import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;


/**
 * {@link StreamPartitionMsgOffsetFactory} implementation for Pulsar streams.
 */
public class MessageIdStreamOffsetFactory implements StreamPartitionMsgOffsetFactory {
  private StreamConfig _streamConfig;

  @Override
  public void init(StreamConfig streamConfig) {
    _streamConfig = streamConfig;
  }

  @Override
  public StreamPartitionMsgOffset create(String offsetStr) {
    return new MessageIdStreamOffset(offsetStr);
  }

  @Override
  public StreamPartitionMsgOffset create(StreamPartitionMsgOffset other) {
    MessageIdStreamOffset messageIdStreamOffset = (MessageIdStreamOffset) other;
    return new MessageIdStreamOffset(messageIdStreamOffset.getMessageId());
  }
}
