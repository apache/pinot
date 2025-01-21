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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link PartitionGroupConsumer} implementation for push-based ingestion
 */
public class PushBasedIngestionConsumer implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PushBasedIngestionConsumer.class);

  private final PushBasedIngestionConfig _config;
  private final PushBasedIngestionBufferManager _bufferManager;

  @VisibleForTesting
  public PushBasedIngestionConsumer(PushBasedIngestionConfig config, PushBasedIngestionBufferManager bufferManager) {
    _config = config;
    _bufferManager = bufferManager;
    LOGGER.info("Created push based consumer for table: {}", config.getTableName());
  }

  @Override
  public synchronized PushBasedIngestionMessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset,
      int timeoutMs) {
    LongMsgOffset startOffset = (LongMsgOffset) startMsgOffset;
    PushBasedIngestionBuffer buffer = _bufferManager.getBufferForTable(_config.getTableName());
    List<BufferedRecord> records = buffer.readNextBatch(startOffset.getOffset());
    LongMsgOffset offsetOfNextBatch = startOffset;
    List<BytesStreamMessage> messages = Collections.emptyList();
    if (!records.isEmpty()) {
      messages = records.stream().map(record -> extractStreamMessage(record)).collect(Collectors.toList());
      StreamMessageMetadata lastMessageMetadata = messages.get(messages.size() - 1).getMetadata();
      offsetOfNextBatch = (LongMsgOffset) lastMessageMetadata.getNextOffset();
    }
    return new PushBasedIngestionMessageBatch(messages, offsetOfNextBatch);
  }

  private BytesStreamMessage extractStreamMessage(BufferedRecord record) {
    LOGGER.info("Consuming record with sequence id {} and ingestion delay of {}ns", record.getOffset(),
        System.nanoTime() - record.getArrivalTimestampNanos());
    StreamMessageMetadata messageMetadata =
        new StreamMessageMetadata.Builder().setRecordIngestionTimeMs(record.getArrivalTimestampNanos() / 1000000)
            .setSerializedValueSize(record.getValueSize())
            .setOffset(new LongMsgOffset(record.getOffset()), new LongMsgOffset(record.getOffset() + 1)).build();
    return new BytesStreamMessage(null, record.getValue(), messageMetadata);
  }

  @Override
  public void close() {
  }
}
