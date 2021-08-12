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

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaPartitionLevelConsumer extends KafkaPartitionLevelConnectionHandler implements PartitionLevelConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPartitionLevelConsumer.class);

  public KafkaPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition);
  }

  @Override
  public MessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset, StreamPartitionMsgOffset endMsgOffset,
      int timeoutMillis)
      throws TimeoutException {
    final long startOffset = ((LongMsgOffset) startMsgOffset).getOffset();
    final long endOffset = endMsgOffset == null ? Long.MAX_VALUE : ((LongMsgOffset) endMsgOffset).getOffset();
    return fetchMessages(startOffset, endOffset, timeoutMillis);
  }

  public MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws TimeoutException {
    _consumer.seek(_topicPartition, startOffset);
    ConsumerRecords<String, Bytes> consumerRecords = _consumer.poll(Duration.ofMillis(timeoutMillis));
    final Iterable<ConsumerRecord<String, Bytes>> messageAndOffsetIterable =
        buildOffsetFilteringIterable(consumerRecords.records(_topicPartition), startOffset, endOffset);
    return new KafkaMessageBatch(messageAndOffsetIterable);
  }

  private Iterable<ConsumerRecord<String, Bytes>> buildOffsetFilteringIterable(
      final List<ConsumerRecord<String, Bytes>> messageAndOffsets, final long startOffset, final long endOffset) {
    return Iterables.filter(messageAndOffsets, input -> {
      // Filter messages that are either null or have an offset ∉ [startOffset, endOffset]
      return input != null && input.value() != null && input.offset() >= startOffset && (endOffset > input.offset()
          || endOffset == -1);
    });
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
