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
package org.apache.pinot.plugin.stream.kafka30;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.mockito.MockedConstruction;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies that factory-created consumers use the caller's creation policy and the stream partition identity.
public class KafkaConsumerFactoryRetryTest {
  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testExplicitRetryPolicyCreatesTheRequestedStreamPartitionConsumer()
      throws Exception {
    AtomicInteger policyCalls = new AtomicInteger();
    RetryPolicy retryPolicy = new RetryPolicy() {
      @Override
      public int attempt(Callable<Boolean> operation)
          throws AttemptsExceededException, RetriableOperationException {
        policyCalls.incrementAndGet();
        try {
          assertTrue(operation.call(), "The supplied policy must execute consumer creation");
        } catch (Exception e) {
          throw new AssertionError(e);
        }
        return 0;
      }
    };
    try (MockedConstruction<KafkaConsumer> consumers = mockConstruction(KafkaConsumer.class,
        (consumer, context) -> when(consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty()))) {
      KafkaConsumerFactory factory = new KafkaConsumerFactory();
      factory.init(new StreamConfig("test_REALTIME", Map.of(
          "streamType", "kafka", "stream.kafka.topic.name", "events", "stream.kafka.broker.list", "localhost:9092",
          "stream.kafka.consumer.factory.class.name", KafkaConsumerFactory.class.getName(),
          "stream.kafka.decoder.class.name", "decoderClass")));
      // Pinot's group ID and the underlying stream partition ID need not be the same.
      PartitionGroupConsumptionStatus status =
          new PartitionGroupConsumptionStatus(3, 7, 0, new LongMsgOffset(0), null, "IN_PROGRESS");
      try (PartitionGroupConsumer consumer =
          factory.createPartitionGroupConsumer("factory-client", status, retryPolicy)) {
        assertEquals(policyCalls.get(), 1);
        assertEquals(consumers.constructed().size(), 1);
        KafkaConsumer kafka = consumers.constructed().get(0);
        TopicPartition partition = new TopicPartition("events", 7);
        verify(kafka).assign(List.of(partition));
        assertEquals(consumer.fetchMessages(new LongMsgOffset(42), 10).getMessageCount(), 0);
        verify(kafka).seek(partition, 42L);
      }
      verify(consumers.constructed().get(0)).close();
    }
  }
}
