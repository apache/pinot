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
package org.apache.pinot.plugin.stream.kafka40;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.plugin.stream.kafka.KafkaMessageBatch;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.annotations.Test;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Unit tests for the data-loss decision in [KafkaPartitionLevelConsumer#fetchMessages] against a mocked Kafka
/// consumer, so that the number of broker round-trips is observable. The same decision is exercised against a real
/// broker, with real transaction control records, in [KafkaPartitionLevelConsumerDataLossIntegrationTest].
public class KafkaPartitionLevelConsumerDataLossCheckTest {
  private static final String TOPIC = "test-topic";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);
  private static final int TIMEOUT_MS = 10000;

  @Test
  public void testContiguousBatchDoesNotLookUpLogStartOffset() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(100L), record(101L)));

    KafkaMessageBatch batch = createConsumerWithMock(getStreamConfig(null), mockConsumer)
        .fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);

    assertEquals(batch.getMessageCount(), 2);
    assertFalse(batch.hasDataLoss());
    verify(mockConsumer, never()).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// The gap left by a transaction control record: the first record is past the requested offset, but the log still
  /// starts at or below the requested offset, so nothing was deleted.
  @Test
  public void testGapWithRetainedStartOffsetIsNotDataLoss() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(101L)));
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenReturn(Map.of(TOPIC_PARTITION, 40L));

    KafkaMessageBatch batch = createConsumerWithMock(getStreamConfig(null), mockConsumer)
        .fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);

    assertEquals(batch.getFirstMessageOffset().toString(), "101");
    assertFalse(batch.hasDataLoss());
    // One round trip per gap, and only on the gap path
    verify(mockConsumer, times(1)).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// Real loss: records at/after the requested offset were deleted, so the log now starts above it.
  @Test
  public void testGapWithStartOffsetBelowLogStartIsDataLoss() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(150L)));
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenReturn(Map.of(TOPIC_PARTITION, 150L));

    KafkaMessageBatch batch = createConsumerWithMock(getStreamConfig(null), mockConsumer)
        .fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);

    assertEquals(batch.getFirstMessageOffset().toString(), "150");
    assertTrue(batch.hasDataLoss());
    verify(mockConsumer, times(1)).beginningOffsets(anyCollection(), any(Duration.class));
  }

  @Test
  public void testGapUnderReadCommittedIsNotDataLossWithoutLookup() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(101L)));

    KafkaMessageBatch batch = createConsumerWithMock(
        getStreamConfig(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL_READ_COMMITTED),
        mockConsumer).fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);

    assertEquals(batch.getFirstMessageOffset().toString(), "101");
    assertFalse(batch.hasDataLoss());
    verify(mockConsumer, never()).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// When the log start offset cannot be determined, the gap is treated as expected rather than as loss, so a broker
  /// hiccup cannot manufacture a false alert, and the failure does not propagate out of fetchMessages.
  @Test
  public void testLogStartOffsetLookupFailureIsNotDataLoss() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(150L)), records(record(150L)));
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenThrow(new TimeoutException("broker unavailable"));

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch batch = consumer.fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);
    assertFalse(batch.hasDataLoss());
    // A repeated failure within the WARN interval is throttled in the log but evaluated the same way
    batch = consumer.fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);
    assertFalse(batch.hasDataLoss());
    verify(mockConsumer, times(2)).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// The client moved past a control record while delivering records (the fetch response ended with the commit
  /// marker of the delivered record): the next, empty poll finds the client already there and must not pay a broker
  /// round trip for it, and the data poll after that starts exactly at the new position, so no gap is seen either.
  @Test
  public void testPositionMoveInsideRecordPollNeedsNoLookup() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(2L)), records(), records(record(4L)));
    // After delivering record 2 the client skipped the commit marker at 3 in the same fetch response
    when(mockConsumer.position(eq(TOPIC_PARTITION), any(Duration.class))).thenReturn(4L);

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch dataBatch = consumer.fetchMessages(new LongMsgOffset(2L), TIMEOUT_MS);
    assertEquals(dataBatch.getMessageCount(), 1);
    assertEquals(dataBatch.getOffsetOfNextBatch().toString(), "3", "the caller continues after the delivered record");
    assertFalse(dataBatch.hasDataLoss());

    KafkaMessageBatch emptyBatch = consumer.fetchMessages(new LongMsgOffset(3L), TIMEOUT_MS);
    assertEquals(emptyBatch.getMessageCount(), 0);
    assertEquals(emptyBatch.getOffsetOfNextBatch().toString(), "4", "the empty batch reports the client position");
    assertFalse(emptyBatch.hasDataLoss());

    KafkaMessageBatch nextDataBatch = consumer.fetchMessages(new LongMsgOffset(4L), TIMEOUT_MS);
    assertEquals(nextDataBatch.getFirstMessageOffset().toString(), "4");
    assertFalse(nextDataBatch.hasDataLoss());
    verify(mockConsumer, never()).beginningOffsets(anyCollection(), any(Duration.class));
    verify(mockConsumer, times(1)).seek(any(TopicPartition.class), anyLong());
  }

  /// The client moved during an empty poll (the commit marker landed in a later fetch than its record). That move
  /// cannot be told apart from an out-of-range reset without the broker, so it is validated against the log start
  /// offset exactly once; it is not loss while the log still starts at or below the position the poll started from.
  /// The following data poll starts at the new position and needs no lookup of its own.
  @Test
  public void testPositionMoveDuringEmptyPollIsValidatedOnce() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(2L)), records(), records(record(4L)));
    // Position after each poll: 3 after record 2, 4 once the marker at 3 is skipped, 5 after record 4
    when(mockConsumer.position(eq(TOPIC_PARTITION), any(Duration.class))).thenReturn(3L, 4L, 5L);
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenReturn(Map.of(TOPIC_PARTITION, 0L));

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch dataBatch = consumer.fetchMessages(new LongMsgOffset(2L), TIMEOUT_MS);
    assertEquals(dataBatch.getOffsetOfNextBatch().toString(), "3");
    assertFalse(dataBatch.hasDataLoss());
    verify(mockConsumer, never()).beginningOffsets(anyCollection(), any(Duration.class));

    KafkaMessageBatch emptyBatch = consumer.fetchMessages(new LongMsgOffset(3L), TIMEOUT_MS);
    assertEquals(emptyBatch.getMessageCount(), 0);
    assertEquals(emptyBatch.getOffsetOfNextBatch().toString(), "4");
    assertFalse(emptyBatch.hasDataLoss());
    verify(mockConsumer, times(1)).beginningOffsets(anyCollection(), any(Duration.class));

    KafkaMessageBatch nextDataBatch = consumer.fetchMessages(new LongMsgOffset(4L), TIMEOUT_MS);
    assertEquals(nextDataBatch.getFirstMessageOffset().toString(), "4");
    assertFalse(nextDataBatch.hasDataLoss());
    verify(mockConsumer, times(1)).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// A move during an empty poll that was an auto.offset.reset: everything from the position the poll started from
  /// up to the reset target is gone for this consumer, and the log now starts above that position. The loss must be
  /// reported on the empty batch, because the caller adopts the reported position and the next data poll then
  /// starts exactly where it was requested, with no gap left to notice.
  @Test
  public void testResetDuringEmptyPollIsDataLoss() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(), records(record(150L)));
    when(mockConsumer.position(eq(TOPIC_PARTITION), any(Duration.class))).thenReturn(150L);
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenReturn(Map.of(TOPIC_PARTITION, 150L));

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch emptyBatch = consumer.fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);
    assertEquals(emptyBatch.getMessageCount(), 0);
    assertEquals(emptyBatch.getOffsetOfNextBatch().toString(), "150");
    assertTrue(emptyBatch.hasDataLoss());

    // Reported once: the data poll from the adopted position sees no gap
    KafkaMessageBatch dataBatch = consumer.fetchMessages(new LongMsgOffset(150L), TIMEOUT_MS);
    assertEquals(dataBatch.getFirstMessageOffset().toString(), "150");
    assertFalse(dataBatch.hasDataLoss());
    verify(mockConsumer, times(1)).beginningOffsets(anyCollection(), any(Duration.class));
  }

  @Test
  public void testEmptyPollWithoutPositionMoveNeedsNoLookup() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records());
    when(mockConsumer.position(eq(TOPIC_PARTITION), any(Duration.class))).thenReturn(5L);

    KafkaMessageBatch batch = createConsumerWithMock(getStreamConfig(null), mockConsumer)
        .fetchMessages(new LongMsgOffset(5L), TIMEOUT_MS);

    assertEquals(batch.getMessageCount(), 0);
    assertEquals(batch.getOffsetOfNextBatch().toString(), "5");
    assertFalse(batch.hasDataLoss());
    verify(mockConsumer, never()).beginningOffsets(anyCollection(), any(Duration.class));
  }

  @Test
  public void testPositionMoveDuringEmptyPollUnderReadCommittedIsNotDataLossWithoutLookup() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records());
    when(mockConsumer.position(eq(TOPIC_PARTITION), any(Duration.class))).thenReturn(150L);

    KafkaMessageBatch batch = createConsumerWithMock(
        getStreamConfig(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL_READ_COMMITTED),
        mockConsumer).fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);

    assertEquals(batch.getMessageCount(), 0);
    assertEquals(batch.getOffsetOfNextBatch().toString(), "150");
    assertFalse(batch.hasDataLoss());
    verify(mockConsumer, never()).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// The gap in front of a record that the client skipped while delivering the previous records (the commit marker
  /// arrived in the same fetch as the record before it) is not re-checked when the next data poll starts at the
  /// caller's offset inside that gap. Before, the gap was compared with the caller's offset, and a log start offset
  /// that had meanwhile moved onto the marker produced a false loss report.
  @Test
  public void testGapSkippedInsideEarlierRecordPollNeedsNoLookupOnNextRecordPoll() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(2L)), records(record(4L)));
    // After delivering record 2 the client skipped the commit marker at 3 in the same fetch response
    when(mockConsumer.position(eq(TOPIC_PARTITION), any(Duration.class))).thenReturn(4L, 5L);
    // Retention has since moved the log start onto the marker
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenReturn(Map.of(TOPIC_PARTITION, 4L));

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch dataBatch = consumer.fetchMessages(new LongMsgOffset(2L), TIMEOUT_MS);
    assertEquals(dataBatch.getOffsetOfNextBatch().toString(), "3");

    KafkaMessageBatch nextDataBatch = consumer.fetchMessages(new LongMsgOffset(3L), TIMEOUT_MS);
    assertEquals(nextDataBatch.getFirstMessageOffset().toString(), "4");
    assertFalse(nextDataBatch.hasDataLoss(), "The marker at 3 was skipped while record 2 was delivered: not a loss");
    verify(mockConsumer, never()).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// When the log start offset cannot be read for a move during an empty poll, the move stays unaccounted for: the
  /// empty batch reports the last validated position, so no caller can adopt the move, and the next poll validates
  /// it again. A broker hiccup therefore cannot hide a reset for good. Here the retry on the following data poll finds
  /// the log start above the position the empty poll started from and reports the loss.
  @Test
  public void testFailedValidationOfEmptyPollMoveIsNotReportedAndRetriedOnNextRecordPoll() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(), records(record(150L)));
    when(mockConsumer.position(eq(TOPIC_PARTITION), any(Duration.class))).thenReturn(150L, 151L);
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenThrow(new TimeoutException("broker unavailable"))
        .thenReturn(Map.of(TOPIC_PARTITION, 150L));

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch emptyBatch = consumer.fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);
    assertEquals(emptyBatch.getOffsetOfNextBatch().toString(), "100", "An unvalidated move is not reported");
    assertFalse(emptyBatch.hasDataLoss(), "A failed lookup does not manufacture a loss report");

    // The caller asks for the reported offset again; the consumer keeps its position instead of seeking back
    KafkaMessageBatch dataBatch = consumer.fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);
    assertEquals(dataBatch.getFirstMessageOffset().toString(), "150");
    assertTrue(dataBatch.hasDataLoss(), "The move from 100 to 150 is validated again and turns out to be a reset");
    verify(mockConsumer, times(2)).beginningOffsets(anyCollection(), any(Duration.class));
    verify(mockConsumer, times(1)).seek(any(TopicPartition.class), anyLong());
  }

  /// The same unvalidated move, retried on an empty poll: once a lookup succeeds and finds no loss, the move is
  /// reported and can be adopted.
  @Test
  public void testFailedValidationOfEmptyPollMoveIsReportedOnceValidated() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(), records());
    when(mockConsumer.position(eq(TOPIC_PARTITION), any(Duration.class))).thenReturn(2L);
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenThrow(new TimeoutException("broker unavailable"))
        .thenReturn(Map.of(TOPIC_PARTITION, 0L));

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch emptyBatch = consumer.fetchMessages(new LongMsgOffset(1L), TIMEOUT_MS);
    assertEquals(emptyBatch.getOffsetOfNextBatch().toString(), "1");
    assertFalse(emptyBatch.hasDataLoss());

    emptyBatch = consumer.fetchMessages(new LongMsgOffset(1L), TIMEOUT_MS);
    assertEquals(emptyBatch.getOffsetOfNextBatch().toString(), "2", "The commit marker at 1 is skipped once validated");
    assertFalse(emptyBatch.hasDataLoss());
    verify(mockConsumer, times(2)).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// An out-of-range reset can surface on an empty poll: the client jumps to the new log start without returning a
  /// record. The move is validated on that empty poll and the loss reported on the empty batch, for every caller:
  /// `RealtimeSegmentDataManager` adopts the empty batch's offset, and the re-ingestion writer keeps its offset and
  /// polls again from it, as this test does. The record that follows starts at the already validated position, so
  /// the loss is reported once, not again on the next data poll.
  @Test
  public void testResetSeenOnEmptyPollIsReportedOnEmptyBatchWhenCallerKeepsItsOffset()
      throws Exception {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(), records(record(150L)));
    when(mockConsumer.position(eq(TOPIC_PARTITION), any(Duration.class))).thenReturn(150L);
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenReturn(Map.of(TOPIC_PARTITION, 150L));

    try (KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer)) {
      KafkaMessageBatch emptyBatch = consumer.fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);
      assertEquals(emptyBatch.getMessageCount(), 0);
      assertEquals(emptyBatch.getOffsetOfNextBatch().toString(), "150");
      assertTrue(emptyBatch.hasDataLoss(), "Records 100..149 were deleted before they were consumed");

      KafkaMessageBatch batch = consumer.fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);
      assertEquals(batch.getFirstMessageOffset().toString(), "150");
      assertFalse(batch.hasDataLoss(), "The move was already validated and reported on the empty batch");
    }
    verify(mockConsumer, times(1)).seek(any(TopicPartition.class), anyLong());
    verify(mockConsumer, times(1)).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// Failed lookups suppressed by the WARN rate limit are reported once lookups succeed again, and once more when the
  /// consumer closes, so a failure storm that ends for good still reports its tail.
  @Test
  public void testSuppressedLookupFailuresAreReportedOnRecoveryAndClose()
      throws Exception {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(150L)));
    TimeoutException failure = new TimeoutException("broker unavailable");
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenThrow(failure, failure, failure)
        .thenReturn(Map.of(TOPIC_PARTITION, 40L))
        .thenThrow(failure, failure);

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    // The first failure is logged at WARN, the next two are suppressed within the interval
    for (int i = 0; i < 3; i++) {
      assertFalse(consumer.fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS).hasDataLoss());
    }
    assertEquals(consumer.getSuppressedLogStartOffsetLookupFailures(), 2);

    // A successful lookup reports the suppressed failures
    assertFalse(consumer.fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS).hasDataLoss());
    assertEquals(consumer.getSuppressedLogStartOffsetLookupFailures(), 0);

    // New failures within the interval are suppressed again, and closing the consumer reports them
    for (int i = 0; i < 2; i++) {
      assertFalse(consumer.fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS).hasDataLoss());
    }
    assertEquals(consumer.getSuppressedLogStartOffsetLookupFailures(), 2);
    consumer.close();
    assertEquals(consumer.getSuppressedLogStartOffsetLookupFailures(), 0);
    verify(mockConsumer, times(1)).close();
  }

  private static KafkaPartitionLevelConsumer createConsumerWithMock(StreamConfig streamConfig,
      Consumer<Bytes, Bytes> mockConsumer) {
    class FakeKafkaPartitionLevelConsumer extends KafkaPartitionLevelConsumer {
      FakeKafkaPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
        super(clientId, streamConfig, partition);
      }

      @Override
      protected Consumer<Bytes, Bytes> createConsumer(Properties consumerProp) {
        return mockConsumer;
      }
    }
    return new FakeKafkaPartitionLevelConsumer("clientId-test", streamConfig, 0);
  }

  @SafeVarargs
  private static ConsumerRecords<Bytes, Bytes> records(ConsumerRecord<Bytes, Bytes>... records) {
    return new ConsumerRecords<>(Map.of(TOPIC_PARTITION, List.of(records)), Map.of());
  }

  private static ConsumerRecord<Bytes, Bytes> record(long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, 3, 5, bytes("key"),
        bytes("value"), new RecordHeaders(), Optional.empty());
  }

  private static Bytes bytes(String value) {
    return new Bytes(value.getBytes(StandardCharsets.UTF_8));
  }

  private static StreamConfig getStreamConfig(String isolationLevel) {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", TOPIC);
    streamConfigMap.put("stream.kafka.broker.list", "localhost:9092");
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", KafkaConsumerFactory.class.getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    if (isolationLevel != null) {
      streamConfigMap.put("stream.kafka.isolation.level", isolationLevel);
    }
    return new StreamConfig("tableName_REALTIME", streamConfigMap);
  }
}
