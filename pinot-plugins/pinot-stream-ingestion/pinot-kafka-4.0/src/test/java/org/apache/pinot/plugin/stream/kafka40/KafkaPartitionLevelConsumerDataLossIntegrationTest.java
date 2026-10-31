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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.plugin.stream.kafka.KafkaMessageBatch;
import org.apache.pinot.plugin.stream.kafka40.utils.MiniKafkaCluster;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// End-to-end (real broker) regression tests for [KafkaPartitionLevelConsumer] data-loss detection
/// (see the fix for false `StreamDataLoss` on transactional Kafka topics).
///
/// Unlike [KafkaPartitionLevelConsumerDataLossTest] (which mocks the Kafka consumer), these tests
/// run against a real [MiniKafkaCluster], so they exercise the real transactional control-record
/// offset gaps and the real `beginningOffsets` round-trip added by the fix.
///
/// Two scenarios, mirroring the reviewer's request:
/// 1. Perform a real transaction and confirm no data loss is reported for the (expected) offset gap
///    left by commit control records while the data is still retained.
/// 2. Delete offsets (advance the log start via [MiniKafkaCluster#deleteRecordsBeforeOffset]) and
///    confirm data loss IS reported.
/// 3. Replay the production pattern: a caught-up consumer on a low-traffic transactional topic with one
///    record per transaction, which sees every new record one offset past where it was parked.
/// 4. Replay what RealtimeSegmentDataManager does with those batches, adopting the next-batch offset of an
///    empty batch: the current offset reaches the high watermark with a records lag of 0, with the
///    beginningOffsets round trips counted, and an out-of-range reset inside an empty poll is still reported.
///
/// Speed/stability: setup uses only synchronous broker calls (createTopics().all().get(),
/// commitTransaction()/flush(), deleteRecords().all().get()), so there are no fixed sleeps. Reads
/// use [#fetchUntilRecords] which polls at the same offset until data arrives, tolerating an empty
/// first poll (and the offset reset in the truncation case) without racing.
public class KafkaPartitionLevelConsumerDataLossIntegrationTest {
  // Short per-poll timeout so an (occasional) empty first poll retries quickly instead of blocking;
  // the happy path returns data on the first poll well within this bound.
  private static final int FETCH_TIMEOUT_MS = 2000;
  // Overall budget for a single logical fetch to return records (covers metadata propagation and,
  // for the truncation case, the offset reset taking effect).
  private static final long FETCH_MAX_WAIT_MS = 30000;

  // Transactional topic: two committed transactions of 10 records each. Under the default
  // read_uncommitted isolation the commit control record after txn-1 occupies offset 10 (never
  // delivered to the consumer), so txn-2's user records start at offset 11 -> a legitimate gap.
  private static final String TXN_TOPIC = "txn-gap";
  private static final int RECORDS_PER_TXN = 10;
  private static final long TXN1_COMMIT_MARKER_OFFSET = 10;
  private static final long TXN2_FIRST_RECORD_OFFSET = 11;

  // Truncated topic: 30 contiguous records, then everything before offset 20 is deleted, so the
  // log start offset advances to 20 (records at/after the requested startOffset were removed).
  private static final String TRUNCATED_TOPIC = "truncated";
  private static final int TRUNCATED_TOPIC_RECORDS = 30;
  private static final long TRUNCATE_BEFORE_OFFSET = 20;

  // Low-traffic transactional topic: one record per transaction, so every record is followed by its commit marker
  // and a caught-up consumer is always parked at a marker offset. Produced during the test, not in setUp.
  private static final String LOW_TRAFFIC_TXN_TOPIC = "txn-low-traffic";

  // Low-traffic transactional topics for the scenarios that replay what RealtimeSegmentDataManager does with the
  // batches (adopting the next-batch offset of an empty batch). Produced during the tests, not in setUp.
  private static final String HIGH_WATERMARK_TXN_TOPIC = "txn-high-watermark";
  private static final String LATE_MARKER_TXN_TOPIC = "txn-late-marker";

  // Emptied topic: 30 records, then every one of them is deleted, so the log start offset equals the high watermark
  // (30) and a fetch below it is out of range with nothing to deliver after the reset.
  private static final String EMPTIED_TOPIC = "emptied";
  private static final int EMPTIED_TOPIC_RECORDS = 30;

  private MiniKafkaCluster _kafkaCluster;
  private String _kafkaBrokerAddress;

  @BeforeClass
  public void setUp()
      throws Exception {
    _kafkaCluster = new MiniKafkaCluster("0");
    _kafkaCluster.start();
    _kafkaBrokerAddress = _kafkaCluster.getKafkaServerAddress();

    // createTopic uses AdminClient.createTopics().all().get() -> synchronous, no sleep needed.
    _kafkaCluster.createTopic(TXN_TOPIC, 1);
    _kafkaCluster.createTopic(TRUNCATED_TOPIC, 1);
    _kafkaCluster.createTopic(LOW_TRAFFIC_TXN_TOPIC, 1);
    _kafkaCluster.createTopic(HIGH_WATERMARK_TXN_TOPIC, 1);
    _kafkaCluster.createTopic(LATE_MARKER_TXN_TOPIC, 1);
    _kafkaCluster.createTopic(EMPTIED_TOPIC, 1);

    // commitTransaction()/flush() are synchronous -> records are durable on return, no sleep needed.
    produceTransactional(TXN_TOPIC, 2, RECORDS_PER_TXN);
    producePlain(TRUNCATED_TOPIC, TRUNCATED_TOPIC_RECORDS);
    producePlain(EMPTIED_TOPIC, EMPTIED_TOPIC_RECORDS);

    // deleteRecords().all().get() is synchronous -> log start offset advanced on return.
    _kafkaCluster.deleteRecordsBeforeOffset(TRUNCATED_TOPIC, 0, TRUNCATE_BEFORE_OFFSET);
    _kafkaCluster.deleteRecordsBeforeOffset(EMPTIED_TOPIC, 0, EMPTIED_TOPIC_RECORDS);
  }

  @AfterClass
  public void tearDown() {
    try {
      _kafkaCluster.deleteTopic(TXN_TOPIC);
      _kafkaCluster.deleteTopic(TRUNCATED_TOPIC);
      _kafkaCluster.deleteTopic(LOW_TRAFFIC_TXN_TOPIC);
      _kafkaCluster.deleteTopic(HIGH_WATERMARK_TXN_TOPIC);
      _kafkaCluster.deleteTopic(LATE_MARKER_TXN_TOPIC);
      _kafkaCluster.deleteTopic(EMPTIED_TOPIC);
    } finally {
      _kafkaCluster.stop();
    }
  }

  /// Scenario 1: a real committed transaction leaves an offset gap at the commit control record,
  /// but all user data at/after the requested startOffset is still retained. This must NOT be
  /// flagged as data loss (the pre-fix code did, raising false StreamDataLoss alerts).
  @Test
  public void testTransactionalGapWithRetainedDataIsNotDataLoss()
      throws Exception {
    // read_uncommitted (default) is the only mode where the pre-fix bug manifested.
    StreamConfig streamConfig = streamConfig(TXN_TOPIC, null, null);
    try (KafkaPartitionLevelConsumer consumer =
        new KafkaPartitionLevelConsumer("txn-gap-client", streamConfig, 0)) {
      // Seek to the commit-marker offset; the first delivered user record is txn-2's at offset 11.
      KafkaMessageBatch batch = fetchUntilRecords(consumer, TXN1_COMMIT_MARKER_OFFSET);

      assertTrue(batch.getMessageCount() > 0, "Expected txn-2 records to be returned");
      // An offset gap MUST exist (first delivered offset is past the requested commit-marker offset)
      // -- otherwise the test would pass without exercising the data-loss code path at all.
      assertTrue(firstOffset(batch) > TXN1_COMMIT_MARKER_OFFSET,
          "Expected an offset gap over the commit control record (first user record is offset "
              + TXN2_FIRST_RECORD_OFFSET + ")");
      assertFalse(batch.hasDataLoss(),
          "Offset gap from a transactional commit marker (data retained, logStart <= startOffset) "
              + "must not be reported as data loss");
    }
  }

  /// Scenario 2: records at/after the requested startOffset were deleted (log start offset advanced
  /// past it). This IS genuine data loss and must be flagged.
  @Test
  public void testTruncatedStartOffsetIsDataLoss()
      throws Exception {
    // auto.offset.reset=earliest so the expired startOffset resets to the (advanced) log start.
    StreamConfig streamConfig = streamConfig(TRUNCATED_TOPIC, null, "earliest");
    try (KafkaPartitionLevelConsumer consumer =
        new KafkaPartitionLevelConsumer("truncated-client", streamConfig, 0)) {
      // Request offset 0, which has been deleted (log start is now 20). The loss is reported on the batch in which
      // the consumer observed the reset: the data batch when the reset and the fetch completed in one poll, else the
      // empty batch before it.
      AtomicBoolean lossReported = new AtomicBoolean();
      KafkaMessageBatch batch = fetchUntilRecords(consumer, 0, lossReported);

      assertTrue(batch.getMessageCount() > 0, "Expected the retained tail of records to be returned");
      assertTrue(firstOffset(batch) >= TRUNCATE_BEFORE_OFFSET,
          "First returned offset should be at/after the advanced log start");
      assertTrue(lossReported.get(),
          "startOffset below the log start offset (records truncated) must be reported as data loss");
    }
  }

  /// Scenario 3: under read_committed the same transactional gap must never be flagged as loss
  /// (aborted/commit control gaps are always expected). This exercises the short-circuit that
  /// skips the beginningOffsets round-trip entirely.
  @Test
  public void testReadCommittedGapIsNotDataLoss()
      throws Exception {
    StreamConfig streamConfig = streamConfig(TXN_TOPIC, "read_committed", null);
    try (KafkaPartitionLevelConsumer consumer =
        new KafkaPartitionLevelConsumer("txn-gap-rc-client", streamConfig, 0)) {
      KafkaMessageBatch batch = fetchUntilRecords(consumer, TXN1_COMMIT_MARKER_OFFSET);

      assertTrue(batch.getMessageCount() > 0, "Expected txn-2 records to be returned");
      assertTrue(firstOffset(batch) > TXN1_COMMIT_MARKER_OFFSET, "Sanity: an offset gap must exist");
      assertFalse(batch.hasDataLoss(), "read_committed must never flag an offset gap as data loss");
    }
  }

  /// Scenario 4, the production pattern behind this check: a caught-up consumer on a low-traffic transactional
  /// topic where every transaction carries a single record. Each record is followed by its commit marker, which
  /// the Kafka client skips without delivering anything, so after record N the consumer is parked at the marker
  /// offset N + 1 and the next record lands at N + 2. RealtimeSegmentDataManager keeps requesting N + 1 (no
  /// record was ever delivered there), so every new transaction shows up as a one-offset gap, e.g. 196 -> 197.
  /// That gap must not be reported as data loss, transaction after transaction.
  @Test
  public void testCaughtUpConsumerOnLowTrafficTransactionalTopicIsNotDataLoss()
      throws Exception {
    StreamConfig streamConfig = streamConfig(LOW_TRAFFIC_TXN_TOPIC, null, null);
    try (KafkaProducer<String, String> producer = createTransactionalProducer();
        KafkaPartitionLevelConsumer consumer =
            new KafkaPartitionLevelConsumer("txn-low-traffic-client", streamConfig, 0)) {
      // Transaction 1: record at offset 0, commit marker at offset 1
      commitSingleRecordTransaction(producer, LOW_TRAFFIC_TXN_TOPIC, "txn-1");
      KafkaMessageBatch batch = fetchUntilRecords(consumer, 0);
      assertEquals(batch.getMessageCount(), 1);
      assertEquals(firstOffset(batch), 0);
      assertFalse(batch.hasDataLoss());

      // Caught up: the caller asks for offset 1, the marker. The client skips it and returns nothing, and the
      // empty batch reports the consumer's real position (2) as the start of the next batch.
      KafkaMessageBatch emptyBatch = fetchUntilNextBatchOffset(consumer, 1, 2);
      assertFalse(emptyBatch.hasDataLoss());

      // Transaction 2: record at offset 2, marker at 3. The caller still asks for offset 1, as
      // RealtimeSegmentDataManager would since no record has been delivered since.
      commitSingleRecordTransaction(producer, LOW_TRAFFIC_TXN_TOPIC, "txn-2");
      batch = fetchUntilRecords(consumer, 1);
      assertEquals(batch.getMessageCount(), 1);
      assertEquals(firstOffset(batch), 2, "The first record is one past the requested offset (the commit marker)");
      assertFalse(batch.hasDataLoss(),
          "A one-offset gap over a commit marker on a caught-up consumer must not be reported as data loss");

      // Transaction 3 repeats the pattern from the new position: the caller asks for 3 (the marker) and gets the
      // record at 4, with the marker skipped either in this poll or in an earlier empty one.
      commitSingleRecordTransaction(producer, LOW_TRAFFIC_TXN_TOPIC, "txn-3");
      batch = fetchUntilRecords(consumer, 3);
      assertEquals(batch.getMessageCount(), 1);
      assertEquals(firstOffset(batch), 4);
      assertFalse(batch.hasDataLoss());
    }
  }

  /// Scenario 5, what RealtimeSegmentDataManager now does with the batches above: it adopts the next-batch offset of
  /// an empty batch, so after each single-record transaction the current offset moves past the commit marker to the
  /// high watermark and the records lag reads 0, instead of parking at the marker with a lag of 1.
  ///
  /// Whether the marker arrives in the same fetch as its record decides whether a beginningOffsets round trip is paid.
  /// The first transaction is fetched by a consumer with nothing in flight, after record and marker are both in the
  /// log, so one fetch delivers the record and skips the marker: no lookup. From then on the consumer always has a
  /// fetch parked on the broker, which is answered as soon as the record is appended, so the marker usually lands in a
  /// later fetch and the empty poll that skips it pays one lookup (scenario 6 pins that case down). The client position
  /// after each data poll, observed through the proxy, tells which case happened, and the lookup count is asserted
  /// against it.
  @Test
  public void testCaughtUpTransactionalConsumerReachesHighWatermarkWithZeroLag()
      throws Exception {
    StreamConfig streamConfig = streamConfig(HIGH_WATERMARK_TXN_TOPIC, null, null);
    ObservedClientCalls clientCalls = new ObservedClientCalls();
    try (KafkaProducer<String, String> producer = createTransactionalProducer();
        KafkaPartitionLevelConsumer consumer = createObservedConsumer("txn-hwm-client", streamConfig, clientCalls);
        KafkaStreamMetadataProvider metadataProvider =
            new KafkaStreamMetadataProvider("txn-hwm-metadata", streamConfig, 0)) {
      long currentOffset = 0;
      int expectedLogStartLookups = 0;
      for (int txn = 1; txn <= 3; txn++) {
        commitSingleRecordTransaction(producer, HIGH_WATERMARK_TXN_TOPIC, "txn-" + txn);
        // One record and one commit marker per transaction. commitTransaction() returns before the coordinator has
        // appended the marker to the partition, so wait for it: both are then in the log when the consumer fetches.
        long highWatermark = awaitHighWatermark(metadataProvider, 2L * txn);

        // The data poll: the consumer parked past the previous marker, so the record is right where it was requested
        AtomicBoolean lossReported = new AtomicBoolean();
        KafkaMessageBatch batch = fetchUntilRecords(consumer, currentOffset, lossReported);
        assertEquals(batch.getMessageCount(), 1);
        assertEquals(firstOffset(batch), currentOffset, "No gap in front of the record");
        assertFalse(lossReported.get());
        assertEquals(clientCalls._logStartLookups.get(), expectedLogStartLookups,
            "A data poll that starts where its first record is needs no lookup");
        currentOffset = nextOffset(batch.getStreamMessage(0).getMetadata());
        assertEquals(currentOffset, highWatermark - 1, "Parked at the commit marker after the record");
        assertEquals(recordsLag(metadataProvider, currentOffset, highWatermark), "1");
        boolean markerInSameFetch = clientCalls._lastPosition.get() == highWatermark;
        if (txn == 1) {
          assertTrue(markerInSameFetch, "Nothing was in flight, so the first fetch delivered the record and skipped "
              + "the marker behind it");
        }

        // The empty poll: the client moves past the marker (or already did), and the data manager adopts its position
        KafkaMessageBatch emptyBatch = fetchUntilNextBatchOffset(consumer, currentOffset, highWatermark);
        assertFalse(emptyBatch.hasDataLoss());
        if (!markerInSameFetch) {
          expectedLogStartLookups++;
        }
        assertEquals(clientCalls._logStartLookups.get(), expectedLogStartLookups,
            "One lookup only for a marker skipped during an empty poll, none for one skipped while delivering");
        currentOffset = adoptEmptyBatchOffset(currentOffset, emptyBatch);
        assertEquals(currentOffset, highWatermark, "Parked past the commit marker");
        assertEquals(recordsLag(metadataProvider, currentOffset, highWatermark), "0");
      }
    }
  }

  /// Scenario 6: the commit marker lands in a later fetch than its record, which is what a caught-up consumer sees
  /// when the broker answers its parked fetch as soon as the record is appended and the marker follows a few
  /// milliseconds later. The empty poll that skips the marker is the one move that cannot be told apart from an
  /// auto.offset.reset without asking the broker, so it pays exactly one beginningOffsets round trip, reports no loss
  /// while the data is retained, and still lands the adopted offset on the high watermark. Idle polls after that
  /// neither move nor look anything up.
  @Test
  public void testMarkerInLaterFetchIsValidatedOnceAndReachesHighWatermark()
      throws Exception {
    StreamConfig streamConfig = streamConfig(LATE_MARKER_TXN_TOPIC, null, null);
    ObservedClientCalls clientCalls = new ObservedClientCalls();
    try (KafkaProducer<String, String> producer = createTransactionalProducer();
        KafkaPartitionLevelConsumer consumer =
            createObservedConsumer("txn-late-marker-client", streamConfig, clientCalls);
        KafkaStreamMetadataProvider metadataProvider =
            new KafkaStreamMetadataProvider("txn-late-marker-metadata", streamConfig, 0)) {
      // The record is appended, and visible under read_uncommitted, while the transaction is still open
      producer.beginTransaction();
      producer.send(new ProducerRecord<>(LATE_MARKER_TXN_TOPIC, 0, null, "txn-1")).get();
      awaitHighWatermark(metadataProvider, 1);
      AtomicBoolean lossReported = new AtomicBoolean();
      KafkaMessageBatch batch = fetchUntilRecords(consumer, 0, lossReported);
      assertEquals(batch.getMessageCount(), 1);
      assertEquals(firstOffset(batch), 0);
      assertFalse(lossReported.get());
      assertEquals(clientCalls._logStartLookups.get(), 0);
      long currentOffset = nextOffset(batch.getStreamMessage(0).getMetadata());
      assertEquals(currentOffset, 1);

      // The commit marker lands at offset 1 after the record was fetched; the next poll skips it and moves to 2
      producer.commitTransaction();
      awaitHighWatermark(metadataProvider, 2);
      KafkaMessageBatch emptyBatch = fetchUntilNextBatchOffset(consumer, currentOffset, 2);
      assertFalse(emptyBatch.hasDataLoss(), "A commit marker skipped during an empty poll is not data loss");
      assertEquals(clientCalls._logStartLookups.get(), 1,
          "A position move during an empty poll is validated against the log start offset, once");
      currentOffset = adoptEmptyBatchOffset(currentOffset, emptyBatch);
      assertEquals(currentOffset, 2);
      assertEquals(recordsLag(metadataProvider, currentOffset, 2), "0");

      emptyBatch = consumer.fetchMessages(new LongMsgOffset(currentOffset), FETCH_TIMEOUT_MS);
      assertEquals(emptyBatch.getMessageCount(), 0);
      assertEquals(nextBatchOffset(emptyBatch), 2);
      assertFalse(emptyBatch.hasDataLoss());
      assertEquals(clientCalls._logStartLookups.get(), 1,
          "An idle poll that does not move the position looks nothing up");
    }
  }

  /// Scenario 7: an auto.offset.reset inside an empty poll. Every record of the topic was deleted, so the log starts
  /// at the high watermark; a fetch at offset 0 is out of range, the client resets to the log start and finds nothing
  /// to deliver there. The empty batch must report both the new position and the loss, since the data manager adopts
  /// the position and a later data poll would start exactly where it was requested.
  @Test
  public void testResetInsideEmptyPollIsDataLoss()
      throws Exception {
    StreamConfig streamConfig = streamConfig(EMPTIED_TOPIC, null, "earliest");
    try (KafkaPartitionLevelConsumer consumer =
        new KafkaPartitionLevelConsumer("emptied-client", streamConfig, 0)) {
      KafkaMessageBatch batch = fetchUntilNextBatchOffset(consumer, 0, EMPTIED_TOPIC_RECORDS);
      assertTrue(batch.hasDataLoss(),
          "A reset past deleted records during an empty poll must be reported on the empty batch");
    }
  }

  /// What RealtimeSegmentDataManager does with an empty batch: adopt its next-batch offset when it is ahead.
  private static long adoptEmptyBatchOffset(long currentOffset, KafkaMessageBatch emptyBatch) {
    assertEquals(emptyBatch.getMessageCount(), 0);
    return Math.max(currentOffset, nextBatchOffset(emptyBatch));
  }

  private static long highWatermark(KafkaStreamMetadataProvider metadataProvider) {
    return ((LongMsgOffset) metadataProvider.fetchStreamPartitionOffset(
        new OffsetCriteria.OffsetCriteriaBuilder().withOffsetLargest(), FETCH_TIMEOUT_MS)).getOffset();
  }

  /// Waits until the partition's high watermark reaches `expected` (a transaction's commit marker is appended by the
  /// coordinator after commitTransaction() has returned) and returns it, failing if it lands anywhere else.
  private static long awaitHighWatermark(KafkaStreamMetadataProvider metadataProvider, long expected)
      throws InterruptedException {
    long deadlineMs = System.currentTimeMillis() + FETCH_MAX_WAIT_MS;
    long highWatermark = highWatermark(metadataProvider);
    while (highWatermark < expected && System.currentTimeMillis() < deadlineMs) {
      Thread.sleep(10);
      highWatermark = highWatermark(metadataProvider);
    }
    assertEquals(highWatermark, expected);
    return highWatermark;
  }

  /// The records lag the server reports for partition 0 at `currentOffset` with the given upstream high watermark.
  private static String recordsLag(KafkaStreamMetadataProvider metadataProvider, long currentOffset,
      long highWatermark) {
    Map<String, PartitionLagState> lagState = metadataProvider.getCurrentPartitionLagState(Map.of("0",
        new ConsumerPartitionState("0", new LongMsgOffset(currentOffset), System.currentTimeMillis(),
            new LongMsgOffset(highWatermark), null)));
    return lagState.get("0").getRecordsLag();
  }

  /// What the consumer asked the Kafka client, observed through a proxy around the real client: the number of
  /// beginningOffsets calls (the log start offset lookup behind the data-loss decision) and the value of the last
  /// position() call (where the client stands after a poll).
  private static final class ObservedClientCalls {
    final AtomicInteger _logStartLookups = new AtomicInteger();
    final AtomicLong _lastPosition = new AtomicLong(-1);
  }

  /// A consumer against the real broker whose calls to the Kafka client are recorded in `clientCalls`.
  private static KafkaPartitionLevelConsumer createObservedConsumer(String clientId, StreamConfig streamConfig,
      ObservedClientCalls clientCalls) {
    return new KafkaPartitionLevelConsumer(clientId, streamConfig, 0) {
      @Override
      @SuppressWarnings("unchecked")
      protected Consumer<Bytes, Bytes> createConsumer(Properties consumerProp) {
        Consumer<Bytes, Bytes> realConsumer = super.createConsumer(consumerProp);
        return (Consumer<Bytes, Bytes>) Proxy.newProxyInstance(Consumer.class.getClassLoader(),
            new Class<?>[]{Consumer.class}, (proxy, method, args) -> {
              if (method.getName().equals("beginningOffsets")) {
                clientCalls._logStartLookups.incrementAndGet();
              }
              Object result;
              try {
                result = method.invoke(realConsumer, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
              if (method.getName().equals("position")) {
                clientCalls._lastPosition.set((Long) result);
              }
              return result;
            });
      }
    };
  }

  /// Polls repeatedly at the same startOffset until a non-empty batch is returned (or the wait
  /// budget elapses). Repeating the same startOffset hits the consumer's "no re-seek" path, so this
  /// does not disturb offset positioning; it only tolerates an empty first poll while data is
  /// fetched (and, for the truncation case, while the offset reset takes effect).
  private KafkaMessageBatch fetchUntilRecords(KafkaPartitionLevelConsumer consumer, long startOffset) {
    return fetchUntilRecords(consumer, startOffset, new AtomicBoolean());
  }

  /// Like [#fetchUntilRecords(KafkaPartitionLevelConsumer, long)], and sets `lossReported` if any batch along the
  /// way, the returned one included, reported data loss. A reset after an out-of-range fetch is reported on the batch
  /// in which the consumer observed the position move, which is an empty batch when the reset completed in a poll
  /// that returned nothing; the data poll after it starts at the new position and does not report the loss again.
  private KafkaMessageBatch fetchUntilRecords(KafkaPartitionLevelConsumer consumer, long startOffset,
      AtomicBoolean lossReported) {
    long deadlineMs = System.currentTimeMillis() + FETCH_MAX_WAIT_MS;
    KafkaMessageBatch batch = consumer.fetchMessages(new LongMsgOffset(startOffset), FETCH_TIMEOUT_MS);
    lossReported.compareAndSet(false, batch.hasDataLoss());
    while (batch.getMessageCount() == 0 && System.currentTimeMillis() < deadlineMs) {
      batch = consumer.fetchMessages(new LongMsgOffset(startOffset), FETCH_TIMEOUT_MS);
      lossReported.compareAndSet(false, batch.hasDataLoss());
    }
    return batch;
  }

  /// Polls repeatedly at the same startOffset until the (empty) batch reports `expectedNextBatchOffset` as the
  /// start of the next batch, i.e. until the consumer has observed and skipped the control record(s) at/after
  /// startOffset, or the wait budget elapses. Fails if a record shows up instead.
  private KafkaMessageBatch fetchUntilNextBatchOffset(KafkaPartitionLevelConsumer consumer, long startOffset,
      long expectedNextBatchOffset) {
    long deadlineMs = System.currentTimeMillis() + FETCH_MAX_WAIT_MS;
    KafkaMessageBatch batch = consumer.fetchMessages(new LongMsgOffset(startOffset), FETCH_TIMEOUT_MS);
    while (batch.getMessageCount() == 0 && nextBatchOffset(batch) < expectedNextBatchOffset
        && System.currentTimeMillis() < deadlineMs) {
      batch = consumer.fetchMessages(new LongMsgOffset(startOffset), FETCH_TIMEOUT_MS);
    }
    assertEquals(batch.getMessageCount(), 0, "Expected no record at/after offset " + startOffset + " yet");
    assertEquals(nextBatchOffset(batch), expectedNextBatchOffset,
        "Expected the empty batch to report the consumer position past the skipped control record");
    return batch;
  }

  private static long nextOffset(StreamMessageMetadata metadata) {
    return Long.parseLong(metadata.getNextOffset().toString());
  }

  private static long firstOffset(KafkaMessageBatch batch) {
    return Long.parseLong(batch.getFirstMessageOffset().toString());
  }

  private static long nextBatchOffset(KafkaMessageBatch batch) {
    return Long.parseLong(batch.getOffsetOfNextBatch().toString());
  }

  private StreamConfig streamConfig(String topic, String isolationLevel, String autoOffsetReset) {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", topic);
    streamConfigMap.put("stream.kafka.broker.list", _kafkaBrokerAddress);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", KafkaConsumerFactory.class.getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    if (isolationLevel != null) {
      streamConfigMap.put("stream.kafka.isolation.level", isolationLevel);
    }
    if (autoOffsetReset != null) {
      streamConfigMap.put("auto.offset.reset", autoOffsetReset);
    }
    return new StreamConfig("tableName_REALTIME", streamConfigMap);
  }

  private void produceTransactional(String topic, int numTransactions, int recordsPerTransaction) {
    int seq = 0;
    try (KafkaProducer<String, String> producer = createTransactionalProducer()) {
      for (int t = 0; t < numTransactions; t++) {
        producer.beginTransaction();
        for (int i = 0; i < recordsPerTransaction; i++) {
          producer.send(new ProducerRecord<>(topic, 0, null, "msg-" + (seq++)));
        }
        producer.commitTransaction();
      }
    }
  }

  /// Commits one transaction holding a single record, i.e. one record followed by one commit marker in the log.
  private static void commitSingleRecordTransaction(KafkaProducer<String, String> producer, String topic,
      String value) {
    producer.beginTransaction();
    producer.send(new ProducerRecord<>(topic, 0, null, value));
    producer.commitTransaction();
  }

  /// Creates a producer with a unique transactional id and initializes it, so callers can begin transactions.
  private KafkaProducer<String, String> createTransactionalProducer() {
    Properties props = producerProps();
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-transaction-" + UUID.randomUUID());
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    producer.initTransactions();
    return producer;
  }

  private void producePlain(String topic, int count) {
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
      for (int i = 0; i < count; i++) {
        producer.send(new ProducerRecord<>(topic, 0, null, "msg-" + i));
      }
      producer.flush();
    }
  }

  private Properties producerProps() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaBrokerAddress);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return props;
  }
}
