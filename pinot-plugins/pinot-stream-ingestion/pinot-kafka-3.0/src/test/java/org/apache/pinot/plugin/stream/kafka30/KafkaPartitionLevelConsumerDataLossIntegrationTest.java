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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pinot.plugin.stream.kafka.KafkaMessageBatch;
import org.apache.pinot.plugin.stream.kafka30.server.EmbeddedKafkaCluster;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// End-to-end (real embedded broker) regression tests for [KafkaPartitionLevelConsumer] data-loss
/// detection (see the fix for false `StreamDataLoss` on transactional Kafka topics).
///
/// Unlike [KafkaPartitionLevelConsumerDataLossTest] (which mocks the Kafka consumer), these tests
/// run against an in-process [EmbeddedKafkaCluster], so they exercise the real transactional
/// control-record offset gaps and the real `beginningOffsets` round-trip added by the fix.
///
/// Two scenarios, mirroring the reviewer's request:
/// 1. Perform a real transaction and confirm no data loss is reported for the (expected) offset gap
///    left by commit control records while the data is still retained.
/// 2. Delete offsets (advance the log start via [EmbeddedKafkaCluster#deleteRecordsBeforeOffset])
///    and confirm data loss IS reported.
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

  private EmbeddedKafkaCluster _kafkaCluster;
  private String _kafkaBrokerAddress;

  @BeforeClass
  public void setUp()
      throws Exception {
    Properties props = new Properties();
    props.setProperty(EmbeddedKafkaCluster.BROKER_COUNT_PROP, "1");
    _kafkaCluster = new EmbeddedKafkaCluster();
    _kafkaCluster.init(props);
    _kafkaCluster.start();
    _kafkaBrokerAddress = _kafkaCluster.bootstrapServers();

    // createTopic uses AdminClient.createTopics().all().get() -> synchronous, no sleep needed.
    _kafkaCluster.createTopic(TXN_TOPIC, 1);
    _kafkaCluster.createTopic(TRUNCATED_TOPIC, 1);

    // commitTransaction()/flush() are synchronous -> records are durable on return, no sleep needed.
    produceTransactional(TXN_TOPIC, 2, RECORDS_PER_TXN);
    producePlain(TRUNCATED_TOPIC, TRUNCATED_TOPIC_RECORDS);

    // deleteRecords().all().get() is synchronous -> log start offset advanced on return.
    _kafkaCluster.deleteRecordsBeforeOffset(TRUNCATED_TOPIC, 0, TRUNCATE_BEFORE_OFFSET);
  }

  @AfterClass
  public void tearDown() {
    try {
      _kafkaCluster.deleteTopic(TXN_TOPIC);
      _kafkaCluster.deleteTopic(TRUNCATED_TOPIC);
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
      // Request offset 0, which has been deleted (log start is now 20).
      KafkaMessageBatch batch = fetchUntilRecords(consumer, 0);

      assertTrue(batch.getMessageCount() > 0, "Expected the retained tail of records to be returned");
      assertTrue(firstOffset(batch) >= TRUNCATE_BEFORE_OFFSET,
          "First returned offset should be at/after the advanced log start");
      assertTrue(batch.hasDataLoss(),
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

  /// Polls repeatedly at the same startOffset until a non-empty batch is returned (or the wait
  /// budget elapses). Repeating the same startOffset hits the consumer's "no re-seek" path, so this
  /// does not disturb offset positioning; it only tolerates an empty first poll while data is
  /// fetched (and, for the truncation case, while the offset reset takes effect).
  private KafkaMessageBatch fetchUntilRecords(KafkaPartitionLevelConsumer consumer, long startOffset) {
    long deadlineMs = System.currentTimeMillis() + FETCH_MAX_WAIT_MS;
    KafkaMessageBatch batch = consumer.fetchMessages(new LongMsgOffset(startOffset), FETCH_TIMEOUT_MS);
    while (batch.getMessageCount() == 0 && System.currentTimeMillis() < deadlineMs) {
      batch = consumer.fetchMessages(new LongMsgOffset(startOffset), FETCH_TIMEOUT_MS);
    }
    return batch;
  }

  private static long firstOffset(KafkaMessageBatch batch) {
    return Long.parseLong(batch.getFirstMessageOffset().toString());
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
    Properties props = producerProps();
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-transaction-" + UUID.randomUUID());
    int seq = 0;
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.initTransactions();
      for (int t = 0; t < numTransactions; t++) {
        producer.beginTransaction();
        for (int i = 0; i < recordsPerTransaction; i++) {
          producer.send(new ProducerRecord<>(topic, 0, null, "msg-" + (seq++)));
        }
        producer.commitTransaction();
      }
    }
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
