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
package org.apache.pinot.integration.tests;

import com.google.common.primitives.Longs;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.pinot.integration.tests.SharedKafkaRealtimeIntegrationTestSuite.ScenarioLease;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;


/// Focused transactional-ingestion scenario for the shared Kafka realtime suite.
public class ExactlyOnceKafkaRealtimeClusterIntegrationTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ExactlyOnceKafkaRealtimeClusterIntegrationTest.class);
  private static final String TABLE_NAME = "mytableExactlyOnce";
  private static final String TOPIC_NAME = "ExactlyOnceKafkaRealtimeClusterIntegrationTest";
  private static final int RECORDS_PER_TRANSACTION = 1_000;
  private static final long RECORDS_PER_TRANSACTIONAL_PHASE = 2L * RECORDS_PER_TRANSACTION;
  private static final int REALTIME_TABLE_CONFIG_RETRY_COUNT = 5;
  private static final long REALTIME_TABLE_CONFIG_RETRY_WAIT_MS = 1_000L;
  private static final long KAFKA_TOPIC_METADATA_READY_TIMEOUT_MS = 60_000L;
  private static final long COUNT_RECORDS_DRAIN_TIMEOUT_MS = 30_000L;
  private static final Duration COUNT_RECORDS_POLL_TIMEOUT = Duration.ofMillis(50);
  private static final Duration COUNT_RECORDS_QUIESCENCE_POLL_TIMEOUT = Duration.ofMillis(200);
  private static final Duration COUNT_RECORDS_CLOSE_TIMEOUT = Duration.ofSeconds(5);

  @Test
  public void testCommittedTransactionsOnly()
      throws Throwable {
    SharedKafkaRealtimeIntegrationTestSuite owner =
        SharedKafkaRealtimeIntegrationTestSuite.getSharedSuiteOwner();
    ScenarioLease lease = owner.newScenario(TABLE_NAME, TOPIC_NAME);
    Throwable primaryFailure = null;
    try {
      owner.createScenarioTopic(lease);
      List<File> avroFiles = owner.unpackScenarioData(lease);
      owner.addScenarioSchema(lease);
      TableConfig tableConfig = owner.createScenarioTableConfig(lease, avroFiles.get(0),
          owner.getScenarioStreamConfigs(lease._topicName, true));
      addTableConfigWithRetry(owner, lease, tableConfig);

      long expectedRecords = RECORDS_PER_TRANSACTIONAL_PHASE;
      pushTransactionalData(owner, lease, avroFiles, expectedRecords);
      waitForPinotCount(owner, lease, expectedRecords, 1_200_000L);
    } catch (Throwable t) {
      primaryFailure = t;
      throw t;
    } finally {
      owner.closeScenario(lease, primaryFailure, null);
    }
  }

  private void addTableConfigWithRetry(SharedKafkaRealtimeIntegrationTestSuite owner, ScenarioLease lease,
      TableConfig tableConfig)
      throws Exception {
    lease._tableCreated = true;
    for (int attempt = 1; attempt <= REALTIME_TABLE_CONFIG_RETRY_COUNT; attempt++) {
      try {
        owner.addTableConfig(tableConfig);
        owner.waitForAllRealtimePartitionsConsuming(
            TableNameBuilder.REALTIME.tableNameWithType(lease._tableName),
            owner.getRealtimePartitionsReadyTimeoutMs());
        return;
      } catch (IOException e) {
        if (owner.scenarioTableExists(lease._tableName)) {
          owner.waitForAllRealtimePartitionsConsuming(
              TableNameBuilder.REALTIME.tableNameWithType(lease._tableName),
              owner.getRealtimePartitionsReadyTimeoutMs());
          return;
        }
        if (!isRetryableRealtimePartitionMetadataError(e, lease._topicName)
            || attempt == REALTIME_TABLE_CONFIG_RETRY_COUNT) {
          throw e;
        }
        waitForKafkaTopicMetadataReadyForConsumer(owner.getKafkaBrokerList(), lease._topicName,
            owner.getNumKafkaPartitions());
        try {
          Thread.sleep(REALTIME_TABLE_CONFIG_RETRY_WAIT_MS);
        } catch (InterruptedException interruptedException) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while retrying table creation: " + lease._tableName,
              interruptedException);
        }
      }
    }
  }

  private void pushTransactionalData(SharedKafkaRealtimeIntegrationTestSuite owner, ScenarioLease lease,
      List<File> avroFiles, long expectedRecords)
      throws Exception {
    String brokerList = owner.getKafkaBrokerList();
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000");
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-transaction-" + UUID.randomUUID());

    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
      producer.initTransactions();
      long abortedCount = pushAvroRecords(owner, lease, producer, avroFiles, false, expectedRecords);
      long committedCount = pushAvroRecords(owner, lease, producer, avroFiles, true, expectedRecords);
      if (abortedCount != expectedRecords || committedCount != expectedRecords) {
        throw new AssertionError(String.format(
            "Unexpected transactional input counts: aborted=%d committed=%d expected=%d", abortedCount,
            committedCount, expectedRecords));
      }
    }

    waitForTransactionalVisibility(brokerList, lease._topicName, expectedRecords);
  }

  private long pushAvroRecords(SharedKafkaRealtimeIntegrationTestSuite owner, ScenarioLease lease,
      KafkaProducer<byte[], byte[]> producer, List<File> avroFiles, boolean commit, long maxRecords)
      throws Exception {
    long counter = 0L;
    int recordsInTransaction = 0;
    boolean hasOpenTransaction = false;
    byte[] header = owner.getKafkaMessageHeader();
    String partitionColumn = owner.getPartitionColumn();

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65_536)) {
      for (File avroFile : avroFiles) {
        try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
          BinaryEncoder binaryEncoder = new EncoderFactory().directBinaryEncoder(outputStream, null);
          GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(reader.getSchema());
          for (GenericRecord genericRecord : reader) {
            if (!hasOpenTransaction) {
              producer.beginTransaction();
              hasOpenTransaction = true;
              recordsInTransaction = 0;
            }

            outputStream.reset();
            if (header != null && header.length > 0) {
              outputStream.write(header);
            }
            datumWriter.write(genericRecord, binaryEncoder);
            binaryEncoder.flush();

            byte[] keyBytes = partitionColumn == null ? Longs.toByteArray(counter)
                : genericRecord.get(partitionColumn).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            producer.send(new ProducerRecord<>(lease._topicName, keyBytes, outputStream.toByteArray()));
            counter++;
            recordsInTransaction++;
            if (recordsInTransaction >= RECORDS_PER_TRANSACTION) {
              finishTransaction(producer, commit);
              hasOpenTransaction = false;
            }
            if (counter >= maxRecords) {
              if (hasOpenTransaction) {
                finishTransaction(producer, commit);
              }
              return counter;
            }
          }
        }
      }
    }
    if (hasOpenTransaction) {
      finishTransaction(producer, commit);
    }
    return counter;
  }

  private static void finishTransaction(KafkaProducer<byte[], byte[]> producer, boolean commit) {
    // Ensure aborted records reach the broker so read_uncommitted observes the complete transaction.
    producer.flush();
    if (commit) {
      producer.commitTransaction();
    } else {
      producer.abortTransaction();
    }
  }

  private void waitForTransactionalVisibility(String brokerList, String topic, long expected) {
    long deadline = System.currentTimeMillis() + 120_000L;
    int lastCommitted = 0;
    int lastUncommitted = 0;
    int iteration = 0;
    while (System.currentTimeMillis() < deadline) {
      iteration++;
      lastCommitted = countRecords(brokerList, topic, "read_committed", expected);
      lastUncommitted = countRecords(brokerList, topic, "read_uncommitted", expected * 2L);
      if (lastCommitted == expected && lastUncommitted == expected * 2L) {
        return;
      }
      if (lastCommitted > expected || lastUncommitted > expected * 2L) {
        throw new AssertionError(String.format(
            "[ExactlyOnce] transactional views overshot: read_committed=%d expected=%d read_uncommitted=%d "
                + "expectedUncommitted=%d", lastCommitted, expected, lastUncommitted, expected * 2L));
      }
      if (iteration == 1 || iteration % 5 == 0) {
        LOGGER.info("Transactional visibility: committed={}/{}, uncommitted={}/{}", lastCommitted, expected,
            lastUncommitted, expected * 2L);
      }
      try {
        Thread.sleep(2_000L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    throw new AssertionError(String.format(
        "[ExactlyOnce] topic views did not converge: read_committed=%d expected=%d read_uncommitted=%d "
            + "expectedUncommitted=%d", lastCommitted, expected, lastUncommitted, expected * 2L));
  }

  private void waitForPinotCount(SharedKafkaRealtimeIntegrationTestSuite owner, ScenarioLease lease, long expected,
      long timeoutMs) {
    long start = System.currentTimeMillis();
    long deadline = start + timeoutMs;
    long lastProgressLog = 0L;
    long lastCount = -1L;
    while (System.currentTimeMillis() < deadline) {
      try {
        lastCount = owner.getCurrentCountStarResult(lease._tableName);
      } catch (Exception e) {
        LOGGER.debug("Count query failed while transactional ingestion was converging", e);
      }
      if (lastCount == expected) {
        return;
      }
      long now = System.currentTimeMillis();
      if (now - lastProgressLog >= 5_000L) {
        LOGGER.error("Transactional Pinot ingestion progress: elapsedMs={} count={} expected={} kafkaCommitted={}",
            now - start, lastCount, expected,
            countRecords(owner.getKafkaBrokerList(), lease._topicName, "read_committed", expected));
        lastProgressLog = now;
      }
      try {
        Thread.sleep(500L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    dumpRelevantThreadStacks();
    throw new AssertionError(String.format(
        "Failed to load %d committed documents into %s (lastCount=%d, elapsed=%dms)", expected, lease._tableName,
        lastCount, System.currentTimeMillis() - start));
  }

  private int countRecords(String brokerList, String topic, String isolationLevel, long expectedRecords) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "txn-diag-" + isolationLevel + "-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100000");
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.toString(50 * 1024 * 1024));
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, Integer.toString(100 * 1024 * 1024));
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "50");

    int totalRecords = 0;
    KafkaConsumer<byte[], byte[]> consumer = null;
    try {
      consumer = new KafkaConsumer<>(props);
      List<PartitionInfo> partitions = consumer.partitionsFor(topic, Duration.ofSeconds(10));
      if (partitions == null || partitions.isEmpty()) {
        return 0;
      }
      List<TopicPartition> topicPartitions = new ArrayList<>(partitions.size());
      for (PartitionInfo partition : partitions) {
        topicPartitions.add(new TopicPartition(partition.topic(), partition.partition()));
      }
      consumer.assign(topicPartitions);
      consumer.seekToBeginning(topicPartitions);
      long deadline = System.currentTimeMillis() + COUNT_RECORDS_DRAIN_TIMEOUT_MS;
      while (System.currentTimeMillis() < deadline) {
        totalRecords += consumer.poll(COUNT_RECORDS_POLL_TIMEOUT).count();
        // Raw Kafka end offsets include transaction control records, so visible data count is the completion signal.
        if (totalRecords >= expectedRecords) {
          if (totalRecords == expectedRecords) {
            totalRecords += consumer.poll(COUNT_RECORDS_QUIESCENCE_POLL_TIMEOUT).count();
          }
          return totalRecords;
        }
      }
      LOGGER.warn("Kafka diagnostic count timed out: isolation={} count={} expected={}",
          isolationLevel, totalRecords, expectedRecords);
    } catch (Exception e) {
      LOGGER.error("Error counting Kafka records with isolation level {}", isolationLevel, e);
      totalRecords = -1;
    } finally {
      if (consumer != null) {
        try {
          consumer.close(COUNT_RECORDS_CLOSE_TIMEOUT);
        } catch (Exception e) {
          LOGGER.warn("Failed to close Kafka diagnostic consumer for isolation level {}", isolationLevel, e);
        }
      }
    }
    return totalRecords;
  }

  private static boolean isRetryableRealtimePartitionMetadataError(Throwable throwable, String topic) {
    String errorToken = "Failed to fetch partition information for topic: " + topic;
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current.getMessage() != null && current.getMessage().contains(errorToken)) {
        return true;
      }
    }
    return false;
  }

  private static void waitForKafkaTopicMetadataReadyForConsumer(String brokerList, String topic,
      int expectedPartitions) {
    TestUtils.waitForCondition(aVoid -> isKafkaTopicMetadataReadyForConsumer(brokerList, topic, expectedPartitions,
            "read_uncommitted"), 200L, KAFKA_TOPIC_METADATA_READY_TIMEOUT_MS,
        "Kafka topic metadata is not visible: " + topic);
    TestUtils.waitForCondition(aVoid -> isKafkaTopicMetadataReadyForConsumer(brokerList, topic, expectedPartitions,
            "read_committed"), 200L, KAFKA_TOPIC_METADATA_READY_TIMEOUT_MS,
        "Kafka transactional topic metadata is not visible: " + topic);
  }

  private static boolean isKafkaTopicMetadataReadyForConsumer(String brokerList, String topic,
      int expectedPartitions, String isolationLevel) {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "pinot-topic-ready-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
    consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    consumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
      List<PartitionInfo> partitions = consumer.partitionsFor(topic, Duration.ofSeconds(5));
      return partitions != null && partitions.size() >= expectedPartitions;
    } catch (Exception e) {
      return false;
    }
  }

  private static void dumpRelevantThreadStacks() {
    int dumped = 0;
    for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
      String name = entry.getKey().getName();
      if (name == null || !(name.contains("RealtimeSegment") || name.contains("kafka") || name.contains("Kafka")
          || name.contains("HelixTaskExecutor") || name.contains("PartitionConsumer"))) {
        continue;
      }
      StringBuilder stack = new StringBuilder("Thread '").append(name).append("' state=")
          .append(entry.getKey().getState());
      for (StackTraceElement element : entry.getValue()) {
        stack.append("\n    at ").append(element);
      }
      LOGGER.error(stack.toString());
      dumped++;
    }
    LOGGER.error("Dumped {} Kafka/Pinot consumer thread stacks", dumped);
  }
}
