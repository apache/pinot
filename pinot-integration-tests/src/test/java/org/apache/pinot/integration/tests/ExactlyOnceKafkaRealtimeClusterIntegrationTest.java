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
import java.util.LinkedHashMap;
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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExactlyOnceKafkaRealtimeClusterIntegrationTest extends BaseRealtimeClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExactlyOnceKafkaRealtimeClusterIntegrationTest.class);
  private static final int REALTIME_TABLE_CONFIG_RETRY_COUNT = 5;
  private static final long REALTIME_TABLE_CONFIG_RETRY_WAIT_MS = 1_000L;
  private static final long KAFKA_TOPIC_METADATA_READY_TIMEOUT_MS = 60_000L;
  private static final long COUNT_RECORDS_DRAIN_TIMEOUT_MS = 60_000L;
  private static final Duration COUNT_RECORDS_END_OFFSETS_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration COUNT_RECORDS_POSITION_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration COUNT_RECORDS_PRIMING_POLL_TIMEOUT = Duration.ofMillis(200);
  private static final Duration COUNT_RECORDS_POLL_TIMEOUT = Duration.ofSeconds(2);

  @Override
  public void addTableConfig(TableConfig tableConfig)
      throws IOException {
    for (int attempt = 1; attempt <= REALTIME_TABLE_CONFIG_RETRY_COUNT; attempt++) {
      try {
        super.addTableConfig(tableConfig);
        LOGGER.info("Successfully added table config on attempt {}", attempt);
        return;
      } catch (IOException e) {
        if (!isRetryableRealtimePartitionMetadataError(e) || attempt == REALTIME_TABLE_CONFIG_RETRY_COUNT) {
          LOGGER.error("Failed to add table config on attempt {} of {}", attempt, REALTIME_TABLE_CONFIG_RETRY_COUNT, e);
          throw e;
        }
        LOGGER.warn("Attempt {} failed with retryable error, waiting for Kafka metadata and retrying...", attempt, e);
        waitForKafkaTopicMetadataReadyForConsumer(getKafkaTopic(), getNumKafkaPartitions());
        try {
          Thread.sleep(REALTIME_TABLE_CONFIG_RETRY_WAIT_MS);
        } catch (InterruptedException interruptedException) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while retrying realtime table creation for topic: " + getKafkaTopic(),
              interruptedException);
        }
      }
    }
  }

  @Override
  protected boolean useKafkaTransaction() {
    return true;
  }

  @Override
  protected Properties getKafkaExtraProperties() {
    Properties props = new Properties();
    props.setProperty("log.flush.interval.messages", "1");
    return props;
  }

  @Override
  protected int getNumKafkaBrokers() {
    return DEFAULT_TRANSACTION_NUM_KAFKA_BROKERS;
  }

  @Override
  protected long getDocsLoadedTimeoutMs() {
    return 1_200_000L;
  }

  @Override
  protected void pushAvroIntoKafka(List<File> avroFiles)
      throws Exception {
    String kafkaBrokerList = getKafkaBrokerList();
    LOGGER.info("Pushing transactional data to Kafka at: {}", kafkaBrokerList);
    LOGGER.info("Avro files count: {}", avroFiles.size());

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerList);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000");
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-transaction-" + UUID.randomUUID());

    // Use a SINGLE producer for both abort and commit transactions.
    // With a single producer, the coordinator's state machine ensures that after
    // abortTransaction() returns, it returns CONCURRENT_TRANSACTIONS for any new
    // transaction operations until the abort is fully done (markers written).
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
      producer.initTransactions();
      LOGGER.info("initTransactions() succeeded");

      // Transaction 1: aborted batch
      long abortedCount = pushAvroRecords(producer, avroFiles, false);
      LOGGER.info("Aborted batch: {} records", abortedCount);

      // Transaction 2: committed batch
      long committedCount = pushAvroRecords(producer, avroFiles, true);
      LOGGER.info("Committed batch: {} records", committedCount);
    }

    // After producer is closed, verify data visibility with independent consumers
    LOGGER.info("Producer closed. Verifying data visibility...");
    waitForCommittedRecordsVisible(kafkaBrokerList);
  }

  /**
   * Wait for committed records to be visible to a read_committed consumer.
   * This ensures transaction markers have been fully propagated before returning.
   */
  private void waitForCommittedRecordsVisible(String brokerList) {
    long deadline = System.currentTimeMillis() + 120_000L;
    int lastCommitted = 0;
    int lastUncommitted = 0;
    int iteration = 0;

    while (System.currentTimeMillis() < deadline) {
      iteration++;
      lastCommitted = countRecords(brokerList, "read_committed");
      if (lastCommitted > 0) {
        LOGGER.info("Verification OK: read_committed={} after {} iterations", lastCommitted, iteration);
        return;
      }
      // Check if data reached Kafka at all
      if (iteration == 1 || iteration % 5 == 0) {
        lastUncommitted = countRecords(brokerList, "read_uncommitted");
        LOGGER.info("Verification iteration {}: read_committed={}, read_uncommitted={}", iteration, lastCommitted,
            lastUncommitted);
      }
      try {
        Thread.sleep(2_000L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    // Final diagnostic dump
    lastUncommitted = countRecords(brokerList, "read_uncommitted");
    LOGGER.error("VERIFICATION FAILED after 120s: read_committed={}, read_uncommitted={}", lastCommitted,
        lastUncommitted);
    throw new AssertionError("[ExactlyOnce] Transaction markers were not propagated within 120s; "
        + "committed records are not visible to read_committed consumers. "
        + "read_committed=" + lastCommitted + ", read_uncommitted=" + lastUncommitted);
  }

  /**
   * Push Avro records to Kafka within a transaction. Does NOT call initTransactions().
   * Returns the number of records sent.
   */
  private long pushAvroRecords(KafkaProducer<byte[], byte[]> producer, List<File> avroFiles, boolean commit)
      throws Exception {
    int maxMessagesPerTransaction =
        getMaxNumKafkaMessagesPerBatch() > 0 ? getMaxNumKafkaMessagesPerBatch() : Integer.MAX_VALUE;
    long counter = 0;
    int recordsInTransaction = 0;
    boolean hasOpenTransaction = false;
    byte[] header = getKafkaMessageHeader();
    String partitionColumn = getPartitionColumn();

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536)) {
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

            byte[] keyBytes = (partitionColumn == null) ? Longs.toByteArray(counter)
                : genericRecord.get(partitionColumn).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] bytes = outputStream.toByteArray();
            producer.send(new ProducerRecord<>(getKafkaTopic(), keyBytes, bytes));
            counter++;

            recordsInTransaction++;
            if (recordsInTransaction >= maxMessagesPerTransaction) {
              if (commit) {
                producer.commitTransaction();
              } else {
                producer.abortTransaction();
              }
              hasOpenTransaction = false;
            }
          }
        }
      }
    }
    if (hasOpenTransaction) {
      if (commit) {
        producer.commitTransaction();
      } else {
        producer.abortTransaction();
      }
    }
    return counter;
  }

  /**
   * Count records visible in the topic with the given isolation level.
   *
   * <p>For {@code read_committed}, {@link KafkaConsumer#endOffsets(java.util.Collection, Duration)}
   * returns the LSO (Last Stable Offset) per partition; for {@code read_uncommitted} it returns the
   * log-end-offset. We poll until every partition's position catches up to that snapshot rather than
   * breaking on the first empty poll. On a freshly assigned consumer the first poll often returns
   * empty while metadata/fetch sessions are being established, and breaking on it produced false
   * zero counts and spurious {@code markers were not propagated} failures.
   */
  private int countRecords(String brokerList, String isolationLevel) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "txn-diag-" + isolationLevel + "-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.toString(10 * 1024 * 1024));

    int totalRecords = 0;
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      List<PartitionInfo> partitions = consumer.partitionsFor(getKafkaTopic(), Duration.ofSeconds(10));
      if (partitions == null || partitions.isEmpty()) {
        LOGGER.warn("No partitions found for topic {}", getKafkaTopic());
        return 0;
      }
      List<TopicPartition> topicPartitions = new ArrayList<>(partitions.size());
      for (PartitionInfo pi : partitions) {
        topicPartitions.add(new TopicPartition(pi.topic(), pi.partition()));
      }
      consumer.assign(topicPartitions);
      consumer.seekToBeginning(topicPartitions);
      // Prime the consumer's metadata/fetch session with a short poll so the subsequent
      // endOffsets() and position() calls do not block on cold metadata resolution. Any records
      // returned here advance the consumer position and must be counted, otherwise we could return
      // a caught-up position with totalRecords==0.
      ConsumerRecords<byte[], byte[]> primingRecords = consumer.poll(COUNT_RECORDS_PRIMING_POLL_TIMEOUT);
      totalRecords += primingRecords.count();
      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions, COUNT_RECORDS_END_OFFSETS_TIMEOUT);
      long totalEndOffset = 0L;
      for (Long offset : endOffsets.values()) {
        totalEndOffset += offset;
      }
      if (totalEndOffset == 0L) {
        // Two cases, both correctly returning 0:
        //  - read_committed: LSO is at 0 on every partition, i.e. no transaction has been finalized yet.
        //  - read_uncommitted: the topic is genuinely empty.
        // A timeout from endOffsets() throws TimeoutException and goes to the catch block below,
        // so reaching here means the broker returned 0 for every partition.
        LOGGER.info("countRecords({}): endOffsets returned 0 for all partitions ({})", isolationLevel, endOffsets);
        return totalRecords;
      }
      long deadline = System.currentTimeMillis() + COUNT_RECORDS_DRAIN_TIMEOUT_MS;
      boolean caughtUp = false;
      while (System.currentTimeMillis() < deadline) {
        if (allPartitionsCaughtUp(consumer, topicPartitions, endOffsets)) {
          caughtUp = true;
          break;
        }
        ConsumerRecords<byte[], byte[]> records = consumer.poll(COUNT_RECORDS_POLL_TIMEOUT);
        totalRecords += records.count();
      }
      if (!caughtUp) {
        LOGGER.warn("countRecords({}) drain timed out after {}ms; returning partial count {}. positions={}, "
                + "endOffsets={}", isolationLevel, COUNT_RECORDS_DRAIN_TIMEOUT_MS, totalRecords,
            currentPositions(consumer, topicPartitions), endOffsets);
      }
    } catch (Exception e) {
      LOGGER.error("Error counting records with {}", isolationLevel, e);
    }
    return totalRecords;
  }

  private boolean allPartitionsCaughtUp(KafkaConsumer<byte[], byte[]> consumer, List<TopicPartition> topicPartitions,
      Map<TopicPartition, Long> endOffsets) {
    for (TopicPartition tp : topicPartitions) {
      Long endOffset = endOffsets.get(tp);
      // position(tp, Duration) bounds the call so a slow metadata/offset fetch cannot exceed the
      // outer drain deadline.
      if (endOffset == null || consumer.position(tp, COUNT_RECORDS_POSITION_TIMEOUT) < endOffset) {
        return false;
      }
    }
    return true;
  }

  private Map<TopicPartition, Long> currentPositions(KafkaConsumer<byte[], byte[]> consumer,
      List<TopicPartition> topicPartitions) {
    Map<TopicPartition, Long> positions = new LinkedHashMap<>();
    for (TopicPartition tp : topicPartitions) {
      try {
        positions.put(tp, consumer.position(tp, COUNT_RECORDS_POSITION_TIMEOUT));
      } catch (Exception e) {
        positions.put(tp, -1L);
      }
    }
    return positions;
  }

  private boolean isRetryableRealtimePartitionMetadataError(Throwable throwable) {
    String errorToken = "Failed to fetch partition information for topic: " + getKafkaTopic();
    Throwable current = throwable;
    while (current != null) {
      String message = current.getMessage();
      if (message != null && message.contains(errorToken)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private void waitForKafkaTopicMetadataReadyForConsumer(String topic, int expectedPartitions) {
    TestUtils.waitForCondition(aVoid -> isKafkaTopicMetadataReadyForConsumer(topic, expectedPartitions), 200L,
        KAFKA_TOPIC_METADATA_READY_TIMEOUT_MS,
        "Kafka topic '" + topic + "' metadata is not visible to consumers");
    // For transactional consumers, verify metadata is visible with read_committed isolation level too
    TestUtils.waitForCondition(
        aVoid -> isKafkaTopicMetadataReadyForConsumer(topic, expectedPartitions, "read_committed"),
        200L, KAFKA_TOPIC_METADATA_READY_TIMEOUT_MS,
        "Kafka topic '" + topic + "' metadata is not visible to read_committed consumers");
  }

  private boolean isKafkaTopicMetadataReadyForConsumer(String topic, int expectedPartitions) {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "pinot-kafka-topic-ready-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    consumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
      List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic, Duration.ofSeconds(5));
      return partitionInfos != null && partitionInfos.size() >= expectedPartitions;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean isKafkaTopicMetadataReadyForConsumer(String topic, int expectedPartitions, String isolationLevel) {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    String groupId = "pinot-kafka-topic-ready-" + isolationLevel + "-" + UUID.randomUUID();
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
    consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    consumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
      List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic, Duration.ofSeconds(5));
      return partitionInfos != null && partitionInfos.size() >= expectedPartitions;
    } catch (Exception e) {
      return false;
    }
  }
}
