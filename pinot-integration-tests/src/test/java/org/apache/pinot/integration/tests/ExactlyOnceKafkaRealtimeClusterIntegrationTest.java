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

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.util.TestUtils;


public class ExactlyOnceKafkaRealtimeClusterIntegrationTest extends BaseRealtimeClusterIntegrationTest {

  private static final int REALTIME_TABLE_CONFIG_RETRY_COUNT = 5;
  private static final long REALTIME_TABLE_CONFIG_RETRY_WAIT_MS = 1_000L;
  private static final long KAFKA_TOPIC_METADATA_READY_TIMEOUT_MS = 30_000L;

  @Override
  public void addTableConfig(TableConfig tableConfig)
      throws IOException {
    for (int attempt = 1; attempt <= REALTIME_TABLE_CONFIG_RETRY_COUNT; attempt++) {
      try {
        super.addTableConfig(tableConfig);
        return;
      } catch (IOException e) {
        if (!isRetryableRealtimePartitionMetadataError(e) || attempt == REALTIME_TABLE_CONFIG_RETRY_COUNT) {
          throw e;
        }
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
    // the first transaction of kafka messages are aborted
    ClusterIntegrationTestUtils
        .pushAvroIntoKafkaWithTransaction(avroFiles, kafkaBrokerList, getKafkaTopic(),
            getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn(), false);
    // the second transaction of kafka messages are committed
    ClusterIntegrationTestUtils
        .pushAvroIntoKafkaWithTransaction(avroFiles, kafkaBrokerList, getKafkaTopic(),
            getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn(), true);
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
}
