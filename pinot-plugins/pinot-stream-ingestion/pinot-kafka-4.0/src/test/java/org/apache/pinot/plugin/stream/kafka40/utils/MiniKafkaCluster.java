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
package org.apache.pinot.plugin.stream.kafka40.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;


/**
 * MiniKafkaCluster for Kafka 4.x using Testcontainers with KRaft mode (no ZooKeeper).
 * Uses the apache/kafka-native image for fast startup.
 */
public final class MiniKafkaCluster implements Closeable {

  private static final String KAFKA_IMAGE = "apache/kafka:4.0.0";

  private final KafkaContainer _kafkaContainer;

  public MiniKafkaCluster(String brokerId)
      throws Exception {
    // Use apache/kafka image which supports KRaft mode
    _kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE));
  }

  public void start()
      throws Exception {
    _kafkaContainer.start();
  }

  @Override
  public void close()
      throws IOException {
    _kafkaContainer.stop();
  }

  public String getKafkaServerAddress() {
    return _kafkaContainer.getBootstrapServers();
  }

  private AdminClient getOrCreateAdminClient() {
    Properties kafkaClientConfig = new Properties();
    kafkaClientConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaServerAddress());
    return AdminClient.create(kafkaClientConfig);
  }

  public void createTopic(String topicName, int numPartitions, int replicationFactor)
      throws ExecutionException, InterruptedException {
    try (AdminClient adminClient = getOrCreateAdminClient()) {
      NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) replicationFactor);
      int retries = 5;
      while (retries > 0) {
        try {
          adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
          return;
        } catch (ExecutionException e) {
          if (e.getCause() instanceof org.apache.kafka.common.errors.TimeoutException) {
            retries--;
            TimeUnit.SECONDS.sleep(1);
          } else {
            throw e;
          }
        }
      }
      throw new ExecutionException("Failed to create topic after retries", null);
    }
  }

  public void deleteTopic(String topicName)
      throws ExecutionException, InterruptedException {
    try (AdminClient adminClient = getOrCreateAdminClient()) {
      adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
    }
  }

  public void deleteRecordsBeforeOffset(String topicName, int partitionId, long offset)
      throws ExecutionException, InterruptedException {
    try (AdminClient adminClient = getOrCreateAdminClient()) {
      Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
      recordsToDelete.put(new TopicPartition(topicName, partitionId), RecordsToDelete.beforeOffset(offset));
      // Wait for the deletion to complete
      adminClient.deleteRecords(recordsToDelete).all().get();
    }
  }
}
