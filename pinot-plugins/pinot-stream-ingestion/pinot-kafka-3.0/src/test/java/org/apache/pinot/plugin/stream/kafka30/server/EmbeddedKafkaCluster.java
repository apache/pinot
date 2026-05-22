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
package org.apache.pinot.plugin.stream.kafka30.server;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * In-process embedded Kafka cluster using KRaft mode for integration tests.
 * Eliminates Docker dependency and provides fast, reliable Kafka for testing.
 */
public class EmbeddedKafkaCluster implements StreamDataServerStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);

  public static final String BROKER_COUNT_PROP = "embedded.kafka.broker.count";

  private static final int TOPIC_MUTATION_RETRIES = 5;

  private int _brokerCount = 1;
  private final Properties _extraConfigProps = new Properties();
  private KafkaClusterTestKit _cluster;
  private String _bootstrapServers;

  @Override
  public void init(Properties props) {
    _brokerCount = Integer.parseInt(props.getProperty(BROKER_COUNT_PROP, "1"));
    _extraConfigProps.clear();
    // Forward any additional properties (excluding our internal ones) as Kafka broker config
    for (String key : props.stringPropertyNames()) {
      if (!key.equals(BROKER_COUNT_PROP)) {
        _extraConfigProps.setProperty(key, props.getProperty(key));
      }
    }
  }

  @Override
  public void start() {
    try {
      int replicationFactor = Math.min(3, _brokerCount);

      TestKitNodes nodes = new TestKitNodes.Builder()
          .setCombined(true)
          .setNumBrokerNodes(_brokerCount)
          .setNumControllerNodes(1)
          .setPerServerProperties(Collections.emptyMap())
          .setBootstrapMetadataVersion(MetadataVersion.latestProduction())
          .build();

      KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes)
          .setConfigProp("offsets.topic.replication.factor", String.valueOf(replicationFactor))
          .setConfigProp("offsets.topic.num.partitions", "1")
          .setConfigProp("transaction.state.log.replication.factor", String.valueOf(replicationFactor))
          .setConfigProp("transaction.state.log.min.isr", "1")
          .setConfigProp("transaction.state.log.num.partitions", "1")
          .setConfigProp("group.initial.rebalance.delay.ms", "0");

      // Apply any extra config properties passed via init()
      for (String key : _extraConfigProps.stringPropertyNames()) {
        builder.setConfigProp(key, _extraConfigProps.getProperty(key));
      }

      _cluster = builder.build();

      _cluster.format();
      _cluster.startup();
      _cluster.waitForReadyBrokers();
      _bootstrapServers = _cluster.bootstrapServers();
      LOGGER.info("Embedded Kafka cluster started with {} broker(s) at {}", _brokerCount, _bootstrapServers);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start embedded Kafka cluster", e);
    }
  }

  @Override
  public void stop() {
    if (_cluster != null) {
      try {
        _cluster.close();
        LOGGER.info("Embedded Kafka cluster stopped");
      } catch (Exception e) {
        LOGGER.warn("Failed to stop embedded Kafka cluster cleanly", e);
      } finally {
        _cluster = null;
        _bootstrapServers = null;
      }
    }
  }

  /**
   * Returns the full bootstrap servers string (e.g. "localhost:12345,localhost:12346").
   */
  public String bootstrapServers() {
    return _bootstrapServers;
  }

  @Override
  public int getPort() {
    if (_bootstrapServers == null) {
      throw new IllegalStateException("Embedded Kafka cluster is not started");
    }
    // Parse the port from the first broker in the bootstrap servers string
    String firstBroker = _bootstrapServers.split(",")[0];
    return Integer.parseInt(firstBroker.substring(firstBroker.lastIndexOf(':') + 1));
  }

  @Override
  public void createTopic(String topic, Properties topicProps) {
    int numPartitions = Integer.parseInt(String.valueOf(topicProps.getOrDefault("partition", "1")));
    int requestedReplicationFactor = Integer.parseInt(
        String.valueOf(topicProps.getOrDefault("replicationFactor", "1")));
    short replicationFactor = (short) Math.max(1, Math.min(_brokerCount, requestedReplicationFactor));
    try (AdminClient adminClient = createAdminClient()) {
      NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
      runAdminWithRetry(() -> adminClient.createTopics(Collections.singletonList(newTopic)).all().get(),
          "create topic: " + topic);
    } catch (Exception e) {
      if (e instanceof ExecutionException
          && e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
        return;
      }
      throw new RuntimeException("Failed to create topic: " + topic, e);
    }
  }

  @Override
  public void deleteTopic(String topic) {
    try (AdminClient adminClient = createAdminClient()) {
      runAdminWithRetry(() -> adminClient.deleteTopics(Collections.singletonList(topic)).all().get(),
          "delete topic: " + topic);
    } catch (Exception e) {
      throw new RuntimeException("Failed to delete topic: " + topic, e);
    }
  }

  @Override
  public void createPartitions(String topic, int numPartitions) {
    try (AdminClient adminClient = createAdminClient()) {
      runAdminWithRetry(() -> {
        adminClient.createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(numPartitions)))
            .all().get();
        return null;
      }, "create partitions for topic: " + topic);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create partitions for topic: " + topic, e);
    }
  }

  @Override
  public void deleteRecordsBeforeOffset(String topic, int partition, long offset) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    try (AdminClient adminClient = createAdminClient()) {
      runAdminWithRetry(() -> {
        adminClient.deleteRecords(Collections.singletonMap(topicPartition, RecordsToDelete.beforeOffset(offset)))
            .all().get();
        return null;
      }, "delete records before offset for topic: " + topic + ", partition: " + partition);
    } catch (Exception e) {
      throw new RuntimeException("Failed to delete records before offset for topic: " + topic
          + ", partition: " + partition + ", offset: " + offset, e);
    }
  }

  private AdminClient createAdminClient() {
    if (_bootstrapServers == null) {
      throw new IllegalStateException("Embedded Kafka cluster is not started");
    }
    Properties props = new Properties();
    props.put("bootstrap.servers", _bootstrapServers);
    return AdminClient.create(props);
  }

  private <T> T runAdminWithRetry(AdminOperation<T> operation, String action)
      throws Exception {
    ExecutionException lastException = null;
    for (int attempt = 1; attempt <= TOPIC_MUTATION_RETRIES; attempt++) {
      try {
        return operation.execute();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof org.apache.kafka.common.errors.TimeoutException) {
          lastException = e;
          if (attempt < TOPIC_MUTATION_RETRIES) {
            try {
              Thread.sleep(1000L);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw ie;
            }
            continue;
          } else {
            break;
          }
        }
        throw e;
      }
    }
    throw new IllegalStateException("Failed to " + action + " after retries", lastException);
  }

  @FunctionalInterface
  private interface AdminOperation<T> {
    T execute()
        throws Exception;
  }
}
