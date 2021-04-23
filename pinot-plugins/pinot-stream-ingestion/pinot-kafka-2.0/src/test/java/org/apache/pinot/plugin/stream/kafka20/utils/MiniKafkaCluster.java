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
package org.apache.pinot.plugin.stream.kafka20.utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Time;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;


public final class MiniKafkaCluster implements Closeable {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "MiniKafkaCluster");

  private final EmbeddedZooKeeper _zkServer;
  private final KafkaServer _kafkaServer;
  private final String _kafkaServerAddress;
  private final AdminClient _adminClient;

  @SuppressWarnings({"rawtypes", "unchecked"})
  public MiniKafkaCluster(String brokerId)
      throws IOException, InterruptedException {
    _zkServer = new EmbeddedZooKeeper();
    int kafkaServerPort = getAvailablePort();
    KafkaConfig kafkaBrokerConfig = new KafkaConfig(createBrokerConfig(brokerId, kafkaServerPort));
    Seq seq = JavaConverters.collectionAsScalaIterableConverter(Collections.emptyList()).asScala().toSeq();
    _kafkaServer = new KafkaServer(kafkaBrokerConfig, Time.SYSTEM, Option.empty(), seq);
    _kafkaServerAddress = "localhost:" + kafkaServerPort;
    Properties kafkaClientConfig = new Properties();
    kafkaClientConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaServerAddress);
    _adminClient = AdminClient.create(kafkaClientConfig);
  }

  private static int getAvailablePort() {
    try {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to find available port to use", e);
    }
  }

  private Properties createBrokerConfig(String brokerId, int port) {
    Properties props = new Properties();
    props.put("broker.id", brokerId);
    // We need to explicitly set the network interface we want to let Kafka bind to.
    // By default, it will bind to all the network interfaces, which might not be accessible always
    // in a container based environment.
    props.put("host.name", "localhost");
    props.put("port", Integer.toString(port));
    props.put("log.dir", new File(TEMP_DIR, "log").getPath());
    props.put("zookeeper.connect", _zkServer.getZkAddress());
    props.put("replica.socket.timeout.ms", "1500");
    props.put("controller.socket.timeout.ms", "1500");
    props.put("controlled.shutdown.enable", "true");
    props.put("delete.topic.enable", "true");
    props.put("auto.create.topics.enable", "true");
    props.put("offsets.topic.replication.factor", "1");
    props.put("controlled.shutdown.retry.backoff.ms", "100");
    props.put("log.cleaner.dedupe.buffer.size", "2097152");
    return props;
  }

  public void start() {
    _kafkaServer.startup();
  }

  @Override
  public void close()
      throws IOException {
    _kafkaServer.shutdown();
    _zkServer.close();
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  public String getKafkaServerAddress() {
    return _kafkaServerAddress;
  }

  public void createTopic(String topicName, int numPartitions, int replicationFactor)
      throws ExecutionException, InterruptedException {
    NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) replicationFactor);
    _adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
  }

  public void deleteTopic(String topicName)
      throws ExecutionException, InterruptedException {
    _adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
  }
}
