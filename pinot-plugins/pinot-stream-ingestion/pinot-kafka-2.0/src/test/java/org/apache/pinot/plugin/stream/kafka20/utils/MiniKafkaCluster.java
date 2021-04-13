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
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.pinot.spi.utils.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Seq;


public final class MiniKafkaCluster implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MiniKafkaCluster.class);
  private final EmbeddedZooKeeper zkServer;
  private final List<KafkaServer> kafkaServer;
  private final List<Integer> kafkaPorts;
  private final Path tempDir;
  private final AdminClient adminClient;
  private final String zkUrl;

  @SuppressWarnings({"rawtypes", "unchecked"})
  private MiniKafkaCluster(List<String> brokerIds)
      throws IOException, InterruptedException {
    this.zkServer = new EmbeddedZooKeeper();
    this.zkUrl = NetUtils.getHostAddress() + ":" + zkServer.getPort();
    LOGGER.info("MiniKafkaCluster Zookeeper Url is {}", zkUrl);
    this.tempDir = Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), "mini-kafka-cluster");
    this.kafkaServer = new ArrayList<>();
    this.kafkaPorts = new ArrayList<>();
    for (String id : brokerIds) {
      int port = getAvailablePort();
      LOGGER.info("Generate broker id = {}, port = {}", id, port);
      KafkaConfig c = new KafkaConfig(createBrokerConfig(id, port));
      Seq seq =
          scala.collection.JavaConverters.collectionAsScalaIterableConverter(Collections.emptyList()).asScala().toSeq();
      kafkaServer.add(new KafkaServer(c, Time.SYSTEM, Option.empty(), seq));
      kafkaPorts.add(port);
    }
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, NetUtils.getHostAddress() + ":" + getKafkaServerPort(0));
    adminClient = AdminClient.create(props);
  }

  static int getAvailablePort() {
    try {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to find available port to use", e);
    }
  }

  private Properties createBrokerConfig(String nodeId, int port)
      throws IOException {
    Properties props = new Properties();
    props.put("broker.id", nodeId);
    // We need to explicitly set the network interface we want to let Kafka bind to.
    // By default, it will bind to all the network interfaces, which might not be accessible always
    // in a container based environment.
    props.put("host.name", NetUtils.getHostAddress());
    props.put("port", Integer.toString(port));
    props.put("log.dir", Files.createTempDirectory(tempDir, "broker-").toAbsolutePath().toString());
    props.put("zookeeper.connect", zkUrl);
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
    for (KafkaServer s : kafkaServer) {
      s.startup();
    }
  }

  @Override
  public void close()
      throws IOException {
    adminClient.close();
    for (KafkaServer s : kafkaServer) {
      s.shutdown();
    }
    this.zkServer.close();
    FileUtils.deleteDirectory(tempDir.toFile());
  }

  public int getKafkaServerPort(int index) {
    return kafkaPorts.get(index);
  }

  public boolean createTopic(String topicName, int numPartitions, int replicationFactor) {
    NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) replicationFactor);
    CreateTopicsResult createTopicsResult = this.adminClient.createTopics(Arrays.asList(newTopic));
    try {
      createTopicsResult.all().get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Failed to create Kafka topic: {}, Exception: {}", newTopic.toString(), e);
      return false;
    }
    return true;
  }

  public boolean deleteTopic(String topicName) {
    final DeleteTopicsResult deleteTopicsResult = this.adminClient.deleteTopics(Collections.singletonList(topicName));
    try {
      deleteTopicsResult.all().get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Failed to delete Kafka topic: {}, Exception: {}", topicName, e);
      return false;
    }
    return true;
  }

  public static class Builder {

    private List<String> brokerIds = new ArrayList<>();

    public Builder newServer(String brokerId) {
      brokerIds.add(brokerId);
      return this;
    }

    public MiniKafkaCluster build()
        throws IOException, InterruptedException {
      return new MiniKafkaCluster(brokerIds);
    }
  }
}