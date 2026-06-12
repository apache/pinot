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
package org.apache.pinot.tools.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaStarterUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStarterUtils.class);

  private KafkaStarterUtils() {
  }

  public static final int DEFAULT_BROKER_ID = 0;
  public static final int DEFAULT_KAFKA_PORT = 19092;
  public static final String DEFAULT_KAFKA_BROKER = "localhost:" + DEFAULT_KAFKA_PORT;

  public static final String KAFKA_SERVER_BOOTSTRAP_SERVERS = "kafka.server.bootstrap.servers";
  public static final String KAFKA_SERVER_PORT = "kafka.server.port";
  public static final String KAFKA_SERVER_BROKER_ID = "kafka.server.broker.id";
  public static final String KAFKA_SERVER_OWNER_NAME = "kafka.server.owner.name";
  public static final String KAFKA_SERVER_ALLOW_MANAGED_FOR_CONFIGURED_BROKER =
      "kafka.server.allow.managed.for.configured.broker";

  public static final String KAFKA_SERVER_STARTABLE_CLASS_NAME =
      getKafkaConnectorPackageName() + ".server.KafkaServerStartable";
  public static final String KAFKA_PRODUCER_CLASS_NAME = getKafkaConnectorPackageName() + ".server.KafkaDataProducer";
  public static final String KAFKA_STREAM_CONSUMER_FACTORY_CLASS_NAME =
      getKafkaConnectorPackageName() + ".KafkaConsumerFactory";
  public static final String KAFKA_STREAM_LEVEL_CONSUMER_CLASS_NAME =
      getKafkaConnectorPackageName() + ".KafkaStreamLevelConsumer";
  public static final String KAFKA_JSON_MESSAGE_DECODER_CLASS_NAME =
      "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder";

  private static String getKafkaConnectorPackageName() {
    Iterator<StreamConsumerFactory> iterator = ServiceLoader.load(StreamConsumerFactory.class).iterator();
    List<String> streamConsumerFactoryList = new ArrayList<>();
    while (iterator.hasNext()) {
      streamConsumerFactoryList.add(iterator.next().getClass().getPackage().getName());
    }
    if (streamConsumerFactoryList.size() > 1) {
      Collections.sort(streamConsumerFactoryList, Collections.reverseOrder());
    }
    return streamConsumerFactoryList.get(0);
  }

  public static Properties getTopicCreationProps(int numKafkaPartitions) {
    return getTopicCreationProps(numKafkaPartitions, 1);
  }

  public static Properties getTopicCreationProps(int numKafkaPartitions, int replicationFactor) {
    Properties topicProps = new Properties();
    topicProps.put("partition", numKafkaPartitions);
    topicProps.put("replicationFactor", Math.max(1, replicationFactor));
    return topicProps;
  }

  public static String resolveKafkaBrokerList(String configuredBrokerList, boolean brokerListOverridden) {
    if (isKafkaAvailable(configuredBrokerList)) {
      return configuredBrokerList;
    }
    if (brokerListOverridden) {
      throw new IllegalStateException("Kafka broker list is not reachable: " + configuredBrokerList);
    }

    String fallback = "localhost:9092";
    if (isKafkaAvailable(fallback)) {
      LOGGER.warn("Configured Kafka broker list {} is not reachable; falling back to default {}", configuredBrokerList,
          fallback);
      return fallback;
    }

    throw new IllegalStateException("Kafka broker list is not reachable: " + configuredBrokerList
        + ". Please start Kafka or pass -kafkaBrokerList.");
  }

  public static boolean isKafkaAvailable(String brokerList) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("request.timeout.ms", "2000");
    props.put("default.api.timeout.ms", "2000");
    try (AdminClient adminClient = AdminClient.create(props)) {
      adminClient.describeCluster().nodes().get(2, TimeUnit.SECONDS);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean isLocalBroker(String brokerList) {
    int separator = brokerList.lastIndexOf(':');
    if (separator <= 0 || separator == brokerList.length() - 1) {
      return false;
    }
    String host = brokerList.substring(0, separator);
    return "localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host);
  }

  public static int parsePort(String brokerList, int defaultPort) {
    int separator = brokerList.lastIndexOf(':');
    if (separator <= 0 || separator == brokerList.length() - 1) {
      return defaultPort;
    }
    try {
      return Integer.parseInt(brokerList.substring(separator + 1));
    } catch (NumberFormatException e) {
      return defaultPort;
    }
  }
}
