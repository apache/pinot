/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import java.io.File;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;


/**
 * Utilities to start Kafka during unit tests.
 *
 */
public class KafkaStarterUtils {
  public static final int DEFAULT_KAFKA_PORT = 19092;
  public static final int DEFAULT_BROKER_ID = 0;
  public static final String DEFAULT_ZK_STR = ZkStarter.DEFAULT_ZK_STR + "/kafka";
  public static final String DEFAULT_KAFKA_BROKER = "localhost:" + DEFAULT_KAFKA_PORT;

  public static Properties getDefaultKafkaConfiguration() {
    final Properties configuration = new Properties();

    // Enable topic deletion by default for integration tests
    configureTopicDeletion(configuration, true);

    // Set host name
    configureHostName(configuration, "localhost");

    return configuration;
  }

  public static List<KafkaServerStartable> startServers(final int brokerCount, final int port, final String zkStr,
      final Properties configuration) {
    List<KafkaServerStartable> startables = new ArrayList<>(brokerCount);

    for (int i = 0; i < brokerCount; i++) {
      startables.add(startServer(port + i, i, zkStr, "/tmp/kafka-" + Double.toHexString(Math.random()), configuration));
    }

    return startables;
  }

  public static KafkaServerStartable startServer(final int port, final int brokerId, final String zkStr,
      final Properties configuration) {
    return startServer(port, brokerId, zkStr, "/tmp/kafka-" + Double.toHexString(Math.random()), configuration);
  }

  public static KafkaServerStartable startServer(final int port, final int brokerId, final String zkStr,
      final String logDirPath, final Properties configuration) {
    // Create the ZK nodes for Kafka, if needed
    int indexOfFirstSlash = zkStr.indexOf('/');
    if (indexOfFirstSlash != -1) {
      String bareZkUrl = zkStr.substring(0, indexOfFirstSlash);
      String zkNodePath = zkStr.substring(indexOfFirstSlash);
      ZkClient client = new ZkClient(bareZkUrl);
      client.createPersistent(zkNodePath, true);
      client.close();
    }

    File logDir = new File(logDirPath);
    logDir.mkdirs();

    configureKafkaPort(configuration, port);
    configureZkConnectionString(configuration, zkStr);
    configureBrokerId(configuration, brokerId);
    configureKafkaLogDirectory(configuration, logDir);
    configuration.put("zookeeper.session.timeout.ms", "60000");
    KafkaConfig config = new KafkaConfig(configuration);

    KafkaServerStartable serverStartable = new KafkaServerStartable(config);
    serverStartable.startup();

    return serverStartable;
  }

  public static void configureSegmentSizeBytes(Properties properties, int segmentSize) {
    properties.put("log.segment.bytes", Integer.toString(segmentSize));
  }

  public static void configureLogRetentionSizeBytes(Properties properties, int logRetentionSizeBytes) {
    properties.put("log.retention.bytes", Integer.toString(logRetentionSizeBytes));
  }

  public static void configureKafkaLogDirectory(Properties configuration, File logDir) {
    configuration.put("log.dirs", logDir.getAbsolutePath());
  }

  public static void configureBrokerId(Properties configuration, int brokerId) {
    configuration.put("broker.id", Integer.toString(brokerId));
  }

  public static void configureZkConnectionString(Properties configuration, String zkStr) {
    configuration.put("zookeeper.connect", zkStr);
  }

  public static void configureKafkaPort(Properties configuration, int port) {
    configuration.put("port", Integer.toString(port));
  }

  public static void configureTopicDeletion(Properties configuration, boolean topicDeletionEnabled) {
    configuration.put("delete.topic.enable", Boolean.toString(topicDeletionEnabled));
  }

  public static void configureHostName(Properties configuration, String hostName) {
    configuration.put("host.name", hostName);
  }

  public static void stopServer(KafkaServerStartable serverStartable) {
    serverStartable.shutdown();
    FileUtils.deleteQuietly(new File(serverStartable.serverConfig().logDirs().apply(0)));
  }

  public static void createTopic(String kafkaTopic, String zkStr, int partitionCount) {
    invokeTopicCommand(
        new String[]{"--create", "--zookeeper", zkStr, "--replication-factor", "1", "--partitions", Integer.toString(
            partitionCount), "--topic",
            kafkaTopic});
  }

  private static void invokeTopicCommand(String[] args) {
    // jfim: Use Java security to trap System.exit in Kafka 0.9's TopicCommand
    System.setSecurityManager(new SecurityManager() {
      @Override
      public void checkPermission(Permission perm) {
        if (perm.getName().startsWith("exitVM")) {
          throw new SecurityException("System.exit is disabled");
        }
      }

      @Override
      public void checkPermission(Permission perm, Object context) {
        checkPermission(perm);
      }
    });

    try {
      TopicCommand.main(args);
    } catch (SecurityException ex) {
      // Do nothing, this is caused by our security manager that disables System.exit
    }

    System.setSecurityManager(null);
  }

  public static void deleteTopic(String kafkaTopic, String zkStr) {
    invokeTopicCommand(new String[]{"--delete", "--zookeeper", zkStr, "--topic", kafkaTopic});
  }
}
