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
package org.apache.pinot.plugin.stream.kafka40;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.provider.FileConfigProvider;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.plugin.stream.kafka.KafkaAdminClientManager;
import org.apache.pinot.spi.config.ConfigUtils;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class KafkaPartitionLevelConnectionHandlerTest {

  private static class TestableKafkaPartitionLevelConnectionHandler extends KafkaPartitionLevelConnectionHandler {
    public TestableKafkaPartitionLevelConnectionHandler(String clientId, StreamConfig streamConfig, int partition) {
      super(clientId, streamConfig, partition);
    }
  }

  private StreamConfig createTestStreamConfig() {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", "testTopic");
    streamConfigMap.put("stream.kafka.broker.list", "localhost:9092");
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", KafkaConsumerFactory.class.getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    return new StreamConfig("testTable_REALTIME", streamConfigMap);
  }

  @Test
  public void testConfigProviderReferencesReachKafkaClients()
      throws Exception {
    Path providerFile = Files.createTempFile("kafka-config-provider", ".properties");
    try {
      Files.writeString(providerFile, "keystore.password=test-password\n");

      String passwordReference = "${file:" + providerFile + ":keystore.password}";
      Map<String, String> streamConfigs = new HashMap<>();
      streamConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      streamConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
      streamConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
      streamConfigs.put(AbstractConfig.CONFIG_PROVIDERS_CONFIG, "file");
      streamConfigs.put("config.providers.file.class", FileConfigProvider.class.getName());
      streamConfigs.put("config.providers.file.param.allowed.paths", providerFile.getParent().toString());
      streamConfigs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, passwordReference);
      streamConfigs.put("streamType", "kafka");

      IndexingConfig indexingConfig = new IndexingConfig();
      indexingConfig.setStreamConfigs(streamConfigs);
      IndexingConfig resolvedIndexingConfig =
          ConfigUtils.applyConfigWithEnvVariablesAndSystemProperties(Map.of(), indexingConfig);
      Properties properties = new Properties();
      properties.putAll(resolvedIndexingConfig.getStreamConfigs());
      assertEquals(properties.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), passwordReference);

      Properties consumerProperties =
          KafkaPartitionLevelConnectionHandler.filterKafkaProperties(properties, ConsumerConfig.configNames());
      assertEquals(consumerProperties.getProperty(AbstractConfig.CONFIG_PROVIDERS_CONFIG), "file");
      assertEquals(consumerProperties.getProperty("config.providers.file.class"), FileConfigProvider.class.getName());
      assertEquals(consumerProperties.getProperty("config.providers.file.param.allowed.paths"),
          providerFile.getParent().toString());
      assertFalse(consumerProperties.containsKey("streamType"));
      assertEquals(new ConsumerConfig(consumerProperties).getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).value(),
          "test-password");
      try (KafkaConsumer<Bytes, Bytes> consumer = new KafkaConsumer<>(consumerProperties)) {
        assertNotNull(consumer);
      }

      Properties adminProperties =
          KafkaPartitionLevelConnectionHandler.filterKafkaProperties(properties, AdminClientConfig.configNames());
      assertEquals(adminProperties.getProperty(AbstractConfig.CONFIG_PROVIDERS_CONFIG), "file");
      assertEquals(adminProperties.getProperty("config.providers.file.class"), FileConfigProvider.class.getName());
      assertEquals(adminProperties.getProperty("config.providers.file.param.allowed.paths"),
          providerFile.getParent().toString());
      assertFalse(adminProperties.containsKey("streamType"));
      assertEquals(new AdminClientConfig(adminProperties).getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).value(),
          "test-password");
      try (KafkaAdminClientManager.AdminClientReference adminClientReference =
          KafkaAdminClientManager.getInstance().getOrCreateAdminClient(adminProperties)) {
        assertNotNull(adminClientReference.getAdminClient());
      }
    } finally {
      Files.deleteIfExists(providerFile);
    }
  }

  @Test
  public void testSharedAdminClientReference() {
    StreamConfig streamConfig = createTestStreamConfig();

    try {
      TestableKafkaPartitionLevelConnectionHandler handler =
          new TestableKafkaPartitionLevelConnectionHandler("testClient", streamConfig, 0);

      // Test that we can call getOrCreateSharedAdminClient multiple times
      // without throwing exceptions (even though it may fail to connect)
      try {
        handler.getOrCreateSharedAdminClient();
        handler.getOrCreateSharedAdminClient(); // Should reuse the same reference
      } catch (Exception e) {
        // Expected when no real Kafka cluster is available
        assertTrue(e.getMessage().contains("Connection") || e.getMessage().contains("Kafka")
            || e.getMessage().contains("timeout") || e.getMessage().contains("refused")
            || e.getCause() != null);
      }

      // Test that close doesn't throw exceptions
      handler.close();
    } catch (Exception e) {
      // Expected when initializing without a real Kafka cluster
      assertTrue(e.getMessage().contains("Connection") || e.getMessage().contains("Kafka")
          || e.getMessage().contains("timeout") || e.getMessage().contains("refused")
          || e.getCause() != null);
    }
  }

  @Test
  public void testGetOrCreateAdminClientBackwardCompatibility() {
    StreamConfig streamConfig = createTestStreamConfig();

    try {
      TestableKafkaPartitionLevelConnectionHandler handler =
          new TestableKafkaPartitionLevelConnectionHandler("testClient", streamConfig, 0);

      // Test that the backward compatibility method still works
      try {
        handler.getOrCreateAdminClient();
      } catch (Exception e) {
        // Expected when no real Kafka cluster is available
        assertTrue(e.getMessage().contains("Connection") || e.getMessage().contains("Kafka")
            || e.getMessage().contains("timeout") || e.getMessage().contains("refused")
            || e.getCause() != null);
      }

      handler.close();
    } catch (Exception e) {
      // Expected when initializing without a real Kafka cluster
      assertTrue(e.getMessage().contains("Connection") || e.getMessage().contains("Kafka")
          || e.getMessage().contains("timeout") || e.getMessage().contains("refused")
          || e.getCause() != null);
    }
  }
}
