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
package org.apache.pinot.plugin.stream.kafka;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.provider.FileConfigProvider;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.pinot.spi.config.ConfigUtils;
import org.apache.pinot.spi.config.table.IndexingConfig;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


/// Test helper that verifies ConfigProvider resolution against the Kafka client version on the caller's classpath.
public class KafkaConfigProviderTestUtils {
  private KafkaConfigProviderTestUtils() {
  }

  public static void assertConfigProviderReferencesReachKafkaClients()
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
      streamConfigs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "$" + passwordReference);
      streamConfigs.put("streamType", "kafka");

      IndexingConfig indexingConfig = new IndexingConfig();
      indexingConfig.setStreamConfigs(streamConfigs);
      IndexingConfig resolvedIndexingConfig =
          ConfigUtils.applyConfigWithEnvVariablesAndSystemProperties(Map.of(), indexingConfig);
      Properties properties = new Properties();
      properties.putAll(resolvedIndexingConfig.getStreamConfigs());
      assertEquals(properties.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), passwordReference);

      Properties consumerProperties =
          KafkaConfigUtils.filterAndValidateKafkaProperties(properties, ConsumerConfig.configNames());
      assertProviderProperties(consumerProperties, providerFile);
      assertEquals(new ConsumerConfig(consumerProperties).getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).value(),
          "test-password");

      Properties adminProperties =
          KafkaConfigUtils.filterAndValidateKafkaProperties(properties, AdminClientConfig.configNames());
      assertProviderProperties(adminProperties, providerFile);
      assertEquals(new AdminClientConfig(adminProperties).getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).value(),
          "test-password");
    } finally {
      Files.deleteIfExists(providerFile);
    }
  }

  private static void assertProviderProperties(Properties properties, Path providerFile) {
    assertEquals(properties.getProperty(AbstractConfig.CONFIG_PROVIDERS_CONFIG), "file");
    assertEquals(properties.getProperty("config.providers.file.class"), FileConfigProvider.class.getName());
    assertEquals(properties.getProperty("config.providers.file.param.allowed.paths"),
        providerFile.getParent().toString());
    assertFalse(properties.containsKey("streamType"));
  }
}
