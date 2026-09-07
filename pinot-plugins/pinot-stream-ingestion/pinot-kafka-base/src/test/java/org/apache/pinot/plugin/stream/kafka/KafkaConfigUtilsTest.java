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

import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.provider.FileConfigProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.expectThrows;


public class KafkaConfigUtilsTest {

  @Test
  public void testFilterKafkaPropertiesPreservesConfigProviders() {
    Properties properties = createFileProviderProperties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("streamType", "kafka");

    Properties filteredProperties =
        KafkaConfigUtils.filterAndValidateKafkaProperties(properties,
            Set.of("bootstrap.servers", "ssl.keystore.password"));

    assertEquals(filteredProperties.getProperty("bootstrap.servers"), "localhost:9092");
    assertEquals(filteredProperties.getProperty(AbstractConfig.CONFIG_PROVIDERS_CONFIG), "file");
    assertEquals(filteredProperties.getProperty("config.providers.file.class"), FileConfigProvider.class.getName());
    assertEquals(filteredProperties.getProperty("config.providers.file.param.allowed.paths"), "/vault/secrets");
    assertFalse(filteredProperties.containsKey("streamType"));
  }

  @Test
  public void testFilterKafkaPropertiesRejectsUndeclaredProvider() {
    Properties properties = new Properties();
    properties.setProperty("ssl.keystore.password", "${file:/vault/secrets/kafka.properties:password}");

    ConfigException exception = expectThrows(ConfigException.class,
        () -> KafkaConfigUtils.filterAndValidateKafkaProperties(properties, Set.of("ssl.keystore.password")));

    assertEquals(exception.getMessage(), "Kafka ConfigProvider alias 'file' referenced by 'ssl.keystore.password' "
        + "is not listed in 'config.providers'");
  }

  @Test
  public void testFilterKafkaPropertiesRejectsProviderWithoutClass() {
    Properties properties = new Properties();
    properties.setProperty(AbstractConfig.CONFIG_PROVIDERS_CONFIG, "file");
    properties.setProperty("ssl.keystore.password", "${file:/vault/secrets/kafka.properties:password}");

    ConfigException exception = expectThrows(ConfigException.class,
        () -> KafkaConfigUtils.filterAndValidateKafkaProperties(properties, Set.of("ssl.keystore.password")));

    assertEquals(exception.getMessage(), "Kafka ConfigProvider alias 'file' referenced by 'ssl.keystore.password' "
        + "does not define 'config.providers.file.class'");
  }

  @Test
  public void testFilterKafkaPropertiesRequiresAllowedPathsForFileProvider() {
    Properties properties = createFileProviderProperties();
    properties.remove("config.providers.file.param.allowed.paths");

    ConfigException exception = expectThrows(ConfigException.class,
        () -> KafkaConfigUtils.filterAndValidateKafkaProperties(properties, Set.of("ssl.keystore.password")));

    assertEquals(exception.getMessage(), "Kafka FileConfigProvider alias 'file' referenced by "
        + "'ssl.keystore.password' must define 'config.providers.file.param.allowed.paths'");
  }

  private static Properties createFileProviderProperties() {
    Properties properties = new Properties();
    properties.setProperty(AbstractConfig.CONFIG_PROVIDERS_CONFIG, "file");
    properties.setProperty("config.providers.file.class", FileConfigProvider.class.getName());
    properties.setProperty("config.providers.file.param.allowed.paths", "/vault/secrets");
    properties.setProperty("ssl.keystore.password", "${file:/vault/secrets/kafka.properties:password}");
    return properties;
  }
}
