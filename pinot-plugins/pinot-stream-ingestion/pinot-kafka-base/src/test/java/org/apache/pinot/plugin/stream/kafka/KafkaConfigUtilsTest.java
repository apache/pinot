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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.provider.FileConfigProvider;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.kafka.clients.CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.expectThrows;


public class KafkaConfigUtilsTest {
  private static final String LEGACY_JMX_CONFIG = "auto.include.jmx.reporter";

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

  @DataProvider
  public Object[][] jmxReporterSettings() {
    String jmx = JmxReporter.class.getName();
    String custom = TestMetricsReporter.class.getName();
    String subclass = TestJmxReporter.class.getName();
    return new Object[][]{
        {"true", null, List.of(jmx)},
        {"false", null, List.of()},
        {"true", "", List.of(jmx)},
        {"false", "", List.of()},
        {"true", custom, List.of(custom, jmx)},
        {"false", custom, List.of(custom)},
        {"true", jmx, List.of(jmx)},
        {"false", jmx, List.of(jmx)},
        {"true", custom + "," + jmx, List.of(custom, jmx)},
        {"false", custom + "," + jmx, List.of(custom, jmx)},
        {"true", subclass, List.of(subclass, jmx)},
        {"false", subclass, List.of(subclass)},
        {null, null, List.of(jmx)},
        {null, "", List.of(jmx)},
        {null, custom, List.of(custom, jmx)},
        {null, custom + "," + jmx, List.of(custom, jmx)},
        {null, subclass, List.of(subclass, jmx)}
    };
  }

  @Test(dataProvider = "jmxReporterSettings")
  public void testLegacyJmxReporterSettings(String legacyFlag, String reporters, List<String> expectedReporters) {
    Properties properties = createClientProperties();
    if (legacyFlag != null) {
      properties.setProperty(LEGACY_JMX_CONFIG, legacyFlag);
    }
    if (reporters != null) {
      properties.setProperty(METRIC_REPORTER_CLASSES_CONFIG, reporters);
    }
    assertKafka4ClientConfigs(properties, expectedReporters, null);
  }

  @DataProvider
  public Object[][] loginCallbackSettings() {
    String oldCallback = legacyLoginCallback();
    return new Object[][]{
        {oldCallback, OAuthBearerLoginCallbackHandler.class},
        {KafkaConfigBackwardCompatibleUtils.PINOT_SHADED_PACKAGE_PREFIX + oldCallback,
            OAuthBearerLoginCallbackHandler.class},
        {OAuthBearerLoginCallbackHandler.class.getName(), OAuthBearerLoginCallbackHandler.class},
        {OAuthBearerUnsecuredLoginCallbackHandler.class.getName(), OAuthBearerUnsecuredLoginCallbackHandler.class}
    };
  }

  @Test(dataProvider = "loginCallbackSettings")
  public void testLegacyOAuthLoginCallback(String callback, Class<?> expectedCallback) {
    Properties properties = createClientProperties();
    properties.setProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, callback);
    assertKafka4ClientConfigs(properties, List.of(JmxReporter.class.getName()), expectedCallback);
  }

  @Test
  public void testProviderBackedLegacySettings()
      throws Exception {
    Path providerFile = Files.createTempFile("kafka-legacy-settings", ".properties");
    try {
      for (String flag : new String[]{"true", "false", null}) {
        Files.writeString(providerFile, "jmx=" + flag + "\nreporters=" + TestMetricsReporter.class.getName()
            + "\ncallback=" + legacyLoginCallback() + "\npassword=test-password\n");
        Properties properties = createClientProperties();
        properties.setProperty(AbstractConfig.CONFIG_PROVIDERS_CONFIG, "file");
        properties.setProperty("config.providers.file.class", FileConfigProvider.class.getName());
        properties.setProperty("config.providers.file.param.allowed.paths", providerFile.getParent().toString());
        if (flag != null) {
          properties.setProperty(LEGACY_JMX_CONFIG, "${file:" + providerFile + ":jmx}");
        }
        properties.setProperty(METRIC_REPORTER_CLASSES_CONFIG, "${file:" + providerFile + ":reporters}");
        properties.setProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "${file:" + providerFile + ":callback}");
        String passwordReference = "${file:" + providerFile + ":password}";
        properties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, passwordReference);
        List<String> reporters = "false".equals(flag)
            ? List.of(TestMetricsReporter.class.getName())
            : List.of(TestMetricsReporter.class.getName(), JmxReporter.class.getName());
        assertKafka4ClientConfigs(properties, reporters, OAuthBearerLoginCallbackHandler.class);
        Properties normalized =
            KafkaConfigUtils.filterAndValidateKafkaProperties(properties, ConsumerConfig.configNames());
        assertEquals(normalized.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), passwordReference);
      }
    } finally {
      Files.deleteIfExists(providerFile);
    }
  }

  @Test
  public void testInvalidLegacyJmxValueIsRejected() {
    Properties properties = createClientProperties();
    properties.setProperty(LEGACY_JMX_CONFIG, "invalid");
    expectThrows(ConfigException.class,
        () -> KafkaConfigUtils.filterAndValidateKafkaProperties(properties, ConsumerConfig.configNames()));
  }

  @Test
  public void testLegacySettingRequiresDeclaredProvider() {
    Properties properties = createClientProperties();
    properties.setProperty(LEGACY_JMX_CONFIG, "${file:/vault/secrets/kafka.properties:jmx}");
    ConfigException exception = expectThrows(ConfigException.class,
        () -> KafkaConfigUtils.filterAndValidateKafkaProperties(properties, ConsumerConfig.configNames()));
    assertEquals(exception.getMessage(), "Kafka ConfigProvider alias 'file' referenced by 'auto.include.jmx.reporter' "
        + "is not listed in 'config.providers'");
  }

  private static void assertKafka4ClientConfigs(Properties properties, List<String> expectedReporters,
      Class<?> expectedCallback) {
    Properties original = new Properties();
    original.putAll(properties);
    for (boolean consumer : new boolean[]{true, false}) {
      Properties normalized = KafkaConfigUtils.filterAndValidateKafkaProperties(properties,
          consumer ? ConsumerConfig.configNames() : AdminClientConfig.configNames());
      assertFalse(normalized.containsKey(LEGACY_JMX_CONFIG));
      if (!properties.containsKey(LEGACY_JMX_CONFIG) && !properties.containsKey(METRIC_REPORTER_CLASSES_CONFIG)) {
        assertFalse(normalized.containsKey(METRIC_REPORTER_CLASSES_CONFIG));
      }
      AbstractConfig config = consumer ? new ConsumerConfig(normalized) : new AdminClientConfig(normalized);
      assertEquals(config.getList(METRIC_REPORTER_CLASSES_CONFIG), expectedReporters);
      assertEquals(config.getClass(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS), expectedCallback);
      List<MetricsReporter> reporters =
          config.getConfiguredInstances(METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
      try {
        assertEquals(reporters.stream().map(reporter -> reporter.getClass().getName()).toList(), expectedReporters);
      } finally {
        reporters.forEach(MetricsReporter::close);
      }
      if (properties.containsKey(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)) {
        assertEquals(config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).value(), "test-password");
      }
    }
    assertEquals(properties, original);
  }

  private static Properties createClientProperties() {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    return properties;
  }

  private static String legacyLoginCallback() {
    return OAuthBearerLoginCallbackHandler.class.getName()
        .replace(KafkaConfigBackwardCompatibleUtils.PINOT_SHADED_PACKAGE_PREFIX, "")
        .replace(".oauthbearer.", ".oauthbearer.secured.");
  }

  /// Minimal non-JMX reporter used to validate real Kafka client reporter construction.
  public static class TestMetricsReporter implements MetricsReporter {
    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
    }

    @Override
    public void metricChange(KafkaMetric metric) {
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
    }

    @Override
    public void close() {
    }
  }

  /// A subclass must not suppress the exact JmxReporter automatically included by Kafka 3.
  public static class TestJmxReporter extends JmxReporter {
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
