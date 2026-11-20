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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.provider.FileConfigProvider;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;


/// Shared helpers for preparing Kafka client configuration. The helpers are stateless and thread-safe.
public class KafkaConfigUtils {
  private static final String AUTO_INCLUDE_JMX_REPORTER_CONFIG = "auto.include.jmx.reporter";
  private static final String CONFIG_PROVIDERS_PREFIX = AbstractConfig.CONFIG_PROVIDERS_CONFIG + ".";
  private static final String CONFIG_PROVIDER_CLASS_SUFFIX = ".class";
  private static final String CONFIG_PROVIDER_PARAM_PREFIX = ".param.";
  private static final List<String> COMPATIBILITY_KEYS = List.of(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
      AUTO_INCLUDE_JMX_REPORTER_CONFIG, CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG);

  private KafkaConfigUtils() {
  }

  /// Translates removed Kafka 3 client settings on a copy, retaining the old JMX reporter behavior, then filters to
  /// the target client's configurations and Kafka's dynamic ConfigProvider namespace. Provider-backed compatibility
  /// settings are resolved before translation; unrelated references remain deferred to Kafka. Kafka may still report
  /// the provider keys themselves as unknown after using them for resolution.
  ///
  /// @param properties properties to filter
  /// @param validConfigNames configuration names recognized by the target Kafka client
  /// @return a new Properties object containing the client and ConfigProvider settings
  /// @throws ConfigException if a referenced provider is undeclared, has no class, or is a FileConfigProvider
  /// without an allowed-path restriction
  public static Properties filterAndValidateKafkaProperties(Properties properties, Set<String> validConfigNames) {
    Properties filteredProperties = new Properties();
    for (String key : properties.stringPropertyNames()) {
      if (validConfigNames.contains(key) || (key.equals(AUTO_INCLUDE_JMX_REPORTER_CONFIG)
          && validConfigNames.contains(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG))
          || key.equals(AbstractConfig.CONFIG_PROVIDERS_CONFIG)
          || key.startsWith(CONFIG_PROVIDERS_PREFIX)) {
        filteredProperties.put(key, properties.get(key));
      }
    }
    validateConfigProviderReferences(filteredProperties);
    normalizeLegacyProperties(filteredProperties);
    return filteredProperties;
  }

  private static void normalizeLegacyProperties(Properties properties) {
    resolveCompatibilityReferences(properties);
    normalizeJmxProperties(properties);
    normalizeOAuthProperties(properties);
  }

  private static void normalizeJmxProperties(Properties properties) {
    boolean hasJmxSettings = properties.containsKey(AUTO_INCLUDE_JMX_REPORTER_CONFIG)
        || properties.containsKey(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG);
    if (!hasJmxSettings) {
      return;
    }
    Object legacyJmxSetting = properties.remove(AUTO_INCLUDE_JMX_REPORTER_CONFIG);
    boolean includeJmx = legacyJmxSetting == null || (Boolean) ConfigDef.parseType(AUTO_INCLUDE_JMX_REPORTER_CONFIG,
        legacyJmxSetting, ConfigDef.Type.BOOLEAN);
    Object configuredReporters = properties.getProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, "");
    List<?> reporterNames = (List<?>) ConfigDef.parseType(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
        configuredReporters, ConfigDef.Type.LIST);
    List<String> reporters = new ArrayList<>();
    for (Object reporterName : reporterNames) {
      String name = (String) reporterName;
      reporters.add(matchesKafkaClassName(name, JmxReporter.class.getName()) ? JmxReporter.class.getName() : name);
    }
    // Kafka 3 checked the exact class, so a JmxReporter subclass does not replace the automatic reporter.
    if (includeJmx && !reporters.contains(JmxReporter.class.getName())) {
      reporters.add(JmxReporter.class.getName());
    }
    properties.setProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, String.join(",", reporters));
  }

  private static void normalizeOAuthProperties(Properties properties) {
    String callback = properties.getProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
    String currentCallback = OAuthBearerLoginCallbackHandler.class.getName();
    String legacyCallback = currentCallback.replace(".oauthbearer.", ".oauthbearer.secured.");
    if (callback != null && matchesKafkaClassName(callback.trim(), legacyCallback)) {
      properties.setProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, currentCallback);
    }
  }

  private static boolean matchesKafkaClassName(String configuredClass, String runtimeClass) {
    String unshadedClass = runtimeClass.replace(KafkaConfigBackwardCompatibleUtils.PINOT_SHADED_PACKAGE_PREFIX, "");
    return configuredClass.equals(unshadedClass)
        || configuredClass.equals(KafkaConfigBackwardCompatibleUtils.PINOT_SHADED_PACKAGE_PREFIX + unshadedClass);
  }

  private static void resolveCompatibilityReferences(Properties properties) {
    Properties resolutionProperties = new Properties();
    boolean needsResolution = false;
    for (String key : COMPATIBILITY_KEYS) {
      String value = properties.getProperty(key);
      if (value != null) {
        resolutionProperties.setProperty(key, value);
        needsResolution |= ConfigTransformer.DEFAULT_PATTERN.matcher(value).find();
      }
    }
    if (!needsResolution) {
      return;
    }
    for (String key : properties.stringPropertyNames()) {
      if (key.equals(AbstractConfig.CONFIG_PROVIDERS_CONFIG) || key.startsWith(CONFIG_PROVIDERS_PREFIX)) {
        resolutionProperties.setProperty(key, properties.getProperty(key));
      }
    }
    // Keep provider declarations in the same map so Kafka's automatic-provider security filter still applies.
    // AbstractConfig closes successfully initialized providers after resolving these values and does not log them.
    Map<String, Object> resolved = new AbstractConfig(new ConfigDef(), resolutionProperties, false).originals();
    for (String key : COMPATIBILITY_KEYS) {
      Object value = resolved.get(key);
      if (value instanceof String) {
        if (ConfigTransformer.DEFAULT_PATTERN.matcher((String) value).find()) {
          throw new ConfigException("Kafka ConfigProvider reference for '" + key + "' could not be resolved");
        }
        properties.setProperty(key, (String) value);
      }
    }
  }

  private static void validateConfigProviderReferences(Properties properties) {
    Set<String> configuredProviders = getConfiguredProviders(properties);
    for (String key : properties.stringPropertyNames()) {
      Matcher matcher = ConfigTransformer.DEFAULT_PATTERN.matcher(properties.getProperty(key));
      while (matcher.find()) {
        String provider = matcher.group(1);
        if (!configuredProviders.contains(provider)) {
          throw new ConfigException("Kafka ConfigProvider alias '" + provider + "' referenced by '" + key
              + "' is not listed in '" + AbstractConfig.CONFIG_PROVIDERS_CONFIG + "'");
        }
        validateProviderConfiguration(properties, provider, key);
      }
    }
  }

  private static Set<String> getConfiguredProviders(Properties properties) {
    Set<String> configuredProviders = new HashSet<>();
    String providers = properties.getProperty(AbstractConfig.CONFIG_PROVIDERS_CONFIG, "");
    for (String provider : providers.split(",")) {
      String trimmedProvider = provider.trim();
      if (!trimmedProvider.isEmpty()) {
        configuredProviders.add(trimmedProvider);
      }
    }
    return configuredProviders;
  }

  private static void validateProviderConfiguration(Properties properties, String provider, String referencingKey) {
    String providerPrefix = CONFIG_PROVIDERS_PREFIX + provider;
    String providerClassKey = providerPrefix + CONFIG_PROVIDER_CLASS_SUFFIX;
    String providerClass = properties.getProperty(providerClassKey);
    if (providerClass == null || providerClass.trim().isEmpty()) {
      throw new ConfigException("Kafka ConfigProvider alias '" + provider + "' referenced by '" + referencingKey
          + "' does not define '" + providerClassKey + "'");
    }
    if (providerClass.equals(FileConfigProvider.class.getName())) {
      String allowedPathsKey = providerPrefix + CONFIG_PROVIDER_PARAM_PREFIX + FileConfigProvider.ALLOWED_PATHS_CONFIG;
      String allowedPaths = properties.getProperty(allowedPathsKey);
      if (allowedPaths == null || allowedPaths.trim().isEmpty()) {
        throw new ConfigException("Kafka FileConfigProvider alias '" + provider + "' referenced by '" + referencingKey
            + "' must define '" + allowedPathsKey + "'");
      }
    }
  }
}
