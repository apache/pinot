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

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.common.config.provider.FileConfigProvider;


/// Shared helpers for preparing Kafka client configuration. The helpers are stateless and thread-safe.
public class KafkaConfigUtils {
  private static final String CONFIG_PROVIDERS_PREFIX = AbstractConfig.CONFIG_PROVIDERS_CONFIG + ".";
  private static final String CONFIG_PROVIDER_CLASS_SUFFIX = ".class";
  private static final String CONFIG_PROVIDER_PARAM_PREFIX = ".param.";

  private KafkaConfigUtils() {
  }

  /// Filters properties to the target Kafka client's known configurations and Kafka's dynamic ConfigProvider
  /// namespace. Kafka may still report the provider keys themselves as unknown after using them for resolution.
  ///
  /// @param properties properties to filter
  /// @param validConfigNames configuration names recognized by the target Kafka client
  /// @return a new Properties object containing the client and ConfigProvider settings
  /// @throws ConfigException if a referenced provider is undeclared, has no class, or is a FileConfigProvider
  /// without an allowed-path restriction
  public static Properties filterAndValidateKafkaProperties(Properties properties, Set<String> validConfigNames) {
    Properties filteredProperties = new Properties();
    for (String key : properties.stringPropertyNames()) {
      if (validConfigNames.contains(key) || key.equals(AbstractConfig.CONFIG_PROVIDERS_CONFIG)
          || key.startsWith(CONFIG_PROVIDERS_PREFIX)) {
        filteredProperties.put(key, properties.get(key));
      }
    }
    validateConfigProviderReferences(filteredProperties);
    return filteredProperties;
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
