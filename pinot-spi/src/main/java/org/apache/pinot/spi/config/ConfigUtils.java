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
package org.apache.pinot.spi.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.utils.JsonUtils;


public class ConfigUtils {
  private ConfigUtils() {
  }

  private static final Map<String, String> ENVIRONMENT_VARIABLES = System.getenv();
  private static final String CONFIG_PROVIDERS = "config.providers";

  /// Apply system properties and environment variables to any given BaseJsonConfig.
  /// Environment variables take precedence over system properties.
  /// Since the System properties are mutable, this method will read it at runtime.
  ///
  /// @return Config with both system properties and environment variables applied.
  public static <T extends BaseJsonConfig> T applyConfigWithEnvVariablesAndSystemProperties(T config) {
    Map<String, String> combinedMap = new HashMap<>();
    // Add all system properties to the map
    System.getProperties().forEach((key, value) -> combinedMap.put(String.valueOf(key), String.valueOf(value)));
    // Add all environment variables to the map, potentially overwriting system properties
    combinedMap.putAll(ENVIRONMENT_VARIABLES);
    return applyConfigWithEnvVariablesAndSystemProperties(combinedMap, config);
  }

  /// Apply a map of config to any given BaseJsonConfig with templates.
  ///
  /// @return Config with the configs applied.
  public static <T extends BaseJsonConfig> T applyConfigWithEnvVariablesAndSystemProperties(
      Map<String, String> configValues, T configTemplate) {
    JsonNode jsonNode;
    try {
      jsonNode = applyConfigWithEnvVariablesAndSystemProperties(configValues, configTemplate.toJsonNode());
    } catch (RuntimeException e) {
      throw new RuntimeException(String
          .format("Unable to apply environment variables on json config class [%s].",
              configTemplate.getClass().getName()), e);
    }
    try {
      return (T) JsonUtils.jsonNodeToObject(jsonNode, configTemplate.getClass());
    } catch (IOException e) {
      throw new RuntimeException(String
          .format("Unable to read JsonConfig to class [%s] after applying environment variables, jsonConfig is: '%s'.",
              configTemplate.getClass().getName(), jsonNode.toString()), e);
    }
  }

  private static JsonNode applyConfigWithEnvVariablesAndSystemProperties(Map<String, String> configValues,
      JsonNode jsonNode) {
    return applyConfigWithEnvVariablesAndSystemProperties(configValues, jsonNode, Set.of());
  }

  private static JsonNode applyConfigWithEnvVariablesAndSystemProperties(Map<String, String> configValues,
      JsonNode jsonNode, Set<String> inheritedConfigProviders) {
    final JsonNodeType nodeType = jsonNode.getNodeType();
    switch (nodeType) {
      case OBJECT:
        if (!jsonNode.isEmpty()) {
          Set<String> configProviders = getConfigProviders(jsonNode, inheritedConfigProviders);
          for (Map.Entry<String, JsonNode> next : jsonNode.properties()) {
            next.setValue(
                applyConfigWithEnvVariablesAndSystemProperties(configValues, next.getValue(), configProviders));
          }
        }
        break;
      case ARRAY:
        if (jsonNode.isArray()) {
          ArrayNode arrayNode = (ArrayNode) jsonNode;
          for (int i = 0; i < arrayNode.size(); i++) {
            JsonNode arrayElement = arrayNode.get(i);
            arrayNode.set(i,
                applyConfigWithEnvVariablesAndSystemProperties(configValues, arrayElement, inheritedConfigProviders));
          }
        }
        break;
      case STRING:
        final String field = jsonNode.asText();
        // Kafka ConfigProvider references share Pinot's ${name:value} syntax. Keep references for providers declared
        // in the containing config object so Kafka can resolve them when constructing the client.
        if (field.startsWith("${") && field.endsWith("}")
            && !isConfigProviderReference(field, inheritedConfigProviders)) {
          String[] envVarSplits = field.substring(2, field.length() - 1).split(":", 2);
          String envVarKey = envVarSplits[0];
          String value = configValues.get(envVarKey);
          if (value != null) {
            return JsonNodeFactory.instance.textNode(value);
          } else if (envVarSplits.length > 1) {
            return JsonNodeFactory.instance.textNode(envVarSplits[1]);
          }
          throw new RuntimeException("Missing environment Variable: " + envVarKey);
        }
        break;
      default:
        break;
    }
    return jsonNode;
  }

  private static Set<String> getConfigProviders(JsonNode jsonNode, Set<String> inheritedConfigProviders) {
    JsonNode configProvidersNode = jsonNode.get(CONFIG_PROVIDERS);
    if (configProvidersNode == null || !configProvidersNode.isTextual()) {
      return inheritedConfigProviders;
    }

    Set<String> configProviders = new HashSet<>(inheritedConfigProviders);
    for (String configProvider : configProvidersNode.asText().split(",")) {
      String trimmedConfigProvider = configProvider.trim();
      if (!trimmedConfigProvider.isEmpty()) {
        configProviders.add(trimmedConfigProvider);
      }
    }
    return configProviders;
  }

  private static boolean isConfigProviderReference(String field, Set<String> configProviders) {
    for (String configProvider : configProviders) {
      if (field.startsWith("${" + configProvider + ":")) {
        return true;
      }
    }
    return false;
  }
}
