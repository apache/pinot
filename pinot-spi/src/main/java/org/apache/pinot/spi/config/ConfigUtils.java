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
import java.util.Iterator;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;


public class ConfigUtils {
  private ConfigUtils() {
  }

  private static final Map<String, String> ENVIRONMENT_VARIABLES = System.getenv();

  /**
   * Apply environment variables to any given BaseJsonConfig.
   *
   * @return Config with environment variable applied.
   */
  public static <T extends BaseJsonConfig> T applyConfigWithEnvVariables(T config) {
    JsonNode jsonNode;
    try {
      jsonNode = applyConfigWithEnvVariables(config.toJsonNode());
    } catch (RuntimeException e) {
      throw new RuntimeException(String
          .format("Unable to apply environment variables on json config class [%s].", config.getClass().getName()), e);
    }
    try {
      return (T) JsonUtils.jsonNodeToObject(jsonNode, config.getClass());
    } catch (IOException e) {
      throw new RuntimeException(String
          .format("Unable to read JsonConfig to class [%s] after applying environment variables, jsonConfig is: '%s'.",
              config.getClass().getName(), jsonNode.toString()), e);
    }
  }

  private static JsonNode applyConfigWithEnvVariables(JsonNode jsonNode) {
    final JsonNodeType nodeType = jsonNode.getNodeType();
    switch (nodeType) {
      case OBJECT:
        if (!jsonNode.isEmpty()) {
          Iterator<Map.Entry<String, JsonNode>> iterator = jsonNode.fields();
          while (iterator.hasNext()) {
            final Map.Entry<String, JsonNode> next = iterator.next();
            next.setValue(applyConfigWithEnvVariables(next.getValue()));
          }
        }
        break;
      case ARRAY:
        if (jsonNode.isArray()) {
          ArrayNode arrayNode = (ArrayNode) jsonNode;
          for (int i = 0; i < arrayNode.size(); i++) {
            JsonNode arrayElement = arrayNode.get(i);
            arrayNode.set(i, applyConfigWithEnvVariables(arrayElement));
          }
        }
        break;
      case STRING:
        final String field = jsonNode.asText();
        if (field.startsWith("${") && field.endsWith("}")) {
          String[] envVarSplits = field.substring(2, field.length() - 1).split(":", 2);
          String envVarKey = envVarSplits[0];
          if (ENVIRONMENT_VARIABLES.containsKey(envVarKey)) {
            return JsonNodeFactory.instance.textNode(ENVIRONMENT_VARIABLES.get(envVarKey));
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
}
