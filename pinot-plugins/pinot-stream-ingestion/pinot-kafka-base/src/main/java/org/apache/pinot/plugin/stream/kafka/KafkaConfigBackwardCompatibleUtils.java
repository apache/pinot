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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.stream.StreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaConfigBackwardCompatibleUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConfigBackwardCompatibleUtils.class);

  private KafkaConfigBackwardCompatibleUtils() {
    // Private constructor to prevent instantiation
  }

  private static final String KAFKA_COMMON_PACKAGE_PREFIX = "org.apache.kafka.common";
  private static final String PINOT_SHADED_PACKAGE_PREFIX = "org.apache.pinot.shaded.";
  private static final String AWS_PROPS_PREFIX = "software.amazon";
  private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

  /**
   * Handles the stream config to replace the Kafka common package with the shaded or unshaded version if needed.
   */
  public static void handleStreamConfig(StreamConfig streamConfig) {
    Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();

    // Process stream configs
    for (Map.Entry<String, String> entry : streamConfigMap.entrySet()) {
      String[] valueParts = StringUtils.split(entry.getValue(), ' ');
      boolean updated = false;

      for (int i = 0; i < valueParts.length; i++) {
        String className = valueParts[i];

        // Handle Kafka common classes
        if (className.startsWith(KAFKA_COMMON_PACKAGE_PREFIX)) {
          if (!isClassAvailable(className)) {
            String shadedClassName = PINOT_SHADED_PACKAGE_PREFIX + className;
            if (isClassAvailable(shadedClassName)) {
              valueParts[i] = shadedClassName;
              updated = true;
              LOGGER.info("Replaced class '{}' with shaded class '{}' in streamConfig {}", className, shadedClassName,
                  entry.getKey());
            }
          }
        }
        // Handle AWS SDK classes differently because we are not shading them anymore
        else if (className.startsWith(PINOT_SHADED_PACKAGE_PREFIX + AWS_PROPS_PREFIX)) {
          if (!isClassAvailable(className)) {
            String unshadedClassName = className.replaceFirst(PINOT_SHADED_PACKAGE_PREFIX, "");
            if (isClassAvailable(unshadedClassName)) {
              valueParts[i] = unshadedClassName;
              updated = true;
              LOGGER.info("Replaced shaded class '{}' with class '{}' in streamConfig {}", className, unshadedClassName,
                  entry.getKey());
            }
          }
        }
      }

      if (updated) {
        entry.setValue(String.join(" ", valueParts));
      }
    }

    // Process JAAS config file if specified
    String loginConfigFile = System.getProperty("java.security.auth.login.config");
    if (loginConfigFile != null) {
      String jaasConfigContent = readFileContent(loginConfigFile);
      if (!jaasConfigContent.isEmpty()) {
        boolean updated = false;
        String[] lines = jaasConfigContent.split("\n");

        for (int i = 0; i < lines.length; i++) {
          String line = lines[i].trim();
          if (line.startsWith(KAFKA_COMMON_PACKAGE_PREFIX)) {
            String className = line.split(" ")[0];
            if (!isClassAvailable(className)) {
              String shadedClassName = PINOT_SHADED_PACKAGE_PREFIX + className;
              if (isClassAvailable(shadedClassName)) {
                lines[i] = line.replace(className, shadedClassName);
                updated = true;
              }
            }
          }
        }

        if (updated) {
          String updatedJaasConfig = String.join("\n", lines);
          String jaasConfigProperty = extractJaasConfigProperty(updatedJaasConfig);
          streamConfigMap.putIfAbsent(SASL_JAAS_CONFIG, jaasConfigProperty);
        }
      }
    }
  }

  /**
   * Checks if a class with the given name is available in the classpath.
   */
  private static boolean isClassAvailable(String className) {
    try {
      Class.forName(className, false, KafkaConfigBackwardCompatibleUtils.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * Reads the entire content of a file into a String.
   */
  private static String readFileContent(String filePath) {
    StringBuilder content = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)))) {
      String line;
      while ((line = br.readLine()) != null) {
        content.append(line).append("\n");
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read file: " + filePath, e);
    }
    return content.toString();
  }

  /**
   * Extracts the JAAS config property from the updated JAAS configuration content.
   */
  private static String extractJaasConfigProperty(String jaasConfig) {
    int startIndex = jaasConfig.indexOf("{");
    int endIndex = jaasConfig.lastIndexOf("}");
    if (startIndex != -1 && endIndex != -1 && endIndex > startIndex) {
      String config = jaasConfig.substring(startIndex + 1, endIndex).replace("\n", " ").trim();
      return config;
    } else {
      throw new IllegalArgumentException("Invalid JAAS config format");
    }
  }
}
