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

  private KafkaConfigBackwardCompatibleUtils() {
  }
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConfigBackwardCompatibleUtils.class);
  public static final String KAFKA_COMMON_PACKAGE_PREFIX = "org.apache.kafka.common";
  public static final String PINOT_SHADED_PACKAGE_PREFIX = "org.apache.pinot.shaded.";
  public static final String AWS_PROPS_PREFIX = "software.amazon";
  public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

  /**
   * Handle the stream config to replace the Kafka common package with the shaded version if needed.
   */
  public static void handleStreamConfig(StreamConfig streamConfig) {
    Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();
    //FIXME: This needs to be done because maven shade plugin also overwrites the constants in the classes
    String prefixToReplace = KAFKA_COMMON_PACKAGE_PREFIX.replace(PINOT_SHADED_PACKAGE_PREFIX, "");

    for (Map.Entry<String, String> entry : streamConfigMap.entrySet()) {
      String[] valueParts = StringUtils.split(entry.getValue(), ' ');
      boolean updated = false;
      for (int i = 0; i < valueParts.length; i++) {
        if (valueParts[i].startsWith(prefixToReplace)) {
          try {
            Class.forName(valueParts[i]);
          } catch (ClassNotFoundException e1) {
            // If not, replace the class with the shaded version
            try {
              String shadedClassName = PINOT_SHADED_PACKAGE_PREFIX + valueParts[i];
              Class.forName(shadedClassName);
              valueParts[i] = shadedClassName;
              updated = true;
            } catch (ClassNotFoundException e2) {
              // Do nothing, shaded class is not found as well, keep the original class
            }
          }
        } else if (valueParts[i].startsWith(PINOT_SHADED_PACKAGE_PREFIX + AWS_PROPS_PREFIX)) {
          // Replace the AWS SDK classes with the shaded version
          try {
            Class.forName(valueParts[i]);
          } catch (ClassNotFoundException e1) {
            // If not, replace the class with the unshaded version
            try {
              String unShadedClassName = valueParts[i].replace(PINOT_SHADED_PACKAGE_PREFIX, "");
              Class.forName(unShadedClassName);
              valueParts[i] = unShadedClassName;
              updated = true;
            } catch (ClassNotFoundException e2) {
              // Do nothing, shaded class is not found as well, keep the original class
            }
          }
        }
      }

      if (updated) {
        String originalValue = entry.getValue();
        entry.setValue(String.join(" ", valueParts));
        LOGGER.info("Updated stream config key: {} fromValue: {} toValue: {}", entry.getKey(), originalValue,
            entry.getValue());
      }
    }

    // Read the file specified by the -Djava.security.auth.login.config VM argument
    String loginConfigFile = System.getProperty("java.security.auth.login.config");
    StringBuilder jaasConfigContent = new StringBuilder();
    if (loginConfigFile != null) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(loginConfigFile)))) {
        String line;
        while ((line = br.readLine()) != null) {
          jaasConfigContent.append(line).append("\n");
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to read JAAS config file: " + loginConfigFile, e);
      }
    }

    // Process JAAS config content
    if (jaasConfigContent.length() > 0) {
      String jaasContent = jaasConfigContent.toString();
      boolean updated = false;
      String[] lines = jaasContent.split("\n");
      for (int i = 0; i < lines.length; i++) {
        if (lines[i].contains(prefixToReplace)) {
          String className = lines[i].trim().split(" ")[0];
          if (className.startsWith(prefixToReplace)) {
            try {
              Class.forName(className);
            } catch (ClassNotFoundException e1) {
              // If not, replace the class with the shaded version
              try {
                String shadedClassName = PINOT_SHADED_PACKAGE_PREFIX + className.substring(prefixToReplace.length());
                Class.forName(shadedClassName);
                lines[i] = lines[i].replace(className, shadedClassName);
                updated = true;
              } catch (ClassNotFoundException e2) {
                // Do nothing, shaded class is not found as well, keep the original class
              }
            }
          }
        }
      }
      if (updated) {
        String updatedJaasConfig = String.join("\n", lines);
        String jassConfigProperty =
            updatedJaasConfig.substring(updatedJaasConfig.indexOf("{") + 1).substring(0, updatedJaasConfig.indexOf("}"))
                .replace("\n", " ");
        streamConfigMap.putIfAbsent(SASL_JAAS_CONFIG, jassConfigProperty);
      }
    }
  }
}
