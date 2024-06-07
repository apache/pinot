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
package org.apache.pinot.plugin.stream.kafka20;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.stream.StreamConfig;


public class KafkaConfigBackwardCompatibleUtils {
  private KafkaConfigBackwardCompatibleUtils() {
  }

  public static final String KAFKA_COMMON_PACKAGE_PREFIX = "org.apache.kafka.common";
  public static final String PINOT_SHADED_PACKAGE_PREFIX = "org.apache.pinot.shaded.";

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
        }
      }
      if (updated) {
        entry.setValue(String.join(" ", valueParts));
      }
    }
  }
}
