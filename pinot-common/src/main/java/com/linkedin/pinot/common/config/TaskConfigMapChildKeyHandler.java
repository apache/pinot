/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.config;

import io.vavr.Tuple;
import io.vavr.collection.HashMap;
import java.util.Map;


/**
 * Child key handler for task configs.
 */
public class TaskConfigMapChildKeyHandler implements ChildKeyHandler<Map<String, Map<String, String>>> {
  @Override
  public Map<String, Map<String, String>> handleChildKeys(io.vavr.collection.Map<String, ?> childKeys,
      String pathPrefix) {
    Map<String, Map<String, String>> returnValue = new java.util.HashMap<>();

    // Deserialize task keys for tasks that have no config
    childKeys
        .filter((key, value) -> key.indexOf('.') == -1)
        .forEach((key, value) -> returnValue.put(key, new java.util.HashMap<>()));

    if (returnValue.isEmpty()) {
      return null;
    } else {
      return returnValue;
    }
  }

  @Override
  public io.vavr.collection.Map<String, ?> unhandleChildKeys(Map<String, Map<String, String>> value,
      String pathPrefix) {
    HashMap<String, String> retVal = HashMap.ofAll(value)
        .flatMap((taskKey, configs) -> {
          if (!configs.isEmpty()) {
            return HashMap.ofAll(configs).map((configKey, configValue) -> Tuple.of(taskKey + "." + configKey, configValue));
          } else {
            return HashMap.of(taskKey, "");
          }
        });

    return retVal;
  }
}
