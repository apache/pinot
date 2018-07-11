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

import io.vavr.collection.Map;
import java.util.HashMap;


/**
 * Child key handler that stuffs all child keys into a map.
 */
public class SimpleMapChildKeyHandler implements ChildKeyHandler<java.util.Map<String, String>> {
  @Override
  public java.util.Map<String, String> handleChildKeys(Map<String, ?> childKeys, String pathPrefix) {
    java.util.Map<String, String> returnedMap = new HashMap<>();

    childKeys
        .forEach((key, value) -> returnedMap.put(key, value.toString()));

    if (returnedMap.isEmpty()) {
      return null;
    } else {
      return returnedMap;
    }
  }

  @Override
  public Map<String, ?> unhandleChildKeys(java.util.Map<String, String> value, String pathPrefix) {
    if (value == null) {
      return null;
    }

    return io.vavr.collection.HashMap.ofAll(value);
  }
}
