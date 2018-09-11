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

package com.linkedin.thirdeye.anomaly.onboard.utils;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class PropertyCheckUtils {
  /**
   * check if the list of property keys exist in the given properties
   * @param properties
   * @param propertyKeys
   */
  public static void checkNotNull(Map<String, String> properties, List<String> propertyKeys) {
    Preconditions.checkNotNull(properties);
    Preconditions.checkNotNull(propertyKeys);

    List<String> missingPropertyKeys = new ArrayList<>();

    for (String propertyKey : propertyKeys) {
      if (properties.get(propertyKey) == null) {
        missingPropertyKeys.add(propertyKey);
      }
    }

    if (!missingPropertyKeys.isEmpty()) {
      throw new IllegalArgumentException("Missing Property Keys: " + missingPropertyKeys);
    }
  }
}
