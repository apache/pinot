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
package org.apache.pinot.plugin.inputformat.json;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Helper methods for converting values from json nodes to
 */
public final class JSONRecordExtractorUtils {

  private JSONRecordExtractorUtils() {}

  /**
   * Converts the value to either a single value (string, number), a multi value (Object[]) or a Map
   *
   * Natively Pinot only understands single values and multi values.
   * Map is useful only if some ingestion transform functions operates on it in the transformation layer
   */
  public static Object convertValue(Object value) {
    if (value == null) {
      return null;
    }
    Object convertedValue;
    if (value instanceof Collection) {
      convertedValue = convertMultiValue((Collection) value);
    } else if (value instanceof Map) {
      convertedValue = convertMap((Map) value);
    } else {
      if (value instanceof Number) {
        convertedValue = value;
      } else {
        convertedValue = value.toString();
      }
    }
    return convertedValue;
  }

  /**
   * Applies conversion to each element of the collection
   */
  @Nullable
  private static Object convertMultiValue(Collection values) {
    if (values.isEmpty()) {
      return null;
    }
    int numValues = values.size();
    Object[] array = new Object[numValues];
    int index = 0;
    for (Object value : values) {
      Object convertedValue = convertValue(value);
      if (convertedValue != null && !convertedValue.toString().equals("")) {
        array[index++] = convertedValue;
      }
    }
    if (index == numValues) {
      return array;
    } else if (index == 0) {
      return null;
    } else {
      return Arrays.copyOf(array, index);
    }
  }

  /**
   * Applies the conversion to each value of the map
   */
  @Nullable
  private static Object convertMap(Map map) {
    if (map.isEmpty()) {
      return null;
    }
    Map<Object, Object> convertedMap = new HashMap<>();
    for (Object key : map.keySet()) {
      convertedMap.put(key, convertValue(map.get(key)));
    }
    return convertedMap;
  }
}
