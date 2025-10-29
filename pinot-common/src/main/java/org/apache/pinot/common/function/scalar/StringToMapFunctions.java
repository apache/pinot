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
package org.apache.pinot.common.function.scalar;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Functions to convert STRING to MAP type.
 * Note: The input STRING must be in valid JSON format.
 *
 * <p>The returned MAP has values of type OBJECT, allowing the schema to determine
 * how to handle different value types (strings, numbers, booleans, etc.).
 *
 * <p>Example usage:
 * <pre>
 * stringToMap('{"key1":"value1","key2":"value2"}') => Map("key1" -> "value1", "key2" -> "value2")
 * stringToMap('{"name":"John","age":30,"active":true}') => Map("name" -> "John", "age" -> 30, "active" -> true)
 * </pre>
 */
public class StringToMapFunctions {
  private StringToMapFunctions() {
  }

  /**
   * Converts a STRING (in JSON format) to a Map<String, Object>.
   * The values are kept as OBJECT type so the schema can handle type conversions.
   * Throws RuntimeException if the input is not valid JSON.
   * Returns null if the input is null or empty string.
   *
   * @param jsonString STRING containing valid JSON to parse
   * @return Map<String, Object> representation where values retain their types, or null if input is null/empty
   * @throws RuntimeException if the string is not valid JSON
   */
  @Nullable
  @ScalarFunction(names = {"stringToMap", "string_to_map"}, nullableParameters = true)
  public static Map<String, Object> stringToMap(@Nullable String jsonString) {
    if (jsonString == null || jsonString.trim().isEmpty()) {
      return null;
    }

    try {
      return JsonUtils.stringToObject(jsonString, Map.class);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse STRING to MAP: " + jsonString, e);
    }
  }

  /**
   * Converts a STRING (in JSON format) to a Map<String, Object> and extracts a specific key's value.
   * The value is returned as OBJECT type so the schema can handle type conversion.
   * Throws RuntimeException if the input is not valid JSON.
   * Returns null if the input is null, empty string, or the key doesn't exist in the map.
   *
   * @param jsonString STRING containing valid JSON to parse
   * @param key Key to extract from the map
   * @return Value associated with the key as OBJECT, or null if input is null/empty or key not found
   * @throws RuntimeException if the string is not valid JSON
   */
  @Nullable
  @ScalarFunction(names = {"stringExtractValue", "string_extract_value"}, nullableParameters = true)
  public static Object stringExtractValue(@Nullable String jsonString, String key) {
    Map<String, Object> map = stringToMap(jsonString);
    if (map == null) {
      return null;
    }
    return map.get(key);
  }
}
