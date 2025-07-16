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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.spi.cache.CacheProvider;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Inbuilt json related transform functions
 * An example DimFieldSpec that needs the toJsonMapStr function:
 * <code>
 * "dimFieldSpecs": [{
 * "name": "jsonMapStr",
 * "dataType": "STRING",
 * "transformFunction": "toJsonMapStr(jsonMap)"
 * }]
 * </code>
 */
public class JsonFunctions {
  private JsonFunctions() {
  }

  private static final Object[] EMPTY = new Object[0];
  private static final Predicate[] NO_PREDICATES = new Predicate[0];
  private static final ParseContext PARSE_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new ArrayAwareJacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS)
          .build());

  // JsonPath context for extracting keys (paths)
  private static final ParseContext KEY_PARSE_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS)
          .build());

  static {
    // Set the JsonPath cache before the cache is accessed
    CacheProvider.setCache(new JsonPathCache());
  }

  /**
   * Convert Map to Json String
   */
  @ScalarFunction(nullableParameters = true)
  public static String toJsonMapStr(@Nullable Map map)
      throws JsonProcessingException {
    return JsonUtils.objectToString(map);
  }

  /**
   * Convert object to Json String
   */
  @ScalarFunction
  public static String jsonFormat(Object object)
      throws JsonProcessingException {
    return JsonUtils.objectToString(object);
  }

  /**
   * Extract object based on Json path
   */
  @Nullable
  @ScalarFunction
  public static Object jsonPath(Object object, String jsonPath) {
    if (object instanceof String) {
      return PARSE_CONTEXT.parse((String) object).read(jsonPath, NO_PREDICATES);
    }
    return PARSE_CONTEXT.parse(object).read(jsonPath, NO_PREDICATES);
  }

  /**
   * Extract object array based on Json path
   */
  @Nullable
  @ScalarFunction
  public static Object[] jsonPathArray(Object object, String jsonPath) {
    if (object instanceof String) {
      return convertObjectToArray(PARSE_CONTEXT.parse((String) object).read(jsonPath, NO_PREDICATES));
    }
    return convertObjectToArray(PARSE_CONTEXT.parse(object).read(jsonPath, NO_PREDICATES));
  }

  @ScalarFunction(nullableParameters = true)
  public static Object[] jsonPathArrayDefaultEmpty(@Nullable Object object, String jsonPath) {
    try {
      Object[] result = object == null ? null : jsonPathArray(object, jsonPath);
      return result == null ? EMPTY : result;
    } catch (Exception e) {
      return EMPTY;
    }
  }

  @Nullable
  private static Object[] convertObjectToArray(@Nullable Object arrayObject) {
    if (arrayObject == null) {
      return null;
    }
    if (arrayObject instanceof List) {
      return ((List) arrayObject).toArray();
    }
    if (arrayObject instanceof Object[]) {
      return (Object[]) arrayObject;
    }
    return new Object[]{arrayObject};
  }

  /**
   * Check if path exists in Json object
   */
  @ScalarFunction
  public static boolean jsonPathExists(Object object, String jsonPath) {
    try {
      return jsonPath(object, jsonPath) != null;
    } catch (Exception ignore) {
      return false;
    }
  }

  /**
   * Extract from Json with path to String
   */
  @Nullable
  @ScalarFunction
  public static String jsonPathString(Object object, String jsonPath)
      throws JsonProcessingException {
    Object jsonValue = jsonPath(object, jsonPath);
    if (jsonValue instanceof String) {
      return (String) jsonValue;
    }
    return jsonValue == null ? null : JsonUtils.objectToString(jsonValue);
  }

  /**
   * Extract from Json with path to String
   */
  @ScalarFunction(nullableParameters = true)
  public static String jsonPathString(@Nullable Object object, String jsonPath, String defaultValue) {
    try {
      Object jsonValue = jsonPath(object, jsonPath);
      if (jsonValue instanceof String) {
        return (String) jsonValue;
      }
      return jsonValue == null ? defaultValue : JsonUtils.objectToString(jsonValue);
    } catch (Exception ignore) {
      return defaultValue;
    }
  }

  /**
   * Extract from Json with path to Long
   */
  @ScalarFunction
  public static long jsonPathLong(Object object, String jsonPath) {
    return jsonPathLong(object, jsonPath, Long.MIN_VALUE);
  }

  /**
   * Extract from Json with path to Long
   */
  @ScalarFunction(nullableParameters = true)
  public static long jsonPathLong(@Nullable Object object, String jsonPath, long defaultValue) {
    try {
      Object jsonValue = jsonPath(object, jsonPath);
      if (jsonValue == null) {
        return defaultValue;
      }
      if (jsonValue instanceof Number) {
        return ((Number) jsonValue).longValue();
      }
      return Long.parseLong(jsonValue.toString());
    } catch (Exception ignore) {
      return defaultValue;
    }
  }

  /**
   * Extract from Json with path to Double
   */
  @ScalarFunction
  public static double jsonPathDouble(Object object, String jsonPath) {
    return jsonPathDouble(object, jsonPath, Double.NaN);
  }

  /**
   * Extract from Json with path to Double
   */
  @ScalarFunction(nullableParameters = true)
  public static double jsonPathDouble(@Nullable Object object, String jsonPath, double defaultValue) {
    try {
      Object jsonValue = jsonPath(object, jsonPath);
      if (jsonValue == null) {
        return defaultValue;
      }
      if (jsonValue instanceof Number) {
        return ((Number) jsonValue).doubleValue();
      }
      return Double.parseDouble(jsonValue.toString());
    } catch (Exception ignore) {
      return defaultValue;
    }
  }

  /**
   * Extract an array of key-value maps to a map.
   * E.g. input: [{"key": "k1", "value": "v1"}, {"key": "k2", "value": "v2"}, {"key": "k3", "value": "v3"}]
   * output: {"k1": "v1", "k2": "v2", "k3": "v3"}
   */
  @ScalarFunction
  public static Object jsonKeyValueArrayToMap(Object keyValueArray) {
    return jsonKeyValueArrayToMap(keyValueArray, "key", "value");
  }

  @ScalarFunction
  public static Object jsonKeyValueArrayToMap(Object keyValueArray, String keyColumnName, String valueColumnName) {
    Map<String, String> result = new java.util.HashMap<>();
    if (keyValueArray instanceof Object[]) {
      Object[] array = (Object[]) keyValueArray;
      for (Object obj : array) {
        setValuesToMap(keyColumnName, valueColumnName, obj, result);
      }
    } else if (keyValueArray instanceof List) {
      List<Object> list = (List<Object>) keyValueArray;
      for (Object obj : list) {
        setValuesToMap(keyColumnName, valueColumnName, obj, result);
      }
    } else {
      JsonNode arrayNode;
      try {
        arrayNode = JsonUtils.stringToJsonNode(keyValueArray.toString());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      for (int i = 0; i < arrayNode.size(); i++) {
        JsonNode node = arrayNode.get(i);
        result.put(node.get(keyColumnName).asText(), node.get(valueColumnName).asText());
      }
    }
    return result;
  }

  @Nullable
  @ScalarFunction
  public static List jsonStringToArray(String jsonString) {
    String json = jsonString.trim();
    try {
      if (json.startsWith("[")) {
        // JSON Array
        return JsonUtils.stringToObject(json, List.class);
      }
    } catch (Exception e) {
      // Ignore
    }
    return null;
  }

  @Nullable
  @ScalarFunction
  public static Map jsonStringToMap(String jsonString) {
    String json = jsonString.trim();
    try {
      if (json.startsWith("{")) {
        return JsonUtils.stringToObject(json, Map.class);
      }
    } catch (Exception e) {
      // Ignore
    }
    return null;
  }

  @Nullable
  @ScalarFunction
  public static Object jsonStringToListOrMap(String jsonString) {
    String json = jsonString.trim();
    try {
      if (json.startsWith("[")) {
        // JSON Array
        return JsonUtils.stringToObject(json, List.class);
      }
      if (json.startsWith("{")) {
        // JSON Object
        return JsonUtils.stringToObject(json, Map.class);
      }
    } catch (Exception e) {
      // Ignore
    }
    return null;
  }

  private static void setValuesToMap(String keyColumnName, String valueColumnName, Object obj,
      Map<String, String> result) {
    if (obj instanceof Map) {
      Map<String, String> objMap = (Map) obj;
      result.put(objMap.get(keyColumnName), objMap.get(valueColumnName));
    } else {
      JsonNode mapNode;
      try {
        mapNode = JsonUtils.stringToJsonNode(obj.toString());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      result.put(mapNode.get(keyColumnName).asText(), mapNode.get(valueColumnName).asText());
    }
  }

  /**
   * Extract keys from JSON object using JsonPath.
   * <p>
   * Examples:
   * - jsonExtractKey('{"a": 1, "b": {"c": 2}}', '$.*') returns ["$['a']", "$['b']"]
   * - jsonExtractKey('{"a": 1, "b": {"c": 2}}', '$.*', 2) returns ["$['a']", "$['b']"]
   * - jsonExtractKey('{"a": [1, 2]}', '$.*', 1) returns ["$['a']"]
   *
   * @param jsonObj  JSON object or string
   * @param jsonPath JsonPath expression to extract keys
   * @return List of key paths matching the JsonPath, empty list if input is null or invalid
   */
  @ScalarFunction
  public static List jsonExtractKey(Object jsonObj, String jsonPath)
      throws IOException {
    return jsonExtractKey(jsonObj, jsonPath, Integer.MAX_VALUE, false);
  }

  /**
   * Extract keys from JSON object using JsonPath with depth limit.
   * <p>
   * Examples:
   * - jsonExtractKey('{"a": 1, "b": {"c": 2}}', '$.*', 1) returns ["$['a']", "$['b']"]
   * - jsonExtractKey('{"a": 1, "b": {"c": 2}}', '$.*', 2) returns ["$['a']", "$['b']"]
   * - jsonExtractKey('{"a": [1, 2]}', '$.*', 1) returns ["$['a']"]
   *
   * @param jsonObj  JSON object or string
   * @param jsonPath JsonPath expression to extract keys
   * @param maxDepth Maximum depth to recurse (must be positive)
   * @return List of key paths matching the JsonPath up to maxDepth, empty list if input is null or invalid
   */
  @ScalarFunction
  public static List jsonExtractKey(Object jsonObj, String jsonPath, int maxDepth)
      throws IOException {
    return jsonExtractKey(jsonObj, jsonPath, maxDepth, false);
  }

  /**
   * Extract keys from JSON object using JsonPath with depth limit and output format option.
   * <p>
   * Examples:
   * - jsonExtractKey('{"a": 1, "b": {"c": 2}}', '$.*', 1, false) returns ["$['a']", "$['b']"]
   * - jsonExtractKey('{"a": 1, "b": {"c": 2}}', '$..**', 2, true) returns ["a", "b", "b.c"]
   * - jsonExtractKey('{"a": [1, 2]}', '$.*', 1, true) returns ["a"]
   *
   * @param jsonObj     JSON object or string
   * @param jsonPath    JsonPath expression to extract keys
   * @param maxDepth    Maximum depth to recurse (must be positive)
   * @param dotNotation If true, return keys in dot notation (e.g., "a.b.c"),
   *                    if false, return JsonPath format (e.g., "$['a']['b']['c']")
   * @return List of key paths matching the JsonPath up to maxDepth, empty list if input is null or invalid
   */
  @ScalarFunction
  public static List jsonExtractKey(Object jsonObj, String jsonPath, int maxDepth, boolean dotNotation)
      throws IOException {
    if (maxDepth <= 0) {
      return java.util.Collections.emptyList();
    }

    // Special handling for $.** and $.. recursive key extraction
    if ("$..**".equals(jsonPath) || "$..".equals(jsonPath)) {
      return extractAllKeysRecursively(jsonObj, maxDepth, dotNotation);
    }

    // For other expressions, try to get keys using AS_PATH_LIST
    List<String> keys = null;
    try {
      keys = KEY_PARSE_CONTEXT.parse(
          jsonObj instanceof String ? (String) jsonObj : jsonObj).read(jsonPath);
    } catch (Exception e) {
      // AS_PATH_LIST might not work for all expressions
    }

    // If AS_PATH_LIST doesn't work, fall back to manual path construction
    if (keys == null || keys.isEmpty()) {
      return extractKeysForNonRecursiveExpression(jsonObj, jsonPath, maxDepth, dotNotation);
    }

    // Filter keys by depth if maxDepth is specified
    if (maxDepth != Integer.MAX_VALUE) {
      keys = keys.stream()
          .filter(key -> getKeyDepth(key) <= maxDepth)
          .collect(java.util.stream.Collectors.toList());
    }

    // Convert to dot notation if requested
    if (dotNotation) {
      keys = keys.stream()
          .map(JsonFunctions::convertToDotNotation)
          .collect(java.util.stream.Collectors.toList());
    }

    return keys;
  }

  /**
   * Extract keys for non-recursive expressions by manually constructing paths
   */
  private static List<String> extractKeysForNonRecursiveExpression(Object jsonObj, String jsonPath,
      int maxDepth, boolean dotNotation)
      throws IOException {
    JsonNode node;
    if (jsonObj instanceof String) {
      node = JsonUtils.stringToJsonNode((String) jsonObj);
    } else {
      node = JsonUtils.stringToJsonNode(JsonUtils.objectToString(jsonObj));
    }

    List<String> keys = new java.util.ArrayList<>();

    // Handle common patterns
    if ("$.*".equals(jsonPath)) {
      // Top level keys
      if (node.isObject()) {
        node.fieldNames().forEachRemaining(fieldName -> {
          String path = "$['" + fieldName + "']";
          if (dotNotation) {
            keys.add(fieldName);
          } else {
            keys.add(path);
          }
        });
      } else if (node.isArray()) {
        for (int i = 0; i < node.size(); i++) {
          String path = "$[" + i + "]";
          if (dotNotation) {
            keys.add(String.valueOf(i));
          } else {
            keys.add(path);
          }
        }
      }
    } else if (jsonPath.matches("\\$\\.[^.]+\\.\\*")) {
      // Pattern like $.field.*
      String fieldPath = jsonPath.substring(2, jsonPath.length() - 2); // Remove $. and .*
      JsonNode targetNode = node.get(fieldPath);
      if (targetNode != null) {
        if (targetNode.isObject()) {
          targetNode.fieldNames().forEachRemaining(fieldName -> {
            String path = "$['" + fieldPath + "']['" + fieldName + "']";
            if (dotNotation) {
              keys.add(fieldPath + "." + fieldName);
            } else {
              keys.add(path);
            }
          });
        } else if (targetNode.isArray()) {
          for (int i = 0; i < targetNode.size(); i++) {
            String path = "$['" + fieldPath + "'][" + i + "]";
            if (dotNotation) {
              keys.add(fieldPath + "." + i);
            } else {
              keys.add(path);
            }
          }
        }
      }
    }
    return keys;
  }

  /**
   * Extract all keys recursively from a JSON object up to maxDepth
   */
  private static List<String> extractAllKeysRecursively(Object jsonObj, int maxDepth) {
    return extractAllKeysRecursively(jsonObj, maxDepth, false);
  }

  /**
   * Extract all keys recursively from a JSON object up to maxDepth with output format option
   */
  private static List<String> extractAllKeysRecursively(Object jsonObj, int maxDepth, boolean dotNotation) {
    List<String> allKeys = new java.util.ArrayList<>();
    try {
      JsonNode node;
      if (jsonObj instanceof String) {
        node = JsonUtils.stringToJsonNode((String) jsonObj);
      } else {
        node = JsonUtils.stringToJsonNode(JsonUtils.objectToString(jsonObj));
      }

      extractKeysFromNode(node, "$", allKeys, maxDepth, 1, dotNotation);
    } catch (Exception e) {
      // Return empty list on error
    }
    return allKeys;
  }

  /**
   * Recursively extract keys from a JsonNode
   */
  private static void extractKeysFromNode(JsonNode node, String currentPath, List<String> keys,
      int maxDepth, int currentDepth) {
    extractKeysFromNode(node, currentPath, keys, maxDepth, currentDepth, false);
  }

  /**
   * Recursively extract keys from a JsonNode with output format option
   */
  private static void extractKeysFromNode(JsonNode node, String currentPath, List<String> keys,
      int maxDepth, int currentDepth, boolean dotNotation) {
    if (currentDepth > maxDepth) {
      return;
    }

    if (node.isObject()) {
      node.fieldNames().forEachRemaining(fieldName -> {
        String newPath = currentPath + "['" + fieldName + "']";
        String keyToAdd = dotNotation ? convertToDotNotation(newPath) : newPath;
        keys.add(keyToAdd);

        JsonNode childNode = node.get(fieldName);
        if (currentDepth < maxDepth && (childNode.isObject() || childNode.isArray())) {
          extractKeysFromNode(childNode, newPath, keys, maxDepth, currentDepth + 1, dotNotation);
        }
      });
    } else if (node.isArray()) {
      for (int i = 0; i < node.size(); i++) {
        String newPath = currentPath + "[" + i + "]";
        String keyToAdd = dotNotation ? convertToDotNotation(newPath) : newPath;
        keys.add(keyToAdd);

        JsonNode childNode = node.get(i);
        if (currentDepth < maxDepth && (childNode.isObject() || childNode.isArray())) {
          extractKeysFromNode(childNode, newPath, keys, maxDepth, currentDepth + 1, dotNotation);
        }
      }
    }
  }

  /**
   * Convert JsonPath format to dot notation
   * Example: $['a']['b']['c'] -> a.b.c
   * $[0]['name'] -> 0.name
   */
  private static String convertToDotNotation(String jsonPath) {
    if (jsonPath == null || jsonPath.isEmpty() || "$".equals(jsonPath)) {
      return "";
    }

    String result = jsonPath.substring(1); // Remove $

    // Replace ['key'] with .key
    result = result.replaceAll("\\['([^']+)'\\]", ".$1");

    // Replace [index] with .index
    result = result.replaceAll("\\[([0-9]+)\\]", ".$1");

    // Remove leading dot if present
    if (result.startsWith(".")) {
      result = result.substring(1);
    }

    return result;
  }

  /**
   * Calculate the depth of a key path.
   * For JsonPath keys like "$['a']" or "$['a']['b']", count the number of levels.
   * For dot notation keys like "a.b.c", count the number of dots + 1.
   */
  private static int getKeyDepth(String key) {
    if (key == null || key.isEmpty()) {
      return 0;
    }

    // Handle JsonPath format like "$['a']" or "$['a']['b']"
    if (key.startsWith("$[")) {
      return (int) key.chars().filter(ch -> ch == '[').count();
    }

    // Handle dot notation format like "a.b.c"
    if (key.contains(".")) {
      return key.split("\\.").length;
    }

    // Single key
    return 1;
  }

  @VisibleForTesting
  static class ArrayAwareJacksonJsonProvider extends JacksonJsonProvider {
    @Override
    public boolean isArray(Object obj) {
      return (obj instanceof List) || (obj instanceof Object[]);
    }

    @Override
    public Object getArrayIndex(Object obj, int idx) {
      if (obj instanceof Object[]) {
        return ((Object[]) obj)[idx];
      }
      return super.getArrayIndex(obj, idx);
    }

    @Override
    public int length(Object obj) {
      if (obj instanceof Object[]) {
        return ((Object[]) obj).length;
      }
      return super.length(obj);
    }

    @Override
    public Iterable<?> toIterable(Object obj) {
      if (obj instanceof Object[]) {
        return Arrays.asList((Object[]) obj);
      }
      return super.toIterable(obj);
    }
  }
}
