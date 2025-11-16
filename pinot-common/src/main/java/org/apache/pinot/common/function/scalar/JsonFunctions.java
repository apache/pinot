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
import com.google.common.base.Preconditions;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.spi.cache.CacheProvider;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.JsonPathCache;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Inbuilt json related transform functions
 *   An example DimFieldSpec that needs the toJsonMapStr function:
 *   <code>
 *     "dimFieldSpecs": [{
 *       "name": "jsonMapStr",
 *       "dataType": "STRING",
 *       "transformFunction": "toJsonMapStr(jsonMap)"
 *     }]
 *   </code>
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
   *      output: {"k1": "v1", "k2": "v2", "k3": "v3"}
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
   * - jsonExtractKey('{"a": 1, "b": {"c": 2}}', '$.*') returns ["$['a']", "$['b']"]
   * - jsonExtractKey('{"a": 1, "b": {"c": 2}}', '$..**') returns ["$['a']", "$['b']", "$['b']['c']"]
   * - jsonExtractKey('{"a": [1, 2]}', '$.*') returns ["$['a']"]
   *
   * @param jsonObj  JSON object or string
   * @param jsonPath JsonPath expression to extract keys
   * @return List of key paths matching the JsonPath, empty list if input is null or invalid
   */
  @ScalarFunction
  public static List<String> jsonExtractKey(Object jsonObj, String jsonPath) {
    // treat empty string as extracting all
    if ("".equals(jsonPath)) {
      jsonPath = "$..**";
    }
    return jsonExtractKeyInternal(jsonObj, JsonPathCache.INSTANCE.getOrCompute(jsonPath));
  }

  public static List<String> jsonExtractKeyInternal(Object jsonObj, JsonPath jsonPath) {
    if (jsonObj instanceof String) {
      return KEY_PARSE_CONTEXT.parse((String) jsonObj).read(jsonPath);
    }
    return KEY_PARSE_CONTEXT.parse(jsonObj).read(jsonPath);
  }

  @ScalarFunction
  public static List<String> jsonExtractKey(Object jsonObj, String jsonPath, String paramString)
      throws IOException {
    JsonExtractFunctionParameters params = new JsonExtractFunctionParameters(paramString);

    if (params._maxDepth == 0) {
      return java.util.Collections.emptyList();
    }
    // Special handling for empty string, '$.**' and '$..' recursive key extraction
    if ("".equals(jsonPath) || "$..**".equals(jsonPath) || "$..".equals(jsonPath)) {
      return jsonExtractAllKeysInternal(jsonObj, params._maxDepth, params._dotNotation);
    }
    return jsonExtractKeyInternal(jsonObj, JsonPathCache.INSTANCE.getOrCompute(jsonPath), params._maxDepth,
        params._dotNotation);
  }

  public static boolean isExtractAllKeys(String jsonPath) {
    return "".equals(jsonPath) || "$..**".equals(jsonPath) || "$..".equals(jsonPath);
  }

  public static List<String> jsonExtractKeyInternal(Object jsonObj, JsonPath jsonPath, int maxDepth,
      boolean dotNotation)
      throws IOException {
    // For other expressions, try to get keys using AS_PATH_LIST
    List<String> keys = jsonObj instanceof String ? KEY_PARSE_CONTEXT.parse((String) jsonObj).read(jsonPath)
        : KEY_PARSE_CONTEXT.parse(jsonObj).read(jsonPath);
    if (dotNotation) {
      List<String> finalKeys = new ArrayList<>();
      for (String key : keys) {
        if (getKeyDepth(key) <= maxDepth) {
          finalKeys.add(convertToDotNotation(key));
        }
      }
      return finalKeys;
    }
    keys.removeIf(s -> getKeyDepth(s) > maxDepth);
    return keys;
  }

  /**
   * Extract all keys recursively from a JSON object up to maxDepth with output format option
   */
  public static List<String> jsonExtractAllKeysInternal(Object jsonObj, int maxDepth, boolean dotNotation) {
    List<String> allKeys = new ArrayList<>();
    try {
      JsonNode node;
      if (jsonObj instanceof String) {
        node = JsonUtils.stringToJsonNode((String) jsonObj);
      } else {
        node = JsonUtils.stringToJsonNode(JsonUtils.objectToString(jsonObj));
      }

      extractKeysFromNode(node, dotNotation ? "" : "$", allKeys, maxDepth, 1, dotNotation);
    } catch (Exception e) {
      // Return empty list on error
    }
    return allKeys;
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
        String newPath = dotNotation ? (currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName)
            : currentPath + "['" + fieldName + "']";
        keys.add(newPath);

        JsonNode childNode = node.get(fieldName);
        if (currentDepth < maxDepth && (childNode.isObject() || childNode.isArray())) {
          extractKeysFromNode(childNode, newPath, keys, maxDepth, currentDepth + 1, dotNotation);
        }
      });
    } else if (node.isArray()) {
      for (int i = 0; i < node.size(); i++) {
        String newPath = dotNotation ? (currentPath.isEmpty() ? String.valueOf(i) : currentPath + "." + i)
            : currentPath + "[" + i + "]";
        keys.add(newPath);

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
  static String convertToDotNotation(String jsonPath) {
    if (jsonPath == null || jsonPath.isEmpty() || "$".equals(jsonPath)) {
      return "";
    }

    String result = jsonPath.substring(1); // Remove $

    StringBuilder sb = new StringBuilder();
    int i = 0;

    while (i < result.length()) {
      if (i + 1 < result.length() && result.charAt(i) == '[' && result.charAt(i + 1) == '\'') {
        // Handle ['key'] pattern
        int closingQuoteIndex = result.indexOf('\'', i + 2);
        if (closingQuoteIndex != -1 && closingQuoteIndex + 1 < result.length()
            && result.charAt(closingQuoteIndex + 1) == ']') {
          sb.append('.');
          sb.append(result, i + 2, closingQuoteIndex);
          i = closingQuoteIndex + 2; // Skip past the closing ']'
        } else {
          sb.append(result.charAt(i));
          i++;
        }
      } else if (result.charAt(i) == '[') {
        // Handle [index] pattern
        int closingBracketIndex = result.indexOf(']', i);
        if (closingBracketIndex != -1) {
          String possibleIndex = result.substring(i + 1, closingBracketIndex);
          boolean isNumeric = true;
          for (int j = 0; j < possibleIndex.length(); j++) {
            if (!Character.isDigit(possibleIndex.charAt(j))) {
              isNumeric = false;
              break;
            }
          }

          if (isNumeric) {
            sb.append('.');
            sb.append(possibleIndex);
            i = closingBracketIndex + 1; // Skip past the closing ']'
          } else {
            sb.append(result.charAt(i));
            i++;
          }
        } else {
          sb.append(result.charAt(i));
          i++;
        }
      } else {
        sb.append(result.charAt(i));
        i++;
      }
    }

    result = sb.toString();

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
  static int getKeyDepth(String key) {
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

  public static class JsonExtractFunctionParameters {
    static final char PARAMETER_DELIMITER = ';';
    static final char PARAMETER_KEY_VALUE_SEPARATOR = '=';

    static final String MAX_DEPTH_KEY = "MAXDEPTH";
    static final String DOT_NOTATION_KEY = "DOTNOTATION";

    int _maxDepth = Integer.MAX_VALUE;
    boolean _dotNotation;

    public JsonExtractFunctionParameters(String parametersString) {
      String[] keyValuePairs = StringUtils.split(parametersString, PARAMETER_DELIMITER);
      for (String keyValuePairRaw : keyValuePairs) {
        String keyValuePair = keyValuePairRaw.trim();
        if (keyValuePair.isEmpty()) {
          continue;
        }
        String[] keyAndValue = StringUtils.split(keyValuePair, PARAMETER_KEY_VALUE_SEPARATOR);
        Preconditions.checkArgument(keyAndValue.length == 2, "Invalid parameter: %s", keyValuePair);
        String key = keyAndValue[0];
        String value = keyAndValue[1].trim();
        switch (key.trim().toUpperCase()) {
          case MAX_DEPTH_KEY:
            _maxDepth = Integer.parseInt(value);
            // Treat the non-positive threshold as unlimited
            if (_maxDepth < 0) {
              _maxDepth = Integer.MAX_VALUE;
            }
            break;
          case DOT_NOTATION_KEY:
            _dotNotation = Boolean.parseBoolean(value);
            break;
          default:
            throw new IllegalArgumentException("Invalid parameter key: " + key);
        }
      }
    }

    public int getMaxDepth() {
      return _maxDepth;
    }

    public boolean isDotNotation() {
      return _dotNotation;
    }
  }
}
