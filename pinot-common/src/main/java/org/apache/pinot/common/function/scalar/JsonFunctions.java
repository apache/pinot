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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.spi.cache.CacheProvider;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
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
      ObjectMapper mapper = new ObjectMapper();
      JsonNode arrayNode;
      try {
        arrayNode = mapper.readTree(keyValueArray.toString());
      } catch (JsonProcessingException e) {
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
      ObjectMapper mapper = new ObjectMapper();
      JsonNode mapNode;
      try {
        mapNode = mapper.readTree(obj.toString());
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      result.put(mapNode.get(keyColumnName).asText(), mapNode.get(valueColumnName).asText());
    }
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
