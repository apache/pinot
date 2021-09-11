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
import com.google.common.annotations.VisibleForTesting;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.internal.ParseContextImpl;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final ParseContext PARSE_CONTEXT;
  private static final Predicate[] NO_PREDICATES = new Predicate[0];
  static {
    Configuration.setDefaults(new Configuration.Defaults() {
      private final JsonProvider _jsonProvider = new ArrayAwareJacksonJsonProvider();
      private final MappingProvider _mappingProvider = new JacksonMappingProvider();

      @Override
      public JsonProvider jsonProvider() {
        return _jsonProvider;
      }

      @Override
      public MappingProvider mappingProvider() {
        return _mappingProvider;
      }

      @Override
      public Set<Option> options() {
        return EnumSet.noneOf(Option.class);
      }
    });
    PARSE_CONTEXT = new ParseContextImpl(Configuration.defaultConfiguration());
  }

  private JsonFunctions() {
  }

  /**
   * Convert Map to Json String
   */
  @ScalarFunction
  public static String toJsonMapStr(Map map)
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
  @ScalarFunction
  public static Object[] jsonPathArray(Object object, String jsonPath)
      throws JsonProcessingException {
    if (object instanceof String) {
      return convertObjectToArray(PARSE_CONTEXT.parse((String) object).read(jsonPath, NO_PREDICATES));
    }
    return convertObjectToArray(PARSE_CONTEXT.parse(object).read(jsonPath, NO_PREDICATES));
  }

  @ScalarFunction
  public static Object[] jsonPathArrayDefaultEmpty(Object object, String jsonPath) {
    try {
      return jsonPathArray(object, jsonPath);
    } catch (Exception e) {
      return new Object[0];
    }
  }

  private static Object[] convertObjectToArray(Object arrayObject) {
    if (arrayObject instanceof List) {
      return ((List) arrayObject).toArray();
    } else if (arrayObject instanceof Object[]) {
      return (Object[]) arrayObject;
    }
    return new Object[]{arrayObject};
  }

  /**
   * Extract from Json with path to String
   */
  @ScalarFunction
  public static String jsonPathString(Object object, String jsonPath)
      throws JsonProcessingException {
    Object jsonValue = jsonPath(object, jsonPath);
    if (jsonValue instanceof String) {
      return (String) jsonValue;
    }
    return JsonUtils.objectToString(jsonValue);
  }

  /**
   * Extract from Json with path to String
   */
  @ScalarFunction
  public static String jsonPathString(Object object, String jsonPath, String defaultValue) {
    try {
      return jsonPathString(object, jsonPath);
    } catch (Exception e) {
      return defaultValue;
    }
  }

  /**
   * Extract from Json with path to Long
   */
  @ScalarFunction
  public static long jsonPathLong(Object object, String jsonPath) {
    final Object jsonValue = jsonPath(object, jsonPath);
    if (jsonValue == null) {
      return Long.MIN_VALUE;
    }
    if (jsonValue instanceof Number) {
      return ((Number) jsonValue).longValue();
    }
    return Long.parseLong(jsonValue.toString());
  }

  /**
   * Extract from Json with path to Long
   */
  @ScalarFunction
  public static long jsonPathLong(Object object, String jsonPath, long defaultValue) {
    try {
      return jsonPathLong(object, jsonPath);
    } catch (Exception e) {
      return defaultValue;
    }
  }

  /**
   * Extract from Json with path to Double
   */
  @ScalarFunction
  public static double jsonPathDouble(Object object, String jsonPath) {
    final Object jsonValue = jsonPath(object, jsonPath);
    if (jsonValue instanceof Number) {
      return ((Number) jsonValue).doubleValue();
    }
    return Double.parseDouble(jsonValue.toString());
  }

  /**
   * Extract from Json with path to Double
   */
  @ScalarFunction
  public static double jsonPathDouble(Object object, String jsonPath, double defaultValue) {
    try {
      return jsonPathDouble(object, jsonPath);
    } catch (Exception e) {
      return defaultValue;
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
