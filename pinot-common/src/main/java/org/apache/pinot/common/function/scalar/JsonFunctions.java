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
import com.jayway.jsonpath.JsonPath;
import java.util.Map;
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
      return JsonPath.read((String) object, jsonPath);
    }
    return JsonPath.read(object, jsonPath);
  }

  /**
   * Extract from Json with path to String
   */
  @ScalarFunction
  public static String jsonPathString(Object object, String jsonPath)
      throws JsonProcessingException {
    return JsonUtils.objectToString(jsonPath(object, jsonPath));
  }

  /**
   * Extract from Json with path to Long
   */
  @ScalarFunction
  public static long jsonPathLong(Object object, String jsonPath)
      throws JsonProcessingException {
    return Long.parseLong(jsonPathString(object, jsonPath));
  }

  /**
   * Extract from Json with path to Double
   */
  @ScalarFunction
  public static double jsonPathDouble(Object object, String jsonPath)
      throws JsonProcessingException {
    return Double.parseDouble(jsonPathString(object, jsonPath));
  }
}
