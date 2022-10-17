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
package org.apache.pinot.query.runtime.operator;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;


public class OperatorUtils {

  private static final Map<String, String> OPERATOR_TOKEN_MAPPING = new HashMap<>();

  static {
    OPERATOR_TOKEN_MAPPING.put("=", "equals");
    OPERATOR_TOKEN_MAPPING.put(">", "greaterThan");
    OPERATOR_TOKEN_MAPPING.put("<", "lessThan");
    OPERATOR_TOKEN_MAPPING.put("<=", "lessThanOrEqual");
    OPERATOR_TOKEN_MAPPING.put(">=", "greaterThanOrEqual");
    OPERATOR_TOKEN_MAPPING.put("<>", "notEquals");
    OPERATOR_TOKEN_MAPPING.put("!=", "notEquals");
    OPERATOR_TOKEN_MAPPING.put("+", "plus");
    OPERATOR_TOKEN_MAPPING.put("-", "minus");
    OPERATOR_TOKEN_MAPPING.put("*", "times");
    OPERATOR_TOKEN_MAPPING.put("/", "divide");
    OPERATOR_TOKEN_MAPPING.put("||", "concat");
  }

  private OperatorUtils() {
    // do not instantiate.
  }

  /**
   * Canonicalize function name since Logical plan uses Parser.jj extracted tokens.
   * @param functionName input Function name
   * @return Canonicalize form of the input function name
   */
  public static String canonicalizeFunctionName(String functionName) {
    functionName = StringUtils.remove(functionName, " ");
    functionName = OPERATOR_TOKEN_MAPPING.getOrDefault(functionName, functionName);
    return functionName;
  }
}
