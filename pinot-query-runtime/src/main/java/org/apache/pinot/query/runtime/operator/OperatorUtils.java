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


public class OperatorUtils {

  private static final Map<String, String> OPERATOR_TOKEN_MAPPING = new HashMap<>();

  static {
    OPERATOR_TOKEN_MAPPING.put("=", "EQ");
    OPERATOR_TOKEN_MAPPING.put(">", "GT");
    OPERATOR_TOKEN_MAPPING.put("<", "LT");
    OPERATOR_TOKEN_MAPPING.put("?", "HOOK");
    OPERATOR_TOKEN_MAPPING.put(":", "COLON");
    OPERATOR_TOKEN_MAPPING.put("<=", "LE");
    OPERATOR_TOKEN_MAPPING.put(">=", "GE");
    OPERATOR_TOKEN_MAPPING.put("<>", "NE");
    OPERATOR_TOKEN_MAPPING.put("!=", "NE2");
    OPERATOR_TOKEN_MAPPING.put("+", "PLUS");
    OPERATOR_TOKEN_MAPPING.put("-", "MINUS");
    OPERATOR_TOKEN_MAPPING.put("*", "STAR");
    OPERATOR_TOKEN_MAPPING.put("/", "DIVIDE");
    OPERATOR_TOKEN_MAPPING.put("%", "PERCENT_REMAINDER");
    OPERATOR_TOKEN_MAPPING.put("||", "CONCAT");
    OPERATOR_TOKEN_MAPPING.put("=>", "NAMED_ARGUMENT_ASSIGNMENT");
    OPERATOR_TOKEN_MAPPING.put("..", "DOUBLE_PERIOD");
    OPERATOR_TOKEN_MAPPING.put("'", "QUOTE");
    OPERATOR_TOKEN_MAPPING.put("\"", "DOUBLE_QUOTE");
    OPERATOR_TOKEN_MAPPING.put("|", "VERTICAL_BAR");
    OPERATOR_TOKEN_MAPPING.put("^", "CARET");
    OPERATOR_TOKEN_MAPPING.put("$", "DOLLAR");
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
    functionName = OPERATOR_TOKEN_MAPPING.getOrDefault(functionName, functionName);
    return functionName;
  }
}
