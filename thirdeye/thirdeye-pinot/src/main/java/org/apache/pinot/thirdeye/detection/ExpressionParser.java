/*
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
package org.apache.pinot.thirdeye.detection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;


/**
 * Util class used by TriggerConditionGrouper to parse trigger condition expression.
 */
public class ExpressionParser {

  static final String PROP_AND = "and";
  static final String PROP_OR = "or";
  static final String PROP_OPERATOR = "operator";
  static final String PROP_LEFT_OP = "leftOp";
  static final String PROP_RIGHT_OP = "rightOp";
  static final String PROP_VALUE = "value";

  static final String OPERATOR_AND = "&&";
  static final String OPERATOR_OR = "||";
  static final String OPERATOR_LEFT_BRACKET = "(";
  static final String OPERATOR_RIGHT_BRACKET = ")";

  static final String[] operators = new String[] {OPERATOR_AND, OPERATOR_OR, OPERATOR_LEFT_BRACKET, OPERATOR_RIGHT_BRACKET};

  /**
   * Parse expression into operation tree.
   *
   * @param expression The expression string.
   * @return Operation tree.
   */
  public static Map<String, Object> generateOperators(String expression) {
    List<String> tokens = ExpressionParser.tokenize(expression);
    List<String> rpn = generateRPN(tokens);
    Stack<Map<String, Object>> stack = new Stack<>();
    for (String token : rpn) {
      Map<String, Object> objectMap = new HashMap<>();
      if (token.equals(OPERATOR_AND)) {
        objectMap.put(PROP_RIGHT_OP, stack.pop());
        objectMap.put(PROP_LEFT_OP, stack.pop());
        objectMap.put(PROP_OPERATOR, PROP_AND);
      } else if (token.equals(OPERATOR_OR)) {
        objectMap.put(PROP_RIGHT_OP, stack.pop());
        objectMap.put(PROP_LEFT_OP, stack.pop());
        objectMap.put(PROP_OPERATOR, PROP_OR);
      } else {
        objectMap.put(PROP_VALUE, token);
      }
      stack.push(objectMap);
    }
    return stack.peek();
  }

  private static String getOperator(String expression, int index) {
    for (String operator : operators) {
      if (expression.length() >= index + operator.length()
          && expression.substring(index, index + operator.length()).equals(operator)) {
        return operator;
      }
    }
    return "";
  }

  private static List<String> tokenize(String expression) {
    List<String> tokens = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    int i = 0;
    while(i < expression.length()) {
      String operator = getOperator(expression, i);
      if (!operator.isEmpty()) {
        if (sb.length() > 0) {
          tokens.add(sb.toString());
          sb.setLength(0);
        }
        tokens.add(operator);
        i += operator.length();
      } else {
        sb.append(expression.charAt(i++));
      }
    }
    if (sb.length() > 0) {
      tokens.add(sb.toString());
    }
    return tokens.stream().map(String::trim).filter(x -> !x.isEmpty()).collect(Collectors.toList());
  }

  private static List<String> generateRPN(List<String> tokens) {
    List<String> rpn = new ArrayList<>();
    Stack<String> stack = new Stack<>();
    for (String token : tokens) {
      switch (token) {
        case OPERATOR_RIGHT_BRACKET:
          while (!stack.isEmpty()) {
            String top = stack.pop();
            if (!top.equals("(")) {
              rpn.add(top);
            } else {
              break;
            }
          }
          break;
        case OPERATOR_AND:
        case OPERATOR_OR:
          if (!stack.isEmpty()) {
            rpn.add(stack.pop());
          }
          stack.push(token);
          break;
        default:
          // push when it is "(" or non operator
          stack.push(token);
      }
    }
    while (!stack.isEmpty()) {
      rpn.add(stack.pop());
    }
    return rpn;
  }
}
