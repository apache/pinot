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
package org.apache.pinot.pql.parsers.pql2.ast;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.pql.parsers.Pql2CompilationException;
import org.apache.pinot.pql.parsers.Pql2Compiler;


/**
 * AST node for function calls, such as count(foo).
 */
public class FunctionCallAstNode extends BaseAstNode {
  private final String _name;
  private String _expression;
  private boolean _isInSelectList;

  public static final String DISTINCT_MULTI_COLUMN_SEPARATOR = ":";
  public static final String COLUMN_KEY_IN_AGGREGATION_INFO = "column";

  public FunctionCallAstNode(String name, String expression) {
    _name = name;
    _expression = expression;
    _isInSelectList = true;
  }

  public boolean equals(Object obj) {
    if (EqualityUtils.isNullOrNotSameClass(this, obj)) {
      return false;
    }

    if (EqualityUtils.isSameReference(this, obj)) {
      return true;
    }

    FunctionCallAstNode functionCallAstNode = (FunctionCallAstNode) obj;
    if (_name.equals(functionCallAstNode.getName()) && _expression.equals(functionCallAstNode.getExpression())) {
      return true;
    } else {
      return false;
    }
  }

  public int hashCode() {
    int hashCode = EqualityUtils.hashCodeOf(_name);
    hashCode = EqualityUtils.hashCodeOf(hashCode, _expression);
    hashCode = EqualityUtils.hashCodeOf(hashCode, _isInSelectList);
    return hashCode;
  }

  public void setIsInSelectList(boolean value) {
    _isInSelectList = value;
  }

  public String getName() {
    return _name;
  }

  public String getExpression() {
    return _expression;
  }

  AggregationInfo buildAggregationInfo() {
    String expression;
    // COUNT aggregation function always works on '*'
    if (_name.equalsIgnoreCase(AggregationFunctionType.COUNT.getName())) {
      expression = "*";
    } else {
      List<? extends AstNode> children = getChildren();
      if (children == null || children.isEmpty()) {
        throw new Pql2CompilationException("Aggregation function expects non-null argument");
      }
      if (!_name.equalsIgnoreCase(AggregationFunctionType.DISTINCT.getName())) {
        if (children.size() != 1) {
          throw new Pql2CompilationException("Aggregation function expects exactly 1 argument as column name");
        } else {
          expression = TransformExpressionTree.getStandardExpression(children.get(0));
        }
      } else {
        // DISTINCT
        if (!Pql2Compiler.ENABLE_DISTINCT) {
          throw new Pql2CompilationException("Support for DISTINCT is currently disabled");
        }
        if (children.size() == 1) {
          // single column DISTINCT query
          // e.g SELECT DISTINCT(col) FROM foo
          if (children.get(0) instanceof StarExpressionAstNode) {
            // DISTINCT(*) is not supported yet. User has to specify each column that should participate in DISTINCT
            throw new Pql2CompilationException(
                "Syntax error: Pinot currently does not support DISTINCT with *. Please specify each column name as argument to DISTINCT function");
          }
          expression = TransformExpressionTree.getStandardExpression(children.get(0));
        } else {
          // multi-column DISTINCT query
          // e.g SELECT DISTINCT(col1, col2) FROM foo
          // we will pass down the column expression to execution code
          // as col1:col2
          Set<String> expressions = new HashSet<>();
          StringBuilder distinctColumnExpr = new StringBuilder();
          int numChildren = children.size();
          for (int i = 0; i < numChildren; ++i) {
            expression = TransformExpressionTree.getStandardExpression(children.get(i));
            if (expressions.add(expression)) {
              // deduplicate the columns
              if (i != 0) {
                distinctColumnExpr.append(DISTINCT_MULTI_COLUMN_SEPARATOR);
              }
              distinctColumnExpr.append(expression);
            }
          }
          expression = distinctColumnExpr.toString();
        }
      }
    }

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(_name);
    aggregationInfo.putToAggregationParams(COLUMN_KEY_IN_AGGREGATION_INFO, expression);
    aggregationInfo.setIsInSelectList(_isInSelectList);

    return aggregationInfo;
  }

  @Override
  public String toString() {
    return "FunctionCallAstNode{" + "_name='" + _name + '\'' + '}';
  }
}
