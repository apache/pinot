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

import java.util.List;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.EqualityUtils;
import org.apache.pinot.pql.parsers.Pql2CompilationException;


/**
 * AST node for function calls, such as count(foo).
 */
public class FunctionCallAstNode extends BaseAstNode {
  private final String _name;
  private String _expression;
  private boolean _isInSelectList;

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

  public AggregationInfo buildAggregationInfo() {
    String expression;
    // COUNT aggregation function always works on '*'
    if (_name.equalsIgnoreCase("count")) {
      expression = "*";
    } else {
      List<? extends AstNode> children = getChildren();
      if (children == null || children.size() != 1) {
        throw new Pql2CompilationException("Aggregation function expects exact 1 argument");
      }
      expression = TransformExpressionTree.getStandardExpression(children.get(0));
    }

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(_name);
    aggregationInfo.putToAggregationParams("column", expression);
    aggregationInfo.setIsInSelectList(_isInSelectList);

    return aggregationInfo;
  }

  @Override
  public String toString() {
    return "FunctionCallAstNode{" + "_name='" + _name + '\'' + '}';
  }
}
