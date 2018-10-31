/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.pql.parsers.pql2.ast;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.pql.parsers.Pql2CompilationException;


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
    String identifier = null;
    for (AstNode astNode : getChildren()) {
      if (astNode instanceof IdentifierAstNode) {
        IdentifierAstNode node = (IdentifierAstNode) astNode;
        if (identifier == null) {
          identifier = node.getName();
        } else {
          throw new Pql2CompilationException("More than one identifier specified to an aggregation function");
        }
      } else if (astNode instanceof StarExpressionAstNode) {
        identifier = "*";
      } else if (astNode instanceof StringLiteralAstNode) {
        // Pinot quirk: Passing a string as an aggregation function is probably a column name
        StringLiteralAstNode node = (StringLiteralAstNode) astNode;
        identifier = node.getText();
      } else {
        throw new Pql2CompilationException("Child node of aggregation function is not an identifier, star or string literal.");
      }
    }

    String function = _name;

    // Pinot quirk: count is always count(*) no matter what
    if ("count".equalsIgnoreCase(function)) {
      function = "count";
      identifier = "*";
    }

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(function);
    aggregationInfo.putToAggregationParams("column", identifier);
    aggregationInfo.setIsInSelectList(_isInSelectList);

    return aggregationInfo;
  }

  @Override
  public String toString() {
    return "FunctionCallAstNode{" + "_name='" + _name + '\'' + '}';
  }
}
