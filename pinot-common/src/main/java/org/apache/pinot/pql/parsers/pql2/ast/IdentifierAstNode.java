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

/**
 * AST node for identifiers (column names).
 */
public class IdentifierAstNode extends BaseAstNode {
  private String _expression;
  private String _name;

  public IdentifierAstNode(String expression) {
    _expression = expression;
    if (expression.charAt(0) == '`' && expression.charAt(expression.length() - 1) == '`') {
      _name = expression.substring(1, expression.length() - 1);
    } else {
      _name = expression;
    }
  }

  public String getName() {
    return _name;
  }

  public String getExpression() {
    return _expression;
  }

  @Override
  public String toString() {
    return "IdentifierAstNode{" + "_name='" + _name + '\'' + '}';
  }
}
