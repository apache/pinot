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
 * AST node for an expression in an ORDER BY clause.
 */
public class OrderByExpressionAstNode extends BaseAstNode {
  private String _column;
  private String _ordering;

  public OrderByExpressionAstNode(String column, String ordering) {
    _column = column;
    _ordering = ordering;
  }

  public String getColumn() {
    return _column;
  }

  public String getOrdering() {
    return _ordering;
  }

  @Override
  public String toString() {
    return "OrderByExpressionAstNode{" + "_column='" + _column + '\'' + ", _ordering='" + _ordering + '\'' + '}';
  }
}
