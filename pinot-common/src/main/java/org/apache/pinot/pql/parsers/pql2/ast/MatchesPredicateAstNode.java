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

import java.util.ArrayList;
import java.util.List;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.HavingQueryTree;
import org.apache.pinot.pql.parsers.Pql2CompilationException;

public class MatchesPredicateAstNode extends PredicateAstNode {

  private String _identifier;

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof IdentifierAstNode) {
      if (_identifier == null) {
        IdentifierAstNode node = (IdentifierAstNode) childNode;
        _identifier = node.getName();
      } else {
        throw new Pql2CompilationException("MATCHES predicate has more than one identifier.");
      }
    } else if (childNode instanceof FunctionCallAstNode) {
      throw new Pql2CompilationException("MATCHES operator can not be called for a function.");
    } else {
      super.addChild(childNode);
    }
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    if (_identifier == null) {
      throw new Pql2CompilationException("MATCHES predicate has no identifier");
    }

    List<String> values = new ArrayList<>();

    for (AstNode astNode : getChildren()) {
      if (astNode instanceof LiteralAstNode) {
        LiteralAstNode node = (LiteralAstNode) astNode;
        String expr = node.getValueAsString();
        values.add(expr);
      }
    }
    if (values.size() != 1 && values.size() != 2) {
      throw new Pql2CompilationException(
          "MATCHES expects columnName, 'queryString', 'queryOptions' (optional)");
    }

    FilterOperator filterOperator = FilterOperator.MATCHES;
    return new FilterQueryTree(_identifier, values, filterOperator, null);
  }

  @Override
  public HavingQueryTree buildHavingQueryTree() {
    throw new Pql2CompilationException("MATCHES predicate is not supported in HAVING clause.");
  }

}
