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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.HavingQueryTree;
import com.linkedin.pinot.pql.parsers.Pql2CompilationException;

public class RegexpLikePredicateAstNode extends PredicateAstNode {
  private static final String SEPERATOR = "\t\t";
  private String _identifier;

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof IdentifierAstNode) {
      if (_identifier == null) {
        IdentifierAstNode node = (IdentifierAstNode) childNode;
        _identifier = node.getName();
      } else {
        throw new Pql2CompilationException("REGEXP_LIKE predicate has more than one identifier.");
      }
    } else if (childNode instanceof FunctionCallAstNode) {
      throw new Pql2CompilationException("REGEXP_LIKE operator can not be called for a function.");
    }
    else {
      super.addChild(childNode);
    }
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    if (_identifier == null) {
      throw new Pql2CompilationException("REGEXP_LIKE predicate has no identifier");
    }

    Set<String> values = new HashSet<>();

    for (AstNode astNode : getChildren()) {
      if (astNode instanceof LiteralAstNode) {
        LiteralAstNode node = (LiteralAstNode) astNode;
        String expr = node.getValueAsString();
        values.add(expr);
      }
    }
    if(values.size() > 1) {
      throw new Pql2CompilationException("Matching more than one regex is NOT supported currently");
    }

    String[] valueArray = values.toArray(new String[values.size()]);
    FilterOperator filterOperator = FilterOperator.REGEXP_LIKE;
    List<String> value = Collections.singletonList(StringUtil.join(SEPERATOR, valueArray));
    return new FilterQueryTree(_identifier, value, filterOperator, null);
  }

  @Override
  public HavingQueryTree buildHavingQueryTree() {
    throw new Pql2CompilationException("REGEXP_LIKE predicate is not supported in HAVING clause.");
  }
}
