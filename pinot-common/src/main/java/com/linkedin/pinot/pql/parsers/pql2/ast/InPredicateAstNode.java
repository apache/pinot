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

import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.pql.parsers.Pql2CompilationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * AST node for IN predicates.
 */
public class InPredicateAstNode extends PredicateAstNode {
  private static final String DELIMITER = "\t\t";
  private final boolean _isNotInClause;
  private String _identifier;
  private final boolean _splitInClause;

  public InPredicateAstNode(boolean isNotInClause, boolean splitInClause) {
    _isNotInClause = isNotInClause;
    _splitInClause = splitInClause;
  }

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof IdentifierAstNode) {
      if (_identifier == null) {
        IdentifierAstNode node = (IdentifierAstNode) childNode;
        _identifier = node.getName();
      } else {
        throw new Pql2CompilationException("IN predicate has more than one identifier.");
      }
    } else {
      super.addChild(childNode);
    }
  }

  public String getIdentifier() {
    return _identifier;
  }

  @Override
  public String toString() {
    return "InPredicateAstNode{" + "_identifier='" + _identifier + '\'' + '}';
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    if (_identifier == null) {
      throw new Pql2CompilationException("IN predicate has no identifier");
    }

    Set<String> values = new HashSet<>();

    for (AstNode astNode : getChildren()) {
      if (astNode instanceof LiteralAstNode) {
        LiteralAstNode node = (LiteralAstNode) astNode;
        values.add(node.getValueAsString());
      }
    }

    FilterOperator filterOperator;
    if (_isNotInClause) {
      filterOperator = FilterOperator.NOT_IN;
    } else {
      filterOperator = FilterOperator.IN;
    }

    if (_splitInClause) {
      return new FilterQueryTree(_identifier, new ArrayList<>(values), filterOperator, null);
    } else {
      String[] valueArray = values.toArray(new String[values.size()]);
      return new FilterQueryTree(_identifier, Collections.singletonList(StringUtil.join(DELIMITER, valueArray)),
          filterOperator, null);
    }
  }
}
