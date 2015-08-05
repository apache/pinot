/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.pql.parsers.Pql2CompilationException;
import java.util.Collections;


/**
 * AST node for comparison predicates.
 */
public class ComparisonPredicateAstNode extends PredicateAstNode {
  private String _operand;
  private IdentifierAstNode _identifier;
  private LiteralAstNode _literal;

  public ComparisonPredicateAstNode(String operand) {
    _operand = operand;
  }

  public String getOperand() {
    return _operand;
  }

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof IdentifierAstNode) {
      if (_identifier == null) {
        _identifier = (IdentifierAstNode) childNode;
      } else {
        throw new Pql2CompilationException("Comparison between two columns is not supported.");
      }
    } else if (childNode instanceof LiteralAstNode) {
      LiteralAstNode node = (LiteralAstNode) childNode;
      if (_literal == null) {
        _literal = node;
      } else {
        throw new Pql2CompilationException("Comparison between two constants is not supported.");
      }
    }

    // Add the child nonetheless
    super.addChild(childNode);
  }

  @Override
  public String toString() {
    return "ComparisonPredicateAstNode{" +
        "_operand='" + _operand + '\'' +
        '}';
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    if ("=".equals(_operand)) {
      if (_identifier != null && _literal != null) {
        return new FilterQueryTree(_identifier.getName(), Collections.singletonList(_literal.getValueAsString()),
            FilterOperator.EQUALITY, null);
      } else {
        throw new Pql2CompilationException("Comparison is not between a column and a constant");
      }
    } else if ("<>".equals(_operand)) {
      if (_identifier != null && _literal != null) {
        return new FilterQueryTree(_identifier.getName(), Collections.singletonList(_literal.getValueAsString()),
            FilterOperator.NOT, null);
      } else {
        throw new Pql2CompilationException("Comparison is not between a column and a constant");
      }
    } else {
      boolean identifierIsOnLeft = true;
      if (getChildren().get(0) instanceof LiteralAstNode) {
        identifierIsOnLeft = false;
      }

      String comparison;
      String value = _literal.getValueAsString();

      if ("<".equals(_operand)) {
        if (identifierIsOnLeft) {
          comparison = "(*\t\t" + value + ")";
        } else {
          comparison = "(" + value + "\t\t*)";
        }
      } else if ("<=".equals(_operand)) {
        if (identifierIsOnLeft) {
          comparison = "(*\t\t" + value + "]";
        } else {
          comparison = "[" + value + "\t\t*)";
        }
      } else if (">".equals(_operand)) {
        if (identifierIsOnLeft) {
          comparison = "(" + value + "\t\t*)";
        } else {
          comparison = "(*\t\t" + value + "*)";
        }
      } else if (">=".equals(_operand)) {
        if (identifierIsOnLeft) {
          comparison = "[" + value + "\t\t*)";
        } else {
          comparison = "(*\t\t" + value + "*)";
        }
      } else {
        throw new Pql2CompilationException("");
      }

      return new FilterQueryTree(_identifier.getName(), Collections.singletonList(comparison), FilterOperator.RANGE, null);
    }
  }
}
