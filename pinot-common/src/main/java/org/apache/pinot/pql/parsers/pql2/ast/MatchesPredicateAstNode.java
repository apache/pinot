package org.apache.pinot.pql.parsers.pql2.ast;

import java.util.ArrayList;
import java.util.List;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.HavingQueryTree;
import org.apache.pinot.pql.parsers.Pql2CompilationException;

public class MatchesPredicateAstNode extends PredicateAstNode {

  private static final String SEPERATOR = "\t\t";
  private String _identifier;

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof IdentifierAstNode) {
      if (_identifier == null) {
        IdentifierAstNode node = (IdentifierAstNode) childNode;
        _identifier = node.getName();
      } else {
        throw new Pql2CompilationException("TEXT_MATCH predicate has more than one identifier.");
      }
    } else if (childNode instanceof FunctionCallAstNode) {
      throw new Pql2CompilationException("TEXT_MATCH operator can not be called for a function.");
    } else {
      super.addChild(childNode);
    }
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    if (_identifier == null) {
      throw new Pql2CompilationException("TEXT_MATCH predicate has no identifier");
    }

    List<String> values = new ArrayList<>();

    for (AstNode astNode : getChildren()) {
      if (astNode instanceof LiteralAstNode) {
        LiteralAstNode node = (LiteralAstNode) astNode;
        String expr = node.getValueAsString();
        values.add(expr);
      }
    }
    if (values.size() != 2) {
      throw new Pql2CompilationException(
          "TEXT_MATCH expects columnName, 'queryString', 'queryOption'");
    }

    FilterOperator filterOperator = FilterOperator.TEXT_MATCH;
    return new FilterQueryTree(_identifier, values, filterOperator, null);
  }

  @Override
  public HavingQueryTree buildHavingQueryTree() {
    throw new Pql2CompilationException("TEXT_MATCH predicate is not supported in HAVING clause.");
  }

}
