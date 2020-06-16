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
package org.apache.pinot.pql.parsers;

import java.util.Stack;
import org.antlr.v4.runtime.misc.NotNull;
import org.apache.pinot.pql.parsers.pql2.ast.AstNode;
import org.apache.pinot.pql.parsers.pql2.ast.BetweenPredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.BinaryMathOpAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.BooleanOperatorAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.ComparisonPredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.ExpressionParenthesisGroupAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.FloatingPointLiteralAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.GroupByAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.IdentifierAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.InPredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.IntegerLiteralAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.IsNullPredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.LimitAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.OptionAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.OptionsAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.OrderByAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.OrderByExpressionAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.OutputColumnAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.OutputColumnListAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.PredicateListAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.PredicateParenthesisGroupAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.RegexpLikePredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.SelectAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.StarColumnListAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.StarExpressionAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.StringLiteralAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.TableNameAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.TextMatchPredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.TopAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.WhereAstNode;


/**
 * PQL 2 parse tree listener that generates an abstract syntax tree.
 */
public class Pql2AstListener extends PQL2BaseListener {
  Stack<AstNode> _nodeStack = new Stack<>();
  AstNode _rootNode = null;
  private String _expression;

  public Pql2AstListener(String expression) {
    _expression = expression; // Original expression being parsed.
  }

  private void pushNode(AstNode node) {
    if (_rootNode == null) {
      _rootNode = node;
    }

    AstNode parentNode = null;

    if (!_nodeStack.isEmpty()) {
      parentNode = _nodeStack.peek();
    }

    if (parentNode != null) {
      parentNode.addChild(node);
    }

    node.setParent(parentNode);

    _nodeStack.push(node);
  }

  private void popNode() {
    AstNode topNode = _nodeStack.pop();

    topNode.doneProcessingChildren();
  }

  public AstNode getRootNode() {
    return _rootNode;
  }

  @Override
  public void enterSelect(@NotNull PQL2Parser.SelectContext ctx) {
    pushNode(new SelectAstNode());
  }

  @Override
  public void exitSelect(@NotNull PQL2Parser.SelectContext ctx) {
    popNode();
  }

  @Override
  public void enterTableName(@NotNull PQL2Parser.TableNameContext ctx) {
    pushNode(new TableNameAstNode(ctx.getText()));
  }

  @Override
  public void exitTableName(@NotNull PQL2Parser.TableNameContext ctx) {
    popNode();
  }

  @Override
  public void enterStarColumnList(@NotNull PQL2Parser.StarColumnListContext ctx) {
    pushNode(new StarColumnListAstNode());
  }

  @Override
  public void exitStarColumnList(@NotNull PQL2Parser.StarColumnListContext ctx) {
    popNode();
  }

  @Override
  public void enterOutputColumnList(@NotNull PQL2Parser.OutputColumnListContext ctx) {
    pushNode(new OutputColumnListAstNode());
  }

  @Override
  public void exitOutputColumnList(@NotNull PQL2Parser.OutputColumnListContext ctx) {
    popNode();
  }

  @Override
  public void enterIsPredicate(@NotNull PQL2Parser.IsPredicateContext ctx) {
    pushNode(new IsNullPredicateAstNode(ctx.isClause().NOT() != null));
  }

  @Override
  public void exitIsPredicate(@NotNull PQL2Parser.IsPredicateContext ctx) {
    popNode();
  }

  @Override
  public void enterPredicateParenthesisGroup(@NotNull PQL2Parser.PredicateParenthesisGroupContext ctx) {
    pushNode(new PredicateParenthesisGroupAstNode());
  }

  @Override
  public void exitPredicateParenthesisGroup(@NotNull PQL2Parser.PredicateParenthesisGroupContext ctx) {
    popNode();
  }

  @Override
  public void enterComparisonPredicate(@NotNull PQL2Parser.ComparisonPredicateContext ctx) {
    pushNode(new ComparisonPredicateAstNode(ctx.getChild(0).getChild(1).getText()));
  }

  @Override
  public void exitComparisonPredicate(@NotNull PQL2Parser.ComparisonPredicateContext ctx) {
    popNode();
  }

  @Override
  public void enterExpressionParenthesisGroup(@NotNull PQL2Parser.ExpressionParenthesisGroupContext ctx) {
    pushNode(new ExpressionParenthesisGroupAstNode());
  }

  @Override
  public void exitExpressionParenthesisGroup(@NotNull PQL2Parser.ExpressionParenthesisGroupContext ctx) {
    popNode();
  }

  @Override
  public void enterOutputColumn(@NotNull PQL2Parser.OutputColumnContext ctx) {
    pushNode(new OutputColumnAstNode());
  }

  @Override
  public void exitOutputColumn(@NotNull PQL2Parser.OutputColumnContext ctx) {
    popNode();
  }

  @Override
  public void enterIdentifier(@NotNull PQL2Parser.IdentifierContext ctx) {
    pushNode(new IdentifierAstNode(ctx.getText()));
  }

  @Override
  public void exitIdentifier(@NotNull PQL2Parser.IdentifierContext ctx) {
    popNode();
  }

  @Override
  public void enterStarExpression(@NotNull PQL2Parser.StarExpressionContext ctx) {
    pushNode(new StarExpressionAstNode());
  }

  @Override
  public void exitStarExpression(@NotNull PQL2Parser.StarExpressionContext ctx) {
    popNode();
  }

  @Override
  public void enterFunctionCall(@NotNull PQL2Parser.FunctionCallContext ctx) {
    String expression = _expression.substring(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex() + 1);
    pushNode(new FunctionCallAstNode(ctx.getChild(0).getText(), expression));
  }

  @Override
  public void exitFunctionCall(@NotNull PQL2Parser.FunctionCallContext ctx) {
    popNode();
  }

  @Override
  public void enterIntegerLiteral(@NotNull PQL2Parser.IntegerLiteralContext ctx) {
    pushNode(new IntegerLiteralAstNode(Long.parseLong(ctx.getText())));
  }

  @Override
  public void exitIntegerLiteral(@NotNull PQL2Parser.IntegerLiteralContext ctx) {
    popNode();
  }

  @Override
  public void enterOrderBy(@NotNull PQL2Parser.OrderByContext ctx) {
    pushNode(new OrderByAstNode());
  }

  @Override
  public void exitOrderBy(@NotNull PQL2Parser.OrderByContext ctx) {
    popNode();
  }

  @Override
  public void enterGroupBy(@NotNull PQL2Parser.GroupByContext ctx) {
    pushNode(new GroupByAstNode());
  }

  @Override
  public void exitGroupBy(@NotNull PQL2Parser.GroupByContext ctx) {
    popNode();
  }

  @Override
  public void enterBetweenPredicate(@NotNull PQL2Parser.BetweenPredicateContext ctx) {
    pushNode(new BetweenPredicateAstNode());
  }

  @Override
  public void exitBetweenPredicate(@NotNull PQL2Parser.BetweenPredicateContext ctx) {
    popNode();
  }

  @Override
  public void enterBinaryMathOp(@NotNull PQL2Parser.BinaryMathOpContext ctx) {
    pushNode(new BinaryMathOpAstNode(ctx.getChild(1).getText()));
  }

  @Override
  public void exitBinaryMathOp(@NotNull PQL2Parser.BinaryMathOpContext ctx) {
    popNode();
  }

  @Override
  public void enterInPredicate(@NotNull PQL2Parser.InPredicateContext ctx) {
    boolean isNotInClause = false;
    if ("not".equalsIgnoreCase(ctx.getChild(0).getChild(1).getText())) {
      isNotInClause = true;
    }
    pushNode(new InPredicateAstNode(isNotInClause));
  }

  @Override
  public void exitInPredicate(@NotNull PQL2Parser.InPredicateContext ctx) {
    popNode();
  }

  @Override
  public void enterRegexpLikePredicate(@NotNull PQL2Parser.RegexpLikePredicateContext ctx) {
    pushNode(new RegexpLikePredicateAstNode());
  }

  @Override
  public void exitRegexpLikePredicate(@NotNull PQL2Parser.RegexpLikePredicateContext ctx) {
    popNode();
  }

  @Override
  public void enterHaving(@NotNull PQL2Parser.HavingContext ctx) {
    throw new UnsupportedOperationException("HAVING clause is not supported");
  }

  @Override
  public void exitHaving(@NotNull PQL2Parser.HavingContext ctx) {
    popNode();
  }

  @Override
  public void enterStringLiteral(@NotNull PQL2Parser.StringLiteralContext ctx) {
    String text = ctx.getText();
    int textLength = text.length();

    // String literals can be either 'foo' or "bar". We support quoting by doubling the beginning character, so that
    // users can write 'Martha''s Vineyard' or """foo"""
    String literalWithoutQuotes = text.substring(1, textLength - 1);
    if (text.charAt(0) == '\'') {
      if (literalWithoutQuotes.contains("''")) {
        literalWithoutQuotes = literalWithoutQuotes.replace("''", "'");
      }

      pushNode(new StringLiteralAstNode(literalWithoutQuotes));
    } else if (text.charAt(0) == '"') {
      if (literalWithoutQuotes.contains("\"\"")) {
        literalWithoutQuotes = literalWithoutQuotes.replace("\"\"", "\"");
      }

      pushNode(new StringLiteralAstNode(literalWithoutQuotes));
    } else {
      throw new Pql2CompilationException("String literal does not start with either '  or \"");
    }
  }

  @Override
  public void exitStringLiteral(@NotNull PQL2Parser.StringLiteralContext ctx) {
    popNode();
  }

  @Override
  public void enterFloatingPointLiteral(@NotNull PQL2Parser.FloatingPointLiteralContext ctx) {
    pushNode(new FloatingPointLiteralAstNode(Double.valueOf(ctx.getText())));
  }

  @Override
  public void exitFloatingPointLiteral(@NotNull PQL2Parser.FloatingPointLiteralContext ctx) {
    popNode();
  }

  @Override
  public void enterLimit(@NotNull PQL2Parser.LimitContext ctx) {
    // Can either be LIMIT <maxRows> or LIMIT <offset>, <maxRows> (the second is a MySQL syntax extension)
    if (ctx.getChild(0).getChildCount() == 2) {
      pushNode(new LimitAstNode(Integer.parseInt(ctx.getChild(0).getChild(1).getText())));
    } else {
      pushNode(new LimitAstNode(Integer.parseInt(ctx.getChild(0).getChild(3).getText()),
          Integer.parseInt(ctx.getChild(0).getChild(1).getText())));
    }
  }

  @Override
  public void exitLimit(@NotNull PQL2Parser.LimitContext ctx) {
    popNode();
  }

  @Override
  public void enterWhere(@NotNull PQL2Parser.WhereContext ctx) {
    pushNode(new WhereAstNode());
  }

  @Override
  public void exitWhere(@NotNull PQL2Parser.WhereContext ctx) {
    popNode();
  }

  @Override
  public void enterTopClause(@NotNull PQL2Parser.TopClauseContext ctx) {
    pushNode(new TopAstNode(Integer.parseInt(ctx.getChild(1).getText())));
  }

  @Override
  public void exitTopClause(@NotNull PQL2Parser.TopClauseContext ctx) {
    popNode();
  }

  @Override
  public void enterOrderByExpression(@NotNull PQL2Parser.OrderByExpressionContext ctx) {
    if (ctx.getChildCount() == 1) {
      pushNode(new OrderByExpressionAstNode(ctx.getChild(0).getText(), OrderByAstNode.ASCENDING_ORDER));
    } else {
      pushNode(new OrderByExpressionAstNode(ctx.getChild(0).getText(), ctx.getChild(1).getText()));
    }
  }

  @Override
  public void exitOrderByExpression(@NotNull PQL2Parser.OrderByExpressionContext ctx) {
    popNode();
  }

  @Override
  public void enterPredicateList(@NotNull PQL2Parser.PredicateListContext ctx) {
    pushNode(new PredicateListAstNode());
  }

  @Override
  public void exitPredicateList(@NotNull PQL2Parser.PredicateListContext ctx) {
    popNode();
  }

  @Override
  public void enterBooleanOperator(@NotNull PQL2Parser.BooleanOperatorContext ctx) {
    pushNode(BooleanOperatorAstNode.valueOf(ctx.getText().toUpperCase()));
  }

  @Override
  public void exitBooleanOperator(@NotNull PQL2Parser.BooleanOperatorContext ctx) {
    popNode();
  }

  @Override
  public void enterOption(PQL2Parser.OptionContext ctx) {
    pushNode(new OptionAstNode());
  }

  @Override
  public void exitOption(PQL2Parser.OptionContext ctx) {
    popNode();
  }

  @Override
  public void enterOptions(PQL2Parser.OptionsContext ctx) {
    pushNode(new OptionsAstNode());
  }

  @Override
  public void exitOptions(PQL2Parser.OptionsContext ctx) {
    popNode();
  }

  @Override
  public void enterTextMatchPredicate(@NotNull PQL2Parser.TextMatchPredicateContext ctx) {
    pushNode(new TextMatchPredicateAstNode());
  }

  @Override
  public void exitTextMatchPredicate(@NotNull PQL2Parser.TextMatchPredicateContext ctx) {
    popNode();
  }
}
