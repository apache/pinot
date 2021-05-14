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
package org.apache.pinot.common.utils.request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.pql.parsers.pql2.ast.AstNode;
import org.apache.pinot.pql.parsers.pql2.ast.FloatingPointLiteralAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.IdentifierAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.IntegerLiteralAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.LiteralAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.PredicateAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.StringLiteralAstNode;
import org.apache.pinot.spi.utils.BytesUtils;


public class RequestUtils {
  private RequestUtils() {
  }

  /**
   * Generates thrift compliant filterQuery and populate it in the broker request
   * @param filterQueryTree
   * @param request
   */
  public static void generateFilterFromTree(FilterQueryTree filterQueryTree, BrokerRequest request) {
    Map<Integer, FilterQuery> filterQueryMap = new HashMap<>();
    MutableInt currentId = new MutableInt(0);
    FilterQuery root = traverseFilterQueryAndPopulateMap(filterQueryTree, filterQueryMap, currentId);
    filterQueryMap.put(root.getId(), root);
    request.setFilterQuery(root);
    FilterQueryMap mp = new FilterQueryMap();
    mp.setFilterQueryMap(filterQueryMap);
    request.setFilterSubQueryMap(mp);
  }

  /**
   * Creates Expression from identifier
   * @param identifier
   * @return
   */
  public static Expression createIdentifierExpression(String identifier) {
    Expression expression = new Expression(ExpressionType.IDENTIFIER);
    expression.setIdentifier(new Identifier(identifier));
    return expression;
  }

  /**
   * Creates Literal Expression from LiteralAstNode.
   * @param value
   * @return
   */
  public static Expression createLiteralExpression(LiteralAstNode value) {
    Expression expression = new Expression(ExpressionType.LITERAL);
    Literal literal = new Literal();
    if (value instanceof StringLiteralAstNode) {
      literal.setStringValue(((StringLiteralAstNode) value).getText());
    }
    if (value instanceof IntegerLiteralAstNode) {
      literal.setLongValue(((IntegerLiteralAstNode) value).getValue());
    }
    if (value instanceof FloatingPointLiteralAstNode) {
      literal.setDoubleValue(((FloatingPointLiteralAstNode) value).getValue());
    }
    expression.setLiteral(literal);
    return expression;
  }

  public static Expression getIdentifierExpression(String identifier) {
    Expression expression = new Expression(ExpressionType.IDENTIFIER);
    expression.setIdentifier(new Identifier(identifier));
    return expression;
  }

  public static Expression getLiteralExpression(SqlLiteral node) {
    Expression expression = new Expression(ExpressionType.LITERAL);
    Literal literal = new Literal();
    if (node instanceof SqlNumericLiteral) {
      // Mitigate calcite NPE bug, we need to check if SqlNumericLiteral.getScale() is null before calling
      // SqlNumericLiteral.isInteger(). TODO: Undo this fix once a Calcite release that contains CALCITE-4199 is
      // available and Pinot has been upgraded to use such a release.
      SqlNumericLiteral sqlNumericLiteral = (SqlNumericLiteral) node;
      if (sqlNumericLiteral.getScale() != null && sqlNumericLiteral.isInteger()) {
        literal.setLongValue(node.bigDecimalValue().longValue());
      } else {
        literal.setDoubleValue(node.bigDecimalValue().doubleValue());
      }
    } else {
      literal.setStringValue(StringUtils.replace(node.toValue(), "''", "'"));
    }
    expression.setLiteral(literal);
    return expression;
  }

  public static Expression createNewLiteralExpression() {
    Expression expression = new Expression(ExpressionType.LITERAL);
    Literal literal = new Literal();
    expression.setLiteral(literal);
    return expression;
  }

  public static Expression getLiteralExpression(boolean value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setBoolValue(value);
    return expression;
  }

  public static Expression getLiteralExpression(long value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setLongValue(value);
    return expression;
  }

  public static Expression getLiteralExpression(double value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setDoubleValue(value);
    return expression;
  }

  public static Expression getLiteralExpression(String value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setStringValue(value);
    return expression;
  }

  public static Expression getLiteralExpression(byte[] value) {
    Expression expression = createNewLiteralExpression();
    expression.getLiteral().setStringValue(BytesUtils.toHexString(value));
    return expression;
  }

  public static Expression getLiteralExpression(Object object) {
    if (object instanceof Integer || object instanceof Long) {
      return RequestUtils.getLiteralExpression(((Number) object).longValue());
    }
    if (object instanceof Float || object instanceof Double) {
      return RequestUtils.getLiteralExpression(((Number) object).doubleValue());
    }
    if (object instanceof byte[]) {
      return RequestUtils.getLiteralExpression((byte[]) object);
    }
    return RequestUtils.getLiteralExpression(object.toString());
  }

  public static Expression getFunctionExpression(String operator) {
    Expression expression = new Expression(ExpressionType.FUNCTION);
    Function function = new Function(operator);
    expression.setFunctionCall(function);
    return expression;
  }

  private static FilterQuery traverseFilterQueryAndPopulateMap(FilterQueryTree tree,
      Map<Integer, FilterQuery> filterQueryMap, MutableInt currentId) {
    int currentNodeId = currentId.intValue();
    currentId.increment();

    final List<Integer> f = new ArrayList<>();
    if (null != tree.getChildren()) {
      for (final FilterQueryTree c : tree.getChildren()) {
        final FilterQuery q = traverseFilterQueryAndPopulateMap(c, filterQueryMap, currentId);
        int childNodeId = q.getId();
        f.add(childNodeId);
        filterQueryMap.put(childNodeId, q);
      }
    }

    FilterQuery query = new FilterQuery();
    query.setColumn(tree.getColumn());
    query.setId(currentNodeId);
    query.setNestedFilterQueryIds(f);
    query.setOperator(tree.getOperator());
    query.setValue(tree.getValue());
    return query;
  }

  /**
   * Generate FilterQueryTree from Broker Request
   * @param request Broker Request
   * @return
   */
  public static FilterQueryTree generateFilterQueryTree(BrokerRequest request) {
    FilterQueryTree root = null;

    FilterQuery q = request.getFilterQuery();

    if (null != q && null != request.getFilterSubQueryMap()) {
      root = buildFilterQuery(q.getId(), request.getFilterSubQueryMap().getFilterQueryMap());
    }

    return root;
  }

  public static FilterQueryTree buildFilterQuery(Integer id, Map<Integer, FilterQuery> queryMap) {
    FilterQuery q = queryMap.get(id);

    List<Integer> children = q.getNestedFilterQueryIds();

    List<FilterQueryTree> c = null;
    if (null != children && !children.isEmpty()) {
      c = new ArrayList<>();
      for (final Integer i : children) {
        final FilterQueryTree t = buildFilterQuery(i, queryMap);
        c.add(t);
      }
    }

    return new FilterQueryTree(q.getColumn(), q.getValue(), q.getOperator(), c);
  }

  /**
   * Returns the expression from a given {@link AstNode}, which can be one of the following:
   * <ul>
   *   <li> {@link FunctionCallAstNode}</li>
   *   <li> {@link LiteralAstNode}</li>
   *   <li> {@link PredicateAstNode}</li>
   * </ul>
   *
   * @return Expression
   */
  public static Expression getExpression(AstNode astNode) {
    if (astNode instanceof IdentifierAstNode) {
      // Column name
      return createIdentifierExpression(((IdentifierAstNode) astNode).getName());
    } else if (astNode instanceof FunctionCallAstNode) {
      // Function expression
      Expression expression = getFunctionExpression(((FunctionCallAstNode) astNode).getName());
      Function func = expression.getFunctionCall();
      final List<? extends AstNode> operandsAstNodes = astNode.getChildren();
      if (operandsAstNodes != null) {
        for (AstNode child : operandsAstNodes) {
          func.addToOperands(getExpression(child));
        }
      }
      return expression;
    } else if (astNode instanceof LiteralAstNode) {
      return createLiteralExpression(((LiteralAstNode) astNode));
    } else if (astNode instanceof PredicateAstNode) {
      return ((PredicateAstNode) astNode).buildFilterExpression();
    } else {
      throw new IllegalStateException("Cannot get expression from " + astNode.getClass().getSimpleName());
    }
  }

  public static String prettyPrint(Expression expression) {
    if (expression == null) {
      return "null";
    }
    if (expression.getIdentifier() != null) {
      return expression.getIdentifier().getName();
    }
    if (expression.getLiteral() != null) {
      if (expression.getLiteral().isSetLongValue()) {
        return Long.toString(expression.getLiteral().getLongValue());
      }
    }
    if (expression.getFunctionCall() != null) {
      String res = expression.getFunctionCall().getOperator() + "(";
      boolean isFirstParam = true;
      for (Expression operand : expression.getFunctionCall().getOperands()) {
        if (!isFirstParam) {
          res += ", ";
        } else {
          isFirstParam = false;
        }
        res += prettyPrint(operand);
      }
      res += ")";
      return res;
    }
    return null;
  }
}
