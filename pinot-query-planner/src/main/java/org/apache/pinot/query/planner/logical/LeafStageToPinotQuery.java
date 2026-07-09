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
package org.apache.pinot.query.planner.logical;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.commons.lang3.EnumUtils;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.parser.CalciteRexExpressionParser;
import org.apache.pinot.sql.FilterKind;


/**
 * Utility to convert a leaf stage to a {@link PinotQuery}.
 */
public class LeafStageToPinotQuery {
  private LeafStageToPinotQuery() {
  }

  /**
   * Converts a leaf stage root to a {@link PinotQuery}. This method only handles Project, Filter and TableScan nodes.
   * Other node types are ignored since they don't impact routing.
   *
   * @param tableName the name of the table. Needs to be provided separately since it needs TableCache.
   * @param leafStageRoot the root of the leaf stage
   * @param skipFilter whether to skip the filter in the query
   * @return a {@link PinotQuery} representing the leaf stage
   */
  public static PinotQuery createPinotQueryForRouting(String tableName, RelNode leafStageRoot, boolean skipFilter) {
    List<RelNode> bottomToTopNodes = new ArrayList<>();
    accumulateBottomToTop(leafStageRoot, bottomToTopNodes);
    Preconditions.checkState(!bottomToTopNodes.isEmpty() && bottomToTopNodes.get(0) instanceof TableScan,
        "Could not find table scan");
    TableScan tableScan = (TableScan) bottomToTopNodes.get(0);
    PinotQuery pinotQuery = initializePinotQueryForTableScan(tableName, tableScan);
    for (int i = 1; i < bottomToTopNodes.size(); i++) {
      RelNode parentNode = bottomToTopNodes.get(i);
      if (parentNode instanceof Filter) {
        if (!skipFilter) {
          handleFilter((Filter) parentNode, pinotQuery);
        }
      } else if (parentNode instanceof Project) {
        handleProject((Project) parentNode, pinotQuery);
      } else {
        // Leaf boundary: the first node that is neither Filter nor Project (e.g. an un-split DIRECT aggregate)
        // changes the row space -- InputRefs in nodes above it index that node's output, not the scan/project
        // columns -- so folding anything above it (e.g. a HAVING filter) would mis-resolve columns and corrupt the
        // routing filter. Everything below this boundary is a genuine row-level condition; ignore everything above.
        break;
      }
    }
    return pinotQuery;
  }

  private static void accumulateBottomToTop(RelNode root, List<RelNode> parentNodes) {
    Preconditions.checkState(root.getInputs().size() <= 1,
        "Leaf stage nodes should have at most one input, found: %s", root.getInputs().size());
    for (RelNode input : root.getInputs()) {
      accumulateBottomToTop(input, parentNodes);
    }
    parentNodes.add(root);
  }

  private static PinotQuery initializePinotQueryForTableScan(String tableName, TableScan tableScan) {
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.setDataSource(new DataSource());
    pinotQuery.getDataSource().setTableName(tableName);
    pinotQuery.setSelectList(tableScan.getRowType().getFieldNames().stream().map(
        RequestUtils::getIdentifierExpression).collect(Collectors.toList()));
    return pinotQuery;
  }

  private static void handleProject(Project project, PinotQuery pinotQuery) {
    if (project != null) {
      List<RexExpression> rexExpressions = RexExpressionUtils.fromRexNodes(project.getProjects());
      List<Expression> selectList = CalciteRexExpressionParser.convertRexNodes(rexExpressions,
          pinotQuery.getSelectList());
      pinotQuery.setSelectList(selectList);
    }
  }

  private static void handleFilter(Filter filter, PinotQuery pinotQuery) {
    if (filter != null) {
      RexExpression rexExpression = RexExpressionUtils.fromRexNode(filter.getCondition());
      Expression filterExpression = CalciteRexExpressionParser.toExpression(rexExpression,
          pinotQuery.getSelectList());
      setOrAndFilterExpression(pinotQuery, ensureFilterIsFunctionExpression(filterExpression));
    }
  }

  /**
   * Sets the given filter on the query, AND-ing it with any previously set filter (rather than overwriting it, which
   * would silently discard a genuine row-level condition when a leaf stage contains multiple filter nodes).
   */
  public static void setOrAndFilterExpression(PinotQuery pinotQuery, @Nullable Expression filterExpression) {
    if (filterExpression == null) {
      return;
    }
    Expression existingFilter = pinotQuery.getFilterExpression();
    if (existingFilter == null) {
      pinotQuery.setFilterExpression(filterExpression);
      return;
    }
    Function andFunction = new Function(FilterKind.AND.name());
    andFunction.setOperands(new ArrayList<>(List.of(existingFilter, filterExpression)));
    Expression combined = new Expression(ExpressionType.FUNCTION);
    combined.setFunctionCall(andFunction);
    pinotQuery.setFilterExpression(combined);
  }

  /**
   * Ensures the filter expression is a FUNCTION type that segment pruners can process.
   * <p>
   * When the V2 physical optimizer passes filters through Calcite's RelNode tree, certain expression types
   * (REINTERPRET on bare boolean columns, constant-folded SEARCH) produce IDENTIFIER or LITERAL Expression
   * objects with null functionCall. Segment pruners assume all filter expressions are FUNCTION type and NPE
   * on these. This method wraps bare IDENTIFIERs as EQUALS(col, true), converts LITERAL false to an
   * always-false predicate EQUALS(0, 1), and drops LITERAL true expressions (null = no filter).
   * For AND/OR/NOT nodes, operands are recursively fixed.
   * <p>
   * Boolean scalar functions used directly as predicates (e.g. {@code WHERE contains(col, 'foo')}) are wrapped as
   * {@code EQUALS(fn(...), true)}, mirroring the single-stage engine's {@code PredicateComparisonRewriter}: segment
   * pruners resolve filter operators via {@code FilterKind.valueOf} and would otherwise throw on them.
   * <p>
   * Note: This method mutates the input expression's operand lists in-place for AND/OR/NOT nodes.
   * It assumes the expression tree is freshly constructed and not shared across concurrent callers.
   */
  public static Expression ensureFilterIsFunctionExpression(Expression expression) {
    if (expression == null) {
      return null;
    }
    if (expression.getFunctionCall() != null) {
      Function function = expression.getFunctionCall();
      String operator = function.getOperator();
      if (FilterKind.AND.name().equals(operator) || FilterKind.OR.name().equals(operator)) {
        // Recursively fix operands of AND/OR, dropping null (LITERAL) results
        List<Expression> operands = function.getOperands();
        List<Expression> fixedOperands = new ArrayList<>();
        for (Expression operand : operands) {
          Expression fixed = ensureFilterIsFunctionExpression(operand);
          if (fixed != null) {
            fixedOperands.add(fixed);
          }
        }
        if (fixedOperands.isEmpty()) {
          return null;  // All operands were constant — drop entire filter
        }
        if (fixedOperands.size() == 1) {
          return fixedOperands.get(0);  // Single operand — unwrap AND/OR
        }
        function.setOperands(fixedOperands);
      } else if (FilterKind.NOT.name().equals(operator)) {
        // Recursively fix the single operand of NOT
        List<Expression> operands = function.getOperands();
        // NOT is always unary — Calcite validates this at parse time and RexExpressionUtils
        // preserves the operand list unchanged. Guard defensively; drop filter if malformed.
        if (operands.size() != 1) {
          return null;
        }
        Expression fixed = ensureFilterIsFunctionExpression(operands.get(0));
        if (fixed == null) {
          return null;  // NOT(constant) — drop entire filter
        }
        operands.set(0, fixed);
      } else if (!EnumUtils.isValidEnum(FilterKind.class, operator)) {
        // Boolean scalar function used directly as a predicate (e.g. contains(col, 'foo')).
        return wrapAsEqualsTrue(expression);
      }
      return expression;
    }
    if (expression.getIdentifier() != null) {
      // Bare boolean column reference (e.g., "is_active" after REINTERPRET stripped).
      // Wrap as EQUALS(col, true) so pruners see a standard predicate.
      return wrapAsEqualsTrue(expression);
    }
    // LITERAL expression (constant-folded predicate, e.g., TRUE/FALSE).
    // Treat LITERAL false as an always-false predicate that pruners can process so they
    // don't unnecessarily scan everything.
    if (expression.getLiteral() != null && expression.getLiteral().isSetBoolValue()
        && !expression.getLiteral().getBoolValue()) {
      Function alwaysFalse = new Function(FilterKind.EQUALS.name());
      alwaysFalse.setOperands(new ArrayList<>(List.of(
          RequestUtils.getLiteralExpression(0),
          RequestUtils.getLiteralExpression(1))));
      Expression wrapped = new Expression(ExpressionType.FUNCTION);
      wrapped.setFunctionCall(alwaysFalse);
      return wrapped;
    }
    // LITERAL true or non-boolean literals have no filter constraint so we skip pruning
    return null;
  }

  /** Wraps a boolean-valued expression as the standard predicate {@code EQUALS(expression, true)}. */
  private static Expression wrapAsEqualsTrue(Expression expression) {
    Function equalsFunction = new Function(FilterKind.EQUALS.name());
    equalsFunction.setOperands(new ArrayList<>(List.of(expression, RequestUtils.getLiteralExpression(true))));
    Expression wrapped = new Expression(ExpressionType.FUNCTION);
    wrapped.setFunctionCall(equalsFunction);
    return wrapped;
  }
}
