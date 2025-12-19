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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;


/**
 * QueryFingerprintVisitor traverses the Calcite SqlNode AST and produces a normalized query fingerprint.
 * Implementation is based on Calcite 1.40.0 version. It may change in future versions of Calcite.
 *
 * <p>
 * <ul>
 *   <li>All data literals are replaced with dynamic parameters (?)</li>
 *   <li>IN/NOT IN clauses with multiple constants are squashed to a single parameter.</li>
 *   <li>EXPLAIN PLAN FOR is NOT preserved.</li>
 *   <li>Symbolic keywords (DISTINCT, ASC, DESC, etc.) and NULL literals are preserved.</li>
 *   <li>Hints are preserved.</li>
 *   <li>Window functions: window specification (operand 1) are preserved.</li>
 * </ul>
 * </p>
 *
 * <p><b>Note:</b> This visitor maintains internal state (dynamic parameter index) that is not reset between visits.
 * A new instance should be created for each query fingerprint.</p>
 */
public class QueryFingerprintVisitor extends SqlShuttle {
  // SqlSelect operand index for hints, see {@link org.apache.calcite.sql.SqlSelect}.
  private static final int SQLSELECT_HINTS_OPERAND_INDEX = 11;

  // SqlJoin operand indices, see {@link org.apache.calcite.sql.SqlJoin}.
  private static final int SQLJOIN_NATURAL_OPERAND_INDEX = 1;
  private static final int SQLJOIN_JOIN_TYPE_OPERAND_INDEX = 2;
  private static final int SQLJOIN_CONDITION_TYPE_OPERAND_INDEX = 4;

  private final boolean _squashNullInList;
  private int _dynamicParamIndex;

  public QueryFingerprintVisitor() {
    this(false);
  }

  /**
   * Creates a QueryFingerprintVisitor with configurable NULL handling in IN-lists.
   *
   * @param squashNullInList if true, NULL values in IN/NOT IN lists are treated as data literals
   *                         and squashed together with other values into a single placeholder.
   */
  public QueryFingerprintVisitor(boolean squashNullInList) {
    _squashNullInList = squashNullInList;
    _dynamicParamIndex = 0;
  }

  @Override
  public SqlNode visit(SqlLiteral literal) {
    if (shouldPreserveLiteral(literal)) {
      return literal;
    }
    // Replace data literals (numbers, strings, dates, etc.) with dynamic parameters
    return new SqlDynamicParam(_dynamicParamIndex++, literal.getParserPosition());
  }

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlNode result = call;
    switch (call.getKind()) {
      case SELECT:
        result = visitSelect((SqlSelect) call);
        break;
      case JOIN:
        result = visitJoin((SqlJoin) call);
        break;
      case WITH:
        result = visitWith((SqlWith) call);
        break;
      case WITH_ITEM:
        result = visitWithItem((SqlWithItem) call);
        break;
      // EXPLAIN PLAN FOR is NOT preserved.
      case EXPLAIN:
        result = call.getOperandList().get(0).accept(this);
        break;
      case CASE:
        result = visitCase((SqlCase) call);
        break;
      case ORDER_BY:
        result = visitOrderBy((SqlOrderBy) call);
        break;
      case OVER:
        // Window functions: only visit the aggregate function (operand 0).
        // Skip the window specification (operand 1) due to its complex structure
        // with ORDER BY and frame clauses. This means literals in PARTITION BY,
        // ORDER BY, and window frames are preserved rather than replaced.
        call.setOperand(0, call.getOperandList().get(0).accept(this));
        result = call;
        break;
      case IN:
      case NOT_IN:
        result = visitIn(call);
        break;
      default:
        return super.visit(call);
    }
    return result;
  }

  @Nullable
  private SqlNode visitCase(SqlCase sqlCase) {
    List<SqlNode> newOperands = new ArrayList<>();
    for (SqlNode child : sqlCase.getOperandList()) {
      if (child == null) {
        newOperands.add(null);
        continue;
      }
      newOperands.add(child.accept(this));
    }
    int i = 0;
    for (SqlNode operand : newOperands) {
      sqlCase.setOperand(i++, operand);
    }
    return sqlCase;
  }

  @Nullable
  private SqlNode visitSelect(SqlSelect select) {
    List<SqlNode> newOperands = new ArrayList<>();
    for (SqlNode child : select.getOperandList()) {
      newOperands.add(child != null ? child.accept(this) : null);
    }
    int i = 0;
    for (SqlNode operand : newOperands) {
      // Preserve hints.
      if (i == SQLSELECT_HINTS_OPERAND_INDEX) {
        break;
      }
      select.setOperand(i++, operand);
    }
    return select;
  }

  @Nullable
  private SqlNode visitJoin(SqlJoin join) {
    List<SqlNode> newOperands = new ArrayList<>();
    for (SqlNode child : join.getOperandList()) {
      newOperands.add(child != null ? child.accept(this) : null);
    }
    int i = 0;
    for (SqlNode operand : newOperands) {
      // Preserve join metadata literals:
      // natural (true/false), joinType (INNER/LEFT/RIGHT/FULL) and conditionType (ON/USING)
      // These are structural keywords, not data literals.
      if (i == SQLJOIN_NATURAL_OPERAND_INDEX
            || i == SQLJOIN_JOIN_TYPE_OPERAND_INDEX
            || i == SQLJOIN_CONDITION_TYPE_OPERAND_INDEX) {
        i++;
        continue;
      }
      join.setOperand(i++, operand);
    }
    return join;
  }

  @Nullable
  private SqlNode visitWith(SqlWith with) {
    List<SqlNode> newList = new ArrayList<>();
    for (SqlNode node : with.withList.getList()) {
      newList.add(node.accept(this));
    }
    SqlNode newBody = with.body.accept(this);
    with.setOperand(0, new SqlNodeList(newList, with.withList.getParserPosition()));
    with.setOperand(1, newBody);
    return with;
  }

  /**
   * SqlWithItem has four fields: SqlIdentifier name, SqlNodeList
   * columnList, SqlNode query, and SqlLiteral recursive.
   * We will visit only the columnList and query since:
   * - name has already been visited in the SqlWith visit method.
   * - recursive is a literal which is a property of the WITH item and not the query itself.
   */
  @Nullable
  private SqlNode visitWithItem(SqlWithItem withItem) {
    if (withItem.columnList != null) {
      for (int i = 0; i < withItem.columnList.size(); i++) {
        SqlNode column = withItem.columnList.get(i);
        if (column != null) {
          withItem.columnList.set(i, column.accept(this));
        }
      }
    }

    if (withItem.query != null) {
      SqlNode newQuery = withItem.query.accept(this);
      withItem.query = newQuery;
    }

    return withItem;
  }

  @Nullable
  private SqlNode visitOrderBy(SqlOrderBy orderBy) {
    return new SqlOrderBy(
        orderBy.getParserPosition(),
        orderBy.query.accept(this),
        orderBy.orderList,
        orderBy.offset != null ? orderBy.offset.accept(this) : null,
        orderBy.fetch != null ? orderBy.fetch.accept(this) : null);
  }

  /**
   * Visit IN/NOT IN clause and normalize data literals.
   * <p>
   * Three cases:
   * <ul>
   *   <li><b>Case 1:</b> All values are data literals → replace entire list with a single ?
   *       <br>Example: IN (1, 2, 3) → IN (?)</li>
   *   <li><b>Case 2:</b> Contains any of the following → visit each value individually and replace only data literals:
   *       <ul>
   *         <li>Expressions: IN (col1 + 1, 2) → IN (col1 + ?, ?)</li>
   *         <li>Function calls: IN (UPPER('a'), LOWER('b')) → IN (UPPER(?), LOWER(?))</li>
   *         <li>Subquery: IN (SELECT ...) → visits the subquery normally</li>
   *       </ul>
   *   </li>
   *   <li><b>Case 3:</b> Contains NULL </li>
   *       <ul>
   *         <li>preserve NULL if _squashNullInList is false: IN (1, NULL, 3) → IN (1, NULL, 3)</li>
   *         <li>squash NULL if _squashNullInList is true: IN (1, NULL, 3) → IN (?)</li>
   *       </ul>
   * </ul>
   * </p>
   */
  @Nullable
  private SqlNode visitIn(SqlCall inCall) {
    List<SqlNode> operands = inCall.getOperandList();
    if (operands.isEmpty()) {
      return inCall;
    }

    // First operand is the column/expression being checked
    SqlNode leftOperand = operands.get(0).accept(this);

    // Second operand can be:
    // - SqlNodeList: contains literal values, expressions, or function calls (e.g., IN (1, 2, 3))
    // - SqlSelect: a subquery (e.g., IN (SELECT ...))
    if (operands.size() > 1 && operands.get(1) instanceof SqlNodeList) {
      SqlNodeList valueList = (SqlNodeList) operands.get(1);

      // Check if all values in the list are data literals (not preserved literals)
      // A SqlNodeList can contain:
      // - SqlLiteral (data): numbers, strings, dates, etc.
      // - SqlLiteral (preserved): NULL, DISTINCT, ASC, DESC, etc.
      // - SqlCall: expressions (col1 + 1), function calls (UPPER('a')), etc.
      boolean allDataLiterals = true;
      for (SqlNode node : valueList.getList()) {
        if (!(node instanceof SqlLiteral)) {
          allDataLiterals = false;
          break;
        }
        SqlLiteral literal = (SqlLiteral) node;
        // Allow NULL to be squashed if _squashNullInList is true
        boolean isNullAndSquashable = _squashNullInList && literal.getTypeName() == SqlTypeName.NULL;
        if (shouldPreserveLiteral(literal) && !isNullAndSquashable) {
          allDataLiterals = false;
          break;
        }
      }

      // If all are data literals, replace the entire list with a single dynamic parameter
      if (allDataLiterals && valueList.size() > 0) {
        SqlDynamicParam singleParam = new SqlDynamicParam(_dynamicParamIndex++, inCall.getParserPosition());
        SqlNodeList newValueList = new SqlNodeList(List.of(singleParam), valueList.getParserPosition());
        inCall.setOperand(0, leftOperand);
        inCall.setOperand(1, newValueList);
        return inCall;
      }

      // Otherwise, visit each value in the list normally
      List<SqlNode> newValues = new ArrayList<>();
      for (SqlNode value : valueList.getList()) {
        newValues.add(value.accept(this));
      }
      inCall.setOperand(0, leftOperand);
      inCall.setOperand(1, new SqlNodeList(newValues, valueList.getParserPosition()));
      return inCall;
    }

    // Fallback: for subqueries or other non-SqlNodeList cases, visit all operands normally
    List<SqlNode> newOperands = new ArrayList<>();
    for (SqlNode operand : operands) {
      newOperands.add(operand.accept(this));
    }
    int i = 0;
    for (SqlNode operand : newOperands) {
      inCall.setOperand(i++, operand);
    }
    return inCall;
  }

  /**
   * Check if a literal should be preserved.
   * Currently, we preserve symbolic keywords (DISTINCT, ASC, DESC, etc.) and NULL literals.
   */
  private boolean shouldPreserveLiteral(SqlLiteral literal) {
    return literal.getTypeName() == SqlTypeName.SYMBOL || literal.getTypeName() == SqlTypeName.NULL;
  }
}
