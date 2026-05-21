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
package org.apache.pinot.materializedview.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.FilterKind;


/// Shared utility methods for materialized view subsumption matching strategies.
///
/// **Thread-safety:** stateless utility class.  All methods are pure functions of their
/// arguments and do not retain references to or mutate inputs.  Safe to invoke concurrently
/// from the broker hot path.  Future contributors adding caching to any method must preserve
/// this contract (use thread-safe data structures and avoid sharing per-thread state).
public final class MaterializedViewMatchUtils {

  private MaterializedViewMatchUtils() {
  }

  // -----------------------------------------------------------------------
  //  Alias handling
  // -----------------------------------------------------------------------

  /// Strips the top-level `AS` alias from an expression if present.
  /// For `as(expr, alias)` returns `expr`; otherwise returns the
  /// expression unchanged.
  ///
  /// @param expr the expression to unwrap
  /// @return the inner expression without the alias wrapper
  public static Expression stripAlias(Expression expr) {
    if (expr.getType() == ExpressionType.FUNCTION) {
      Function func = expr.getFunctionCall();
      if (func != null && "as".equals(func.getOperator())) {
        return func.getOperands().get(0);
      }
    }
    return expr;
  }

  /// Extracts the output column name that a SELECT expression maps to in the
  /// MV table schema.
  ///
  ///
  ///   - `as(expr, alias)` &rarr; alias identifier name
  ///   - bare identifier &rarr; identifier name
  ///   - anything else &rarr; throws [IllegalStateException]
  ///
  ///
  /// @param expr a SELECT-list expression from the MV's compiled query
  /// @return the MV table column name
  public static String extractMaterializedViewColumnName(Expression expr) {
    Function func = expr.getFunctionCall();
    if (func != null && "as".equals(func.getOperator())) {
      Expression aliasExpr = func.getOperands().get(1);
      return aliasExpr.getIdentifier().getName();
    }
    if (expr.getType() == ExpressionType.IDENTIFIER) {
      return expr.getIdentifier().getName();
    }
    throw new IllegalStateException(
        "Cannot extract MV column name from expression: " + RequestUtils.prettyPrint(expr));
  }

  /// Builds a mapping from alias-stripped expressions to MV table column names
  /// for all entries in the MV's SELECT list.
  ///
/// Example: for `SELECT city, SUM(revenue) AS sum_rev FROM ...`
/// the returned map is:
/// ```text
///   Identifier("city")                       -> "city"
///   Function("SUM", [Identifier("revenue")]) -> "sum_rev"
/// ```
  ///
  /// @param viewQuery the MV's compiled PinotQuery
  /// @return map from stripped expression to MV column name
  public static Map<Expression, String> buildMaterializedViewProjectionMap(PinotQuery viewQuery) {
    List<Expression> selectList = viewQuery.getSelectList();
    if (selectList == null) {
      return Map.of();
    }
    Map<Expression, String> map = new HashMap<>(selectList.size());
    for (Expression expr : selectList) {
      Expression stripped = stripAlias(expr);
      String columnName = extractMaterializedViewColumnName(expr);
      map.put(stripped, columnName);
    }
    return map;
  }

  /// Extracts the user-specified alias name from an expression, if present.
  /// Returns `null` for expressions without an alias.
  ///
  /// @param expr a SELECT-list expression from the user's query
  /// @return the alias name, or `null` if the expression has no alias
  @Nullable
  public static String extractUserAlias(Expression expr) {
    Function func = expr.getFunctionCall();
    if (func != null && "as".equals(func.getOperator())) {
      return func.getOperands().get(1).getIdentifier().getName();
    }
    return null;
  }

  /// Rewrites the user query's SELECT list by replacing each expression with
  /// its corresponding MV table column name, while preserving the user's
  /// original alias if one was specified.
  ///
/// For example, given:
/// ```sql
///   MV defined:  SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city
///   User query:  SELECT city, SUM(revenue) AS r_sum  FROM orders GROUP BY city
/// ```
/// the rewritten SELECT list will be:
/// ```sql
///   SELECT city, sum_rev AS r_sum FROM materialized_view_table GROUP BY city
/// ```
  ///
  /// @param userSelectList  the user query's original SELECT expressions
  /// @param viewProjectionMap alias-stripped expression &rarr; MV column name
  /// @return the rewritten SELECT expression list
  public static List<Expression> rewriteSelectList(List<Expression> userSelectList,
      Map<Expression, String> viewProjectionMap) {
    List<Expression> rewritten = new ArrayList<>(userSelectList.size());
    for (Expression expr : userSelectList) {
      Expression stripped = stripAlias(expr);
      String materializedViewColumnName = viewProjectionMap.get(stripped);
      Expression materializedViewColExpr = RequestUtils.getIdentifierExpression(materializedViewColumnName);

      String userAlias = extractUserAlias(expr);
      String resultAlias = userAlias != null ? userAlias
          : implicitAliasForUserExpression(stripped, materializedViewColumnName);
      if (resultAlias != null) {
        materializedViewColExpr = RequestUtils.getFunctionExpression("as", materializedViewColExpr,
            RequestUtils.getIdentifierExpression(resultAlias));
      }
      rewritten.add(materializedViewColExpr);
    }
    return rewritten;
  }

  /// Computes an implicit alias so the rewritten result column carries the same name the user
  /// would have seen against the base table. Without this, a user query like
  /// `SELECT SUM(revenue) FROM orders` rewritten against an MV column `sum_rev` would surface
  /// `sum_rev` to the client (silently breaking any consumer reading results by column name).
  ///
  /// Returns `null` (no alias needed) when the user expression is already a bare identifier whose
  /// name matches the MV column name &mdash; in that case the natural result column name already
  /// matches the user's expectation.
  @Nullable
  private static String implicitAliasForUserExpression(Expression strippedUserExpr, String materializedViewColumnName) {
    if (strippedUserExpr.getType() == ExpressionType.IDENTIFIER
        && strippedUserExpr.getIdentifier().getName().equals(materializedViewColumnName)) {
      return null;
    }
    return RequestUtils.prettyPrint(strippedUserExpr);
  }

  // -----------------------------------------------------------------------
  //  WHERE / residual filter
  // -----------------------------------------------------------------------

  /// Checks whether the user's WHERE filter is an AND-superset of the MV's
  /// WHERE filter and extracts the residual predicates.
  ///
  /// Rules:
  ///
  ///   - MV has no filter, user has a filter &rarr; entire user filter is residual
  ///   - User has no filter, MV has a filter &rarr; not a superset, returns null
  ///   - User conjuncts contain all MV conjuncts &rarr; difference is residual
  ///   - Otherwise &rarr; returns null (no match)
  ///
  ///
  /// **TODO(materialized-view): OR / IN subsumption is currently correctness-safe but conservative.**
  /// OR and IN nodes are treated as opaque atomic conjuncts and compared via
  /// `Objects.equals` (structure + operand-order sensitive), not by semantic
  /// set containment. The following patterns are rejected today even though the MV
  /// technically covers the user's rows; users fall back to the base table:
  ///
  ///   - OR-operand reorder &mdash; MV `a OR b` vs user `b OR a`
  ///   - IN-operand reorder &mdash; MV `x IN (1,2,3)` vs user `x IN (3,1,2)`
  ///   - IN / OR superset &mdash; MV `x IN (1,2,3)` (or `a OR b OR c`)
  ///       covers user `x IN (1,2)`, `x = 1`, or `a OR b`
  ///   - IN and chained-OR equivalence &mdash; `x IN (1,2)` vs `x = 1 OR x = 2`
  ///
  /// Before relaxing any of the above, add regression tests covering the opposite
  /// direction (MV narrower than user, e.g. MV `x IN (1,2)` vs user
  /// `x IN (1,2,3)`) so the correctness guardrail is not eroded.
  ///
  /// @param userFilter the user query's filter expression (may be null)
  /// @param materializedViewFilter   the MV query's filter expression (may be null)
  /// @return the residual filter expression, or `null` if the user filter
  ///         is not a valid superset of the MV filter
  @Nullable
  public static Expression tryExtractResidualFilter(@Nullable Expression userFilter,
      @Nullable Expression materializedViewFilter) {
    if (materializedViewFilter == null && userFilter != null) {
      return userFilter;
    }
    if (userFilter == null) {
      return null;
    }

    Set<Expression> userConjuncts = flattenAnd(userFilter);
    Set<Expression> materializedViewConjuncts = flattenAnd(materializedViewFilter);

    if (!userConjuncts.containsAll(materializedViewConjuncts)) {
      return null;
    }

    Set<Expression> residual = new HashSet<>(userConjuncts);
    residual.removeAll(materializedViewConjuncts);

    if (residual.isEmpty()) {
      return null;
    }

    return buildAndExpression(new ArrayList<>(residual));
  }

  /// Flattens a filter expression into a set of AND conjuncts. If the top-level
  /// operator is AND, it is recursively unwrapped; otherwise the entire expression
  /// is treated as a single conjunct.
  public static Set<Expression> flattenAnd(Expression filter) {
    Set<Expression> conjuncts = new HashSet<>();
    collectAndConjuncts(filter, conjuncts);
    return conjuncts;
  }

  private static void collectAndConjuncts(Expression expr, Set<Expression> conjuncts) {
    if (expr.getType() == ExpressionType.FUNCTION) {
      Function function = expr.getFunctionCall();
      if (function != null && FilterKind.AND.name().equals(function.getOperator())) {
        for (Expression operand : function.getOperands()) {
          collectAndConjuncts(operand, conjuncts);
        }
        return;
      }
    }
    conjuncts.add(expr);
  }

  /// Builds an AND expression from the given list of operands. Returns the
  /// single operand directly when the list has exactly one element.
  public static Expression buildAndExpression(List<Expression> operands) {
    if (operands.size() == 1) {
      return operands.get(0);
    }
    return RequestUtils.getFunctionExpression(FilterKind.AND.name(), operands);
  }

  // -----------------------------------------------------------------------
  //  Expression remapping
  // -----------------------------------------------------------------------

  /// Recursively rewrites an expression tree by replacing any sub-expression
  /// found in `viewProjectionMap` with a simple identifier referencing the
  /// corresponding MV column name.
  ///
  /// This is used to transform HAVING, ORDER BY, and residual WHERE
  /// expressions from base-table semantics to MV-table semantics. For example,
  /// `SUM(revenue) > 1000` becomes `sum_rev > 1000` when the
  /// projection map contains `SUM(revenue) -> "sum_rev"`.
  ///
  /// @param expr            the expression to remap
  /// @param viewProjectionMap alias-stripped expression &rarr; MV column name
  /// @return the remapped expression (may be the same instance if nothing changed)
  public static Expression remapExpression(Expression expr, Map<Expression, String> viewProjectionMap) {
    if (viewProjectionMap.containsKey(expr)) {
      return RequestUtils.getIdentifierExpression(viewProjectionMap.get(expr));
    }

    if (expr.getType() == ExpressionType.FUNCTION) {
      Function func = expr.getFunctionCall();
      if (func != null && func.getOperands() != null) {
        List<Expression> originalOperands = func.getOperands();
        List<Expression> remapped = new ArrayList<>(originalOperands.size());
        boolean changed = false;
        for (Expression operand : originalOperands) {
          Expression result = remapExpression(operand, viewProjectionMap);
          remapped.add(result);
          if (result != operand) {
            changed = true;
          }
        }
        if (changed) {
          return RequestUtils.getFunctionExpression(func.getOperator(), remapped);
        }
      }
    }

    return expr;
  }

  // -----------------------------------------------------------------------
  //  Column reference collection
  // -----------------------------------------------------------------------

  /// Collects all column identifiers referenced by the given expression.
  /// Useful for verifying that a residual filter or ORDER BY clause only
  /// references columns available in the MV table.
  ///
  /// @param expr the expression to scan
  /// @return set of column name strings
  public static Set<String> collectReferencedColumns(Expression expr) {
    Set<String> columns = new HashSet<>();
    collectColumnsRecursive(expr, columns);
    return columns;
  }

  private static void collectColumnsRecursive(Expression expr, Set<String> columns) {
    if (expr.getType() == ExpressionType.IDENTIFIER) {
      String name = expr.getIdentifier().getName();
      if (!"*".equals(name)) {
        columns.add(name);
      }
      return;
    }
    if (expr.getType() == ExpressionType.FUNCTION) {
      Function func = expr.getFunctionCall();
      if (func != null && func.getOperands() != null) {
        for (Expression operand : func.getOperands()) {
          collectColumnsRecursive(operand, columns);
        }
      }
    }
  }
}
