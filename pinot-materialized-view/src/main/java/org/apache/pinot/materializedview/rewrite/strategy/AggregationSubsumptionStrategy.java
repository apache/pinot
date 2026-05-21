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
package org.apache.pinot.materializedview.rewrite.strategy;

import java.util.ArrayList;
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
import org.apache.pinot.materializedview.rewrite.MatchType;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMatchUtils;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMetadataCache.MaterializedViewCacheEntry;
import org.apache.pinot.materializedview.rewrite.MaterializedViewQueryShape;
import org.apache.pinot.materializedview.rewrite.MaterializedViewRewritePlan;
import org.apache.pinot.materializedview.rewrite.equivalence.AggregationEquivalence;
import org.apache.pinot.materializedview.rewrite.equivalence.AggregationEquivalenceRegistry;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/// Subsumption strategy for **aggregation** queries.
///
/// The rewritten query always retains GROUP BY and wraps aggregation
/// columns with re-aggregation functions via
/// [AggregationEquivalenceRegistry] (e.g. `SUM(col)` on the user
/// query becomes `SUM(sum_col)` on the MV). When the MV's GROUP BY
/// granularity matches the user query exactly, each group contains a single
/// pre-computed row, so the re-aggregation is effectively a no-op — but it
/// guarantees that the server produces an aggregation intermediate DataTable,
/// which is critical for split-mode merging where both sides must use the
/// same schema.
///
/// Cost model:
///
///   - `6.0` — re-aggregation without residual WHERE
///   - `7.0` — re-aggregation with residual WHERE
///
///
/// Implementations should be stateless and thread-safe.
public class AggregationSubsumptionStrategy extends AbstractSubsumptionStrategy {

  private static final double COST_REAGG = 6.0;
  private static final double COST_REAGG_WITH_RESIDUAL = 7.0;

  @Override
  protected boolean acceptsShape(PinotQuery userQuery, PinotQuery viewQuery) {
    return MaterializedViewQueryShape.classify(userQuery) == MaterializedViewQueryShape.AGGREGATION
        && MaterializedViewQueryShape.classify(viewQuery) == MaterializedViewQueryShape.AGGREGATION;
  }

  @Override
  protected boolean groupByMatches(PinotQuery userQuery, PinotQuery viewQuery) {
    List<Expression> userGroupBy = userQuery.getGroupByList();
    List<Expression> materializedViewGroupBy = viewQuery.getGroupByList();

    boolean userIsWholeTable = userGroupBy == null || userGroupBy.isEmpty();
    boolean materializedViewIsWholeTable =
        materializedViewGroupBy == null || materializedViewGroupBy.isEmpty();

    // User aggregates over the whole table while the MV pre-aggregates by some grouping —
    // legal: re-aggregate the MV's per-group rows into one whole-table result on the MV side.
    if (userIsWholeTable) {
      return !materializedViewIsWholeTable;
    }
    // User has a non-empty GROUP BY but the MV is whole-table — the MV cannot supply the
    // per-group rows the user wants.
    if (materializedViewIsWholeTable) {
      return false;
    }

    Set<Expression> userSet = new HashSet<>(userGroupBy);
    Set<Expression> materializedViewSet = new HashSet<>(materializedViewGroupBy);
    return materializedViewSet.containsAll(userSet);
  }

  @Override
  protected boolean projectionSubsumes(List<Expression> userSelectList,
      Map<Expression, String> viewProjectionMap) {
    if (userSelectList == null || userSelectList.isEmpty()) {
      return false;
    }
    for (Expression expr : userSelectList) {
      Expression stripped = MaterializedViewMatchUtils.stripAlias(expr);
      // Plain column reference: direct MV projection hit is sufficient.
      if (stripped.getFunctionCall() == null) {
        if (!viewProjectionMap.containsKey(stripped)) {
          return false;
        }
        continue;
      }
      // Aggregate function: an exact projection match is NOT enough on its own. We need an
      // AggregationEquivalence rule to re-aggregate the pre-computed MV column correctly.
      // Without a rule we would fall back to a bare column reference (e.g. AVG(revenue) →
      // avg_rev), which produces wrong results for non-distributive functions.
      if (findEquivalentMaterializedViewEntry(stripped, viewProjectionMap) == null) {
        return false;
      }
    }
    return true;
  }

  @Nullable
  private static Object[] findEquivalentMaterializedViewEntry(Expression userExpr,
      Map<Expression, String> viewProjectionMap) {
    Function userFunc = userExpr.getFunctionCall();
    if (userFunc == null) {
      return null;
    }
    String userFuncName = userFunc.getOperator();
    List<Expression> userOperands = userFunc.getOperands();

    for (Map.Entry<Expression, String> materializedViewEntry : viewProjectionMap.entrySet()) {
      Expression materializedViewExpr = materializedViewEntry.getKey();
      Function materializedViewFunc = materializedViewExpr.getFunctionCall();
      if (materializedViewFunc == null) {
        continue;
      }
      if (!operandsMatch(userOperands, materializedViewFunc.getOperands())) {
        continue;
      }
      AggregationEquivalence rule =
          AggregationEquivalenceRegistry.findRule(userFuncName, materializedViewFunc.getOperator());
      if (rule != null) {
        return new Object[]{materializedViewEntry.getValue(), rule};
      }
    }
    return null;
  }

  private static boolean operandsMatch(@Nullable List<Expression> a, @Nullable List<Expression> b) {
    if (a == null && b == null) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    // MV definitions are authored without broker-injected config literals (e.g. log2m for HLL).
    // The broker may append trailing literals to the user query at runtime. We tolerate those
    // by comparing only up to the MV's operand count — but only when the extra operands in the
    // user query are all literals (so PERCENTILE(col,50) vs PERCENTILE(col,99) still rejects,
    // because the MV also carries the literal 99 as a semantically significant operand).
    int materializedViewSize = b.size();
    int userSize = a.size();
    if (userSize < materializedViewSize) {
      return false;
    }
    // All operands up to materializedViewSize must match exactly.
    for (int i = 0; i < materializedViewSize; i++) {
      if (!a.get(i).equals(b.get(i))) {
        return false;
      }
    }
    // Extra user operands (beyond the MV operand count) must all be literals —
    // otherwise this is a semantically distinct call, not just a config injection.
    for (int i = materializedViewSize; i < userSize; i++) {
      if (a.get(i).getType() != ExpressionType.LITERAL) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean validateResidual(@Nullable Expression residualFilter, PinotQuery viewQuery,
      Map<Expression, String> viewProjectionMap) {
    if (residualFilter == null) {
      return true;
    }

    List<Expression> materializedViewGroupBy = viewQuery.getGroupByList();
    if (materializedViewGroupBy == null || materializedViewGroupBy.isEmpty()) {
      return false;
    }

    Set<String> groupByColumnNames = new HashSet<>(materializedViewGroupBy.size());
    for (Expression gbExpr : materializedViewGroupBy) {
      groupByColumnNames.addAll(MaterializedViewMatchUtils.collectReferencedColumns(gbExpr));
    }

    Set<String> residualColumns = MaterializedViewMatchUtils.collectReferencedColumns(residualFilter);
    return groupByColumnNames.containsAll(residualColumns);
  }

  @Override
  protected boolean orderByCompatible(PinotQuery userQuery, PinotQuery viewQuery,
      Map<Expression, String> viewProjectionMap) {
    List<Expression> orderByList = userQuery.getOrderByList();
    if (orderByList == null || orderByList.isEmpty()) {
      return true;
    }
    for (Expression orderByExpr : orderByList) {
      Expression inner = CalciteSqlParser.removeOrderByFunctions(orderByExpr);
      if (!isResolvable(inner, viewProjectionMap)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean havingCompatible(PinotQuery userQuery, PinotQuery viewQuery,
      Map<Expression, String> viewProjectionMap) {
    Expression having = userQuery.getHavingExpression();
    if (having == null) {
      return true;
    }
    return allReferencesResolvableWithEquivalence(having, viewProjectionMap);
  }

  private boolean allReferencesResolvableWithEquivalence(Expression expr,
      Map<Expression, String> viewProjectionMap) {
    // Resolve via the projection map first: an IDENTIFIER for a grouping column is keyed
    // directly in viewProjectionMap (e.g. `carrier` -> `carrier`) and must resolve true here.
    // The earlier ordering returned false for any IDENTIFIER before consulting the map, which
    // rejected legitimate HAVING / SELECT references like `carrier = 'AA'` even when the MV
    // exposed `carrier`.
    if (isResolvable(expr, viewProjectionMap)) {
      return true;
    }
    if (expr.getType() == ExpressionType.LITERAL) {
      return true;
    }
    if (expr.getType() == ExpressionType.IDENTIFIER) {
      return false;
    }
    if (expr.getFunctionCall() != null && expr.getFunctionCall().getOperands() != null) {
      for (Expression operand : expr.getFunctionCall().getOperands()) {
        if (!allReferencesResolvableWithEquivalence(operand, viewProjectionMap)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private static boolean isResolvable(Expression expr, Map<Expression, String> viewProjectionMap) {
    if (viewProjectionMap.containsKey(expr)) {
      return true;
    }
    return findEquivalentMaterializedViewEntry(expr, viewProjectionMap) != null;
  }

  @Override
  protected MaterializedViewRewritePlan buildResult(PinotQuery userQuery, MaterializedViewCacheEntry candidateEntry,
      @Nullable Expression residualFilter, Map<Expression, String> viewProjectionMap, boolean filtersEqual) {
    PinotQuery rewritten = userQuery.deepCopy();
    rewritten.getDataSource().setTableName(candidateEntry.getMaterializedViewTableNameWithType());

    // Check whether all re-aggregation rules applied to this query are split-safe
    // (i.e. produce intermediates compatible with what the original function's reducer expects).
    // COUNT->SUM is NOT split-safe: base side produces COUNT intermediates but MV side produces
    // SUM intermediates, causing a schema mismatch in the broker reducer during split-mode merge.
    boolean splitSafe = isSplitSafe(userQuery.getSelectList(), viewProjectionMap);

    rewritten.setSelectList(
        buildReAggSelectList(userQuery.getSelectList(), viewProjectionMap));

    List<Expression> userGroupBy = userQuery.getGroupByList();
    if (userGroupBy != null && !userGroupBy.isEmpty()) {
      List<Expression> remappedGroupBy = new ArrayList<>(userGroupBy.size());
      for (Expression gbExpr : userGroupBy) {
        String materializedViewCol = viewProjectionMap.get(gbExpr);
        remappedGroupBy.add(RequestUtils.getIdentifierExpression(materializedViewCol));
      }
      rewritten.setGroupByList(remappedGroupBy);
    } else {
      // Whole-table re-aggregation: user query has no GROUP BY but MV is grouped.  Strip the
      // GROUP BY from the deepCopy so the re-aggregation sums per-group rows into one whole-
      // table result on the MV side.  groupByMatches has already validated this is legal.
      rewritten.setGroupByList(null);
    }

    Expression originalHaving = userQuery.getHavingExpression();
    if (originalHaving != null) {
      rewritten.setHavingExpression(
          remapExpressionWithEquivalence(originalHaving, viewProjectionMap));
    } else {
      rewritten.setHavingExpression(null);
    }

    Expression remappedResidual = null;
    if (residualFilter != null) {
      remappedResidual = MaterializedViewMatchUtils.remapExpression(residualFilter, viewProjectionMap);
    }
    rewritten.setFilterExpression(remappedResidual);

    if (userQuery.getOrderByList() != null && !userQuery.getOrderByList().isEmpty()) {
      List<Expression> remappedOrderBy = new ArrayList<>(userQuery.getOrderByList().size());
      for (Expression orderByExpr : userQuery.getOrderByList()) {
        remappedOrderBy.add(remapExpressionWithEquivalence(orderByExpr, viewProjectionMap));
      }
      rewritten.setOrderByList(remappedOrderBy);
    }

    double cost = filtersEqual ? COST_REAGG : COST_REAGG_WITH_RESIDUAL;
    return new MaterializedViewRewritePlan(candidateEntry.getMaterializedViewTableNameWithType(),
        MatchType.AGG_REAGG, null, rewritten, cost, splitSafe);
  }

  /// Returns true if all aggregation expressions in the SELECT list use split-safe
  /// equivalence rules (i.e. the re-aggregation function is identical to the user
  /// function, so intermediates from base and MV sides are schema-compatible).
  private static boolean isSplitSafe(List<Expression> userSelectList,
      Map<Expression, String> viewProjectionMap) {
    for (Expression expr : userSelectList) {
      Expression stripped = MaterializedViewMatchUtils.stripAlias(expr);
      if (stripped.getFunctionCall() != null) {
        Object[] match = findEquivalentMaterializedViewEntry(stripped, viewProjectionMap);
        if (match != null) {
          AggregationEquivalence rule = (AggregationEquivalence) match[1];
          if (!rule.isSplitSafe()) {
            return false;
          }
        }
      }
    }
    return true;
  }

  private List<Expression> buildReAggSelectList(List<Expression> userSelectList,
      Map<Expression, String> viewProjectionMap) {
    List<Expression> result = new ArrayList<>(userSelectList.size());
    for (Expression expr : userSelectList) {
      Expression stripped = MaterializedViewMatchUtils.stripAlias(expr);
      String userAlias = MaterializedViewMatchUtils.extractUserAlias(expr);
      Expression rewritten;

      if (viewProjectionMap.containsKey(stripped)
          && stripped.getFunctionCall() == null) {
        rewritten = RequestUtils.getIdentifierExpression(viewProjectionMap.get(stripped));
      } else {
        rewritten = rewriteAggregationExpression(stripped, viewProjectionMap);
      }

      // Preserve the user's expected result-column name. Bare-identifier dimensions that map 1:1
      // to an identically-named MV column need no alias wrap (e.g. city -> city); but when the
      // user expression was an aggregation (or a renamed dimension), the rewritten form would
      // otherwise surface the MV-side name and silently break clients reading by column name.
      String resultAlias;
      if (userAlias != null) {
        resultAlias = userAlias;
      } else if (stripped.getType() == ExpressionType.IDENTIFIER
          && rewritten.getType() == ExpressionType.IDENTIFIER
          && stripped.getIdentifier().getName().equals(rewritten.getIdentifier().getName())) {
        resultAlias = null;
      } else {
        resultAlias = RequestUtils.prettyPrint(stripped);
      }
      if (resultAlias != null) {
        rewritten = RequestUtils.getFunctionExpression("as", rewritten,
            RequestUtils.getIdentifierExpression(resultAlias));
      }
      result.add(rewritten);
    }
    return result;
  }

  private Expression rewriteAggregationExpression(Expression stripped,
      Map<Expression, String> viewProjectionMap) {
    Object[] match = findEquivalentMaterializedViewEntry(stripped, viewProjectionMap);
    if (match != null) {
      String materializedViewCol = (String) match[0];
      AggregationEquivalence rule = (AggregationEquivalence) match[1];
      return rule.rewrite(stripped, materializedViewCol);
    }

    throw new IllegalStateException(
        "Cannot rewrite aggregation expression: " + stripped);
  }

  private Expression remapExpressionWithEquivalence(Expression expr,
      Map<Expression, String> viewProjectionMap) {
    if (viewProjectionMap.containsKey(expr) && expr.getFunctionCall() == null) {
      return RequestUtils.getIdentifierExpression(viewProjectionMap.get(expr));
    }

    if (expr.getFunctionCall() != null) {
      if (viewProjectionMap.containsKey(expr)) {
        return rewriteAggregationExpression(expr, viewProjectionMap);
      }
      Object[] match = findEquivalentMaterializedViewEntry(expr, viewProjectionMap);
      if (match != null) {
        return ((AggregationEquivalence) match[1]).rewrite(expr, (String) match[0]);
      }
    }

    if (expr.getFunctionCall() != null && expr.getFunctionCall().getOperands() != null) {
      Function func = expr.getFunctionCall();
      List<Expression> original = func.getOperands();
      List<Expression> remapped = new ArrayList<>(original.size());
      boolean changed = false;
      for (Expression operand : original) {
        Expression result = remapExpressionWithEquivalence(operand, viewProjectionMap);
        remapped.add(result);
        if (result != operand) {
          changed = true;
        }
      }
      if (changed) {
        return RequestUtils.getFunctionExpression(func.getOperator(), remapped);
      }
    }

    return expr;
  }
}
