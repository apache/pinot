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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.materializedview.rewrite.MatchType;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMatchUtils;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMetadataCache.MaterializedViewCacheEntry;
import org.apache.pinot.materializedview.rewrite.MaterializedViewRewritePlan;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/// Subsumption strategy that requires an **exact structural match** between
/// the user query and the MV definition (after stripping aliases and ignoring
/// column order in the SELECT list).
///
/// This is the tightest form of subsumption: the query's SELECT, WHERE,
/// GROUP BY, ORDER BY, and HAVING must all be semantically identical to the
/// MV's definition. No residual WHERE filter is allowed — if the WHERE clauses
/// differ in any way, the match fails.
///
/// The rewritten query maps all SELECT expressions to MV table column names
/// while preserving the user's original aliases.
///
/// Cost: `0.0` (perfect match, highest priority).
///
/// This strategy accepts any query shape (SCAN or AGGREGATION) because
/// exact matching is valid regardless of whether aggregation is present.
public class ExactSubsumptionStrategy extends AbstractSubsumptionStrategy {

  private static final double COST_EXACT = 0.0;

  /// Accepts any query shape — exact matching is universally applicable.
  @Override
  protected boolean acceptsShape(PinotQuery userQuery, PinotQuery viewQuery) {
    return true;
  }

  /// Requires GROUP BY lists to be identical (same expressions, same order).
  @Override
  protected boolean groupByMatches(PinotQuery userQuery, PinotQuery viewQuery) {
    List<Expression> userList = userQuery.getGroupByList();
    List<Expression> materializedViewList = viewQuery.getGroupByList();
    if (userList == null && materializedViewList == null) {
      return true;
    }
    if (userList == null || materializedViewList == null) {
      return false;
    }
    return userList.equals(materializedViewList);
  }

  /// Requires SELECT lists to contain exactly the same expressions (order-insensitive,
  /// alias-insensitive). The stripped expression sets must be equal.
  @Override
  protected boolean projectionSubsumes(List<Expression> userSelectList,
      Map<Expression, String> viewProjectionMap) {
    if (userSelectList == null) {
      return viewProjectionMap.isEmpty();
    }
    if (userSelectList.size() != viewProjectionMap.size()) {
      return false;
    }
    Set<Expression> userStripped = new HashSet<>(userSelectList.size());
    for (Expression expr : userSelectList) {
      userStripped.add(MaterializedViewMatchUtils.stripAlias(expr));
    }
    return userStripped.equals(viewProjectionMap.keySet());
  }

  /// Rejects any residual filter — exact match requires WHERE clauses to be
  /// identical between the user query and the MV definition.
  @Override
  protected boolean validateResidual(@Nullable Expression residualFilter, PinotQuery viewQuery,
      Map<Expression, String> viewProjectionMap) {
    return residualFilter == null;
  }

  /// Requires ORDER BY lists to be compatible between user query and MV query.
  /// If both have ORDER BY, they must be identical. If the user has ORDER BY but the MV
  /// does not (the common case — MVs rarely define ORDER BY), the check passes as long as
  /// all referenced columns are present in the MV projection, since the MV table can
  /// serve any ORDER BY over its columns.
  @Override
  protected boolean orderByCompatible(PinotQuery userQuery, PinotQuery viewQuery,
      Map<Expression, String> viewProjectionMap) {
    List<Expression> userList = userQuery.getOrderByList();
    List<Expression> materializedViewList = viewQuery.getOrderByList();
    if (userList == null || userList.isEmpty()) {
      return true;
    }
    if (materializedViewList != null && !materializedViewList.isEmpty()) {
      return userList.equals(materializedViewList);
    }
    /// MV has no ORDER BY — allow only when each user ORDER BY sort key is rewritable through
    /// the projection map.  An ORDER BY entry comes in as `asc(...)` / `desc(...)`; strip the
    /// direction wrapper, then accept the sort key if:
    ///   1. it is present as-is in `viewProjectionMap` (so `remapExpression` will rewrite it
    ///      to the MV column), OR
    ///   2. it is a simple identifier whose name appears as a key in the projection map
    ///      (covers the aliased-MV case `base_col AS mv_col`).
    ///
    /// Critically, a bare `coversReferencedColumns(collectReferencedColumns(orderByExpr), map)`
    /// is NOT safe: for an aggregate like `ORDER BY count(*)`, `collectReferencedColumns`
    /// returns the empty set (the `*` identifier is filtered out), `coversReferencedColumns`
    /// returns true vacuously, and `remapExpression` then leaves `count(*)` unchanged in the
    /// rewritten query — producing a count over the pre-aggregated MV bucket rows instead of
    /// the original base-table rows.  Restricting the relaxed match to bare-identifier sort
    /// keys closes that wrong-result hole.
    for (Expression orderByExpr : userList) {
      /// Use the canonical wrapper-stripper so `asc` / `desc` / `nullsFirst` / `nullsLast`
      /// nesting all collapses to the inner sort key.  The sibling [AggregationSubsumptionStrategy]
      /// already uses the same helper; keeping the implementations aligned avoids the silent
      /// over-rejection that an `asc`-only stripper would produce for `ORDER BY col NULLS LAST`.
      Expression sortKey = CalciteSqlParser.removeOrderByFunctions(orderByExpr);
      if (viewProjectionMap.containsKey(sortKey)) {
        continue;
      }
      if (sortKey.getType() == ExpressionType.IDENTIFIER
          && MaterializedViewMatchUtils.coversReferencedColumns(
              MaterializedViewMatchUtils.collectReferencedColumns(sortKey), viewProjectionMap)) {
        continue;
      }
      return false;
    }
    return true;
  }

  /// Requires HAVING expressions to be identical between user query and MV query.
  @Override
  protected boolean havingCompatible(PinotQuery userQuery, PinotQuery viewQuery,
      Map<Expression, String> viewProjectionMap) {
    return Objects.equals(userQuery.getHavingExpression(), viewQuery.getHavingExpression());
  }

  /// Builds the rewritten query by swapping the table name to the MV table
  /// and mapping each SELECT expression to its MV column name while preserving
  /// the user's original alias.
  ///
  /// The strategy no longer checks split-mode compatibility here — that
  /// concern is handled by `MaterializedViewQueryRewriteEngine.resolvePlan`.
  @Override
  protected MaterializedViewRewritePlan buildResult(PinotQuery userQuery, MaterializedViewCacheEntry candidateEntry,
      @Nullable Expression residualFilter, Map<Expression, String> viewProjectionMap, boolean filtersEqual) {
    PinotQuery rewritten = userQuery.deepCopy();
    rewritten.getDataSource().setTableName(candidateEntry.getMaterializedViewTableNameWithType());
    rewritten.setSelectList(MaterializedViewMatchUtils.rewriteSelectList(userQuery.getSelectList(), viewProjectionMap));
    /// EXACT match implies user filter == MV filter, so the rewritten query carries no WHERE.
    /// RLS conjuncts injected by the broker before MV rewrite would also be dropped here.  Today
    /// an MV definition cannot encode RLS predicates, so an exact filter equality can NEVER hold
    /// when RLS was applied — `userFilter` would include the RLS predicate, `viewFilter` would
    /// not, and `match` would have returned null long before reaching here.  Fail-loud guard so
    /// a future relaxation of filter equivalence (e.g. OR / IN canonicalization in
    /// MaterializedViewMatchUtils.filtersEqual) cannot silently drop the RLS predicate.
    if (userQuery.getQueryOptions() != null) {
      for (String optionKey : userQuery.getQueryOptions().keySet()) {
        Preconditions.checkState(!optionKey.startsWith(CommonConstants.RLS_FILTERS),
            "ExactSubsumptionStrategy must not clear the WHERE clause when row-level security "
                + "filters are present (query option %s set). filtersEqual is structurally false "
                + "in this case; reaching here indicates a filter-equivalence regression.",
            optionKey);
      }
    }
    rewritten.setFilterExpression(null);
    /// Remap GROUP BY and ORDER BY from user column names to MV column names.
    if (userQuery.getGroupByList() != null) {
      List<Expression> remappedGroupBy = new ArrayList<>(userQuery.getGroupByList().size());
      for (Expression expr : userQuery.getGroupByList()) {
        remappedGroupBy.add(MaterializedViewMatchUtils.remapExpression(expr, viewProjectionMap));
      }
      rewritten.setGroupByList(remappedGroupBy);
    }
    if (userQuery.getOrderByList() != null) {
      List<Expression> remappedOrderBy = new ArrayList<>(userQuery.getOrderByList().size());
      for (Expression expr : userQuery.getOrderByList()) {
        remappedOrderBy.add(MaterializedViewMatchUtils.remapExpression(expr, viewProjectionMap));
      }
      rewritten.setOrderByList(remappedOrderBy);
    }
    if (userQuery.getHavingExpression() != null) {
      rewritten.setHavingExpression(
          MaterializedViewMatchUtils.remapExpression(userQuery.getHavingExpression(), viewProjectionMap));
    }
    return new MaterializedViewRewritePlan(candidateEntry.getMaterializedViewTableNameWithType(),
        MatchType.EXACT, null, rewritten, COST_EXACT);
  }
}
