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
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.materializedview.rewrite.MatchType;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMatchUtils;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMetadataCache.MaterializedViewCacheEntry;
import org.apache.pinot.materializedview.rewrite.MaterializedViewQueryShape;
import org.apache.pinot.materializedview.rewrite.MaterializedViewRewritePlan;


/// Subsumption strategy for **non-aggregation (scan)** queries where the
/// user's SELECT list is a subset of the MV's SELECT list.
///
/// Both the user query and the MV must have [MaterializedViewQueryShape#SCAN] shape
/// (no aggregation functions, no GROUP BY). The MV table is treated as a
/// pre-filtered / pre-projected physical table whose columns correspond to the
/// columns listed in the MV's `definedSql`.
///
/// Cost model:
///
///   - `2.0` — projection-subset match without residual WHERE
///   - `3.0` — projection-subset match with residual WHERE
///
public class ScanSubsumptionStrategy extends AbstractSubsumptionStrategy {

  private static final double COST_SCAN_SUBSUMPTION = 2.0;
  private static final double COST_SCAN_WITH_RESIDUAL = 3.0;

  @Override
  protected boolean acceptsShape(PinotQuery userQuery, PinotQuery viewQuery) {
    return MaterializedViewQueryShape.classify(userQuery) == MaterializedViewQueryShape.SCAN
        && MaterializedViewQueryShape.classify(viewQuery) == MaterializedViewQueryShape.SCAN;
  }

  @Override
  protected boolean groupByMatches(PinotQuery userQuery, PinotQuery viewQuery) {
    return !userQuery.isSetGroupByList() && !viewQuery.isSetGroupByList();
  }

  @Override
  protected boolean projectionSubsumes(List<Expression> userSelectList,
      Map<Expression, String> viewProjectionMap) {
    if (userSelectList == null || userSelectList.isEmpty()) {
      return false;
    }
    for (Expression expr : userSelectList) {
      Expression stripped = MaterializedViewMatchUtils.stripAlias(expr);
      if (!viewProjectionMap.containsKey(stripped)) {
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
    /// Check whether the residual filter's user/base-side columns are all reachable through the
    /// MV projection map.  Comparing against `viewProjectionMap.values()` (MV-side names) would
    /// over-reject MVs that alias the column — [MaterializedViewMatchUtils#coversReferencedColumns]
    /// resolves against KEYS so an aliased projection (`base_col AS mv_col`) still matches.
    Set<String> residualColumns = MaterializedViewMatchUtils.collectReferencedColumns(residualFilter);
    return MaterializedViewMatchUtils.coversReferencedColumns(residualColumns, viewProjectionMap);
  }

  @Override
  protected boolean orderByCompatible(PinotQuery userQuery, PinotQuery viewQuery,
      Map<Expression, String> viewProjectionMap) {
    List<Expression> orderByList = userQuery.getOrderByList();
    if (orderByList == null || orderByList.isEmpty()) {
      return true;
    }
    /// ORDER BY columns are user/base-side names; resolve them against MV projection keys to
    /// support aliased MV columns — mirrors `validateResidual` above.
    for (Expression orderByExpr : orderByList) {
      Set<String> referencedColumns = MaterializedViewMatchUtils.collectReferencedColumns(orderByExpr);
      if (!MaterializedViewMatchUtils.coversReferencedColumns(referencedColumns, viewProjectionMap)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean havingCompatible(PinotQuery userQuery, PinotQuery viewQuery,
      Map<Expression, String> viewProjectionMap) {
    return userQuery.getHavingExpression() == null;
  }

  @Override
  protected MaterializedViewRewritePlan buildResult(PinotQuery userQuery, MaterializedViewCacheEntry candidateEntry,
      @Nullable Expression residualFilter, Map<Expression, String> viewProjectionMap, boolean filtersEqual) {
    PinotQuery rewritten = userQuery.deepCopy();
    rewritten.getDataSource().setTableName(candidateEntry.getMaterializedViewTableNameWithType());
    rewritten.setSelectList(MaterializedViewMatchUtils.rewriteSelectList(userQuery.getSelectList(), viewProjectionMap));
    /// Remap residual filter columns to MV column names in case the MV uses aliased columns.
    Expression remappedResidual = residualFilter != null
        ? MaterializedViewMatchUtils.remapExpression(residualFilter, viewProjectionMap) : null;
    rewritten.setFilterExpression(remappedResidual);
    if (userQuery.getOrderByList() != null && !userQuery.getOrderByList().isEmpty()) {
      List<Expression> remappedOrderBy = new ArrayList<>(userQuery.getOrderByList().size());
      for (Expression orderByExpr : userQuery.getOrderByList()) {
        remappedOrderBy.add(MaterializedViewMatchUtils.remapExpression(orderByExpr, viewProjectionMap));
      }
      rewritten.setOrderByList(remappedOrderBy);
    }

    double cost = filtersEqual ? COST_SCAN_SUBSUMPTION : COST_SCAN_WITH_RESIDUAL;
    return new MaterializedViewRewritePlan(candidateEntry.getMaterializedViewTableNameWithType(),
        MatchType.SCAN_SUBSUME, null, rewritten, cost);
  }
}
