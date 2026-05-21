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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMatchUtils;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMetadataCache.MaterializedViewCacheEntry;
import org.apache.pinot.materializedview.rewrite.MaterializedViewQueryShape;
import org.apache.pinot.materializedview.rewrite.MaterializedViewRewritePlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Base class for materialized view matching strategies that follow the
/// [subsumption](https://en.wikipedia.org/wiki/Subsumption) model:
/// a query can be answered by an MV if the MV's definition logically
/// *subsumes* (covers) the query's requirements.
///
/// This class implements the [MaterializedViewMatchStrategy] interface using a
/// **template method** pattern. The overall matching flow is fixed:
///
///   - Shape gate — reject queries whose structural shape is incompatible
///   - GROUP BY check
///   - Projection (SELECT) subsumption check
///   - WHERE filter matching and residual extraction
///   - Residual filter validation
///   - ORDER BY compatibility
///   - HAVING compatibility
///   - Build the rewritten query and cost
///
///
/// Concrete subclasses customize individual steps by overriding the
/// `protected abstract` hook methods. This design allows adding new
/// matching strategies (scan subsumption, aggregation subsumption, etc.)
/// while reusing the shared matching infrastructure in [MaterializedViewMatchUtils].
///
/// Strategies produce plan fragments — [MaterializedViewRewritePlan] instances with
/// `execMode = null`. The
/// The rewrite engine resolves execution mode and split compatibility after the
/// strategy returns.
///
/// Implementations should be stateless and thread-safe.
///
/// @see ExactSubsumptionStrategy
/// @see ScanSubsumptionStrategy
/// @see AggregationSubsumptionStrategy
public abstract class AbstractSubsumptionStrategy implements MaterializedViewMatchStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSubsumptionStrategy.class);

  /// Template method that drives the subsumption matching pipeline.
  ///
  /// This method is `final` to enforce a consistent matching order
  /// across all strategies. Subclasses customize behavior through the
  /// `protected abstract` hook methods.
  @Nullable
  @Override
  public final MaterializedViewRewritePlan match(PinotQuery userQuery, MaterializedViewCacheEntry candidateEntry) {
    PinotQuery viewQuery = candidateEntry.getCompiledQuery();
    String strategyName = getClass().getSimpleName();
    String materializedViewName = candidateEntry.getMaterializedViewTableNameWithType();

    if (viewQuery == null) {
      LOGGER.debug("MV match [{}] strategy={}: compiled query is null", materializedViewName, strategyName);
      return null;
    }

    // Step 1: shape gate
    if (!acceptsShape(userQuery, viewQuery)) {
      LOGGER.debug("MV match [{}] strategy={}: rejected at SHAPE_GATE (userShape={}, materializedViewShape={})",
          materializedViewName, strategyName, MaterializedViewQueryShape.classify(userQuery),
          MaterializedViewQueryShape.classify(viewQuery));
      return null;
    }

    // Step 2: GROUP BY
    if (!groupByMatches(userQuery, viewQuery)) {
      LOGGER.debug("MV match [{}] strategy={}: rejected at GROUP_BY", materializedViewName, strategyName);
      return null;
    }

    // Step 3: projection subsumption — use the projection map cached on the entry.
    Map<Expression, String> viewProjectionMap = candidateEntry.getViewProjectionMap();
    if (!projectionSubsumes(userQuery.getSelectList(), viewProjectionMap)) {
      LOGGER.debug("MV match [{}] strategy={}: rejected at PROJECTION", materializedViewName, strategyName);
      return null;
    }

    // Step 4: WHERE matching + residual extraction (shared logic)
    Expression userFilter = userQuery.getFilterExpression();
    Expression materializedViewFilter = viewQuery.getFilterExpression();
    boolean filtersEqual = Objects.equals(userFilter, materializedViewFilter);

    // AND is commutative and associative. Two filters with the same conjunct set are equivalent
    // even if the underlying List<Expression> order differs (e.g. user wrote "b AND a" but the MV
    // was defined with "a AND b"). Promote to filtersEqual so we don't fall into the residual
    // extraction branch and misreport "not a superset" when the residual set is in fact empty.
    // The MV-side flattening is cached on the entry (computed once per definition update).  The
    // user-side flatten is computed once per (query, strategy, candidate) triple; for workloads
    // with many candidate MVs on the same base table the redundancy could be removed by hoisting
    // userFilterConjuncts into the rewrite engine, but the cost is bounded by the number of
    // candidates (typically 1–5 per table) and the feature is opt-in via useMaterializedView.
    if (!filtersEqual && userFilter != null && materializedViewFilter != null) {
      if (MaterializedViewMatchUtils.flattenAnd(userFilter).equals(candidateEntry.getViewFilterConjuncts())) {
        filtersEqual = true;
      }
    }

    Expression residualFilter = null;
    if (!filtersEqual) {
      residualFilter = MaterializedViewMatchUtils.tryExtractResidualFilter(userFilter, materializedViewFilter);
      if (residualFilter == null && userFilter != null) {
        LOGGER.debug("MV match [{}] strategy={}: rejected at WHERE_FILTER "
            + "(user filter is not a superset of MV filter)", materializedViewName, strategyName);
        return null;
      }
      if (userFilter == null) {
        LOGGER.debug("MV match [{}] strategy={}: rejected at WHERE_FILTER "
            + "(MV has filter but user query does not)", materializedViewName, strategyName);
        return null;
      }
    }

    // Step 5: residual validation (subclass-specific)
    if (!validateResidual(residualFilter, viewQuery, viewProjectionMap)) {
      LOGGER.debug("MV match [{}] strategy={}: rejected at RESIDUAL_VALIDATION", materializedViewName, strategyName);
      return null;
    }

    // Step 6: ORDER BY
    if (!orderByCompatible(userQuery, viewQuery, viewProjectionMap)) {
      LOGGER.debug("MV match [{}] strategy={}: rejected at ORDER_BY", materializedViewName, strategyName);
      return null;
    }

    // Step 7: HAVING
    if (!havingCompatible(userQuery, viewQuery, viewProjectionMap)) {
      LOGGER.debug("MV match [{}] strategy={}: rejected at HAVING", materializedViewName, strategyName);
      return null;
    }

    // Step 8: build result
    return buildResult(userQuery, candidateEntry, residualFilter, viewProjectionMap, filtersEqual);
  }

  // -----------------------------------------------------------------------
  //  Hook methods — to be implemented by concrete strategies
  // -----------------------------------------------------------------------

  /// Returns `true` if this strategy is applicable to the given
  /// combination of user query shape and MV query shape.
  protected abstract boolean acceptsShape(PinotQuery userQuery, PinotQuery viewQuery);

  /// Returns `true` if the user query's GROUP BY clause is compatible
  /// with the MV's GROUP BY clause.
  protected abstract boolean groupByMatches(PinotQuery userQuery, PinotQuery viewQuery);

  /// Returns `true` if the MV's projection (SELECT list) covers all
  /// expressions required by the user query.
  ///
  /// @param userSelectList  the user query's SELECT expressions
  /// @param viewProjectionMap alias-stripped expression &rarr; MV column name
  protected abstract boolean projectionSubsumes(List<Expression> userSelectList,
      Map<Expression, String> viewProjectionMap);

  /// Returns `true` if the residual filter (extra WHERE predicates beyond
  /// the MV's definition) is valid for this strategy.
  ///
  /// @param residualFilter  the residual WHERE filter, or `null` if filters are identical
  /// @param viewQuery         the MV's compiled query
  /// @param viewProjectionMap alias-stripped expression &rarr; MV column name (already computed by caller)
  protected abstract boolean validateResidual(@Nullable Expression residualFilter, PinotQuery viewQuery,
      Map<Expression, String> viewProjectionMap);

  /// Returns `true` if the user query's ORDER BY clause can be satisfied
  /// by the MV table.
  protected abstract boolean orderByCompatible(PinotQuery userQuery, PinotQuery viewQuery,
      Map<Expression, String> viewProjectionMap);

  /// Returns `true` if the user query's HAVING clause can be satisfied
  /// by the MV table.
  protected abstract boolean havingCompatible(PinotQuery userQuery, PinotQuery viewQuery,
      Map<Expression, String> viewProjectionMap);

  /// Constructs the [MaterializedViewRewritePlan] fragment containing the rewritten query,
  /// match type, and cost score. The plan's execution mode is left unset (null)
  /// — it is resolved by the engine after the strategy returns.
  ///
  /// @param userQuery       the original user query
  /// @param candidateEntry  the matched MV cache entry
  /// @param residualFilter  residual WHERE filter (null if none)
  /// @param viewProjectionMap alias-stripped expression &rarr; MV column name
  /// @param filtersEqual    true if user and MV filters are identical
  protected abstract MaterializedViewRewritePlan buildResult(PinotQuery userQuery,
      MaterializedViewCacheEntry candidateEntry, @Nullable Expression residualFilter,
      Map<Expression, String> viewProjectionMap, boolean filtersEqual);
}
