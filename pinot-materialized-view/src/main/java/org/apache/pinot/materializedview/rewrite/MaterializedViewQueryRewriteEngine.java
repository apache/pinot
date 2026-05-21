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

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata.MaterializedViewSplitSpec;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMetadataCache.MaterializedViewCacheEntry;
import org.apache.pinot.materializedview.rewrite.strategy.MaterializedViewMatchStrategy;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.sql.FilterKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Orchestrates materialized view query rewriting in two phases:
///
///
///   - **Static matching** — strategies produce [MaterializedViewRewritePlan]
///       fragments (rewritten MV query + match type + cost) without considering
///       runtime state.
///   - **Runtime gating and plan resolution** — freshness is checked
///       before matching; after matching, execution mode is determined and
///       split-compatibility is verified.
///
///
/// For each candidate MV, strategies are tried in registration order (highest
/// precision first). Once a strategy produces a plan that also passes runtime
/// compatibility checks ([#resolvePlan]), no lower-precision strategies
/// are attempted for the same MV. If a strategy matches structurally but its
/// plan is rejected by runtime checks (e.g. EXACT match incompatible with
/// split-mode GROUP BY), the engine falls through to the next strategy.
/// The overall best plan across all candidates is then selected by lowest cost.
///
/// Thread-safety: this class is immutable after construction and safe to share.
public class MaterializedViewQueryRewriteEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewQueryRewriteEngine.class);

  private final MaterializedViewMetadataCache _materializedViewMetadataCache;
  private final List<MaterializedViewMatchStrategy> _strategies;

  public MaterializedViewQueryRewriteEngine(MaterializedViewMetadataCache materializedViewMetadataCache,
      List<MaterializedViewMatchStrategy> strategies) {
    _materializedViewMetadataCache = materializedViewMetadataCache;
    _strategies = List.copyOf(strategies);
  }

  /// Removes all MV cache entries whose base tables include the given raw table name.
  /// Called when a base table is deleted so stale reverse-index entries do not survive.
  public void invalidateBaseTable(String rawBaseTableName) {
    _materializedViewMetadataCache.invalidateBaseTable(rawBaseTableName);
  }

  /// Rebuilds the MV cache entry for the given table name from ZK if the cache is currently
  /// missing it.  Called from the broker's OFFLINE→ONLINE state transition so an MV whose
  /// broker resource was cycled (without deleting the definition znode) becomes queryable
  /// again without requiring a broker restart or a definition-znode republish.
  public void refreshTable(String rawTableName) {
    _materializedViewMetadataCache.refreshTable(rawTableName);
  }

  /// Number of MV entries currently held in the underlying metadata cache.  Exposed so the
  /// broker can wire a gauge on this value.
  public int getCacheEntryCount() {
    return _materializedViewMetadataCache.size();
  }

  /// Attempts to rewrite the given query to use a materialized view.
  ///
  /// @param pinotQuery       the compiled user query
  /// @param rawBaseTableName the raw (no type suffix) name of the base table being queried
  /// @return the rewrite result containing candidate names and optional plan,
  ///         or `null` if no candidate MVs exist for the base table
  @Nullable
  public MaterializedViewRewriteResult tryRewrite(PinotQuery pinotQuery, String rawBaseTableName) {
    List<MaterializedViewCacheEntry> candidates =
        _materializedViewMetadataCache.getMaterializedViewEntriesForBaseTable(rawBaseTableName);
    if (candidates == null || candidates.isEmpty()) {
      return null;
    }

    List<String> candidateNames = new ArrayList<>(candidates.size());
    for (MaterializedViewCacheEntry candidate : candidates) {
      candidateNames.add(candidate.getMaterializedViewTableNameWithType());
    }

    MaterializedViewRewritePlan bestPlan = null;
    String bestStrategyName = null;

    for (MaterializedViewCacheEntry candidate : candidates) {
      if (!isEligible(candidate)) {
        continue;
      }

      for (MaterializedViewMatchStrategy strategy : _strategies) {
        // Strategies signal "no structural match" by returning null and "fully resolved plan" by
        // returning non-null; any thrown exception is a strategy bug or contract violation and is
        // surfaced here rather than silently swallowed.  Swallowing would mask a bug behind a
        // less-precise strategy match (or a base-only fallback) without operator visibility on a
        // hot path.  The caller (`BaseSingleStageBrokerRequestHandler`) wraps the whole rewrite
        // in its own try/catch so a thrown exception still falls back to the base query, but
        // surfaces the stack via the standard query-error path.
        MaterializedViewRewritePlan plan = strategy.match(pinotQuery, candidate);
        if (plan == null) {
          continue;
        }

        plan = resolvePlan(plan, candidate, pinotQuery);
        if (plan == null) {
          // Strategy matched structurally but the plan is incompatible with
          // the required execution mode (e.g. EXACT + split + GROUP BY).
          // Try lower-precision strategies for this candidate.
          continue;
        }

        if (bestPlan == null || plan.compareTo(bestPlan) < 0) {
          bestPlan = plan;
          bestStrategyName = strategy.getClass().getSimpleName();
        }
        // A fully resolved plan was found; skip lower-precision strategies.
        break;
      }
    }

    if (bestPlan != null) {
      LOGGER.info("MV rewrite succeeded for table [{}]: strategy={}, materializedViewTable={}, "
              + "matchType={}, execMode={}, cost={}, originalQuery=[{}], rewrittenQuery=[{}]",
          rawBaseTableName, bestStrategyName, bestPlan.getMaterializedViewTableNameWithType(),
          bestPlan.getMatchType(), bestPlan.getExecMode(), bestPlan.getCost(),
          pinotQuery, bestPlan.getMaterializedViewQuery());
    } else {
      LOGGER.debug("MV rewrite miss for table [{}]: evaluated {} candidate(s)={}, "
              + "queryShape={}, userQuery=[{}]",
          rawBaseTableName, candidates.size(), candidateNames,
          MaterializedViewQueryShape.classify(pinotQuery), pinotQuery);
    }

    return new MaterializedViewRewriteResult(candidateNames, bestPlan);
  }

  // -----------------------------------------------------------------------
  //  Runtime gating — separated from AST matching
  // -----------------------------------------------------------------------

  /// Checks whether a candidate MV is eligible for rewrite based on runtime state.
  /// Under Design C eligibility is:
  ///   1. `rewriteEnabled=true` on the MV definition (operator kill switch)
  ///   2. `watermarkMs > 0` (cold-start guard — the scheduler has committed coverage up to here)
  ///   3. If `stalenessThresholdMs > 0`: `now - watermarkMs <= stalenessThresholdMs` (SLO)
  ///
  /// Per-bucket eligibility is enforced later by the handler / dispatcher.  Note: an empty
  /// partition map is permitted — V1 routing uses `watermarkMs` as the split point, so the
  /// partition map is only consulted for V2 N-way routing.
  private static boolean isEligible(MaterializedViewCacheEntry candidate) {
    if (!candidate.getDefinition().isRewriteEnabled()) {
      LOGGER.debug("MV skip [{}]: rewriteEnabled=false on definition",
          candidate.getMaterializedViewTableNameWithType());
      return false;
    }
    if (candidate.getWatermarkMs() <= 0) {
      LOGGER.debug("MV skip [{}]: watermarkMs={} (cold-start, no data yet)",
          candidate.getMaterializedViewTableNameWithType(), candidate.getWatermarkMs());
      return false;
    }
    long stalenessThresholdMs = candidate.getDefinition().getStalenessThresholdMs();
    if (stalenessThresholdMs > 0) {
      long ageMs = System.currentTimeMillis() - candidate.getWatermarkMs();
      if (ageMs > stalenessThresholdMs) {
        LOGGER.debug("MV skip [{}]: staleness {}ms exceeds threshold {}ms",
            candidate.getMaterializedViewTableNameWithType(), ageMs, stalenessThresholdMs);
        return false;
      }
    }
    return true;
  }

  // -----------------------------------------------------------------------
  //  Execution mode resolution — separated from AST matching
  // -----------------------------------------------------------------------

  /// Resolves the execution mode for a plan fragment produced by a strategy,
  /// and checks split-mode compatibility.
  ///
  /// The decision is driven by whether the MV definition includes a
  /// [MaterializedViewSplitSpec]:
  ///
  ///   - **No split spec** (batch MV): the MV is assumed to cover all base
  ///       table data. Returns [ExecutionMode#FULL_REWRITE].
  ///   - **Has split spec** (incremental MV): the MV covers only historical
  ///       data. `watermarkMs` must be &gt; 0 (i.e. at least one APPEND
  ///       has successfully completed) for the MV to be queryable. If coverage is
  ///       not yet confirmed, returns `null` to skip this MV.
  ///
  ///
  /// In split mode, EXACT match is rejected when the user query has GROUP BY
  /// because it replaces aggregation functions with plain MV column references,
  /// producing incompatible DataTable schemas during merge.
  ///
  /// @return the resolved plan with execution mode set, or `null` if the
  ///         plan is incompatible with the required execution mode
  @Nullable
  private static MaterializedViewRewritePlan resolvePlan(MaterializedViewRewritePlan plan,
      MaterializedViewCacheEntry candidate, PinotQuery userQuery) {
    MaterializedViewSplitSpec splitSpec = candidate.getSplitSpec();

    if (splitSpec != null) {
      long watermarkMs = candidate.getWatermarkMs();
      if (watermarkMs <= 0) {
        // Incremental MV has no confirmed coverage yet (cold-start: generator
        // initialized the watermark but no APPEND has completed). Skip this MV
        // to avoid split queries against an empty MV table.
        LOGGER.info("MV skip [{}]: cold-start, no coverage confirmed yet "
                + "(watermark initialized but no APPEND completed), matchType={}",
            candidate.getMaterializedViewTableNameWithType(), plan.getMatchType());
        return null;
      }

      // Split mode needs both sides of the boundary filter:
      //   base branch: sourceTime >= watermarkMs
      //   MV branch:   materializedViewTime    <  watermarkMs
      // Both columns are TIMESTAMP (enforced at create time), so only the column names matter
      // here.  A missing MV column name indicates malformed metadata or a partial upgrade —
      // skip the candidate rather than emit an unguarded split.
      if (splitSpec.getMaterializedViewTimeColumn() == null || splitSpec.getMaterializedViewTimeColumn().isEmpty()) {
        LOGGER.warn("MV skip [{}]: split spec missing MV-side time column; "
                + "cannot attach materializedViewTime < boundary filter. Skipping to avoid double-counting.",
            candidate.getMaterializedViewTableNameWithType());
        return null;
      }

      if (isQueryFullyCoveredByMaterializedView(userQuery, splitSpec, watermarkMs)) {
        LOGGER.info("MV full rewrite [{}]: user time filter is fully covered by watermarkMs={}, matchType={}",
            candidate.getMaterializedViewTableNameWithType(), watermarkMs, plan.getMatchType());
        return plan.withExecMode(ExecutionMode.FULL_REWRITE, null, 0);
      }

      // EXACT match replaces aggregation functions (e.g. SUM(col)) with plain
      // MV column references (e.g. col_sum), producing physical column types
      // on the MV side that are incompatible with the base table's aggregation
      // intermediate types during split-mode merge. The schema mismatch applies
      // whenever the MV definition has any aggregation (GROUP BY or aggregate
      // functions in SELECT) — not only when the user query has GROUP BY. Reject
      // and let AggregationSubsumptionStrategy handle the query instead.
      if (plan.getMatchType() == MatchType.EXACT) {
        // EXACT match is only produced by ExactSubsumptionStrategy, which itself
        // requires a compiled MV definition to perform the structural match. Reaching
        // EXACT with a null compiled query indicates a contract violation in the
        // strategy chain — fail loud rather than silently fall through to the
        // schema-incompatible split-merge path.
        PinotQuery viewQuery = candidate.getCompiledQuery();
        Preconditions.checkState(viewQuery != null,
            "EXACT match without a compiled MV definition for table: %s",
            candidate.getMaterializedViewTableNameWithType());
        if (MaterializedViewQueryShape.classify(viewQuery) == MaterializedViewQueryShape.AGGREGATION) {
          LOGGER.info("MV skip [{}]: EXACT match rejected in split mode for aggregation MV "
                  + "(schema incompatibility during merge, falling back to AGG_REAGG)",
              candidate.getMaterializedViewTableNameWithType());
          return null;
        }
      }

      // AGG_REAGG plans that use non-distributive re-aggregation (e.g. COUNT->SUM) are
      // not split-safe: the base side produces COUNT intermediates while the MV side
      // produces SUM intermediates, and the broker reducer (using the original COUNT
      // function) will misinterpret the MV DataTables. For incremental MVs, FULL_REWRITE
      // is also unsafe because watermarkMs only proves historical coverage before the
      // boundary; routing only to the MV would silently drop newer base-table rows.
      if (!plan.isSplitSafe()) {
        LOGGER.info("MV skip [{}]: AGG_REAGG plan is not split-safe "
                + "(non-distributive re-aggregation such as COUNT->SUM). "
                + "Cannot use SPLIT_REWRITE or partial FULL_REWRITE safely.",
            candidate.getMaterializedViewTableNameWithType());
        return null;
      }

      return plan.withExecMode(ExecutionMode.SPLIT_REWRITE, splitSpec, watermarkMs);
    }

    return plan.withExecMode(ExecutionMode.FULL_REWRITE, null, 0);
  }

  private static boolean isQueryFullyCoveredByMaterializedView(PinotQuery userQuery,
      MaterializedViewSplitSpec splitSpec, long watermarkMs) {
    Expression filter = userQuery.getFilterExpression();
    if (filter == null) {
      return false;
    }
    // The user's WHERE filter is expressed in the base column's native unit (which may be
    // INT-days, INT-seconds, TIMESTAMP-millis, etc.), so the boundary literal must be converted
    // to the same unit before comparison.
    String sourceTimeFormat = splitSpec.getSourceTimeFormat();
    if (sourceTimeFormat == null || sourceTimeFormat.isEmpty()) {
      return false;
    }
    BigDecimal coverageBoundary =
        parseDecimal(new DateTimeFormatSpec(sourceTimeFormat).fromMillisToFormat(watermarkMs));
    if (coverageBoundary == null) {
      return false;
    }
    String sourceTimeColumn = splitSpec.getSourceTimeColumn();
    for (Expression conjunct : MaterializedViewMatchUtils.flattenAnd(filter)) {
      if (isUpperBoundWithinCoverage(conjunct, sourceTimeColumn, coverageBoundary)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isUpperBoundWithinCoverage(Expression expr, String sourceTimeColumn,
      BigDecimal coverageBoundary) {
    Function function = expr.getFunctionCall();
    if (function == null || function.getOperandsSize() != 2) {
      return false;
    }
    List<Expression> operands = function.getOperands();
    Expression left = operands.get(0);
    Expression right = operands.get(1);
    String operator = function.getOperator();

    if (isIdentifier(left, sourceTimeColumn)) {
      BigDecimal literal = getNumericLiteral(right);
      if (literal == null) {
        return false;
      }
      if (FilterKind.LESS_THAN.name().equalsIgnoreCase(operator)) {
        return literal.compareTo(coverageBoundary) <= 0;
      }
      if (FilterKind.LESS_THAN_OR_EQUAL.name().equalsIgnoreCase(operator)) {
        return literal.compareTo(coverageBoundary) < 0;
      }
      return false;
    }

    if (isIdentifier(right, sourceTimeColumn)) {
      BigDecimal literal = getNumericLiteral(left);
      if (literal == null) {
        return false;
      }
      if (FilterKind.GREATER_THAN.name().equalsIgnoreCase(operator)) {
        return literal.compareTo(coverageBoundary) <= 0;
      }
      if (FilterKind.GREATER_THAN_OR_EQUAL.name().equalsIgnoreCase(operator)) {
        return literal.compareTo(coverageBoundary) < 0;
      }
    }
    return false;
  }

  private static boolean isIdentifier(Expression expr, String column) {
    return expr.getType() == ExpressionType.IDENTIFIER
        && column.equalsIgnoreCase(expr.getIdentifier().getName());
  }

  @Nullable
  private static BigDecimal getNumericLiteral(Expression expr) {
    if (expr.getType() != ExpressionType.LITERAL || expr.getLiteral() == null) {
      return null;
    }
    Object value = RequestUtils.getLiteralValue(expr.getLiteral());
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }
    // For every numeric type go through the value's canonical String form to preserve full
    // precision.  In particular a Long beyond 2^53 (e.g. modern epoch millis) cannot be
    // round-tripped through Double without precision loss, which would mis-classify the user's
    // upper bound vs the watermark and silently emit FULL_REWRITE against partial coverage.
    if (value instanceof Number) {
      return new BigDecimal(value.toString());
    }
    if (value instanceof String) {
      return parseDecimal((String) value);
    }
    return null;
  }

  @Nullable
  private static BigDecimal parseDecimal(String value) {
    try {
      return new BigDecimal(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
