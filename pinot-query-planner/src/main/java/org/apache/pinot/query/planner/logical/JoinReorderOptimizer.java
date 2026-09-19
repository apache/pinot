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

import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexUtil;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.query.catalog.PinotTable;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// A scoped, gated, cost-based join-reordering phase for the multi-stage query planner.
///
/// ### Scope
/// This phase runs after the existing logical `HepPlanner` programs and before the trait /
/// `POST_LOGICAL` / physical phases. It reorders inner-join trees using cardinality estimates
/// sourced from the `RelMetadataQuery` that is already backed by
/// `org.apache.pinot.calcite.rel.metadata.PinotDefaultRelMetadataProvider` (statistics-driven
/// row counts). The phase is off by default and has zero impact when disabled.
///
/// ### Strategy
/// Reordering is performed with the canonical Calcite `Programs.heuristicJoinOrder` design:
/// a dedicated [HepPlanner] runs [CoreRules#JOIN_TO_MULTI_JOIN] to collect an inner-join
/// tree into a single `MultiJoin`, then [CoreRules#MULTI_JOIN_OPTIMIZE]
/// (`LoptOptimizeJoinRule`) to re-derive the join order from `mq.getRowCount()` estimates.
/// The [HepPlanner] is created over the SAME [RelOptCluster] as the input tree (via the
/// cluster's planner-agnostic factories), so the statistics-backed metadata provider installed on the
/// cluster is reused directly — no cross-cluster tree copy is needed.
///
/// A true scoped `VolcanoPlanner` (with a deep cross-cluster tree copy) was considered but
/// rejected: a [RelNode] tree is bound to its originating cluster, that cluster is bound to the
/// logical `HepPlanner`, and Calcite 1.42 offers no clean, supported utility to re-host an
/// arbitrary rel tree in a fresh cluster owned by a second planner. The Hep + `MultiJoin` path
/// is the documented, well-tested approach and keeps the cost signal (our row counts) intact. This
/// class is a facade ([#maybeReorder(RelNode, int, boolean)]) so the internal strategy can be swapped
/// later without touching callers.
///
/// ### Eligibility gates
/// The phase is skipped (the input tree is returned unchanged) unless ALL of the following hold:
/// - The tree contains at least one [Join] (fast path: no join => nothing to do).
/// - There are at least two joins (a single join only swaps sides; v1 keeps the bar at two).
/// - The join count does not exceed the configured `maxJoins` cap. Plans beyond the cap
///   skip the phase to bound planning time.
/// - Every [Join] in the tree is an [JoinRelType#INNER] join (v1 scope).
/// - No [Join] carries a Pinot `joinOptions` hint — a hint signals explicit user
///   intent, so the whole phase is skipped in v1 (per-join veto is a later task).
/// - No [Join] carries correlation state. `MultiJoin` has no `variablesSet` component, so
///   folding a correlated join into one silently discards the correlation the plan depends on.
///   Pinot decorrelates in `QueryEnvironment#toRelation` and the shapes that survive
///   (UNNEST / CROSS JOIN UNNEST) put an `Uncollect` under a `Correlate` rather than a `Join`, so
///   this gate should not fire today — it is here so that the invariant is enforced locally
///   instead of inherited from a comment in another rule. Note a `Correlate` itself is NOT
///   disqualifying: it is a [org.apache.calcite.rel.BiRel], never matched by
///   `JoinToMultiJoinRule`, so it stays an opaque factor and its binder travels with the input it
///   binds.
/// - Every leaf of the tree is a [TableScan] with a known row count, established through
///   [PinotTable#getUsableRowCount()] — the single source of truth for whether statistics are
///   trustworthy enough to cost with. The gate is on LEAVES, not merely on the scans present: a
///   non-scan leaf (`Uncollect`, `LogicalValues`, a table function) gets a Calcite default guess,
///   which is precisely the mixed known/guessed case that makes reordering noise. So all leaves
///   must be known scans; otherwise the phase is skipped.
///
/// ### Skip-reason visibility
/// Each skip path is identified by a [SkipReason] value, logged at DEBUG and — when the query
/// asks for it via `joinReorderFeedback` — reported in the query response. The response channel
/// is the one that matters in practice: the shipped root log level is `info`, so the DEBUG line
/// is not visible on a default-configured broker.
///
/// ### Wall-clock observability
/// The Hep + `LoptOptimizeJoinRule` strategy is a single, deterministic pass — it is not
/// an exponential search — so a hard mid-flight timeout is not required. Instead, elapsed time is
/// measured around the reorder call and a WARN is emitted when it exceeds
/// [#SLOW_REORDER_WARN_THRESHOLD_MS] (100 ms). This is the canary that signals when a
/// budget / interrupt mechanism becomes necessary.
///
/// ### Fallback / robustness
/// [#maybeReorder(RelNode, int, boolean)] never throws: any unexpected error is caught, logged at
/// WARN, and the original (un-reordered) plan is returned. The reorder phase must never fail a query.
///
/// ### Thread-safety
/// Stateless aside from a static [Logger]. A new [HepPlanner] is created per
/// invocation on the planner thread, so the class is safe for concurrent use across queries.
public final class JoinReorderOptimizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(JoinReorderOptimizer.class);

  /// If a reorder takes longer than this threshold (in milliseconds), a WARN is logged. The
  /// Hep + LoptOptimizeJoinRule strategy is a single deterministic pass, so this threshold is a
  /// canary rather than a hard limit: if it fires regularly, a proper budget/interrupt mechanism
  /// should be introduced.
  static final long SLOW_REORDER_WARN_THRESHOLD_MS = 100L;

  private JoinReorderOptimizer() {
  }

  /// Reason why the join-reorder phase was skipped for a given plan. Logged at DEBUG on skip so
  /// that operators can trace non-obvious skip decisions without enabling verbose logging.
  public enum SkipReason {
    /// Plan has fewer than two joins: with one join there is only a side swap, which this phase
    /// does not do, so there is nothing useful to reorder.
    TOO_FEW_JOINS,
    /// Plan contains a non-INNER join; v1 scope restricts to inner joins only.
    NON_INNER_JOIN,
    /// A join carries an explicit hint; user intent takes precedence.
    HINTED_JOIN,
    /// A join carries correlation state, which `MultiJoin` cannot represent.
    CORRELATED_JOIN,
    /// At least one leaf is not a table scan with a known row count; the cost signal is unreliable.
    UNKNOWN_ROW_COUNT,
    /// Join count exceeds the configured cap; skipping to bound planning time.
    TOO_MANY_JOINS,
    /// An unexpected error occurred; the original plan was returned as a fallback.
    ERROR
  }

  /// Reorders the inner-join tree of `logicalPlan` when the eligibility gates pass, otherwise
  /// returns `logicalPlan` unchanged. Never throws — on any error the original plan is returned.
  ///
  /// @param logicalPlan the logical plan emitted by the preceding `HepPlanner` phases; must
  ///                    belong to a cluster whose metadata provider supplies statistics-backed row
  ///                    counts
  /// @param maxJoins        maximum number of joins allowed in the plan for the reorder phase to
  ///                        run; plans with more joins are returned unchanged
  /// @param collectFeedback whether the caller will report this outcome. The cost estimates and
  ///                        the plan-changed check each walk the whole tree forcing row-count
  ///                        metadata, so they are only worth computing when someone reads them
  /// @return the (possibly) reordered plan, or `logicalPlan` when the phase is skipped or fails
  public static Result maybeReorder(RelNode logicalPlan, int maxJoins, boolean collectFeedback) {
    int joinCount = -1;
    try {
      GateVisitor visitor = new GateVisitor();
      visitor.visit(logicalPlan);
      joinCount = visitor._joinCount;
      SkipReason skipReason = skipReason(visitor, maxJoins);
      if (skipReason != null) {
        LOGGER.debug("Join reorder phase skipped: {}", skipReason);
        return Result.skipped(logicalPlan, skipReason, joinCount);
      }
      // What the optimizer BELIEVED this plan would cost, either side of the phase. Recording both
      // is the point: comparing the predicted saving against measured latency across a workload is
      // how a miscalibrated cost model is detected, and a systematically over-optimistic estimator
      // otherwise degrades plans silently.
      double estCostBefore = collectFeedback ? estimatedCost(logicalPlan) : -1;
      long startNanos = System.nanoTime();
      RelNode result = reorder(logicalPlan);
      long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
      if (elapsedMs > SLOW_REORDER_WARN_THRESHOLD_MS) {
        LOGGER.warn("Join reorder phase took {}ms (join count: {}); consider a budget/interrupt "
            + "mechanism if this fires regularly.", elapsedMs, joinCount);
      }
      double estCostAfter = collectFeedback ? estimatedCost(result) : -1;
      boolean planChanged = collectFeedback && !result.deepEquals(logicalPlan);
      return Result.applied(result, joinCount, estCostBefore, estCostAfter, elapsedMs, planChanged);
    } catch (Exception | StackOverflowError t) {
      // Robustness: a failed reorder must never fail the query (StackOverflowError covers very
      // deep join trees). Other Errors (OOM etc.) propagate — swallowing them would hide real
      // JVM-level problems. Fall back to the original plan. The join count is reported when the
      // gate pass had already established it, which is the case operators most want it for.
      LOGGER.warn("Join reorder phase failed ({}: {}); continuing with the un-reordered plan",
          SkipReason.ERROR, t.getMessage(), t);
      return Result.skipped(logicalPlan, SkipReason.ERROR, joinCount);
    }
  }

  /// What the join-reorder phase did, for diagnostics.
  ///
  /// Returned rather than published directly so the optimizer stays free of any response or
  /// logging concern: the caller decides whether anyone is listening. That also keeps this
  /// testable without an active query context.
  ///
  /// Construct through [#skipped] / [#applied] rather than the canonical constructor: the two
  /// cost components are adjacent doubles, and the named factories make transposing them
  /// impossible at the only call sites that matter.
  ///
  /// @param plan          the plan to continue planning with: reordered when the phase ran, the
  ///                      original otherwise
  /// @param skipReason    `null` when the phase actually ran; otherwise why it did not
  /// @param joinCount     joins found in the tree, or -1 when never established
  /// @param estCostBefore cumulative cost before reordering, or -1 when not collected
  /// @param estCostAfter  cumulative cost after reordering, or -1 when not collected
  /// @param reorderTimeMs wall-clock time the reorder pass took
  /// @param planChanged   whether reordering actually produced a different tree
  public record Result(RelNode plan, @Nullable SkipReason skipReason, int joinCount,
                       double estCostBefore, double estCostAfter, long reorderTimeMs,
                       boolean planChanged) {

    static Result skipped(RelNode plan, SkipReason reason, int joinCount) {
      return new Result(plan, reason, joinCount, -1, -1, 0, false);
    }

    static Result applied(RelNode plan, int joinCount, double estCostBefore, double estCostAfter,
        long reorderTimeMs, boolean planChanged) {
      return new Result(plan, null, joinCount, estCostBefore, estCostAfter, reorderTimeMs, planChanged);
    }

    public boolean isApplied() {
      return skipReason == null;
    }

    /// Renders this outcome as the value of the `joinReorder` response-metadata entry.
    ///
    /// Deliberately small and fixed-shape: it is attached per query, so it must not grow with the
    /// plan. Costs are omitted when the phase never ran, when the caller did not ask for feedback,
    /// or when Calcite could not supply one, because a sentinel there would read as a real
    /// estimate of zero.
    public ObjectNode toJson() {
      ObjectNode node = JsonUtils.newObjectNode();
      node.put("outcome", isApplied() ? "APPLIED" : "SKIPPED");
      if (skipReason != null) {
        node.put("reason", skipReason.name());
      }
      if (joinCount >= 0) {
        node.put("numJoins", joinCount);
      }
      if (isApplied()) {
        node.put("planChanged", planChanged);
        if (estCostBefore >= 0) {
          node.put("estimatedCostBefore", estCostBefore);
        }
        if (estCostAfter >= 0) {
          node.put("estimatedCostAfter", estCostAfter);
        }
        node.put("reorderTimeMs", reorderTimeMs);
      }
      return node;
    }
  }

  /// Cumulative cost of the whole subtree, as the row-dominated term.
  ///
  /// Deliberately NOT the root's row count. Reordering changes the size of the INTERMEDIATE joins,
  /// which is exactly what the root hides: a `COUNT(*)` query has a root row count of 1 and a
  /// `LIMIT 20` query has 20, whatever happens underneath. Cumulative cost aggregates the subtree,
  /// so it moves when the join order moves.
  ///
  /// Returns -1 when the cost is unavailable, which Calcite permits.
  private static double estimatedCost(RelNode plan) {
    try {
      // Fetched fresh: reorder() invalidates the cluster's cached metadata query.
      RelOptCost cost = plan.getCluster().getMetadataQuery().getCumulativeCost(plan);
      if (cost == null) {
        return -1;
      }
      // Calcite costs legitimately carry +Infinity; Jackson would render that as a non-numeric
      // token, so it is reported the same way as an unavailable cost: omitted.
      double rows = cost.getRows();
      return Double.isFinite(rows) ? rows : -1;
    } catch (RuntimeException e) {
      // Diagnostics must never be the reason a query fails.
      LOGGER.debug("Could not compute cumulative cost for join-reorder feedback", e);
      return -1;
    }
  }

  private static RelNode reorder(RelNode logicalPlan) {
    RelOptCluster cluster = logicalPlan.getCluster();
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        // Collect the contiguous inner-join tree into a single MultiJoin, merging any intervening
        // Project / Filter nodes into it so joins separated by a Project (e.g. the projection that
        // exposes a join key) are still collected. Mirrors Calcite's Programs.heuristicJoinOrder.
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .addRuleInstance(CoreRules.PROJECT_MULTI_JOIN_MERGE)
        .addRuleInstance(CoreRules.FILTER_MULTI_JOIN_MERGE)
        // ...then re-derive the join order from statistics-backed row-count estimates.
        .addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE)
        .build();
    // The new RelNodes are built through the cluster's planner-agnostic factories and therefore
    // stay in the same cluster, which is what preserves the statistics-backed metadata provider:
    // MULTI_JOIN_OPTIMIZE compares orderings through mq.getCumulativeCost, which resolves the cost
    // via rel.getCluster().getPlanner() -- not through this sub-planner. So this sub-planner's cost
    // factory would never be consulted, and none is supplied; the ordering is decided by the
    // statistics-backed row counts that reach the cost through that cluster planner.
    HepPlanner planner = new HepPlanner(program, cluster.getPlanner().getContext());
    planner.setRoot(logicalPlan);
    return planner.findBestExp();
  }

  /// Returns the [SkipReason] if the plan should be skipped, or `null` if all gates pass and the
  /// reorder phase should run, based on a [GateVisitor] that has already walked the plan.
  ///
  /// The order is fixed so the reported reason is deterministic rather than an artefact of
  /// traversal order, and runs cheapest-and-most-actionable first.
  private static SkipReason skipReason(GateVisitor visitor, int maxJoins) {
    if (visitor._joinCount < 2) {
      return SkipReason.TOO_FEW_JOINS;
    }
    if (visitor._joinCount > maxJoins) {
      return SkipReason.TOO_MANY_JOINS;
    }
    if (visitor._disqualifyReason != null) {
      return visitor._disqualifyReason;
    }
    if (!visitor._allLeavesHaveKnownRowCount) {
      return SkipReason.UNKNOWN_ROW_COUNT;
    }
    return null;
  }

  /// Single-pass collector for the eligibility gates. Not thread-safe; a fresh instance is used per
  /// [#maybeReorder] call.
  ///
  /// The walk always completes rather than short-circuiting on the first disqualification, so that
  /// `_joinCount` is the real number of joins. It is reported to operators, who use it to retune
  /// the cap — a count that stopped early would be an arbitrary prefix, and for `TOO_MANY_JOINS`
  /// would always read as exactly the cap plus one. One extra walk of an already-built tree is
  /// negligible next to the metadata traversals planning has already done.
  private static final class GateVisitor {
    private int _joinCount;
    /// First per-join disqualification found, or `null`. First rather than last so the reason is
    /// stable under changes to traversal order.
    @Nullable
    private SkipReason _disqualifyReason;
    private boolean _allLeavesHaveKnownRowCount = true;

    private void visit(RelNode node) {
      if (node instanceof Join) {
        Join join = (Join) node;
        _joinCount++;
        disqualify(gateJoin(join));
      } else if (node.getInputs().isEmpty()) {
        // Gate on LEAVES, not on scans: a non-scan leaf (Uncollect, Values, a table function) has
        // no statistics and would be costed from a Calcite default, which is the mixed
        // known/guessed case this phase must not reorder on.
        if (!(node instanceof TableScan) || !hasKnownRowCount((TableScan) node)) {
          _allLeavesHaveKnownRowCount = false;
        }
      }
      for (RelNode input : node.getInputs()) {
        visit(input);
      }
    }

    private void disqualify(@Nullable SkipReason reason) {
      if (reason != null && _disqualifyReason == null) {
        _disqualifyReason = reason;
      }
    }

    /// Returns why this join disqualifies the phase, or `null` if it does not.
    @Nullable
    private static SkipReason gateJoin(Join join) {
      if (join.getJoinType() != JoinRelType.INNER) {
        // v1 scope: any non-inner join in the tree disqualifies the whole phase.
        return SkipReason.NON_INNER_JOIN;
      }
      if (PinotHintOptions.JoinHintOptions.getJoinHintOptions(join) != null) {
        // A join hint signals explicit user intent; skip the whole phase in v1.
        return SkipReason.HINTED_JOIN;
      }
      if (!join.getVariablesSet().isEmpty() || RexUtil.containsCorrelation(join.getCondition())) {
        // MultiJoin has no variablesSet component, so JoinToMultiJoinRule would drop this join's
        // correlation state without complaint and reordering could move a reference away from what
        // binds it.
        return SkipReason.CORRELATED_JOIN;
      }
      return null;
    }

    /// Returns `true` if the scan's backing table is a [PinotTable] with a row count usable for
    /// costing. Delegates to [PinotTable#getUsableRowCount()] rather than re-deriving the
    /// confidence policy: that method is the single source of truth, so this gate cannot drift out
    /// of step with the row counts the cost model actually sees.
    private static boolean hasKnownRowCount(TableScan scan) {
      PinotTable pinotTable = scan.getTable().unwrap(PinotTable.class);
      return pinotTable != null && pinotTable.getUsableRowCount() >= 0;
    }
  }
}
