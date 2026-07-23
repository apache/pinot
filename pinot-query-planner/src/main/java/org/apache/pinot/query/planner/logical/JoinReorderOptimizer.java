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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.Statistic;
import org.apache.pinot.calcite.plan.PinotRelOptCost;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.query.catalog.PinotTable;
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
/// class is a facade ([#maybeReorder(RelNode, int)]) so the internal strategy can be swapped
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
/// - Every [TableScan] in the tree has a known row count. This is checked by unwrapping
///   the scan's `RelOptTable` to [PinotTable] and asking its
///   [Statistic#getRowCount()] (which returns `null` when statistics are absent or
///   LOW-confidence) — `RelOptTable.getRowCount()` cannot be used because it returns a
///   primitive that silently falls back to a Calcite default. Mixed known/guessed cardinalities
///   are the dangerous case, so ALL scans must be known; otherwise reorder is noise and skipped.
///
/// ### Skip-reason visibility
/// Each skip path is identified by a [SkipReason] value that is logged at DEBUG. This makes
/// it straightforward to trace why a particular query was not reordered without enabling verbose
/// logging in production.
///
/// ### Wall-clock observability
/// The Hep + `LoptOptimizeJoinRule` strategy is a single, deterministic pass — it is not
/// an exponential search — so a hard mid-flight timeout is not required. Instead, elapsed time is
/// measured around the reorder call and a WARN is emitted when it exceeds
/// [#SLOW_REORDER_WARN_THRESHOLD_MS] (100 ms). This is the canary that signals when a
/// budget / interrupt mechanism becomes necessary.
///
/// ### Fallback / robustness
/// [#maybeReorder(RelNode, int)] never throws: any unexpected error is caught, logged at
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
  enum SkipReason {
    /// Plan has fewer than 2 joins — nothing useful to reorder.
    NO_JOINS,
    /// Plan contains a non-INNER join; v1 scope restricts to inner joins only.
    NON_INNER_JOIN,
    /// A join carries an explicit hint; user intent takes precedence.
    HINTED_JOIN,
    /// At least one table scan has no known row count; the cost signal is unreliable.
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
  /// @param maxJoins    maximum number of joins allowed in the plan for the reorder phase to run;
  ///                    plans with more joins are returned unchanged
  /// @return the (possibly) reordered plan, or `logicalPlan` when the phase is skipped or fails
  public static RelNode maybeReorder(RelNode logicalPlan, int maxJoins) {
    try {
      GateVisitor visitor = new GateVisitor(maxJoins);
      visitor.visit(logicalPlan);
      SkipReason skipReason = skipReason(visitor);
      if (skipReason != null) {
        LOGGER.debug("Join reorder phase skipped: {}", skipReason);
        return logicalPlan;
      }
      long startMs = System.currentTimeMillis();
      RelNode result = reorder(logicalPlan);
      long elapsedMs = System.currentTimeMillis() - startMs;
      if (elapsedMs > SLOW_REORDER_WARN_THRESHOLD_MS) {
        LOGGER.warn("Join reorder phase took {}ms (join count: {}); consider a budget/interrupt "
            + "mechanism if this fires regularly.", elapsedMs, visitor._joinCount);
      }
      return result;
    } catch (Exception | StackOverflowError t) {
      // Robustness: a failed reorder must never fail the query (StackOverflowError covers very
      // deep join trees). Other Errors (OOM etc.) propagate — swallowing them would hide real
      // JVM-level problems. Fall back to the original plan.
      LOGGER.warn("Join reorder phase failed ({}: {}); continuing with the un-reordered plan",
          SkipReason.ERROR, t.getMessage(), t);
      return logicalPlan;
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
    // Drive the dedicated Hep sub-phase with Pinot's rows-dominated cost factory so any cost
    // comparison uses the same (rows, cpu, io) semantics as the rest of CBO. The new RelNodes are
    // built through the cluster's planner-agnostic factories and therefore stay in the same cluster,
    // preserving the statistics-backed metadata provider.
    HepPlanner planner = new HepPlanner(program, cluster.getPlanner().getContext(), false,
        null, PinotRelOptCost.Factory.INSTANCE);
    planner.setRoot(logicalPlan);
    return planner.findBestExp();
  }

  /// Returns the [SkipReason] if the plan should be skipped, or `null` if all gates
  /// pass and the reorder phase should run, based on a [GateVisitor] that has already walked
  /// the plan.
  private static SkipReason skipReason(GateVisitor visitor) {
    if (visitor._disqualified) {
      return visitor._disqualifyReason;
    }
    if (visitor._joinCount < 2) {
      return SkipReason.NO_JOINS;
    }
    if (!visitor._sawTableScan || !visitor._allScansHaveKnownRowCount) {
      return SkipReason.UNKNOWN_ROW_COUNT;
    }
    return null;
  }

  /// Single-pass collector for the eligibility gates. Not thread-safe; a fresh instance is used per
  /// [#skipReason] call.
  private static final class GateVisitor {
    private final int _maxJoins;
    private int _joinCount;
    private boolean _disqualified;
    private SkipReason _disqualifyReason;
    private boolean _sawTableScan;
    private boolean _allScansHaveKnownRowCount = true;

    GateVisitor(int maxJoins) {
      _maxJoins = maxJoins;
    }

    private void visit(RelNode node) {
      if (_disqualified) {
        return;
      }
      if (node instanceof Join) {
        Join join = (Join) node;
        _joinCount++;
        if (_joinCount > _maxJoins) {
          _disqualified = true;
          _disqualifyReason = SkipReason.TOO_MANY_JOINS;
          return;
        }
        if (join.getJoinType() != JoinRelType.INNER) {
          // v1 scope: any non-inner join in the tree disqualifies the whole phase.
          _disqualified = true;
          _disqualifyReason = SkipReason.NON_INNER_JOIN;
          return;
        }
        if (PinotHintOptions.JoinHintOptions.getJoinHintOptions(join) != null) {
          // A join hint signals explicit user intent; skip the whole phase in v1.
          _disqualified = true;
          _disqualifyReason = SkipReason.HINTED_JOIN;
          return;
        }
      } else if (node instanceof TableScan) {
        _sawTableScan = true;
        if (!hasKnownRowCount((TableScan) node)) {
          _allScansHaveKnownRowCount = false;
        }
      }
      for (RelNode input : node.getInputs()) {
        visit(input);
        if (_disqualified) {
          return;
        }
      }
    }

    /// Returns `true` if the scan's backing table is a [PinotTable] whose
    /// [Statistic#getRowCount()] is non-null (statistics present and at least ESTIMATED
    /// confidence). A non-Pinot table or an absent/LOW-confidence row count counts as unknown.
    private static boolean hasKnownRowCount(TableScan scan) {
      PinotTable pinotTable = scan.getTable().unwrap(PinotTable.class);
      if (pinotTable == null) {
        return false;
      }
      Statistic statistic = pinotTable.getStatistic();
      return statistic != null && statistic.getRowCount() != null;
    }
  }
}
