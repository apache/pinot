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
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.Statistic;
import org.apache.pinot.calcite.plan.PinotRelOptCost;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.query.catalog.PinotTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A scoped, gated, cost-based join-reordering phase for the multi-stage query planner.
 *
 * <h3>Scope</h3>
 * <p>This phase runs after the existing logical {@code HepPlanner} programs and before the trait /
 * {@code POST_LOGICAL} / physical phases. It reorders inner-join trees using cardinality estimates
 * sourced from the {@link RelMetadataQuery} that is already backed by
 * {@code org.apache.pinot.calcite.rel.metadata.PinotDefaultRelMetadataProvider} (statistics-driven
 * row counts). The phase is off by default and has zero impact when disabled.
 *
 * <h3>Strategy</h3>
 * <p>Reordering is performed with the canonical Calcite {@code Programs.heuristicJoinOrder} design:
 * a dedicated {@link HepPlanner} runs {@link CoreRules#JOIN_TO_MULTI_JOIN} to collect an inner-join
 * tree into a single {@code MultiJoin}, then {@link CoreRules#MULTI_JOIN_OPTIMIZE}
 * ({@code LoptOptimizeJoinRule}) to re-derive the join order from {@code mq.getRowCount()} estimates.
 * The {@link HepPlanner} is created over the SAME {@link RelOptCluster} as the input tree (via the
 * cluster's planner-agnostic factories), so the statistics-backed metadata provider installed on the
 * cluster is reused directly — no cross-cluster tree copy is needed.
 *
 * <p>A true scoped {@code VolcanoPlanner} (with a deep cross-cluster tree copy) was considered but
 * rejected: a {@link RelNode} tree is bound to its originating cluster, that cluster is bound to the
 * logical {@code HepPlanner}, and Calcite 1.42 offers no clean, supported utility to re-host an
 * arbitrary rel tree in a fresh cluster owned by a second planner. The Hep + {@code MultiJoin} path
 * is the documented, well-tested approach and keeps the cost signal (our row counts) intact. This
 * class is a facade ({@link #maybeReorder(RelNode)}) so the internal strategy can be swapped later
 * without touching callers.
 *
 * <h3>Eligibility gates</h3>
 * <p>The phase is skipped (the input tree is returned unchanged) unless ALL of the following hold:
 * <ul>
 *   <li>The tree contains at least one {@link Join} (fast path: no join ⇒ nothing to do).</li>
 *   <li>There are at least two joins (a single join only swaps sides; v1 keeps the bar at two).</li>
 *   <li>Every {@link Join} in the tree is an {@link JoinRelType#INNER} join (v1 scope).</li>
 *   <li>No {@link Join} carries a Pinot {@code joinOptions} hint — a hint signals explicit user
 *       intent, so the whole phase is skipped in v1 (per-join veto is a later task).</li>
 *   <li>Every {@link TableScan} in the tree has a known row count. This is checked by unwrapping
 *       the scan's {@code RelOptTable} to {@link PinotTable} and asking its
 *       {@link Statistic#getRowCount()} (which returns {@code null} when statistics are absent or
 *       LOW-confidence) — {@code RelOptTable.getRowCount()} cannot be used because it returns a
 *       primitive that silently falls back to a Calcite default. Mixed known/guessed cardinalities
 *       are the dangerous case, so ALL scans must be known; otherwise reorder is noise and skipped.</li>
 * </ul>
 *
 * <h3>Fallback / robustness</h3>
 * <p>{@link #maybeReorder(RelNode)} never throws: any unexpected error is caught, logged at WARN,
 * and the original (un-reordered) plan is returned. The reorder phase must never fail a query.
 *
 * <h3>Thread-safety</h3>
 * <p>Stateless aside from a static {@link Logger}. A new {@link HepPlanner} is created per invocation
 * on the planner thread, so the class is safe for concurrent use across queries.
 */
public final class JoinReorderOptimizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(JoinReorderOptimizer.class);

  private JoinReorderOptimizer() {
  }

  /**
   * Reorders the inner-join tree of {@code logicalPlan} when the eligibility gates pass, otherwise
   * returns {@code logicalPlan} unchanged. Never throws — on any error the original plan is returned.
   *
   * @param logicalPlan the logical plan emitted by the preceding {@code HepPlanner} phases; must
   *                    belong to a cluster whose metadata provider supplies statistics-backed row
   *                    counts
   * @return the (possibly) reordered plan, or {@code logicalPlan} when the phase is skipped or fails
   */
  public static RelNode maybeReorder(RelNode logicalPlan) {
    try {
      if (!isEligible(logicalPlan)) {
        return logicalPlan;
      }
      return reorder(logicalPlan);
    } catch (Throwable t) {
      // Robustness: a failed reorder must never fail the query. Fall back to the original plan.
      LOGGER.warn("Join reorder phase failed; continuing with the un-reordered plan", t);
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

  /**
   * Walks the plan once and evaluates every eligibility gate. See the class Javadoc for the full
   * list. Returns {@code true} only when reordering is both safe and useful.
   */
  private static boolean isEligible(RelNode root) {
    GateVisitor visitor = new GateVisitor();
    visitor.visit(root);
    if (visitor._disqualified) {
      return false;
    }
    if (visitor._joinCount < 2) {
      // No join, or a single join (which would only swap sides): nothing useful to reorder in v1.
      return false;
    }
    if (!visitor._sawTableScan || !visitor._allScansHaveKnownRowCount) {
      // Without a known row count on every scan, the row-count signal is unreliable.
      return false;
    }
    return true;
  }

  /**
   * Single-pass collector for the eligibility gates. Not thread-safe; a fresh instance is used per
   * {@link #isEligible(RelNode)} call.
   */
  private static final class GateVisitor {
    private int _joinCount;
    private boolean _disqualified;
    private boolean _sawTableScan;
    private boolean _allScansHaveKnownRowCount = true;

    private void visit(RelNode node) {
      if (_disqualified) {
        return;
      }
      if (node instanceof Join) {
        Join join = (Join) node;
        _joinCount++;
        if (join.getJoinType() != JoinRelType.INNER) {
          // v1 scope: any non-inner join in the tree disqualifies the whole phase.
          _disqualified = true;
          return;
        }
        if (PinotHintOptions.JoinHintOptions.getJoinHintOptions(join) != null) {
          // A join hint signals explicit user intent; skip the whole phase in v1.
          _disqualified = true;
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

    /**
     * Returns {@code true} if the scan's backing table is a {@link PinotTable} whose
     * {@link Statistic#getRowCount()} is non-null (statistics present and at least ESTIMATED
     * confidence). A non-Pinot table or an absent/LOW-confidence row count counts as unknown.
     */
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
