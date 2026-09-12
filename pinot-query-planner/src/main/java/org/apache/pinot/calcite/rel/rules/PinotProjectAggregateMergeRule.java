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
package org.apache.pinot.calcite.rel.rules;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelHintsPropagator;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.ProjectAggregateMergeRule;


/// Pinot customized version of [ProjectAggregateMergeRule] that preserves the matched [Aggregate]'s hints.
///
/// [ProjectAggregateMergeRule] rebuilds the aggregate from scratch with a `RelBuilder`, so the rebuilt node
/// starts out with no hints. Calcite normally repairs this automatically: [RelOptRuleCall#transformTo(RelNode)]
/// defaults to propagating the hints of `rels[0]` (the node the rule matched on) into the new sub-tree. That
/// repair does not apply here, because `rels[0]` is the `Project`, and `aggOptions` hints are attached to the
/// `Aggregate` (see `HintPredicates.AGGREGATE` in
/// [org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable]). The aggregate's hints are therefore dropped
/// silently, and every `aggOptions` option on it is lost — including `is_partitioned_by_group_by_keys`,
/// `is_skip_leaf_stage_group_by`, `is_leaf_return_final_result` and the group-trim options.
///
/// The user-visible symptom is a colocation hint that stops working as soon as a `Project` lands directly above
/// the aggregate. The most common way that happens is a `SUM` over a *nullable* argument:
/// [PinotAggregateReduceFunctionsRule] rewrites it to `$SUM0(x) + COUNT(x)` plus a
/// `CASE(COUNT(x) = 0, NULL, $SUM0(x))` project, which is exactly the pattern this rule matches. With a
/// non-nullable argument the reduction collapses to a bare `$SUM0` with no project, no match, and the hint
/// survives — which is why the problem only shows up on some queries.
///
/// This rule reuses Calcite's transformation verbatim (no forked rule body to drift out of sync) and only
/// re-attaches the hints afterwards. The rule never changes the aggregate's group set, so its hints remain
/// valid for the rebuilt node.
public class PinotProjectAggregateMergeRule extends ProjectAggregateMergeRule {

  public static PinotProjectAggregateMergeRule instanceWithDescription(String description) {
    return new PinotProjectAggregateMergeRule((Config) Config.DEFAULT.withDescription(description));
  }

  private PinotProjectAggregateMergeRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(1);
    if (aggregate.getHints().isEmpty()) {
      super.onMatch(call);
    } else {
      super.onMatch(new HintPreservingRuleCall(call, aggregate));
    }
  }

  /// Delegating [RelOptRuleCall] that re-attaches the matched aggregate's hints to the rebuilt aggregate before
  /// handing the rule's output to the real call.
  ///
  /// The hints are copied verbatim rather than through
  /// [org.apache.calcite.plan.RelOptUtil#propagateRelHints(RelNode, RelNode)]: that helper appends the child
  /// index to each hint's [RelHint#inheritPath] as it descends, so re-propagating on every rule application grows
  /// the inherit path without bound. The rebuilt node then never compares equal to the previous one, the rule
  /// keeps re-firing on its own output, and planning dies with a `StackOverflowError`. Copying the hint list
  /// unchanged makes the rewrite a fixpoint, so the planner terminates after it has been applied once.
  private static class HintPreservingRuleCall extends RelOptRuleCall {
    private final RelOptRuleCall _delegate;
    private final Aggregate _aggregate;

    HintPreservingRuleCall(RelOptRuleCall delegate, Aggregate aggregate) {
      super(delegate.getPlanner(), delegate.getOperand0(), delegate.getRels(), Map.of(), delegate.getParents());
      _delegate = delegate;
      _aggregate = aggregate;
    }

    @Override
    public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv, RelHintsPropagator handler) {
      _delegate.transformTo(rel.accept(new RestoreAggregateHintsShuttle(_aggregate.getHints())), equiv, handler);
    }

    @Nullable
    @Override
    public List<RelNode> getChildRels(RelNode rel) {
      return _delegate.getChildRels(rel);
    }
  }

  /// Attaches the given hints to the topmost [Aggregate] of the tree it visits, which is the aggregate
  /// [ProjectAggregateMergeRule] rebuilt. Nothing below it is touched: the rule reuses the original aggregate's
  /// input unchanged, so the only hintable node it recreates is the aggregate itself.
  private static class RestoreAggregateHintsShuttle extends RelShuttleImpl {
    private final List<RelHint> _hints;
    private boolean _restored;

    RestoreAggregateHintsShuttle(List<RelHint> hints) {
      _hints = hints;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      return restoreHints(aggregate);
    }

    @Override
    public RelNode visit(RelNode other) {
      return other instanceof Aggregate ? restoreHints((Aggregate) other) : super.visit(other);
    }

    private RelNode restoreHints(Aggregate aggregate) {
      if (_restored) {
        return aggregate;
      }
      _restored = true;
      return aggregate.withHints(_hints);
    }
  }
}
