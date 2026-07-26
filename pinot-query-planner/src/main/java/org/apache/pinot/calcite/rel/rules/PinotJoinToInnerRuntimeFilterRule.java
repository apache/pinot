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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions.RuntimeFilterMode;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.calcite.rel.logical.RuntimeFilterRel;
import org.apache.pinot.calcite.rel.logical.RuntimeFilterRel.RuntimeFilterType;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Special Pinot rule that adds an additive probe-side runtime filter to an equi-INNER-JOIN.
 *
 * <p>This is the INNER-join counterpart of {@link PinotJoinToDynamicBroadcastRule}. The SEMI rule
 * <em>replaces</em> the join with a leaf {@code IN} filter (sound only because a semi-join emits left
 * columns only). An inner join projects <em>both</em> sides, so the join must keep running as a real
 * intermediate-stage hash join. This rule therefore keeps the join and both of its exchanges intact
 * and only <em>adds</em> a {@link RuntimeFilterRel} on top of the probe (left) leaf subtree, carrying a
 * {@code PIPELINE_BREAKER} exchange of the build-side join keys. At runtime the probe leaf
 * scan ANDs in a no-false-negative reducer (exact {@code IN} and/or bloom) built from those keys,
 * dropping probe rows that cannot match before they are shuffled into the join.
 *
 * <p>Before (after exchange insertion):
 * <pre>
 *           [ Inner Join ]
 *           /            \
 *      [xChange L]    [xChange R]
 *         /                \
 *    [probe leaf]      [build subtree]
 * </pre>
 * After:
 * <pre>
 *           [ Inner Join ]                         (unchanged — still shuffles both sides)
 *           /            \
 *      [xChange L]    [xChange R]
 *         /                \
 *   [RuntimeFilter]    [build subtree]
 *      /        \
 * [probe leaf]  [PIPELINE_BREAKER xChange]
 *                      |
 *                [build keys: Project(rightKeys) -> Filter(notNull) -> limit(maxBuildRows + 1)]
 * </pre>
 *
 * <p>Disabled by default; enabled per-cluster/query (then defaulting to {@code AUTO}) or per-join via
 * the {@code runtime_filter} join hint. Restricted to a leaf-pushable probe (TableScan with optional
 * single-in single-out Project/Filter). Multi-key joins use an exact IN per key; the bloom tier is
 * single-key only.
 */
public class PinotJoinToInnerRuntimeFilterRule extends RelOptRule {
  /**
   * Placeholder instance registered in {@code PinotQueryRuleSets#POST_LOGICAL_RULES} to fix this rule's
   * position in the post-logical sequence (right after the SEMI dynamic-broadcast rule).
   * {@code QueryEnvironment#getTraitProgram} swaps it for a per-query instance carrying the resolved
   * enable flag, because a Calcite rule cannot read query options at match time so the cluster/query-level
   * enable state must be injected via the constructor.
   */
  public static final PinotJoinToInnerRuntimeFilterRule INSTANCE =
      new PinotJoinToInnerRuntimeFilterRule(PinotRuleUtils.PINOT_REL_FACTORY, false);

  private final boolean _queryLevelEnabled;

  public PinotJoinToInnerRuntimeFilterRule(RelBuilderFactory factory, boolean queryLevelEnabled) {
    super(operand(Join.class, any()), factory, null);
    _queryLevelEnabled = queryLevelEnabled;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(0);

    // Resolve enablement: an explicit hint wins; otherwise fall back to the cluster/query-level default.
    RuntimeFilterMode hintMode = PinotHintOptions.JoinHintOptions.getRuntimeFilterMode(join);
    boolean enabled = hintMode != null ? hintMode != RuntimeFilterMode.OFF : _queryLevelEnabled;
    if (!enabled) {
      return false;
    }

    // Lookup joins keep the right table local; a runtime filter does not apply.
    if (PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join)) {
      return false;
    }

    // Only equi-INNER joins with at least one equi-key (non-equi conditions are allowed — the join still
    // runs, we only reduce the probe by the equi-keys). Multi-key uses an exact IN per key, which is a
    // sound reducer: a matching probe row equals some build tuple, so it passes every per-key IN; the
    // (less selective) per-key form may admit cross-key false positives, which the real join discards.
    JoinInfo joinInfo = join.analyzeCondition();
    if (join.getJoinType() != JoinRelType.INNER || joinInfo.leftKeys.isEmpty()) {
      return false;
    }

    // Both sides must already be behind exchanges (i.e. exchange insertion has run), and the probe
    // (left) subtree must be pushable to a single leaf scan. Reject if already rewritten (idempotence).
    RelNode left = PinotRuleUtils.unboxRel(join.getLeft());
    RelNode right = PinotRuleUtils.unboxRel(join.getRight());
    if (!(left instanceof Exchange) || !(right instanceof Exchange)) {
      return false;
    }
    RelNode probe = PinotRuleUtils.unboxRel(left.getInput(0));
    if (probe instanceof RuntimeFilterRel) {
      return false;
    }
    return PinotRuleUtils.canPushRuntimeFilterToLeaf(probe);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    Exchange left = (Exchange) PinotRuleUtils.unboxRel(join.getLeft());
    Exchange right = (Exchange) PinotRuleUtils.unboxRel(join.getRight());

    JoinInfo joinInfo = join.analyzeCondition();
    List<Integer> probeKeys = joinInfo.leftKeys;
    List<Integer> rightKeys = joinInfo.rightKeys;

    RelNode probe = left.getInput();
    RelNode buildInput = right.getInput();

    // Build the non-null build-key sub-tree:
    //   Project(rightKeys) -> Filter(key IS NOT NULL ...) -> limit(maxBuildRows + 1)
    // Intentionally no DISTINCT here: a DISTINCT would introduce a plain Calcite LogicalAggregate, which
    // is not converted at this point because the aggregate-conversion rules run earlier than this rule,
    // and the leaf-side IN/bloom dedups the keys anyway. The leaf fetch cap of maxBuildRows + 1 bounds the
    // pipeline-breaker memory at the source; the runtime abandons the filter if the cap was hit.
    RelBuilder builder = call.builder();
    builder.push(buildInput);
    List<RexNode> keyRefs = new ArrayList<>(rightKeys.size());
    for (int rightKey : rightKeys) {
      keyRefs.add(builder.field(rightKey));
    }
    builder.project(keyRefs);
    List<RexNode> notNullConditions = new ArrayList<>(rightKeys.size());
    for (int i = 0; i < rightKeys.size(); i++) {
      notNullConditions.add(builder.isNotNull(builder.field(i)));
    }
    builder.filter(notNullConditions);
    // Cap the build-key broadcast. The leaf abandons the filter if this cap is hit (the truncated key set
    // would be incomplete), so both sides MUST use the same constant.
    builder.limit(0, CommonConstants.Broker.DEFAULT_RUNTIME_FILTER_MAX_BUILD_ROWS + 1);
    RelNode buildKeys = builder.build();

    // Wrap in a PIPELINE_BREAKER exchange: colocated -> SINGLETON (pre-partitioned), else BROADCAST.
    boolean colocatedByJoinKeys = Boolean.TRUE.equals(PinotHintOptions.JoinHintOptions.isColocatedByJoinKeys(join));
    PinotLogicalExchange buildKeyExchange;
    if (colocatedByJoinKeys) {
      buildKeyExchange = PinotLogicalExchange.create(buildKeys, RelDistributions.SINGLETON,
          PinotRelExchangeType.PIPELINE_BREAKER, true);
    } else {
      buildKeyExchange = PinotLogicalExchange.create(buildKeys, RelDistributions.BROADCAST_DISTRIBUTED,
          PinotRelExchangeType.PIPELINE_BREAKER, false);
    }

    // The projected build keys occupy positions 0..n-1 in the pipeline-breaker schema.
    List<Integer> buildKeyPositions = new ArrayList<>(rightKeys.size());
    for (int i = 0; i < rightKeys.size(); i++) {
      buildKeyPositions.add(i);
    }

    RuntimeFilterType filterType = resolveFilterType(join);
    RuntimeFilterRel runtimeFilter =
        RuntimeFilterRel.create(probe, buildKeyExchange, probeKeys, buildKeyPositions, filterType);

    // Keep the join and both exchanges intact; only swap the left exchange's input with the runtime
    // filter wrapper (which is a pass-through for the probe rows).
    RelNode newLeft = left.copy(left.getTraitSet(), List.of(runtimeFilter));
    call.transformTo(join.copy(join.getTraitSet(), join.getCondition(), newLeft, join.getRight(), join.getJoinType(),
        join.isSemiJoinDone()));
  }

  private static RuntimeFilterType resolveFilterType(Join join) {
    RuntimeFilterMode hintMode = PinotHintOptions.JoinHintOptions.getRuntimeFilterMode(join);
    if (hintMode == null) {
      // Enabled via the cluster/query-level default without an explicit mode -> AUTO (tiered).
      return RuntimeFilterType.AUTO;
    }
    switch (hintMode) {
      case IN:
        return RuntimeFilterType.IN;
      case BLOOM:
        return RuntimeFilterType.BLOOM;
      case AUTO:
        return RuntimeFilterType.AUTO;
      default:
        // OFF never reaches here (matches() returns false).
        throw new IllegalStateException("Unexpected runtime filter mode: " + hintMode);
    }
  }
}
