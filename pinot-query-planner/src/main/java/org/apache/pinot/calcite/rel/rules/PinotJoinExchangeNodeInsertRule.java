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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;


/// Special rule for Pinot, this rule is fixed to always insert exchange after JOIN node.
public class PinotJoinExchangeNodeInsertRule extends RelOptRule {
  public static final PinotJoinExchangeNodeInsertRule INSTANCE =
      new PinotJoinExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotJoinExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(Join.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(0);
    return !PinotRuleUtils.isExchange(join.getLeft()) && !PinotRuleUtils.isExchange(join.getRight());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelNode left = PinotRuleUtils.unboxRel(join.getInput(0));
    RelNode right = PinotRuleUtils.unboxRel(join.getInput(1));
    JoinInfo joinInfo = join.analyzeCondition();
    Map<String, String> joinHintOptions = PinotHintOptions.JoinHintOptions.getJoinHintOptions(join);
    PinotHintOptions.DistributionType leftDistributionType;
    PinotHintOptions.DistributionType rightDistributionType;
    if (joinHintOptions != null) {
      leftDistributionType = PinotHintOptions.JoinHintOptions.getLeftDistributionType(joinHintOptions);
      rightDistributionType = PinotHintOptions.JoinHintOptions.getRightDistributionType(joinHintOptions);
    } else {
      leftDistributionType = null;
      rightDistributionType = null;
    }
    RelNode newLeft;
    RelNode newRight;
    if (PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join)) {
      if (leftDistributionType == null) {
        // By default, use local distribution for the left side
        leftDistributionType = PinotHintOptions.DistributionType.LOCAL;
      }
      newLeft = createExchangeForLookupJoin(leftDistributionType, joinInfo.leftKeys, left);
      Preconditions.checkArgument(rightDistributionType == null,
          "Right distribution type hint is not supported for lookup join");
      newRight = right;
    } else if (joinInfo.leftKeys.isEmpty() && join.getJoinType() == JoinRelType.FULL
        && leftDistributionType == null && rightDistributionType == null) {
      // FULL OUTER JOIN with no equi keys: use hash with empty key to explicitly route all data to one destination.
      // DispatchablePlanVisitor sets requireSingleton on the join stage so WorkerManager picks a single random
      // worker, avoiding hotspots.
      newLeft = PinotLogicalExchange.create(left, RelDistributions.hash(List.of()));
      newRight = PinotLogicalExchange.create(right, RelDistributions.hash(List.of()));
    } else {
      // Hash join
      // Force pre-partitioned exchange when colocated join hint is provided
      Boolean prePartitioned = PinotHintOptions.JoinHintOptions.isColocatedByJoinKeys(join);
      if (leftDistributionType == null) {
        leftDistributionType = inferLeftDistributionType(joinInfo, join.getJoinType());
      }
      if (rightDistributionType == null) {
        rightDistributionType = inferRightDistributionType(joinInfo, join.getJoinType());
      }
      validateDistributionTypes(leftDistributionType, rightDistributionType, isReplicatedTableScan(left),
          isReplicatedTableScan(right), joinInfo, join.getJoinType());
      newLeft = createExchangeForHashJoin(leftDistributionType, joinInfo.leftKeys, left, prePartitioned);
      newRight = createExchangeForHashJoin(rightDistributionType, joinInfo.rightKeys, right, prePartitioned);
    }

    // TODO: Consider creating different JOIN Rel for each join strategy
    if (join instanceof LogicalAsofJoin) {
      // Note that we don't use the MATCH_CONDITION in an ASOF JOIN to determine the distribution, only the join keys
      // in the ON clause of the ASOF JOIN.
      call.transformTo(((LogicalAsofJoin) join).copy(join.getTraitSet(), List.of(newLeft, newRight)));
    } else {
      call.transformTo(join.copy(join.getTraitSet(), join.getCondition(), newLeft, newRight, join.getJoinType(),
          join.isSemiJoinDone()));
    }
  }

  private static PinotLogicalExchange createExchangeForLookupJoin(PinotHintOptions.DistributionType distributionType,
      List<Integer> keys, RelNode child) {
    switch (distributionType) {
      case LOCAL:
        // NOTE: We use SINGLETON to represent local distribution. Add keys to the exchange because we might want to
        //       switch it to HASH distribution to increase parallelism. See MailboxAssignmentVisitor for details.
        return PinotLogicalExchange.create(child, RelDistributions.SINGLETON, keys, null);
      case HASH:
        Preconditions.checkArgument(!keys.isEmpty(), "Hash distribution requires join keys");
        return PinotLogicalExchange.create(child, RelDistributions.hash(keys));
      case RANDOM:
        return PinotLogicalExchange.create(child, RelDistributions.RANDOM_DISTRIBUTED);
      default:
        throw new IllegalArgumentException("Unsupported distribution type: " + distributionType + " for lookup join");
    }
  }

  /// Rejects the pairs of distribution types that cannot produce the join's rows, which are otherwise silently
  /// answered wrong. A pair is workable only if every pair of rows that could match meets on one worker, and only if
  /// each row is emitted by one worker.
  ///
  /// Broadcasting a side gives every worker all of its rows, so it stands in for any partitioning of the other side. A
  /// LOCAL scan of a table hinted `is_replicated='true'` does the same, since its leaf hands every worker the whole
  /// table. Two such sides give every worker all the rows of both, and each joined row is then emitted once per worker,
  /// whether the sides are broadcast, replicated tables read locally, or one of each. Broadcasting neither leaves both
  /// sides partitioned, and they only line up when both are partitioned the same way on the join keys, which also
  /// requires the join to have keys to partition on. LOCAL on both sides is accepted on the user's word that the two
  /// inputs are co-partitioned, like the colocated join hint: nothing here can check it.
  ///
  /// A join that decides a left row's output from the right rows it saw -- LEFT emits the left rows that matched
  /// nothing, SEMI and ANTI emit a left row on the presence or absence of a match, and ASOF / LEFT_ASOF pick the
  /// closest match -- can only decide on a worker that holds every right row. So the broadcast side must be the right
  /// one; RIGHT is the mirror image. SEMI is the exception when the right side is hashed on the join keys: every right
  /// row sharing a left row's key is on the one worker that key hashes to, so only that worker can emit the left row,
  /// and it does exactly when a match exists. ANTI and LEFT get no such exception, since every other worker would emit
  /// the left row as unmatched. A FULL join emits the unmatched rows of both sides, so with keys it cannot use a
  /// broadcast side at all, and without keys it runs on a single worker, where only an exchange that delivers every
  /// row there (RANDOM or BROADCAST) is workable.
  @VisibleForTesting
  static void validateDistributionTypes(PinotHintOptions.DistributionType leftDistributionType,
      PinotHintOptions.DistributionType rightDistributionType, boolean leftReplicated, boolean rightReplicated,
      JoinInfo joinInfo, JoinRelType joinType) {
    if (joinType == JoinRelType.FULL && joinInfo.leftKeys.isEmpty()) {
      // DispatchablePlanVisitor gives a FULL join without equi keys a single worker, so every row reaches it once.
      Preconditions.checkArgument(isSingletonFeed(leftDistributionType) && isSingletonFeed(rightDistributionType),
          "FULL join without equi keys runs on a single worker, so each side must be distributed RANDOM or BROADCAST "
              + "to reach it, got left: %s, right: %s", leftDistributionType, rightDistributionType);
      return;
    }
    boolean leftWhole = holdsWholeSide(leftDistributionType, leftReplicated);
    boolean rightWhole = holdsWholeSide(rightDistributionType, rightReplicated);
    Preconditions.checkArgument(!(leftWhole && rightWhole),
        "Cannot give every worker the whole of both sides of the join (broadcast, or a LOCAL scan of a table hinted "
            + "is_replicated='true'): every worker would emit each joined row, repeating the join result once per "
            + "worker. At most one side can be whole, got left: %s, right: %s", leftDistributionType,
        rightDistributionType);
    if (leftWhole || rightWhole) {
      switch (joinType) {
        case SEMI:
          if (rightDistributionType == PinotHintOptions.DistributionType.HASH && !joinInfo.leftKeys.isEmpty()) {
            // Every right row with a given key is on one worker, so a left row can only be emitted there.
            break;
          }
          Preconditions.checkArgument(rightWhole,
              "SEMI join requires every right row on each worker (broadcast the right side, read a replicated right "
                  + "table locally, or hash the right side on the join keys): a worker decides each left row from "
                  + "the right rows it holds, got left: %s, right: %s", leftDistributionType, rightDistributionType);
          break;
        case LEFT:
        case ANTI:
        case ASOF:
        case LEFT_ASOF:
        case LEFT_MARK:
          Preconditions.checkArgument(rightWhole,
              "%s join requires every right row on each worker (broadcast the right side, or read a replicated right "
                  + "table locally): a worker decides each left row from the right rows it holds, got left: %s, "
                  + "right: %s", joinType, leftDistributionType, rightDistributionType);
          break;
        case RIGHT:
          Preconditions.checkArgument(leftWhole,
              "RIGHT join requires every left row on each worker (broadcast the left side, or read a replicated left "
                  + "table locally): a worker can only tell that a right row matched nothing if it holds every left "
                  + "row, got left: %s, right: %s", leftDistributionType, rightDistributionType);
          break;
        case FULL:
          throw new IllegalArgumentException(
              "FULL join cannot broadcast a side: both sides emit the rows that matched nothing, so neither side can "
                  + "be the partitioned one. Distribute both sides by hash on the join keys.");
        case INNER:
          break;
        default:
          // A join type not listed above may be asymmetric, so reject it rather than accept a whole side unchecked
          throw new IllegalArgumentException("Unsupported join type for a broadcast or replicated side: " + joinType);
      }
      return;
    }
    Preconditions.checkArgument(!joinInfo.leftKeys.isEmpty(),
        "Join without equi keys requires one side to be broadcast: with both sides partitioned, a pair of rows that "
            + "could match may land on different workers, got left: %s, right: %s", leftDistributionType,
        rightDistributionType);
    Preconditions.checkArgument(
        leftDistributionType == rightDistributionType
            && leftDistributionType != PinotHintOptions.DistributionType.RANDOM,
        "Both sides of the join must be distributed the same way on the join keys when neither side is broadcast, "
            + "otherwise rows sharing a key land on different workers, got left: %s, right: %s", leftDistributionType,
        rightDistributionType);
  }

  /// Whether every worker of the join ends up with every row of this side.
  private static boolean holdsWholeSide(PinotHintOptions.DistributionType distributionType, boolean replicated) {
    return distributionType == PinotHintOptions.DistributionType.BROADCAST
        || (replicated && distributionType == PinotHintOptions.DistributionType.LOCAL);
  }

  /// Whether this exchange delivers every row of its side to a single receiving worker.
  private static boolean isSingletonFeed(PinotHintOptions.DistributionType distributionType) {
    return distributionType == PinotHintOptions.DistributionType.RANDOM
        || distributionType == PinotHintOptions.DistributionType.BROADCAST;
  }

  /// Whether `input` is a scan of a table hinted `is_replicated='true'`, reached only through projects, filters and
  /// calcs.
  /// Anything else in between (an exchange, an aggregate, another join) is not assumed to keep the whole table.
  private static boolean isReplicatedTableScan(RelNode input) {
    RelNode node = PinotRuleUtils.unboxRel(input);
    // A Sort is not unwrapped: with a LIMIT or OFFSET it no longer keeps the whole table
    while (node instanceof Project || node instanceof Filter || node instanceof Calc) {
      node = PinotRuleUtils.unboxRel(node.getInput(0));
    }
    return node instanceof TableScan && Boolean.parseBoolean(
        PinotHintStrategyTable.getHintOption(((TableScan) node).getHints(), PinotHintOptions.TABLE_HINT_OPTIONS,
            PinotHintOptions.TableHintOptions.IS_REPLICATED));
  }

  /// For non-equi RIGHT JOINs (no equi keys), the default RANDOM(left)+BROADCAST(right) is incorrect because each
  /// worker independently tracks matched right rows via a local BitSet. Workers that receive only a subset of left rows
  /// will incorrectly emit right rows as unmatched. Invert to BROADCAST(left)+RANDOM(right) so each worker has the
  /// full left table and can correctly determine which of its local right rows are unmatched.
  @VisibleForTesting
  static PinotHintOptions.DistributionType inferLeftDistributionType(JoinInfo joinInfo,
      JoinRelType joinType) {
    if (!joinInfo.leftKeys.isEmpty()) {
      return PinotHintOptions.DistributionType.HASH;
    }
    if (joinType == JoinRelType.RIGHT) {
      return PinotHintOptions.DistributionType.BROADCAST;
    }
    return PinotHintOptions.DistributionType.RANDOM;
  }

  @VisibleForTesting
  static PinotHintOptions.DistributionType inferRightDistributionType(JoinInfo joinInfo,
      JoinRelType joinType) {
    if (!joinInfo.rightKeys.isEmpty()) {
      return PinotHintOptions.DistributionType.HASH;
    }
    if (joinType == JoinRelType.RIGHT) {
      return PinotHintOptions.DistributionType.RANDOM;
    }
    return PinotHintOptions.DistributionType.BROADCAST;
  }

  private static PinotLogicalExchange createExchangeForHashJoin(PinotHintOptions.DistributionType distributionType,
      List<Integer> keys, RelNode child, @Nullable Boolean prePartitioned) {
    switch (distributionType) {
      case LOCAL:
        // NOTE: We use SINGLETON to represent local distribution. Add keys to the exchange because we might want to
        //       switch it to HASH distribution to increase parallelism. See MailboxAssignmentVisitor for details.
        return PinotLogicalExchange.create(child, RelDistributions.SINGLETON, keys, prePartitioned);
      case HASH:
        Preconditions.checkArgument(!keys.isEmpty(), "Hash distribution requires join keys");
        return PinotLogicalExchange.create(child, RelDistributions.hash(keys), prePartitioned);
      case BROADCAST:
        return PinotLogicalExchange.create(child, RelDistributions.BROADCAST_DISTRIBUTED, prePartitioned);
      case RANDOM:
        return PinotLogicalExchange.create(child, RelDistributions.RANDOM_DISTRIBUTED, prePartitioned);
      default:
        throw new IllegalArgumentException("Unsupported distribution type: " + distributionType + " for hash join");
    }
  }
}
