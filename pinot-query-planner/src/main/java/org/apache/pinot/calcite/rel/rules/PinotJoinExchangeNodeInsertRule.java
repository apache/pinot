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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
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
      validateDistributionTypes(leftDistributionType, rightDistributionType, joinInfo, join.getJoinType());
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
  /// Broadcasting a side gives every worker all of its rows, so it stands in for any partitioning of the other side.
  /// Broadcasting both sides gives every worker all the rows of both, and each joined row is then emitted once per
  /// worker. Broadcasting neither leaves both sides partitioned, and they only line up when both are partitioned the
  /// same way on the join keys, which also requires the join to have keys to partition on.
  ///
  /// An outer join additionally emits the rows of its outer side that matched nothing, which a worker can only tell
  /// once it holds every row of the opposite side. So the broadcast side must be the side opposite the outer one, and
  /// a FULL join, whose two sides are both outer, cannot be answered with a broadcast side at all.
  private static void validateDistributionTypes(PinotHintOptions.DistributionType leftDistributionType,
      PinotHintOptions.DistributionType rightDistributionType, JoinInfo joinInfo, JoinRelType joinType) {
    boolean leftBroadcast = leftDistributionType == PinotHintOptions.DistributionType.BROADCAST;
    boolean rightBroadcast = rightDistributionType == PinotHintOptions.DistributionType.BROADCAST;
    if (leftBroadcast && rightBroadcast) {
      throw new IllegalArgumentException(
          "Cannot broadcast both sides of the join: every worker would hold every row of both sides and emit each "
              + "joined row, repeating the join result once per worker. Broadcast at most one side.");
    }
    if (leftBroadcast || rightBroadcast) {
      switch (joinType) {
        case LEFT:
          Preconditions.checkArgument(rightBroadcast,
              "LEFT join requires the right side to be the broadcast one: a worker can only tell that a left row "
                  + "matched nothing if it holds every right row, got left: %s, right: %s", leftDistributionType,
              rightDistributionType);
          break;
        case RIGHT:
          Preconditions.checkArgument(leftBroadcast,
              "RIGHT join requires the left side to be the broadcast one: a worker can only tell that a right row "
                  + "matched nothing if it holds every left row, got left: %s, right: %s", leftDistributionType,
              rightDistributionType);
          break;
        case FULL:
          throw new IllegalArgumentException(
              "FULL join cannot broadcast a side: both sides emit the rows that matched nothing, so neither side can "
                  + "be the partitioned one. Distribute both sides by hash on the join keys.");
        default:
          break;
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

  /// For non-equi RIGHT JOINs (no equi keys), the default RANDOM(left)+BROADCAST(right) is incorrect because each
  /// worker independently tracks matched right rows via a local BitSet. Workers that receive only a subset of left rows
  /// will incorrectly emit right rows as unmatched. Invert to BROADCAST(left)+RANDOM(right) so each worker has the
  /// full left table and can correctly determine which of its local right rows are unmatched.
  private static PinotHintOptions.DistributionType inferLeftDistributionType(JoinInfo joinInfo,
      JoinRelType joinType) {
    if (!joinInfo.leftKeys.isEmpty()) {
      return PinotHintOptions.DistributionType.HASH;
    }
    if (joinType == JoinRelType.RIGHT) {
      return PinotHintOptions.DistributionType.BROADCAST;
    }
    return PinotHintOptions.DistributionType.RANDOM;
  }

  private static PinotHintOptions.DistributionType inferRightDistributionType(JoinInfo joinInfo,
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
