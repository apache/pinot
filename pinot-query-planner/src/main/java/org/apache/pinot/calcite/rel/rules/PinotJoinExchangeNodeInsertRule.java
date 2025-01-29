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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;


/**
 * Special rule for Pinot, this rule is fixed to always insert exchange after JOIN node.
 */
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
    } else {
      // Hash join
      // TODO: Validate if the configured distribution types are valid
      if (leftDistributionType == null) {
        // By default, hash distribute the left side if there are join keys, otherwise randomly distribute
        leftDistributionType = !joinInfo.leftKeys.isEmpty() ? PinotHintOptions.DistributionType.HASH
            : PinotHintOptions.DistributionType.RANDOM;
      }
      newLeft = createExchangeForHashJoin(leftDistributionType, joinInfo.leftKeys, left);
      if (rightDistributionType == null) {
        // By default, hash distribute the right side if there are join keys, otherwise broadcast
        rightDistributionType = !joinInfo.rightKeys.isEmpty() ? PinotHintOptions.DistributionType.HASH
            : PinotHintOptions.DistributionType.BROADCAST;
      }
      newRight = createExchangeForHashJoin(rightDistributionType, joinInfo.rightKeys, right);
    }

    // TODO: Consider creating different JOIN Rel for each join strategy
    call.transformTo(join.copy(join.getTraitSet(), join.getCondition(), newLeft, newRight, join.getJoinType(),
        join.isSemiJoinDone()));
  }

  private static PinotLogicalExchange createExchangeForLookupJoin(PinotHintOptions.DistributionType distributionType,
      List<Integer> keys, RelNode child) {
    switch (distributionType) {
      case LOCAL:
        // NOTE: We use SINGLETON to represent local distribution. Add keys to the exchange because we might want to
        //       switch it to HASH distribution to increase parallelism. See MailboxAssignmentVisitor for details.
        return PinotLogicalExchange.create(child, RelDistributions.SINGLETON, keys);
      case HASH:
        Preconditions.checkArgument(!keys.isEmpty(), "Hash distribution requires join keys");
        return PinotLogicalExchange.create(child, RelDistributions.hash(keys));
      case RANDOM:
        return PinotLogicalExchange.create(child, RelDistributions.RANDOM_DISTRIBUTED);
      default:
        throw new IllegalArgumentException("Unsupported distribution type: " + distributionType + " for lookup join");
    }
  }

  private static PinotLogicalExchange createExchangeForHashJoin(PinotHintOptions.DistributionType distributionType,
      List<Integer> keys, RelNode child) {
    switch (distributionType) {
      case LOCAL:
        // NOTE: We use SINGLETON to represent local distribution. Add keys to the exchange because we might want to
        //       switch it to HASH distribution to increase parallelism. See MailboxAssignmentVisitor for details.
        return PinotLogicalExchange.create(child, RelDistributions.SINGLETON, keys);
      case HASH:
        Preconditions.checkArgument(!keys.isEmpty(), "Hash distribution requires join keys");
        return PinotLogicalExchange.create(child, RelDistributions.hash(keys));
      case BROADCAST:
        return PinotLogicalExchange.create(child, RelDistributions.BROADCAST_DISTRIBUTED);
      case RANDOM:
        return PinotLogicalExchange.create(child, RelDistributions.RANDOM_DISTRIBUTED);
      default:
        throw new IllegalArgumentException("Unsupported distribution type: " + distributionType + " for hash join");
    }
  }
}
