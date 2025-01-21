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
      // Lookup join
      if (leftDistributionType == null) {
        // By default, use local distribution for the left side
        newLeft = PinotLogicalExchange.create(left, RelDistributions.SINGLETON);
      } else {
        switch (leftDistributionType) {
          case LOCAL:
            newLeft = PinotLogicalExchange.create(left, RelDistributions.SINGLETON);
            break;
          case HASH:
            Preconditions.checkArgument(!joinInfo.leftKeys.isEmpty(), "Hash distribution requires join keys");
            newLeft = PinotLogicalExchange.create(left, RelDistributions.hash(joinInfo.leftKeys));
            break;
          case RANDOM:
            newLeft = PinotLogicalExchange.create(left, RelDistributions.RANDOM_DISTRIBUTED);
            break;
          default:
            throw new IllegalArgumentException(
                "Unsupported left distribution type: " + leftDistributionType + " for lookup join");
        }
      }
      Preconditions.checkArgument(rightDistributionType == null,
          "Right distribution type hint is not supported for lookup join");
      newRight = right;
    } else {
      // Hash join
      // TODO: Validate if the configured distribution types are valid
      if (joinInfo.leftKeys.isEmpty()) {
        // No join key, cannot use hash distribution
        if (leftDistributionType == null) {
          // By default, randomly distribute the left side
          newLeft = PinotLogicalExchange.create(left, RelDistributions.RANDOM_DISTRIBUTED);
        } else {
          switch (leftDistributionType) {
            case LOCAL:
              newLeft = PinotLogicalExchange.create(left, RelDistributions.SINGLETON);
              break;
            case RANDOM:
              newLeft = PinotLogicalExchange.create(left, RelDistributions.RANDOM_DISTRIBUTED);
              break;
            case BROADCAST:
              newLeft = PinotLogicalExchange.create(left, RelDistributions.BROADCAST_DISTRIBUTED);
              break;
            default:
              throw new IllegalArgumentException(
                  "Unsupported left distribution type: " + leftDistributionType + " for hash join without join keys");
          }
        }
        if (rightDistributionType == null) {
          // By default, broadcast the right side
          newRight = PinotLogicalExchange.create(right, RelDistributions.BROADCAST_DISTRIBUTED);
        } else {
          switch (rightDistributionType) {
            case LOCAL:
              newRight = PinotLogicalExchange.create(right, RelDistributions.SINGLETON);
              break;
            case RANDOM:
              newRight = PinotLogicalExchange.create(right, RelDistributions.RANDOM_DISTRIBUTED);
              break;
            case BROADCAST:
              newRight = PinotLogicalExchange.create(right, RelDistributions.BROADCAST_DISTRIBUTED);
              break;
            default:
              throw new IllegalStateException(
                  "Unsupported right distribution type: " + rightDistributionType + " for hash join without join keys");
          }
        }
      } else {
        // There are join keys, hash distribution is supported
        if (leftDistributionType == null) {
          // By default, hash distribute the left side
          newLeft = PinotLogicalExchange.create(left, RelDistributions.hash(joinInfo.leftKeys));
        } else {
          switch (leftDistributionType) {
            case LOCAL:
              newLeft = PinotLogicalExchange.create(left, RelDistributions.SINGLETON);
              break;
            case HASH:
              newLeft = PinotLogicalExchange.create(left, RelDistributions.hash(joinInfo.leftKeys));
              break;
            case BROADCAST:
              newLeft = PinotLogicalExchange.create(left, RelDistributions.BROADCAST_DISTRIBUTED);
              break;
            case RANDOM:
              newLeft = PinotLogicalExchange.create(left, RelDistributions.RANDOM_DISTRIBUTED);
              break;
            default:
              throw new IllegalArgumentException(
                  "Unsupported left distribution type: " + leftDistributionType + " for hash join with join keys");
          }
        }
        if (rightDistributionType == null) {
          // By default, hash distribute the right side
          newRight = PinotLogicalExchange.create(right, RelDistributions.hash(joinInfo.rightKeys));
        } else {
          switch (rightDistributionType) {
            case LOCAL:
              newRight = PinotLogicalExchange.create(right, RelDistributions.SINGLETON);
              break;
            case HASH:
              newRight = PinotLogicalExchange.create(right, RelDistributions.hash(joinInfo.rightKeys));
              break;
            case BROADCAST:
              newRight = PinotLogicalExchange.create(right, RelDistributions.BROADCAST_DISTRIBUTED);
              break;
            case RANDOM:
              newRight = PinotLogicalExchange.create(right, RelDistributions.RANDOM_DISTRIBUTED);
              break;
            default:
              throw new IllegalStateException(
                  "Unsupported right distribution type: " + rightDistributionType + " for hash join with join keys");
          }
        }
      }
    }

    // TODO: Consider creating different JOIN Rel for each join strategy
    call.transformTo(join.copy(join.getTraitSet(), join.getCondition(), newLeft, newRight, join.getJoinType(),
        join.isSemiJoinDone()));
  }
}
