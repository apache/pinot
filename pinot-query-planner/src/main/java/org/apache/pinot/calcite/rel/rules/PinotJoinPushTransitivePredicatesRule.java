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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.tools.RelBuilder;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;


public class PinotJoinPushTransitivePredicatesRule extends JoinPushTransitivePredicatesRule {

  protected PinotJoinPushTransitivePredicatesRule(Config config) {
    super(config);
  }

  public static final PinotJoinPushTransitivePredicatesRule INSTANCE
      = new PinotJoinPushTransitivePredicatesRule(Config.DEFAULT);

  public static PinotJoinPushTransitivePredicatesRule instanceWithDescription(String description) {
      return new PinotJoinPushTransitivePredicatesRule((Config) Config.DEFAULT.withDescription(description));
  }

  // Following code are copy-pasted from Calcite, and modified to not push down filter into right side of lookup join.
  //@formatter:off
  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    final RelMetadataQuery mq = call.getMetadataQuery();
    RelOptPredicateList preds = mq.getPulledUpPredicates(join);

    if (preds.leftInferredPredicates.isEmpty()
        && preds.rightInferredPredicates.isEmpty()) {
      return;
    }

    final RelBuilder relBuilder = call.builder();

    RelNode left = join.getLeft();
    if (!preds.leftInferredPredicates.isEmpty()) {
      RelNode curr = left;
      left = relBuilder.push(left)
          .filter(preds.leftInferredPredicates).build();
      call.getPlanner().onCopy(curr, left);
    }

    // PINOT MODIFICATION to not push down filter into right side of lookup join.
    boolean canPushRight = !PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join);

    RelNode right = join.getRight();
    if (canPushRight && !preds.rightInferredPredicates.isEmpty()) {
      RelNode curr = right;
      right = relBuilder.push(right)
          .filter(preds.rightInferredPredicates).build();
      call.getPlanner().onCopy(curr, right);
    }

    RelNode newRel =
        join.copy(join.getTraitSet(), join.getCondition(), left, right,
            join.getJoinType(), join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newRel);

    call.transformTo(newRel);
  }
  //@formatter:on
}
