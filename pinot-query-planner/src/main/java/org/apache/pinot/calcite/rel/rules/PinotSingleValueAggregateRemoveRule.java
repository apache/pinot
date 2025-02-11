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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilderFactory;


/**
 * SingleValueAggregateRemoveRule that matches an Aggregate function SINGLE_VALUE and remove it
 *
 */
public class PinotSingleValueAggregateRemoveRule extends RelOptRule {
  public static final PinotSingleValueAggregateRemoveRule INSTANCE =
      new PinotSingleValueAggregateRemoveRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotSingleValueAggregateRemoveRule(RelBuilderFactory factory) {
    super(operand(Aggregate.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Aggregate agg = call.rel(0);
    List<AggregateCall> aggCalls = agg.getAggCallList();
    if (aggCalls.size() != 1) {
      return false;
    }
    return aggCalls.get(0).getAggregation().getName().equals("SINGLE_VALUE");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate agg = call.rel(0);
    call.transformTo(((HepRelVertex) agg.getInput()).getCurrentRel());
  }
}
