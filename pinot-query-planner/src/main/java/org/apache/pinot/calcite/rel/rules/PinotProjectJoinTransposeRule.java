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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.pinot.calcite.rel.logical.PinotLogicalJoin;
import org.apache.pinot.query.planner.plannode.JoinNode.JoinStrategy;


/**
 * Similar to {@link ProjectJoinTransposeRule} but do not transpose project into right side of lookup join.
 *
 * TODO: Allow transposing project into left side of lookup join.
 */
public class PinotProjectJoinTransposeRule extends ProjectJoinTransposeRule {
  public static final PinotProjectJoinTransposeRule INSTANCE = new PinotProjectJoinTransposeRule(
      (Config) Config.DEFAULT.withOperandFor(LogicalProject.class, PinotLogicalJoin.class)
          .withRelBuilderFactory(PinotRuleUtils.PINOT_REL_FACTORY));

  private PinotProjectJoinTransposeRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    PinotLogicalJoin join = call.rel(1);
    return join.getJoinStrategy() != JoinStrategy.LOOKUP;
  }
}
