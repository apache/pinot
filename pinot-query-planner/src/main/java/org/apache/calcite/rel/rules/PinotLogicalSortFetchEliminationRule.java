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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.tools.RelBuilderFactory;


/**
 * Special rule for Pinot, Pinot's top level sort fetch doesn't guarantee order without order by clause.
 */
public class PinotLogicalSortFetchEliminationRule extends RelOptRule {
  public static final PinotLogicalSortFetchEliminationRule INSTANCE =
      new PinotLogicalSortFetchEliminationRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotLogicalSortFetchEliminationRule(RelBuilderFactory factory) {
    super(operand(LogicalSort.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof LogicalSort) {
      Sort sort = call.rel(0);
      return sort.collation.getFieldCollations().size() == 0;
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Sort sort = call.rel(0);
    call.transformTo(sort.getInputs().get(0));
  }
}
