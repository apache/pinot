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
package org.apache.pinot.query.planner.physical.v2.opt;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.opt.rules.AggregatePushdownRule;
import org.apache.pinot.query.planner.physical.v2.opt.rules.LeafStageAggregateRule;
import org.apache.pinot.query.planner.physical.v2.opt.rules.LeafStageBoundaryRule;
import org.apache.pinot.query.planner.physical.v2.opt.rules.LeafStageWorkerAssignmentRule;
import org.apache.pinot.query.planner.physical.v2.opt.rules.SortPushdownRule;
import org.apache.pinot.query.planner.physical.v2.opt.rules.WorkerExchangeAssignmentRule;


public class PhysicalOptRuleSet {
  private PhysicalOptRuleSet() {
  }

  public static List<PRelNodeTransformer> create(PhysicalPlannerContext context, TableCache tableCache) {
    List<PRelNodeTransformer> transformers = new ArrayList<>();
    transformers.add(create(LeafStageBoundaryRule.INSTANCE, RuleExecutors.Type.POST_ORDER, context));
    transformers.add(create(new LeafStageWorkerAssignmentRule(context, tableCache), RuleExecutors.Type.POST_ORDER,
        context));
    transformers.add(create(new LeafStageAggregateRule(context), RuleExecutors.Type.POST_ORDER, context));
    transformers.add(new WorkerExchangeAssignmentRule(context));
    transformers.add(create(new AggregatePushdownRule(context), RuleExecutors.Type.POST_ORDER, context));
    transformers.add(create(new SortPushdownRule(context), RuleExecutors.Type.POST_ORDER, context));
    return transformers;
  }

  private static PRelNodeTransformer create(PRelOptRule rule, RuleExecutors.Type type, PhysicalPlannerContext context) {
    return (pRelNode) -> RuleExecutors.create(type, rule, context).execute(pRelNode);
  }
}
