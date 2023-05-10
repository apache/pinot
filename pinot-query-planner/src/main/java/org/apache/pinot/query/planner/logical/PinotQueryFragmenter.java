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
package org.apache.pinot.query.planner.logical;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelRoot;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.PlanFragmentMetadata;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.planner.SubPlanMetadata;
import org.apache.pinot.query.planner.plannode.PlanNode;


/**
 * PinotLogicalQueryPlanner walks top-down from {@link RelRoot} and construct a forest of trees with {@link PlanNode}.
 *
 * This class is non-threadsafe. Do not reuse the stage planner for multiple query plans.
 */
public class PinotQueryFragmenter {
  private long _requestId;

  public PinotQueryFragmenter(long requestId) {
    _requestId = requestId;
  }

  /**
   * Fragment Query Plan into running groups and construct the sub plan from logical plan.
   *
   * @param queryPlan a Logical Query Plan.
   * @return SubPlan root.
   */
  public SubPlan fragment(QueryPlan queryPlan) {
    PlanFragment rootPlanFragment = queryPlanToFragments(queryPlan.getPlanRoot());
    SubPlan subPlanRoot = groupFragmentsToSubPlan(rootPlanFragment);
    return subPlanRoot;
  }

  private PlanFragment queryPlanToFragments(PlanNode rootPlanFragment) {
    List<PlanFragment> children = new ArrayList<>();
    PlanFragmentVisitor visitor = new PlanFragmentVisitor();
    PlanFragment root = new PlanFragment(0, rootPlanFragment, createPlanFragmentMetadata(), children);
    return root;
  }

  private PlanFragmentMetadata createPlanFragmentMetadata() {
    PlanFragmentMetadata planFragmentMetadata = new PlanFragmentMetadata();
    planFragmentMetadata.getCustomProperties().put(PlanFragmentMetadata.PLAN_FRAGMENT_ID_KEY, String.valueOf(0));
    return planFragmentMetadata;
  }

  private SubPlan groupFragmentsToSubPlan(PlanFragment rootPlanFragment) {
    return new SubPlan(rootPlanFragment, createPlanMetadata(), ImmutableList.of());
  }

  private SubPlanMetadata createPlanMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(SubPlanMetadata.SUBPLAN_ID_KEY, String.valueOf(0));
    return new SubPlanMetadata(metadata);
  }
}
