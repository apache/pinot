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
package org.apache.pinot.query.planner;

import java.util.List;


/**
 * The {@code SubPlan} is the logical sub query plan that should be scheduled together from the result of
 * {@link org.apache.pinot.query.planner.logical.SubPlanFragmenter}.
 *
 */
public class SubPlan {
  /**
   * The root node of the sub query plan.
   */
  private final PlanFragment _subPlanRoot;
  /**
   * The metadata of the sub query plan.
   */
  private final SubPlanMetadata _subPlanMetadata;
  /**
   * The list of children sub query plans.
   */
  private final List<SubPlan> _children;

  public SubPlan(PlanFragment subPlanRoot, SubPlanMetadata subPlanMetadata, List<SubPlan> children) {
    _subPlanRoot = subPlanRoot;
    _subPlanMetadata = subPlanMetadata;
    _children = children;
  }

  public PlanFragment getSubPlanRoot() {
    return _subPlanRoot;
  }

  public SubPlanMetadata getSubPlanMetadata() {
    return _subPlanMetadata;
  }

  public List<SubPlan> getChildren() {
    return _children;
  }
}
