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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utilities used by planner.
 */
public class PlannerUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PlannerUtils.class);

  private PlannerUtils() {
    // do not instantiate.
  }

  public static boolean isRootPlanFragment(int planFragmentId) {
    return planFragmentId == 0;
  }

  public static boolean isFinalPlanFragment(int planFragmentId) {
    return planFragmentId == 1;
  }

  public static String explainPlan(RelNode relRoot, SqlExplainFormat format, SqlExplainLevel explainLevel) {
    return RelOptUtil.dumpPlan("Execution Plan", relRoot, format, explainLevel);
  }
}
