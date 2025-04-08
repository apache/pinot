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
package org.apache.pinot.query.planner.physical.v2.opt.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRuleCall;


/**
 * The leaf stage consists of a table-scan and an optional project and/or filter. The filter and project nodes
 * may be in any order. We don't include sort or aggregate in the leaf stage in this rule, because they will made part
 * of Leaf stage (if appropriate) as part of the Aggregate and Sort pushdown rules.
 * <p>
 *  The idea is that you can and should always make filter and project part of the leaf stage and compute them locally
 *  on the server where the table-scan is computed. Whether it makes sense to run the aggregate or sort in the leaf
 *  stage depends on a few conditions, and hence it is handled as part of their respective pushdown rules.
 * </p>
 */
public class LeafStageBoundaryRule extends PRelOptRule {
  public static final LeafStageBoundaryRule INSTANCE = new LeafStageBoundaryRule();

  private LeafStageBoundaryRule() {
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    RelNode currentRel = call._currentNode.unwrap();
    if (currentRel instanceof TableScan) {
      return true;
    }
    if (!isProjectOrFilter(currentRel)) {
      return false;
    }
    if (isTableScan(currentRel.getInput(0))) {
      // (Project|Filter) > Table Scan
      return true;
    }
    if (isProject(currentRel) && isFilter(currentRel.getInput(0))
        && isTableScan(currentRel.getInput(0).getInput(0))) {
      // Project > Filter > Table Scan
      return true;
    }
    // Filter > Project > Table Scan
    return isFilter(currentRel) && isProject(currentRel.getInput(0))
        && isTableScan(currentRel.getInput(0).getInput(0));
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    PRelNode currentNode = call._currentNode;
    return currentNode.asLeafStage();
  }

  private boolean isProjectOrFilter(RelNode relNode) {
    return isProject(relNode) || isFilter(relNode);
  }

  private boolean isProject(RelNode relNode) {
    return relNode instanceof Project;
  }

  private boolean isFilter(RelNode relNode) {
    return relNode instanceof Filter;
  }

  private boolean isTableScan(RelNode relNode) {
    return relNode instanceof TableScan;
  }
}
