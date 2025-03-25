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
package org.apache.pinot.query.planner.physical.v2.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.PRelOptRuleCall;


public class LeafStageBoundaryRule extends PRelOptRule {
  public static final LeafStageBoundaryRule INSTANCE = new LeafStageBoundaryRule();

  private LeafStageBoundaryRule() {
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    RelNode currentRel = call._currentNode.getRelNode();
    if (currentRel instanceof TableScan) {
      return true;
    }
    if (!(currentRel instanceof Project) && !(currentRel instanceof Filter)) {
      return false;
    }
    if (currentRel.getInput(0) instanceof TableScan) {
      return true;
    }
    return currentRel.getInput(0).getInput(0) instanceof TableScan;
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    PRelNode currentNode = call._currentNode;
    return new PRelNode(currentNode.getNodeId(), currentNode.getRelNode(), currentNode.getPinotDataDistribution(),
        currentNode.getInputs(), true, currentNode.getTableScanMetadata());
  }
}
