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
package org.apache.pinot.query.planner.physical.v2.nodes;

import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;


public class PhysicalWindow extends Window implements PRelNode {
  private final int _nodeId;
  private final List<PRelNode> _pRelInputs;
  @Nullable
  private final PinotDataDistribution _pinotDataDistribution;

  public PhysicalWindow(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints,
      List<RexLiteral> constants, RelDataType rowType, List<Group> groups, int nodeId, PRelNode pRelInput,
      @Nullable PinotDataDistribution pinotDataDistribution) {
    super(cluster, traitSet, hints, pRelInput.unwrap(), constants, rowType, groups);
    _nodeId = nodeId;
    _pRelInputs = List.of(pRelInput);
    _pinotDataDistribution = pinotDataDistribution;
  }

  @Override
  public Window copy(RelTraitSet traitSet, List<RelNode> inputs) {
    Preconditions.checkState(inputs.size() == 1, "Expect exactly 1 input to window. Found: %s", inputs);
    return new PhysicalWindow(getCluster(), traitSet, getHints(), getConstants(), getRowType(), groups, _nodeId,
        (PRelNode) inputs.get(0), _pinotDataDistribution);
  }

  @Override
  public Window copy(List<RexLiteral> constants) {
    return new PhysicalWindow(getCluster(), getTraitSet(), getHints(), constants, getRowType(), groups, _nodeId,
        _pRelInputs.get(0), _pinotDataDistribution);
  }

  @Override
  public int getNodeId() {
    return _nodeId;
  }

  @Override
  public List<PRelNode> getPRelInputs() {
    return _pRelInputs;
  }

  @Override
  public RelNode unwrap() {
    return this;
  }

  @Nullable
  @Override
  public PinotDataDistribution getPinotDataDistribution() {
    return _pinotDataDistribution;
  }

  @Override
  public boolean isLeafStage() {
    return false;
  }

  @Override
  public PRelNode with(int newNodeId, List<PRelNode> newInputs, PinotDataDistribution newDistribution) {
    return new PhysicalWindow(getCluster(), getTraitSet(), getHints(), getConstants(), getRowType(), groups, newNodeId,
        newInputs.get(0), newDistribution);
  }

  @Override
  public PhysicalWindow asLeafStage() {
    throw new UnsupportedOperationException("Window cannot be in the leaf stage");
  }
}
