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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;


public class PhysicalProject extends Project implements PRelNode {
  private final int _nodeId;
  private final List<PRelNode> _pRelInputs;
  @Nullable
  private final PinotDataDistribution _pinotDataDistribution;
  private final boolean _leafStage;

  public PhysicalProject(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints,
      List<? extends RexNode> projects, RelDataType rowType, Set<CorrelationId> variableSet, int nodeId,
      PRelNode pRelInput, @Nullable PinotDataDistribution pinotDataDistribution, boolean leafStage) {
    super(cluster, traits, hints, pRelInput.unwrap(), projects, rowType, variableSet);
    _nodeId = nodeId;
    _pRelInputs = List.of(pRelInput);
    _pinotDataDistribution = pinotDataDistribution;
    _leafStage = leafStage;
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    Preconditions.checkState(input instanceof PRelNode, "Expected input of PhysicalProject to be "
        + "a PRelNode. Found: %s", input);
    return new PhysicalProject(getCluster(), traitSet, getHints(), projects, rowType, getVariablesSet(),
        _nodeId, (PRelNode) input, _pinotDataDistribution, _leafStage);
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
    return _leafStage;
  }

  @Override
  public PRelNode with(int newNodeId, List<PRelNode> newInputs, PinotDataDistribution newDistribution) {
    return new PhysicalProject(getCluster(), getTraitSet(), getHints(), getProjects(), getRowType(), getVariablesSet(),
        newNodeId, newInputs.get(0), newDistribution, _leafStage);
  }

  @Override
  public PRelNode asLeafStage() {
    if (isLeafStage()) {
      return this;
    }
    return new PhysicalProject(getCluster(), getTraitSet(), getHints(), getProjects(), getRowType(),
        getVariablesSet(), _nodeId, _pRelInputs.get(0), _pinotDataDistribution, true);
  }
}
