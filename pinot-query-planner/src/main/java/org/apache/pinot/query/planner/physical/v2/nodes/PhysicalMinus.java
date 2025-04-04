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

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;


public class PhysicalMinus extends Minus implements PRelNode {
  private final int _nodeId;
  private final List<PRelNode> _pRelInputs;
  @Nullable
  private final PinotDataDistribution _pinotDataDistribution;

  public PhysicalMinus(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints, List<PRelNode> inputs,
      boolean all, int nodeId, @Nullable PinotDataDistribution pinotDataDistribution) {
    super(cluster, traits, hints, inputs.stream().map(PRelNode::unwrap).collect(Collectors.toList()), all);
    _nodeId = nodeId;
    _pRelInputs = inputs;
    _pinotDataDistribution = pinotDataDistribution;
  }

  @Override
  public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new PhysicalMinus(getCluster(), traitSet, getHints(), inputs.stream().map(PRelNode.class::cast)
        .collect(Collectors.toList()), all, _nodeId, _pinotDataDistribution);
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
    return new PhysicalMinus(getCluster(), getTraitSet(), getHints(), newInputs, all, newNodeId, newDistribution);
  }

  @Override
  public PhysicalMinus asLeafStage() {
    throw new UnsupportedOperationException("Minus cannot be in the leaf stage");
  }
}
