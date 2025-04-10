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
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;


public class PhysicalSort extends Sort implements PRelNode {
  private final int _nodeId;
  private final List<PRelNode> _pRelInputs;
  @Nullable
  private final PinotDataDistribution _pinotDataDistribution;
  private final boolean _leafStage;

  public PhysicalSort(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints,
      RelCollation collation, @Nullable RexNode offset, @Nullable RexNode fetch,
      PRelNode input, int nodeId, @Nullable PinotDataDistribution pinotDataDistribution,
      boolean leafStage) {
    super(cluster, traits, hints, input.unwrap(), collation, offset, fetch);
    _nodeId = nodeId;
    _pRelInputs = List.of(input);
    _pinotDataDistribution = pinotDataDistribution;
    _leafStage = leafStage;
  }

  @Override
  public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    return new PhysicalSort(getCluster(), traitSet, getHints(), newCollation, offset, fetch, (PRelNode) newInput,
        _nodeId, _pinotDataDistribution, _leafStage);
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
    return new PhysicalSort(getCluster(), getTraitSet(), getHints(), getCollation(), offset, fetch, newInputs.get(0),
        newNodeId, newDistribution, _leafStage);
  }
}
