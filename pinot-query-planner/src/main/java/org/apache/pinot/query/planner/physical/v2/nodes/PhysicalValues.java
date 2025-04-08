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
import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;


public class PhysicalValues extends Values implements PRelNode {
  private final int _nodeId;
  @Nullable
  private final PinotDataDistribution _pinotDataDistribution;

  public PhysicalValues(RelOptCluster cluster, List<RelHint> hints, RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits, int nodeId,
      @Nullable PinotDataDistribution pinotDataDistribution) {
    super(cluster, hints, rowType, tuples, traits);
    _nodeId = nodeId;
    _pinotDataDistribution = pinotDataDistribution;
  }

  @Override
  public Values copy(RelTraitSet traitSet, List<RelNode> newInputs) {
    Preconditions.checkState(newInputs.isEmpty(), "PhysicalValues should not have any inputs. Found: %s", newInputs);
    return new PhysicalValues(getCluster(), getHints(), getRowType(), getTuples(), traitSet, _nodeId,
        _pinotDataDistribution);
  }

  @Override
  public int getNodeId() {
    return _nodeId;
  }

  @Override
  public List<PRelNode> getPRelInputs() {
    return List.of();
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
    return new PhysicalValues(getCluster(), getHints(), getRowType(), getTuples(), getTraitSet(), newNodeId,
        newDistribution);
  }
}
