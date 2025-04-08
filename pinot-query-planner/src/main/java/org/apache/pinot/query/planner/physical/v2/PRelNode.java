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
package org.apache.pinot.query.planner.physical.v2;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;


/**
 * The common interface for Physical Rel Nodes, allowing us to develop our own physical planning layer, while
 * at the same time still relying on the plan tree/dag formed by RelNodes.
 */
public interface PRelNode {
  /**
   * Node ID for this plan node in the query plan. During copy, this ID may be preserved or changed. Calcite RelNode
   * also has its own integer IDs, but those are generated via a statically incrementing counter. This Node ID is
   * supposed to make debugging easier.
   */
  int getNodeId();

  /**
   * Returns a typed reference to the inputs of this node. You can also use {@link RelNode#getInputs()} to get the
   * RelNode inputs corresponding to this node.
   */
  List<PRelNode> getPRelInputs();

  /**
   * Same as {@link #getPRelInputs()} but allows returning a specific input.
   */
  default PRelNode getPRelInput(int index) {
    return getPRelInputs().get(index);
  }

  /**
   * Returns a typed reference to the corresponding RelNode.
   */
  RelNode unwrap();

  /**
   * Distribution of the data. This may be null, because this is assigned after the PRelNode tree is created.
   */
  @Nullable
  PinotDataDistribution getPinotDataDistribution();

  /**
   * Same as {@link #getPinotDataDistribution()} but throws an exception if the distribution is null.
   */
  default PinotDataDistribution getPinotDataDistributionOrThrow() {
    return Objects.requireNonNull(getPinotDataDistribution(), "Pinot Data Distribution is missing from PRelNode");
  }

  /**
   * Whether this node is part of the leaf stage.
   */
  boolean isLeafStage();

  /**
   * Only set for {@link TableScan} nodes. Returns metadata computed during segment assignment for the leaf stage.
   */
  @Nullable
  default TableScanMetadata getTableScanMetadata() {
    return null;
  }

  /**
   * TODO(mse-physical): This does not check PinotExecStrategyTrait. We should revisit whether exec strategy should be
   *   a trait or not.
   */
  default boolean areTraitsSatisfied() {
    RelNode relNode = unwrap();
    RelDistribution distribution = relNode.getTraitSet().getDistribution();
    PinotDataDistribution dataDistribution = getPinotDataDistributionOrThrow();
    if (dataDistribution.satisfies(distribution)) {
      RelCollation collation = relNode.getTraitSet().getCollation();
      return dataDistribution.satisfies(collation);
    }
    return false;
  }

  PRelNode with(int newNodeId, List<PRelNode> newInputs, PinotDataDistribution newDistribution);

  default PRelNode with(List<PRelNode> newInputs, PinotDataDistribution newDistribution) {
    return with(getNodeId(), newInputs, newDistribution);
  }

  default PRelNode with(List<PRelNode> newInputs) {
    return with(getNodeId(), newInputs, getPinotDataDistributionOrThrow());
  }

  default PRelNode asLeafStage() {
    throw new UnsupportedOperationException(String.format("Cannot make %s a leaf stage node", unwrap()));
  }
}
