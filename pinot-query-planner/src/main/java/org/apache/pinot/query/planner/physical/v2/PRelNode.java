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
import org.apache.calcite.rel.RelNode;


/**
 * The common interface for Physical Rel Nodes, allowing us to develop our own physical planning layer, while
 * at the same time still relying on the plan tree/dag formed by RelNodes.
 */
public interface PRelNode {
  int getNodeId();

  List<PRelNode> getPRelInputs();

  default PRelNode getPRelInput(int index) {
    return getPRelInputs().get(index);
  }

  RelNode unwrap();

  @Nullable
  PinotDataDistribution getPinotDataDistribution();

  default PinotDataDistribution getPinotDataDistributionOrThrow() {
    return Objects.requireNonNull(getPinotDataDistribution(), "Pinot Data Distribution is missing from PRelNode");
  }

  boolean isLeafStage();

  @Nullable
  default TableScanMetadata getTableScanMetadata() {
    return null;
  }

  PRelNode copy(int newNodeId, List<PRelNode> newInputs, PinotDataDistribution newDistribution);

  default PRelNode copy(List<PRelNode> newInputs, PinotDataDistribution newDistribution) {
    return copy(getNodeId(), newInputs, newDistribution);
  }

  default PRelNode copy(List<PRelNode> newInputs) {
    return copy(getNodeId(), newInputs, getPinotDataDistributionOrThrow());
  }
}
