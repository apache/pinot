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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.apache.pinot.query.planner.physical.v2.TableScanMetadata;


public class PhysicalTableScan extends TableScan implements PRelNode {
  private final int _nodeId;
  @Nullable
  private final PinotDataDistribution _pinotDataDistribution;
  @Nullable
  private final TableScanMetadata _tableScanMetadata;

  public PhysicalTableScan(TableScan tableScan, int nodeId, PinotDataDistribution pinotDataDistribution,
      @Nullable TableScanMetadata tableScanMetadata) {
    this(tableScan.getCluster(), tableScan.getTraitSet(), tableScan.getHints(), tableScan.getTable(), nodeId,
        pinotDataDistribution, tableScanMetadata);
  }

  public PhysicalTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table,
      int nodeId, PinotDataDistribution pinotDataDistribution, @Nullable TableScanMetadata tableScanMetadata) {
    super(cluster, traitSet, hints, table);
    _nodeId = nodeId;
    _pinotDataDistribution = pinotDataDistribution;
    _tableScanMetadata = tableScanMetadata;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    Preconditions.checkState(inputs.isEmpty(), "No inputs expected for PhysicalTableScan. Found: %s", inputs);
    return new PhysicalTableScan(getCluster(), traitSet, getHints(), getTable(), _nodeId, _pinotDataDistribution,
        _tableScanMetadata);
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
    return true;
  }

  @Nullable
  @Override
  public TableScanMetadata getTableScanMetadata() {
    return _tableScanMetadata;
  }

  @Override
  public PRelNode with(int newNodeId, List<PRelNode> newInputs, PinotDataDistribution newDistribution) {
    Preconditions.checkState(newInputs.isEmpty(), "No inputs expected for PhysicalTableScan. Found: %s",
        newInputs.size());
    return new PhysicalTableScan(getCluster(), getTraitSet(), getHints(), getTable(), newNodeId,
        newDistribution, _tableScanMetadata);
  }

  @Override
  public PRelNode asLeafStage() {
    return this;
  }

  public PhysicalTableScan with(PinotDataDistribution pinotDataDistribution, TableScanMetadata metadata) {
    return new PhysicalTableScan(getCluster(), getTraitSet(), getHints(), getTable(), _nodeId,
        pinotDataDistribution, metadata);
  }
}
