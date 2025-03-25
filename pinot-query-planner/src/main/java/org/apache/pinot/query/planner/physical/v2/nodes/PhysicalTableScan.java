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
    return new PhysicalTableScan(getCluster(), traitSet, getHints(), getTable(), _nodeId, _pinotDataDistribution, _tableScanMetadata);
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
  public PRelNode copy(int newNodeId, List<PRelNode> newInputs, PinotDataDistribution newDistribution) {
    Preconditions.checkState(newInputs.isEmpty(), "No inputs expected for PhysicalTableScan. Found: %s", newInputs.size());
    return new PhysicalTableScan(getCluster(), getTraitSet(), getHints(), getTable(), newNodeId,
        newDistribution, _tableScanMetadata);
  }
}
