package org.apache.pinot.query.planner.nodes;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.logical.LogicalTableScan;


public class TableScanNode extends AbstractStageNode {
  private final List<String> _tableName;

  public TableScanNode(LogicalTableScan tableScan, String stageId) {
    super(stageId);
    _tableName = tableScan.getTable().getQualifiedName();
  }

  @Override
  public List<StageNode> getInputs() {
    return Collections.emptyList();
  }

  @Override
  public void addInput(StageNode queryStageRoot) {
    throw new UnsupportedOperationException("table scan cannot add input");
  }

  public List<String> getTableName() {
    return _tableName;
  }
}
