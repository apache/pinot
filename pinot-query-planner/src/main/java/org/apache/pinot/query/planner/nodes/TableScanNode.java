package org.apache.pinot.query.planner.nodes;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeField;


public class TableScanNode extends AbstractStageNode {
  private final List<String> _tableName;
  private final List<String> _tableScanColumns;

  public TableScanNode(LogicalTableScan tableScan, String stageId) {
    super(stageId);
    _tableName = tableScan.getTable().getQualifiedName();
    // TODO: optimize this, table field is not directly usable as name.
    _tableScanColumns = tableScan.getRowType().getFieldList().stream().map(RelDataTypeField::getName)
        .collect(Collectors.toList());
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

  public List<String> getTableScanColumns() {
    return _tableScanColumns;
  }
}
