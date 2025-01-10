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
package org.apache.pinot.query.planner.plannode;

import java.util.List;
import java.util.Objects;
import org.apache.pinot.common.utils.DataSchema;


public class TableScanNode extends BasePlanNode {
  private final String _tableName;
  private final List<String> _columns;
  private final boolean _isLogicalTable;
  private final List<String> _physicalTableNames;

  public TableScanNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs, String tableName,
      List<String> columns) {
    this(stageId, dataSchema, nodeHint, inputs, tableName, columns, false, null);
  }

  public TableScanNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs, String tableName,
      List<String> columns, boolean isLogicalTable, List<String> physicalTableNames) {
    super(stageId, dataSchema, nodeHint, inputs);
    _tableName = tableName;
    _columns = columns;
    _isLogicalTable = isLogicalTable;
    _physicalTableNames = physicalTableNames;
  }

  public String getTableName() {
    return _tableName;
  }

  public boolean isLogicalTable() {
    return _isLogicalTable;
  }

  public List<String> getPhysicalTableNames() {
    return _physicalTableNames;
  }

  public List<String> getColumns() {
    return _columns;
  }

  @Override
  public String explain() {
    return "TABLE SCAN (" + _tableName + ")";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitTableScan(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new TableScanNode(_stageId, _dataSchema, _nodeHint, inputs, _tableName, _columns);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableScanNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TableScanNode that = (TableScanNode) o;
    return Objects.equals(_tableName, that._tableName) && Objects.equals(_columns, that._columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _tableName, _columns);
  }
}
