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
import org.apache.calcite.rel.hint.RelHint;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class TableScanNode extends AbstractPlanNode {
  @ProtoProperties
  private NodeHint _nodeHint;
  @ProtoProperties
  private String _tableName;
  @ProtoProperties
  private List<String> _tableScanColumns;

  public TableScanNode(int planFragmentId) {
    super(planFragmentId);
  }

  public TableScanNode(int planFragmentId, DataSchema dataSchema, List<RelHint> relHints, String tableName,
      List<String> tableScanColumns) {
    super(planFragmentId, dataSchema);
    _tableName = tableName;
    _nodeHint = new NodeHint(relHints);
    _tableScanColumns = tableScanColumns;
  }

  public String getTableName() {
    return _tableName;
  }

  public List<String> getTableScanColumns() {
    return _tableScanColumns;
  }

  public NodeHint getNodeHint() {
    return _nodeHint;
  }

  @Override
  public String explain() {
    return "TABLE SCAN (" + _tableName + ")";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitTableScan(this, context);
  }
}
