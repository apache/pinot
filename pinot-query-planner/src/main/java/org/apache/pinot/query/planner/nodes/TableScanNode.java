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
package org.apache.pinot.query.planner.nodes;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.proto.Plan;


public class TableScanNode extends AbstractStageNode {
  private String _tableName;
  private List<String> _tableScanColumns;

  public TableScanNode(int stageId) {
    super(stageId);
  }

  public TableScanNode(int stageId, String tableName, List<String> tableScanColumns) {
    super(stageId);
    _tableName = tableName;
    _tableScanColumns = tableScanColumns;
  }

  public String getTableName() {
    return _tableName;
  }

  public List<String> getTableScanColumns() {
    return _tableScanColumns;
  }

  @Override
  public void setFields(Plan.ObjectFields objFields) {
    _tableName = objFields.getLiteralFieldOrThrow("tableName").getStringField();
    _tableScanColumns = new ArrayList<>();
    for (Plan.LiteralField literalField : objFields.getListFieldsOrThrow("columns").getLiteralsList()) {
      _tableScanColumns.add(literalField.getStringField());
    }
  }

  @Override
  public Plan.ObjectFields getFields() {
    Plan.ListField.Builder listBuilder = Plan.ListField.newBuilder();
    for (String column : _tableScanColumns) {
      listBuilder.addLiterals(SerDeUtils.stringField(column));
    }
    return Plan.ObjectFields.newBuilder()
        .putLiteralField("tableName", SerDeUtils.stringField(_tableName))
        .putListFields("columns", listBuilder.build())
        .build();
  }
}
