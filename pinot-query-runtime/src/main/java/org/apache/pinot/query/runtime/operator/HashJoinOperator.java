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
package org.apache.pinot.query.runtime.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.runtime.blocks.DataTableBlock;
import org.apache.pinot.query.runtime.blocks.DataTableBlockUtils;


/**
 * This basic {@code BroadcastJoinOperator} implement a basic broadcast join algorithm.
 *
 * <p>It takes the right table as the broadcast side and materialize a hash table. Then for each of the left table row,
 * it looks up for the corresponding row(s) from the hash table and create a joint row.
 *
 * <p>For each of the data block received from the left table, it will generate a joint data block.
 */
public class HashJoinOperator extends BaseOperator<DataTableBlock> {
  private static final String OPERATOR_NAME = "BroadcastJoinOperator";
  private static final String EXPLAIN_NAME = "BROADCAST_JOIN";

  private final HashMap<Object, List<Object[]>> _broadcastHashTable;
  private final BaseOperator<DataTableBlock> _leftTableOperator;
  private final BaseOperator<DataTableBlock> _rightTableOperator;

  private DataSchema _leftTableSchema;
  private DataSchema _rightTableSchema;
  private int _resultRowSize;
  private boolean _isHashTableBuilt;
  private KeySelector<Object[], Object> _leftKeySelector;
  private KeySelector<Object[], Object> _rightKeySelector;

  public HashJoinOperator(BaseOperator<DataTableBlock> leftTableOperator,
      BaseOperator<DataTableBlock> rightTableOperator, List<JoinNode.JoinClause> criteria) {
    // TODO: this assumes right table is broadcast.
    _leftKeySelector = criteria.get(0).getLeftJoinKeySelector();
    _rightKeySelector = criteria.get(0).getRightJoinKeySelector();
    _leftTableOperator = leftTableOperator;
    _rightTableOperator = rightTableOperator;
    _isHashTableBuilt = false;
    _broadcastHashTable = new HashMap<>();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    // WorkerExecutor doesn't use getChildOperators, returns null here.
    return null;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected DataTableBlock getNextBlock() {
    buildBroadcastHashTable();
    try {
      return new DataTableBlock(buildJoinedDataTable(_leftTableOperator.nextBlock()));
    } catch (Exception e) {
      return DataTableBlockUtils.getErrorDatatableBlock(e);
    }
  }

  private void buildBroadcastHashTable() {
    if (!_isHashTableBuilt) {
      DataTableBlock rightBlock = _rightTableOperator.nextBlock();
      while (!DataTableBlockUtils.isEndOfStream(rightBlock)) {
        DataTable dataTable = rightBlock.getDataTable();
        _rightTableSchema = dataTable.getDataSchema();
        int numRows = dataTable.getNumberOfRows();
        // put all the rows into corresponding hash collections keyed by the key selector function.
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] objects = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          List<Object[]> hashCollection =
              _broadcastHashTable.computeIfAbsent(_rightKeySelector.getKey(objects), k -> new ArrayList<>());
          hashCollection.add(objects);
        }
        rightBlock = _rightTableOperator.nextBlock();
      }
      _isHashTableBuilt = true;
    }
  }

  private DataTable buildJoinedDataTable(DataTableBlock block)
      throws Exception {
    if (DataTableBlockUtils.isEndOfStream(block)) {
      return DataTableBlockUtils.getEndOfStreamDataTable();
    }
    List<Object[]> rows = new ArrayList<>();
    DataTable dataTable = block.getDataTable();
    _leftTableSchema = dataTable.getDataSchema();
    _resultRowSize = _leftTableSchema.size() + _rightTableSchema.size();
    int numRows = dataTable.getNumberOfRows();
    for (int rowId = 0; rowId < numRows; rowId++) {
      Object[] leftRow = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
      List<Object[]> hashCollection =
          _broadcastHashTable.getOrDefault(_leftKeySelector.getKey(leftRow), Collections.emptyList());
      for (Object[] rightRow : hashCollection) {
        rows.add(joinRow(leftRow, rightRow));
      }
    }
    return SelectionOperatorUtils.getDataTableFromRows(rows, computeSchema());
  }

  private Object[] joinRow(Object[] leftRow, Object[] rightRow) {
    Object[] resultRow = new Object[_resultRowSize];
    int idx = 0;
    for (Object obj : leftRow) {
      resultRow[idx++] = obj;
    }
    for (Object obj : rightRow) {
      resultRow[idx++] = obj;
    }
    return resultRow;
  }

  private DataSchema computeSchema() {
    String[] columnNames = new String[_resultRowSize];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[_resultRowSize];
    int idx = 0;
    for (int index = 0; index < _leftTableSchema.size(); index++) {
      columnNames[idx] = _leftTableSchema.getColumnName(index);
      columnDataTypes[idx++] = _leftTableSchema.getColumnDataType(index);
    }
    for (int index = 0; index < _rightTableSchema.size(); index++) {
      columnNames[idx] = _rightTableSchema.getColumnName(index);
      columnDataTypes[idx++] = _rightTableSchema.getColumnDataType(index);
    }
    return new DataSchema(columnNames, columnDataTypes);
  }
}
