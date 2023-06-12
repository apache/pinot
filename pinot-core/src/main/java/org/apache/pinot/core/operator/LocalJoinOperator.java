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
package org.apache.pinot.core.operator;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.RowBasedValueBlock;
import org.apache.pinot.core.operator.blocks.RowBlock;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.SharedValueKey;


// TODO: Support null handling
// TODO: Support extra filter condition
public class LocalJoinOperator extends BaseProjectOperator<RowBasedValueBlock> {
  private static final String EXPLAIN_NAME = "LOCAL_JOIN";

  private final QueryContext _queryContext;
  private final BaseProjectOperator<?> _leftProjectOperator;
  private final LookUpContext _context;

  public LocalJoinOperator(QueryContext queryContext, BaseProjectOperator<?> leftProjectOperator, String leftTableName,
      String rightTableName, ExpressionContext leftJoinKey, String rightJoinKey, String[] projectColumns) {
    _queryContext = queryContext;
    _leftProjectOperator = leftProjectOperator;
    _context = queryContext.getOrComputeSharedValue(LookUpContext.class, rightTableName,
        k -> initContext(leftTableName, rightTableName, leftJoinKey, rightJoinKey, projectColumns));
  }

  private LookUpContext initContext(String leftTableName, String rightTableName, ExpressionContext leftJoinKey,
      String rightJoinKey, String[] projectColumns) {
    LookUpContext context = new LookUpContext();

    String rightTableKey = SharedValueKey.LOCAL_JOIN_RIGHT_TABLE_PREFIX + rightTableName;
    String serializedRightDataTable = _queryContext.getQueryOptions().get(rightTableKey);
    RowBlock rightTable = _queryContext.getSharedValue(RowBlock.class, rightTableKey);

    // Compute the right column to index map
    DataTable rightDataTable = null;
    DataSchema rightDataSchema;
    if (rightTable == null) {
      Preconditions.checkState(serializedRightDataTable != null, "Cannot find right table: %s", rightTableName);
      try {
        rightDataTable = DataTableFactory.getDataTable(Base64.getDecoder().decode(serializedRightDataTable));
        rightDataSchema = rightDataTable.getDataSchema();
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while deserializing the right table: " + rightTableName);
      }
    } else {
      rightDataSchema = rightTable.getDataSchema();
    }
    String[] rightRawColumns = rightDataSchema.getColumnNames();
    ColumnDataType[] rightColumnDataTypes = rightDataSchema.getColumnDataTypes();
    Object2IntMap<String> rightRawColumnToIndexMap = new Object2IntOpenHashMap<>(rightRawColumns.length);
    rightRawColumnToIndexMap.defaultReturnValue(-1);
    for (int i = 0; i < rightRawColumns.length; i++) {
      rightRawColumnToIndexMap.put(rightRawColumns[i], i);
    }

    // Construct the look-up table
    int rightJoinColumnIndex = rightRawColumnToIndexMap.getInt(rightJoinKey);
    Preconditions.checkArgument(rightJoinColumnIndex != -1, "Failed to find right join column: %s", rightJoinKey);
    Map<Object, List<Object[]>> lookupTable = new HashMap<>();
    if (rightTable == null) {
      int numRows = rightDataTable.getNumberOfRows();
      for (int i = 0; i < numRows; i++) {
        Object[] row = SelectionOperatorUtils.extractRowFromDataTable(rightDataTable, i);
        Object key = row[rightJoinColumnIndex];
        lookupTable.computeIfAbsent(key, k1 -> new ArrayList<>()).add(row);
      }
    } else {
      List<Object[]> rows = rightTable.getRows();
      for (Object[] row : rows) {
        Object key = row[rightJoinColumnIndex];
        lookupTable.computeIfAbsent(key, k1 -> new ArrayList<>()).add(row);
      }
    }
    context._lookupTable = lookupTable;

    Object2IntMap<ExpressionContext> leftRawColumnToIndexMap = new Object2IntOpenHashMap<>();
    leftRawColumnToIndexMap.defaultReturnValue(-1);
    List<ExpressionContext> leftRawColumns = new ArrayList<>();
    leftRawColumnToIndexMap.put(leftJoinKey, 0);
    leftRawColumns.add(leftJoinKey);

    // Compute the project column indexes and types
    String leftTableColumnPrefix = leftTableName + ".";
    int leftPrefixLength = leftTableColumnPrefix.length();
    String rightTableColumnPrefix = rightTableName + ".";
    int rightPrefixLength = rightTableColumnPrefix.length();
    int numProjectColumns = projectColumns.length;
    boolean[] projectColumnInLeftTable = new boolean[numProjectColumns];
    int[] projectColumnIndexes = new int[numProjectColumns];
    ColumnDataType[] projectColumnTypes = new ColumnDataType[numProjectColumns];
    Map<String, ColumnContext> columnContextMap = new HashMap<>(HashUtil.getHashMapCapacity(numProjectColumns));
    for (int i = 0; i < numProjectColumns; i++) {
      String projectColumn = projectColumns[i];
      ColumnDataType columnDataType;
      if (projectColumn.startsWith(leftTableColumnPrefix)) {
        projectColumnInLeftTable[i] = true;
        ExpressionContext leftRawColumn = ExpressionContext.forIdentifier(projectColumn.substring(leftPrefixLength));
        int index = leftRawColumnToIndexMap.computeIntIfAbsent(leftRawColumn, k1 -> {
          leftRawColumns.add(k1);
          return leftRawColumnToIndexMap.size();
        });
        projectColumnIndexes[i] = index;
        ColumnContext columnContext = _leftProjectOperator.getResultColumnContext(leftRawColumn);
        columnDataType = ColumnDataType.fromDataType(columnContext.getDataType(), columnContext.isSingleValue());
      } else {
        Preconditions.checkArgument(projectColumn.startsWith(rightTableColumnPrefix),
            "Illegal project column: %s (no table prefix)", projectColumn);
        String rightRawColumn = projectColumn.substring(rightPrefixLength);
        int index = rightRawColumnToIndexMap.getInt(rightRawColumn);
        Preconditions.checkArgument(index != -1, "Failed to find right project column: %s", rightRawColumn);
        projectColumnIndexes[i] = index;
        columnDataType = rightColumnDataTypes[index];
      }
      projectColumnTypes[i] = columnDataType;
      columnContextMap.put(projectColumn, ColumnContext.fromColumnDataType(columnDataType));
    }

    context._leftRawColumns = leftRawColumns.toArray(new ExpressionContext[0]);
    context._projectColumnInLeftTable = projectColumnInLeftTable;
    context._projectColumnIndexes = projectColumnIndexes;
    context._columnContextMap = columnContextMap;
    context._resultDataSchema = new DataSchema(projectColumns, projectColumnTypes);
    return context;
  }

  @Override
  public Map<String, ColumnContext> getSourceColumnContextMap() {
    return _context._columnContextMap;
  }

  @Override
  public ColumnContext getResultColumnContext(ExpressionContext expression) {
    assert expression.getType() == ExpressionContext.Type.IDENTIFIER;
    return _context._columnContextMap.get(expression.getIdentifier());
  }

  @Override
  protected RowBasedValueBlock getNextBlock() {
    ValueBlock valueBlock = _leftProjectOperator.nextBlock();
    if (valueBlock == null) {
      return null;
    }
    List<Object[]> rows = new ArrayList<>();
    int numLeftColumns = _context._leftRawColumns.length;
    Object[][] leftColumnValues = new Object[numLeftColumns][];
    int numDocs = valueBlock.getNumDocs();
    for (int i = 0; i < numLeftColumns; i++) {
      ExpressionContext leftColumn = _context._leftRawColumns[i];
      BlockValSet valueSet = valueBlock.getBlockValueSet(leftColumn);
      switch (valueSet.getValueType().getStoredType()) {
        case INT: {
          Object[] values = leftColumnValues[i];
          if (values == null || values.length < numDocs) {
            values = new Object[numDocs];
            leftColumnValues[i] = values;
          }
          int[] intValuesSV = valueSet.getIntValuesSV();
          for (int j = 0; j < numDocs; j++) {
            values[j] = intValuesSV[j];
          }
          break;
        }
        case LONG: {
          Object[] values = leftColumnValues[i];
          if (values == null || values.length < numDocs) {
            values = new Object[numDocs];
            leftColumnValues[i] = values;
          }
          long[] longValuesSV = valueSet.getLongValuesSV();
          for (int j = 0; j < numDocs; j++) {
            values[j] = longValuesSV[j];
          }
          break;
        }
        case FLOAT: {
          Object[] values = leftColumnValues[i];
          if (values == null || values.length < numDocs) {
            values = new Object[numDocs];
            leftColumnValues[i] = values;
          }
          float[] floatValuesSV = valueSet.getFloatValuesSV();
          for (int j = 0; j < numDocs; j++) {
            values[j] = floatValuesSV[j];
          }
          break;
        }
        case DOUBLE: {
          Object[] values = leftColumnValues[i];
          if (values == null || values.length < numDocs) {
            values = new Object[numDocs];
            leftColumnValues[i] = values;
          }
          double[] doubleValuesSV = valueSet.getDoubleValuesSV();
          for (int j = 0; j < numDocs; j++) {
            values[j] = doubleValuesSV[j];
          }
          break;
        }
        case BIG_DECIMAL:
          leftColumnValues[i] = valueSet.getBigDecimalValuesSV();
          break;
        case STRING:
          leftColumnValues[i] = valueSet.getStringValuesSV();
          break;
        case BYTES:
          leftColumnValues[i] = valueSet.getBytesValuesSV();
          break;
        default:
          throw new IllegalStateException("Unsupported value type: " + valueSet.getValueType().getStoredType());
      }
    }
    int numProjectColumns = _context._projectColumnIndexes.length;
    for (int i = 0; i < numDocs; i++) {
      Object key = leftColumnValues[0][i];
      List<Object[]> rightRows = _context._lookupTable.get(key);
      if (rightRows != null) {
        for (Object[] rightRow : rightRows) {
          Object[] row = new Object[numProjectColumns];
          for (int j = 0; j < numProjectColumns; j++) {
            int columnIndex = _context._projectColumnIndexes[j];
            if (_context._projectColumnInLeftTable[j]) {
              row[j] = leftColumnValues[columnIndex][i];
            } else {
              row[j] = rightRow[columnIndex];
            }
          }
          rows.add(row);
        }
      }
    }

    return new RowBasedValueBlock(_context._resultDataSchema, rows);
  }

  @Override
  public List<BaseProjectOperator<?>> getChildOperators() {
    return Collections.singletonList(_leftProjectOperator);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _leftProjectOperator.getExecutionStatistics();
  }

  public static class LookUpContext {
    // TODO: Consider adding a single value lookup table when all the lists are single element
    public Map<Object, List<Object[]>> _lookupTable;
    public ExpressionContext[] _leftRawColumns;
    public boolean[] _projectColumnInLeftTable;
    public int[] _projectColumnIndexes;
    public Map<String, ColumnContext> _columnContextMap;
    public DataSchema _resultDataSchema;
  }
}
