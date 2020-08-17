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
package org.apache.pinot.core.common.datatable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.QueryOptions;


/**
 * The <code>DataTableUtils</code> class provides utility methods for data table.
 */
@SuppressWarnings("rawtypes")
public class DataTableUtils {
  private DataTableUtils() {
  }

  /**
   * Given a {@link DataSchema}, compute each column's offset and fill them into the passed in array, then return the
   * row size in bytes.
   *
   * @param dataSchema data schema.
   * @param columnOffsets array of column offsets.
   * @return row size in bytes.
   */
  public static int computeColumnOffsets(DataSchema dataSchema, int[] columnOffsets) {
    int numColumns = columnOffsets.length;
    assert numColumns == dataSchema.size();

    int rowSizeInBytes = 0;
    for (int i = 0; i < numColumns; i++) {
      columnOffsets[i] = rowSizeInBytes;
      switch (dataSchema.getColumnDataType(i)) {
        case INT:
          rowSizeInBytes += 4;
          break;
        case LONG:
          rowSizeInBytes += 8;
          break;
        // TODO: fix float size (should be 4).
        // For backward compatible, DON'T CHANGE.
        case FLOAT:
          rowSizeInBytes += 8;
          break;
        case DOUBLE:
          rowSizeInBytes += 8;
          break;
        case STRING:
          rowSizeInBytes += 4;
          break;
        // Object and array. (POSITION|LENGTH)
        default:
          rowSizeInBytes += 8;
          break;
      }
    }

    return rowSizeInBytes;
  }

  /**
   * Builds an empty data table based on the broker request.
   */
  public static DataTable buildEmptyDataTable(QueryContext queryContext)
      throws IOException {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();

    // Selection query.
    if (aggregationFunctions == null) {
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      int numSelectExpressions = selectExpressions.size();
      String[] columnNames = new String[numSelectExpressions];
      for (int i = 0; i < numSelectExpressions; i++) {
        columnNames[i] = selectExpressions.get(i).toString();
      }
      DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numSelectExpressions];
      // NOTE: Use STRING column data type as default for selection query.
      Arrays.fill(columnDataTypes, DataSchema.ColumnDataType.STRING);
      DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
      return new DataTableBuilder(dataSchema).build();
    }

    // Aggregation query.
    int numAggregations = aggregationFunctions.length;
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    if (groupByExpressions != null) {
      // Aggregation group-by query.

      if (new QueryOptions(queryContext.getQueryOptions()).isGroupByModeSQL()) {
        // SQL format

        int numColumns = groupByExpressions.size() + numAggregations;
        String[] columnNames = new String[numColumns];
        DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numColumns];
        int index = 0;
        for (ExpressionContext groupByExpression : groupByExpressions) {
          columnNames[index] = groupByExpression.toString();
          // Use STRING column data type as default for group-by expressions
          columnDataTypes[index] = DataSchema.ColumnDataType.STRING;
          index++;
        }
        for (AggregationFunction aggregationFunction : aggregationFunctions) {
          // NOTE: Use AggregationFunction.getResultColumnName() for SQL format response
          columnNames[index] = aggregationFunction.getResultColumnName();
          columnDataTypes[index] = aggregationFunction.getIntermediateResultColumnType();
          index++;
        }
        return new DataTableBuilder(new DataSchema(columnNames, columnDataTypes)).build();
      } else {
        // PQL format

        String[] columnNames = new String[]{"functionName", "GroupByResultMap"};
        DataSchema.ColumnDataType[] columnDataTypes =
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.OBJECT};

        // Build the data table.
        DataTableBuilder dataTableBuilder = new DataTableBuilder(new DataSchema(columnNames, columnDataTypes));
        for (AggregationFunction aggregationFunction : aggregationFunctions) {
          dataTableBuilder.startRow();
          // NOTE: For backward-compatibility, use AggregationFunction.getColumnName() for PQL format response
          dataTableBuilder.setColumn(0, aggregationFunction.getColumnName());
          dataTableBuilder.setColumn(1, Collections.emptyMap());
          dataTableBuilder.finishRow();
        }
        return dataTableBuilder.build();
      }
    } else {
      // Aggregation only query.

      String[] aggregationColumnNames = new String[numAggregations];
      DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numAggregations];
      Object[] aggregationResults = new Object[numAggregations];
      for (int i = 0; i < numAggregations; i++) {
        AggregationFunction aggregationFunction = aggregationFunctions[i];
        // NOTE: For backward-compatibility, use AggregationFunction.getColumnName() for aggregation only query
        aggregationColumnNames[i] = aggregationFunction.getColumnName();
        columnDataTypes[i] = aggregationFunction.getIntermediateResultColumnType();
        aggregationResults[i] =
            aggregationFunction.extractAggregationResult(aggregationFunction.createAggregationResultHolder());
      }

      // Build the data table.
      DataTableBuilder dataTableBuilder = new DataTableBuilder(new DataSchema(aggregationColumnNames, columnDataTypes));
      dataTableBuilder.startRow();
      for (int i = 0; i < numAggregations; i++) {
        switch (columnDataTypes[i]) {
          case LONG:
            dataTableBuilder.setColumn(i, ((Number) aggregationResults[i]).longValue());
            break;
          case DOUBLE:
            dataTableBuilder.setColumn(i, ((Double) aggregationResults[i]).doubleValue());
            break;
          case OBJECT:
            dataTableBuilder.setColumn(i, aggregationResults[i]);
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported aggregation column data type: " + columnDataTypes[i] + " for column: "
                    + aggregationColumnNames[i]);
        }
      }
      dataTableBuilder.finishRow();
      return dataTableBuilder.build();
    }
  }
}
