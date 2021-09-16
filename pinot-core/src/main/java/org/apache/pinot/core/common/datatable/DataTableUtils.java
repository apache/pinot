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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.spi.utils.DataSchema;
import org.apache.pinot.spi.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.DataTable;


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

    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    int rowSizeInBytes = 0;
    for (int i = 0; i < numColumns; i++) {
      columnOffsets[i] = rowSizeInBytes;
      switch (storedColumnDataTypes[i]) {
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
    if (QueryContextUtils.isSelectionQuery(queryContext)) {
      return buildEmptyDataTableForSelectionQuery(queryContext);
    } else if (QueryContextUtils.isAggregationQuery(queryContext)) {
      return buildEmptyDataTableForAggregationQuery(queryContext);
    } else {
      assert QueryContextUtils.isDistinctQuery(queryContext);
      return buildEmptyDataTableForDistinctQuery(queryContext);
    }
  }

  /**
   * Helper method to build an empty data table for selection query.
   */
  private static DataTable buildEmptyDataTableForSelectionQuery(QueryContext queryContext) {
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    int numSelectExpressions = selectExpressions.size();
    String[] columnNames = new String[numSelectExpressions];
    for (int i = 0; i < numSelectExpressions; i++) {
      columnNames[i] = selectExpressions.get(i).toString();
    }
    ColumnDataType[] columnDataTypes = new ColumnDataType[numSelectExpressions];
    // NOTE: Use STRING column data type as default for selection query
    Arrays.fill(columnDataTypes, ColumnDataType.STRING);
    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    return new DataTableBuilder(dataSchema).build();
  }

  /**
   * Helper method to build an empty data table for aggregation query.
   */
  private static DataTable buildEmptyDataTableForAggregationQuery(QueryContext queryContext)
      throws IOException {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    int numAggregations = aggregationFunctions.length;
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    if (groupByExpressions != null) {
      // Aggregation group-by query

      if (new QueryOptions(queryContext.getQueryOptions()).isGroupByModeSQL()) {
        // SQL format

        int numColumns = groupByExpressions.size() + numAggregations;
        String[] columnNames = new String[numColumns];
        ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];
        int index = 0;
        for (ExpressionContext groupByExpression : groupByExpressions) {
          columnNames[index] = groupByExpression.toString();
          // Use STRING column data type as default for group-by expressions
          columnDataTypes[index] = ColumnDataType.STRING;
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
        ColumnDataType[] columnDataTypes = new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.OBJECT};

        // Build the data table
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
      // Aggregation only query

      String[] aggregationColumnNames = new String[numAggregations];
      ColumnDataType[] columnDataTypes = new ColumnDataType[numAggregations];
      Object[] aggregationResults = new Object[numAggregations];
      for (int i = 0; i < numAggregations; i++) {
        AggregationFunction aggregationFunction = aggregationFunctions[i];
        // NOTE: For backward-compatibility, use AggregationFunction.getColumnName() for aggregation only query
        aggregationColumnNames[i] = aggregationFunction.getColumnName();
        columnDataTypes[i] = aggregationFunction.getIntermediateResultColumnType();
        aggregationResults[i] =
            aggregationFunction.extractAggregationResult(aggregationFunction.createAggregationResultHolder());
      }

      // Build the data table
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

  /**
   * Helper method to build an empty data table for distinct query.
   */
  private static DataTable buildEmptyDataTableForDistinctQuery(QueryContext queryContext)
      throws IOException {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null && aggregationFunctions.length == 1
        && aggregationFunctions[0] instanceof DistinctAggregationFunction;
    DistinctAggregationFunction distinctAggregationFunction = (DistinctAggregationFunction) aggregationFunctions[0];

    // Create the distinct table
    String[] columnNames = distinctAggregationFunction.getColumns();
    ColumnDataType[] columnDataTypes = new ColumnDataType[columnNames.length];
    // NOTE: Use STRING column data type as default for distinct query
    Arrays.fill(columnDataTypes, ColumnDataType.STRING);
    DistinctTable distinctTable =
        new DistinctTable(new DataSchema(columnNames, columnDataTypes), Collections.emptySet());

    // Build the data table
    DataTableBuilder dataTableBuilder = new DataTableBuilder(
        new DataSchema(new String[]{distinctAggregationFunction.getColumnName()},
            new ColumnDataType[]{ColumnDataType.OBJECT}));
    dataTableBuilder.startRow();
    dataTableBuilder.setColumn(0, distinctTable);
    dataTableBuilder.finishRow();
    return dataTableBuilder.build();
  }

  /**
   * Helper method to decode string.
   */
  public static String decodeString(DataInputStream dataInputStream)
      throws IOException {
    int length = dataInputStream.readInt();
    if (length == 0) {
      return StringUtils.EMPTY;
    } else {
      byte[] buffer = new byte[length];
      int numBytesRead = dataInputStream.read(buffer);
      assert numBytesRead == length;
      return StringUtil.decodeUtf8(buffer);
    }
  }

  /**
   * Helper method to decode int.
   */
  public static int decodeInt(DataInputStream dataInputStream)
      throws IOException {
    int length = Integer.BYTES;
    byte[] buffer = new byte[length];
    int numBytesRead = dataInputStream.read(buffer);
    assert numBytesRead == length;
    return Ints.fromByteArray(buffer);
  }

  /**
   * Helper method to decode long.
   */
  public static long decodeLong(DataInputStream dataInputStream)
      throws IOException {
    int length = Long.BYTES;
    byte[] buffer = new byte[length];
    int numBytesRead = dataInputStream.read(buffer);
    assert numBytesRead == length;
    return Longs.fromByteArray(buffer);
  }
}
