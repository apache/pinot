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
package org.apache.pinot.core.operator.blocks;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilderUtils;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;


public final class InstanceResponseUtils {
  private InstanceResponseUtils() {
    // do not instantiate.
  }

  public static InstanceResponseBlock getEmptyResponseBlock(DataSchema explainResultSchema) {
    return new InstanceResponseBlock(new MetadataResultsBlock(explainResultSchema), null);
  }

  public static InstanceResponseBlock getExceptionBlock(ProcessingException exception) {
    return new InstanceResponseBlock(new ExceptionResultsBlock(exception), null);
  }

  public static DataTable toDataTable(InstanceResponseBlock instanceResponseBlock) {
    DataTable dataTable;
    try {
      if (instanceResponseBlock.getBaseResultsBlock() == null) {
        dataTable = DataTableBuilderUtils.buildEmptyDataTable();
      } else {
         dataTable = instanceResponseBlock.getBaseResultsBlock().getDataTable(instanceResponseBlock.getQueryContext());
      }
      if (MapUtils.isNotEmpty(instanceResponseBlock.getExceptionMap())) {
        for (Map.Entry<Integer, String> e : instanceResponseBlock.getExceptionMap().entrySet()) {
          dataTable.addException(e.getKey(), e.getValue());
        }
      }
      if (MapUtils.isNotEmpty(instanceResponseBlock.getInstanceResponseMetadata())) {
        dataTable.getMetadata().putAll(instanceResponseBlock.getInstanceResponseMetadata());
      }
      return dataTable;
    } catch (ProcessingException pe) {
      return toDataTable(getExceptionBlock(pe));
    } catch (Exception e) {
      return toDataTable(getExceptionBlock(QueryException.UNKNOWN_ERROR));
    }
  }

  public static Collection<Object[]> toRows(InstanceResponseBlock instanceResponseBlock) {
    try {
      return instanceResponseBlock.getBaseResultsBlock().getRows(instanceResponseBlock.getQueryContext());
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while building data table", e);
    }
  }

  public static DataTable getDataSchema(InstanceResponseBlock instanceResponseBlock) {
    try {
      return instanceResponseBlock.getBaseResultsBlock().getDataTable(instanceResponseBlock.getQueryContext());
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while building data table", e);
    }
  }

  /**
   * Builds an empty data table based on the broker request.
   */
  public static InstanceResponseBlock buildEmptyResultBlock(QueryContext queryContext)
      throws Exception {
    if (QueryContextUtils.isSelectionQuery(queryContext)) {
      return buildEmptyResultForSelectionQuery(queryContext);
    } else if (QueryContextUtils.isAggregationQuery(queryContext)) {
      return buildEmptyResultForAggregationQuery(queryContext);
    } else {
      assert QueryContextUtils.isDistinctQuery(queryContext);
      return buildEmptyResultForDistinctQuery(queryContext);
    }
  }

  /**
   * Helper method to build an empty data table for selection query.
   */
  private static InstanceResponseBlock buildEmptyResultForSelectionQuery(QueryContext queryContext) {
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    int numSelectExpressions = selectExpressions.size();
    String[] columnNames = new String[numSelectExpressions];
    for (int i = 0; i < numSelectExpressions; i++) {
      columnNames[i] = selectExpressions.get(i).toString();
    }
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numSelectExpressions];
    // NOTE: Use STRING column data type as default for selection query
    Arrays.fill(columnDataTypes, DataSchema.ColumnDataType.STRING);
    return getEmptyResponseBlock(new DataSchema(columnNames, columnDataTypes));
  }

  /**
   * Helper method to build an empty data table for aggregation query.
   */
  private static InstanceResponseBlock buildEmptyResultForAggregationQuery(QueryContext queryContext)
      throws Exception {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    int numAggregations = aggregationFunctions.length;
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    if (groupByExpressions != null) {
      // Aggregation group-by query

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
      return getEmptyResponseBlock(new DataSchema(columnNames, columnDataTypes));
    } else {
      // Aggregation only query

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

      // Build the result
      InstanceResponseBlock instanceResponseBlock = getEmptyResponseBlock(
          new DataSchema(aggregationColumnNames, columnDataTypes));
      MetadataResultsBlock baseResultsBlock = (MetadataResultsBlock) instanceResponseBlock.getBaseResultsBlock();
      Collection<Object[]> rows = baseResultsBlock.getRows(instanceResponseBlock.getQueryContext());
      Object[] row = new Object[columnDataTypes.length];
      for (int i = 0; i < numAggregations; i++) {
        switch (columnDataTypes[i]) {
          case LONG:
            row[i] = ((Number) aggregationResults[i]).longValue();
            break;
          case DOUBLE:
            row[i] = ((Double) aggregationResults[i]).doubleValue();
            break;
          case OBJECT:
            row[i] = aggregationResults[i];
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported aggregation column data type: " + columnDataTypes[i] + " for column: "
                    + aggregationColumnNames[i]);
        }
      }
      rows.add(row);
      return instanceResponseBlock;
    }
  }

  /**
   * Helper method to build an empty data table for distinct query.
   */
  private static InstanceResponseBlock buildEmptyResultForDistinctQuery(QueryContext queryContext)
      throws Exception {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null && aggregationFunctions.length == 1
        && aggregationFunctions[0] instanceof DistinctAggregationFunction;
    DistinctAggregationFunction distinctAggregationFunction = (DistinctAggregationFunction) aggregationFunctions[0];

    // Create the distinct table
    String[] columnNames = distinctAggregationFunction.getColumns();
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[columnNames.length];
    // NOTE: Use STRING column data type as default for distinct query
    Arrays.fill(columnDataTypes, DataSchema.ColumnDataType.STRING);
    DistinctTable distinctTable =
        new DistinctTable(new DataSchema(columnNames, columnDataTypes), Collections.emptySet(),
            queryContext.isNullHandlingEnabled());

    // Build the result
    InstanceResponseBlock instanceResponseBlock = getEmptyResponseBlock(
        new DataSchema(new String[]{distinctAggregationFunction.getColumnName()},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.OBJECT}));
    MetadataResultsBlock baseResultsBlock = (MetadataResultsBlock) instanceResponseBlock.getBaseResultsBlock();
    Collection<Object[]> rows = baseResultsBlock.getRows(instanceResponseBlock.getQueryContext());
    rows.add(new Object[]{distinctTable});
    return instanceResponseBlock;
  }
}
