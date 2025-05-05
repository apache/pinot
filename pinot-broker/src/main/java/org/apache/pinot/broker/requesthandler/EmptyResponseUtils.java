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
package org.apache.pinot.broker.requesthandler;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.logical.RelToPlanNodeConverter;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/// Similar to [org.apache.pinot.core.operator.blocks.results.ResultsBlockUtils] which handles empty results on the
/// server side, this class handles empty results on the broker side.
// TODO: Consider extracting common code for these 2 classes.
@SuppressWarnings({"rawtypes", "unchecked"})
public class EmptyResponseUtils {
  private EmptyResponseUtils() {
  }

  // TODO: Use schema to fill the correct data types for empty results.
  public static ResultTable buildEmptyResultTable(QueryContext queryContext) {
    if (queryContext.getAggregationFunctions() == null) {
      return buildEmptySelectionResultTable(queryContext);
    }
    if (queryContext.getGroupByExpressions() == null) {
      return buildEmptyAggregationResultTable(queryContext);
    }
    return buildEmptyGroupByResultTable(queryContext);
  }

  private static ResultTable buildEmptySelectionResultTable(QueryContext queryContext) {
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    int numSelectExpressions = selectExpressions.size();
    String[] columnNames = new String[numSelectExpressions];
    for (int i = 0; i < numSelectExpressions; i++) {
      columnNames[i] = selectExpressions.get(i).toString();
    }
    ColumnDataType[] columnDataTypes = new ColumnDataType[numSelectExpressions];
    // NOTE: Use STRING column data type as default for selection query
    Arrays.fill(columnDataTypes, ColumnDataType.STRING);
    return new ResultTable(new DataSchema(columnNames, columnDataTypes), List.of());
  }

  private static ResultTable buildEmptyAggregationResultTable(QueryContext queryContext) {
    List<Pair<AggregationFunction, FilterContext>> filteredAggregationFunctions =
        queryContext.getFilteredAggregationFunctions();
    assert filteredAggregationFunctions != null;
    int numAggregations = filteredAggregationFunctions.size();
    String[] columnNames = new String[numAggregations];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numAggregations];
    Object[] row = new Object[numAggregations];
    for (int i = 0; i < numAggregations; i++) {
      Pair<AggregationFunction, FilterContext> pair = filteredAggregationFunctions.get(i);
      AggregationFunction aggregationFunction = pair.getLeft();
      columnNames[i] = AggregationFunctionUtils.getResultColumnName(aggregationFunction, pair.getRight());
      columnDataTypes[i] = aggregationFunction.getFinalResultColumnType();
      row[i] = aggregationFunction.extractFinalResult(
          aggregationFunction.extractAggregationResult(aggregationFunction.createAggregationResultHolder()));
    }
    return new ResultTable(new DataSchema(columnNames, columnDataTypes), List.<Object[]>of(row));
  }

  private static ResultTable buildEmptyGroupByResultTable(QueryContext queryContext) {
    List<Pair<AggregationFunction, FilterContext>> filteredAggregationFunctions =
        queryContext.getFilteredAggregationFunctions();
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert filteredAggregationFunctions != null && groupByExpressions != null;
    int numColumns = groupByExpressions.size() + filteredAggregationFunctions.size();
    String[] columnNames = new String[numColumns];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];
    int index = 0;
    for (ExpressionContext groupByExpression : groupByExpressions) {
      columnNames[index] = groupByExpression.toString();
      // Use STRING column data type as default for group-by expressions
      columnDataTypes[index] = ColumnDataType.STRING;
      index++;
    }
    for (Pair<AggregationFunction, FilterContext> pair : filteredAggregationFunctions) {
      AggregationFunction aggregationFunction = pair.getLeft();
      columnNames[index] = AggregationFunctionUtils.getResultColumnName(aggregationFunction, pair.getRight());
      columnDataTypes[index] = aggregationFunction.getFinalResultColumnType();
      index++;
    }
    return new ResultTable(new DataSchema(columnNames, columnDataTypes), List.of());
  }

  /// Tries to fill an [DataSchema] when no row has been returned.
  ///
  /// Response data schema can be inaccurate (all columns set to STRING) when all segments are pruned on broker or
  /// server.
  ///
  /// Priority is:
  /// - Types from multi-stage engine validation for the given query (if allowed).
  /// - Types from schema for the given table (only applicable to identifiers).
  /// - Types from single-stage engine response (no action).
  ///
  /// Multi-stage engine schema will be available only if query compiles.
  public static void fillEmptyResponseSchema(boolean useMSE, BrokerResponse response, TableCache tableCache,
      Schema schema, String database, String query) {
    Preconditions.checkState(response.getNumRowsResultSet() == 0, "Cannot fill schema for non-empty response");

    DataSchema dataSchema = response.getResultTable() != null ? response.getResultTable().getDataSchema() : null;

    List<RelDataTypeField> dataTypeFields = null;
    // Turn on (with pinot.broker.use.mse.to.fill.empty.response.schema=true or query option
    // useMSEToFillEmptyResponseSchema=true) only for clusters where no queries with huge IN clauses are expected
    // (see https://github.com/apache/pinot/issues/15064)
    if (useMSE) {
      QueryEnvironment queryEnvironment = new QueryEnvironment(database, tableCache, null);
      RelRoot root;
      try (QueryEnvironment.CompiledQuery compiledQuery = queryEnvironment.compile(query)) {
        root = compiledQuery.getRelRoot();
      } catch (Exception ignored) {
        root = null;
      }
      if (root != null && root.validatedRowType != null) {
        dataTypeFields = root.validatedRowType.getFieldList();
      }
    }

    if (dataSchema == null && dataTypeFields == null) {
      // No schema available, nothing we can do
      return;
    }

    if (dataSchema == null || (dataTypeFields != null && dataSchema.size() != dataTypeFields.size())) {
      // If data schema is not available or has different number of columns than the validated row type, we use the
      // validated row type to populate the schema.
      int numColumns = dataTypeFields.size();
      String[] columnNames = new String[numColumns];
      ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];
      for (int i = 0; i < numColumns; i++) {
        RelDataTypeField dataTypeField = dataTypeFields.get(i);
        columnNames[i] = dataTypeField.getName();
        ColumnDataType columnDataType;
        try {
          columnDataType = RelToPlanNodeConverter.convertToColumnDataType(dataTypeField.getType());
        } catch (Exception ignored) {
          columnDataType = ColumnDataType.UNKNOWN;
        }
        columnDataTypes[i] = columnDataType;
      }
      response.setResultTable(new ResultTable(new DataSchema(columnNames, columnDataTypes), List.of()));
      return;
    }

    // When data schema is available, try to fix the data types within it.
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    if (dataTypeFields != null) {
      // Fill data type with the validated row type when it is available.
      for (int i = 0; i < numColumns; i++) {
        try {
          columnDataTypes[i] = RelToPlanNodeConverter.convertToColumnDataType(dataTypeFields.get(i).getType());
        } catch (Exception ignored) {
          // Ignore exception and keep the type from response
        }
      }
    } else {
      // Fill data type with the schema when validated row type is not available.
      String[] columnNames = dataSchema.getColumnNames();
      for (int i = 0; i < numColumns; i++) {
        FieldSpec fieldSpec = schema.getFieldSpecFor(columnNames[i]);
        if (fieldSpec != null) {
          try {
            columnDataTypes[i] = ColumnDataType.fromDataType(fieldSpec.getDataType(), fieldSpec.isSingleValueField());
          } catch (Exception ignored) {
            // Ignore exception and keep the type from response
          }
        }
      }
    }
  }
}
