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
package org.apache.pinot.core.query.reduce;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.query.aggregation.DistinctTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;


/**
 * Helper class to set Aggregation results into the BrokerResponseNative
 * Also handles Distinct results
 */
public class AggregationResultSetter extends ResultSetter {

  public AggregationResultSetter(String tableName, BrokerRequest brokerRequest, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      BrokerMetrics brokerMetrics) {
    super(tableName, brokerRequest, dataSchema, dataTableMap, brokerResponseNative, brokerMetrics);
  }

  /**
   * Sets aggregations results into ResultTable, if responseFormat = sql
   * By default, sets aggregation results into AggregationResults, and distinct results into SelectionResults
   */
  public void setAggregationResults() {

    AggregationFunction[] aggregationFunctions = AggregationFunctionUtils.getAggregationFunctions(_brokerRequest);
    List<AggregationInfo> aggregationInfos = _brokerRequest.getAggregationsInfo();
    int numAggregationFunctions = aggregationFunctions.length;

    if (_dataTableMap.isEmpty()) {
      if (_responseFormatSql) {
        DataSchema finalDataSchema =
            getResultTableDataSchema(aggregationInfos, aggregationFunctions, numAggregationFunctions);
        _brokerResponseNative.setResultTable(new ResultTable(finalDataSchema, new ArrayList<>(0)));
      }
      return;
    }

    assert _dataSchema != null;

    Object[] intermediateResults = mergeAggregationResults(aggregationFunctions, numAggregationFunctions);
    if (isDistinct(aggregationFunctions)) {
      setDistinctResults(intermediateResults);
    } else {
      setAggregationResults(aggregationFunctions, numAggregationFunctions, aggregationInfos, intermediateResults);
    }
  }

  /**
   * Constructs the data schema for the final results table
   */
  private DataSchema getResultTableDataSchema(List<AggregationInfo> aggregationInfos,
      AggregationFunction[] aggregationFunctions, int numAggregationFunctions) {
    String[] finalColumnNames = new String[numAggregationFunctions];
    DataSchema.ColumnDataType[] finalColumnDataTypes = new DataSchema.ColumnDataType[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      finalColumnNames[i] = AggregationFunctionUtils.getAggregationColumnName(aggregationInfos.get(i));
      finalColumnDataTypes[i] = aggregationFunctions[i].getFinalResultColumnType();
    }
    return new DataSchema(finalColumnNames, finalColumnDataTypes);
  }

  /**
   * Merge results from all data tables.
   */
  private Object[] mergeAggregationResults(AggregationFunction[] aggregationFunctions, int numAggregationFunctions) {
    Object[] intermediateResults = new Object[numAggregationFunctions];
    for (DataTable dataTable : _dataTables) {
      for (int i = 0; i < numAggregationFunctions; i++) {
        Object intermediateResultToMerge;
        DataSchema.ColumnDataType columnDataType = _dataSchema.getColumnDataType(i);
        switch (columnDataType) {
          case LONG:
            intermediateResultToMerge = dataTable.getLong(0, i);
            break;
          case DOUBLE:
            intermediateResultToMerge = dataTable.getDouble(0, i);
            break;
          case OBJECT:
            intermediateResultToMerge = dataTable.getObject(0, i);
            break;
          default:
            throw new IllegalStateException("Illegal column data type in aggregation results: " + columnDataType);
        }
        Object mergedIntermediateResult = intermediateResults[i];
        if (mergedIntermediateResult == null) {
          intermediateResults[i] = intermediateResultToMerge;
        } else {
          intermediateResults[i] = aggregationFunctions[i].merge(mergedIntermediateResult, intermediateResultToMerge);
        }
      }
    }
    return intermediateResults;
  }

  private boolean isDistinct(final AggregationFunction[] aggregationFunctions) {
    return aggregationFunctions.length == 1 && aggregationFunctions[0].getType() == AggregationFunctionType.DISTINCT;
  }

  /**
   * The DISTINCT query is just another SELECTION style query from the user's point of view
   * and will return one or records in the result table for the column selected.
   * Internally the execution is happening as an aggregation function (but that is an implementation
   * detail) and so for that reason, response from broker should be a selection query result.
   *
   * Up until now, we have treated DISTINCT similar to another aggregation function even in terms
   * of the result from function since it has been implemented as an aggregation function.
   * However, the broker response will be a selection query response as that makes sense from SQL
   * perspective
   */
  private void setDistinctResults(Object[] intermediateResults) {
    Object merged = intermediateResults[0];
    Preconditions.checkState(merged instanceof DistinctTable, "Error: Expecting merged result of type DistinctTable");
    DistinctTable distinctTable = (DistinctTable) merged;
    if (_responseFormatSql) {
      setSqlDistinctResults(distinctTable);
    } else {
      setPqlDistinctResults(distinctTable);
    }
  }

  /**
   * Sets distinct results into ResultsTable
   */
  private void setSqlDistinctResults(DistinctTable distinctTable) {
    String[] columnNames = distinctTable.getColumnNames();
    List<Object[]> resultSet = new ArrayList<>(distinctTable.size());
    Iterator<Key> iterator = distinctTable.getIterator();

    while (iterator.hasNext()) {
      Key key = iterator.next();
      Object[] columns = key.getColumns();
      Preconditions.checkState(columns.length == columnNames.length,
          "Error: unexpected number of columns in RecordHolder for DISTINCT");
      resultSet.add(columns);
    }
    // FIXME: how do we get the correct data types?
    DataSchema.ColumnDataType[] finalColumnDataTypes = new DataSchema.ColumnDataType[columnNames.length];
    Arrays.fill(finalColumnDataTypes, DataSchema.ColumnDataType.OBJECT);
    DataSchema finalDataSchema = new DataSchema(columnNames, finalColumnDataTypes);
    _brokerResponseNative.setResultTable(new ResultTable(finalDataSchema, resultSet));
  }

  /**
   * Sets distinct results into SelectionResults
   */
  private void setPqlDistinctResults(DistinctTable distinctTable) {
    String[] columnNames = distinctTable.getColumnNames();
    List<Serializable[]> resultSet = new ArrayList<>(distinctTable.size());
    Iterator<Key> iterator = distinctTable.getIterator();

    while (iterator.hasNext()) {
      Key key = iterator.next();
      Object[] columns = key.getColumns();
      Preconditions.checkState(columns.length == columnNames.length,
          "Error: unexpected number of columns in RecordHolder for DISTINCT");
      Serializable[] distinctRow = new Serializable[columns.length];
      for (int col = 0; col < columns.length; col++) {
        final Serializable columnValue = AggregationFunctionUtils.getSerializableValue(columns[col]);
        distinctRow[col] = columnValue;
      }
      resultSet.add(distinctRow);
    }
    _brokerResponseNative.setSelectionResults((new SelectionResults(Arrays.asList(columnNames), resultSet)));
  }

  private void setAggregationResults(AggregationFunction[] aggregationFunctions, int numAggregationFunctions,
      List<AggregationInfo> aggregationInfos, Object[] intermediateResults) {
    if (_responseFormatSql) {
      setSqlAggregationResult(aggregationFunctions, numAggregationFunctions, aggregationInfos, intermediateResults);
    } else {
      setPqlAggregationResult(aggregationFunctions, numAggregationFunctions, intermediateResults);
    }
  }

  /**
   * Sets aggregation results into ResultsTable
   */
  private void setSqlAggregationResult(AggregationFunction[] aggregationFunctions, int numAggregationFunctions,
      List<AggregationInfo> aggregationInfos, Object[] intermediateResults) {
    List<Object[]> rows = new ArrayList<>(1);
    Object[] row = new Object[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      row[i] = aggregationFunctions[i].extractFinalResult(intermediateResults[i]);
      // Format the value into string if required
      if (!_preserveType) {
        row[i] = AggregationFunctionUtils.formatValue(row[i]);
      }
    }
    rows.add(row);

    DataSchema finalDataSchema =
        getResultTableDataSchema(aggregationInfos, aggregationFunctions, numAggregationFunctions);
    _brokerResponseNative.setResultTable(new ResultTable(finalDataSchema, rows));
  }

  /**
   * Sets aggregation results into AggregationResults
   */
  private void setPqlAggregationResult(AggregationFunction[] aggregationFunctions, int numAggregationFunctions,
      Object[] intermediateResults) {
    // Extract final results and set them into the broker response.
    List<AggregationResult> reducedAggregationResults = new ArrayList<>(numAggregationFunctions);
    for (int i = 0; i < numAggregationFunctions; i++) {
      Serializable resultValue = AggregationFunctionUtils
          .getSerializableValue(aggregationFunctions[i].extractFinalResult(intermediateResults[i]));

      // Format the value into string if required
      if (!_preserveType) {
        resultValue = AggregationFunctionUtils.formatValue(resultValue);
      }
      reducedAggregationResults.add(new AggregationResult(_dataSchema.getColumnName(i), resultValue));
    }
    _brokerResponseNative.setAggregationResults(reducedAggregationResults);
  }
}
