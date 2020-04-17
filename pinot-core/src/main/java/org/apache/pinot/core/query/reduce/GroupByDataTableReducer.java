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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.HavingFilterQuery;
import org.apache.pinot.common.request.HavingFilterQueryMap;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByTrimmingService;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.QueryOptions;


/**
 * Helper class to reduce data tables and set group by results into the BrokerResponseNative
 */
public class GroupByDataTableReducer implements DataTableReducer {
  private final BrokerRequest _brokerRequest;
  private final AggregationFunction[] _aggregationFunctions;
  private final List<AggregationInfo> _aggregationInfos;
  private final AggregationFunctionContext[] _aggregationFunctionContexts;
  private final List<SelectionSort> _orderBy;
  private final GroupBy _groupBy;
  private final int _numAggregationFunctions;
  private final int _numGroupBy;
  private final int _numColumns;
  private final boolean _preserveType;
  private final boolean _groupByModeSql;
  private final boolean _responseFormatSql;
  private final List<Expression> _sqlSelectionList;
  private final List<Expression> _groupByList;

  GroupByDataTableReducer(BrokerRequest brokerRequest, AggregationFunction[] aggregationFunctions,
      QueryOptions queryOptions) {
    _brokerRequest = brokerRequest;
    _aggregationFunctions = aggregationFunctions;
    _aggregationInfos = brokerRequest.getAggregationsInfo();
    _aggregationFunctionContexts = AggregationFunctionUtils.getAggregationFunctionContexts(_brokerRequest);
    _numAggregationFunctions = aggregationFunctions.length;
    _groupBy = brokerRequest.getGroupBy();
    _numGroupBy = _groupBy.getExpressionsSize();
    _orderBy = brokerRequest.getOrderBy();
    _numColumns = _numAggregationFunctions + _numGroupBy;
    _preserveType = queryOptions.isPreserveType();
    _groupByModeSql = queryOptions.isGroupByModeSQL();
    _responseFormatSql = queryOptions.isResponseFormatSQL();
    if (_responseFormatSql && brokerRequest.getPinotQuery() != null) {
      _sqlSelectionList = brokerRequest.getPinotQuery().getSelectList();
      _groupByList = brokerRequest.getPinotQuery().getGroupByList();
    } else {
      _sqlSelectionList = null;
      _groupByList = null;
    }
  }

  /**
   * Reduces and sets group by results into ResultTable, if responseFormat = sql
   * By default, sets group by results into GroupByResults
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      BrokerMetrics brokerMetrics) {
    assert dataSchema != null;
    int resultSize = 0;
    Collection<DataTable> dataTables = dataTableMap.values();

    // For group by, PQL behavior is different than the SQL behavior. In the PQL way,
    // a result is generated for each aggregation in the query,
    // and the group by keys are not the same across the aggregations
    // This PQL style of execution makes it impossible to support order by on group by.
    //
    // We could not simply change the group by execution behavior,
    // as that would not be backward compatible for existing users of group by.
    // As a result, we have 2 modes of group by execution - pql and sql - which can be controlled via query options
    //
    // Long term, we may completely move to sql, and keep only full sql mode alive
    // Until then, we need to support responseFormat = sql for both the modes of execution.
    // The 4 variants are as described below:

    if (_groupByModeSql) {

      if (_responseFormatSql) {
        // 1. groupByMode = sql, responseFormat = sql
        // This is the primary SQL compliant group by

        setSQLGroupByInResultTable(brokerResponseNative, dataSchema, dataTables);
        resultSize = brokerResponseNative.getResultTable().getRows().size();
      } else {
        // 2. groupByMode = sql, responseFormat = pql
        // This mode will invoke SQL style group by execution, but present results in PQL way
        // This mode is useful for users who want to avail of SQL compliant group by behavior,
        // w/o having to forcefully move to a new result type

        setSQLGroupByInAggregationResults(brokerResponseNative, dataSchema, dataTables);
        if (!brokerResponseNative.getAggregationResults().isEmpty()) {
          resultSize = brokerResponseNative.getAggregationResults().get(0).getGroupByResult().size();
        }
      }
    } else {

      // 3. groupByMode = pql, responseFormat = sql
      // This mode is for users who want response presented in SQL style, but want PQL style group by behavior
      // Multiple aggregations in PQL violates the tabular nature of results
      // As a result, in this mode, only single aggregations are supported

      // 4. groupByMode = pql, responseFormat = pql
      // This is the primary PQL compliant group by

      boolean[] aggregationFunctionSelectStatus =
          AggregationFunctionUtils.getAggregationFunctionsSelectStatus(_aggregationInfos);
      setGroupByHavingResults(brokerResponseNative, aggregationFunctionSelectStatus, dataTables,
          _brokerRequest.getHavingFilterQuery(), _brokerRequest.getHavingFilterSubQueryMap());

      if (_responseFormatSql) {
        resultSize = brokerResponseNative.getResultTable().getRows().size();
      } else {
        // We emit the group by size when the result isn't empty. All the sizes among group-by results should be the same.
        // Thus, we can just emit the one from the 1st result.
        if (!brokerResponseNative.getAggregationResults().isEmpty()) {
          resultSize = brokerResponseNative.getAggregationResults().get(0).getGroupByResult().size();
        }
      }
    }

    if (brokerMetrics != null && resultSize > 0) {
      brokerMetrics.addMeteredQueryValue(_brokerRequest, BrokerMeter.GROUP_BY_SIZE, resultSize);
    }
  }

  /**
   * Extract group by order by results and set into {@link ResultTable}
   * @param brokerResponseNative broker response
   * @param dataSchema data schema
   * @param dataTables Collection of data tables
   */
  private void setSQLGroupByInResultTable(BrokerResponseNative brokerResponseNative, DataSchema dataSchema,
      Collection<DataTable> dataTables) {

    IndexedTable indexedTable = getIndexedTable(dataSchema, dataTables);

    int[] finalSchemaMapIdx = null;
    if (_sqlSelectionList != null) {
      finalSchemaMapIdx = getFinalSchemaMapIdx();
    }
    List<Object[]> rows = new ArrayList<>();
    Iterator<Record> sortedIterator = indexedTable.iterator();
    int numRows = 0;
    while (numRows < _groupBy.getTopN() && sortedIterator.hasNext()) {
      Record nextRecord = sortedIterator.next();
      Object[] values = nextRecord.getValues();

      int index = _numGroupBy;
      int aggNum = 0;
      while (index < _numColumns) {
        values[index] = _aggregationFunctions[aggNum++].extractFinalResult(values[index]);
        index++;
      }
      if (_sqlSelectionList != null) {
        Object[] finalValues = new Object[_sqlSelectionList.size()];
        for (int i = 0; i < finalSchemaMapIdx.length; i++) {
          finalValues[i] = values[finalSchemaMapIdx[i]];
        }
        rows.add(finalValues);
      } else {
        rows.add(values);
      }
      numRows++;
    }

    DataSchema finalDataSchema = getSQLResultTableSchema(dataSchema);
    if (_sqlSelectionList != null) {
      int columnSize = _sqlSelectionList.size();
      String[] columns = new String[columnSize];
      DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[columnSize];
      for (int i = 0; i < columnSize; i++) {
        columns[i] = finalDataSchema.getColumnName(finalSchemaMapIdx[i]);
        columnDataTypes[i] = finalDataSchema.getColumnDataType(finalSchemaMapIdx[i]);
      }
      finalDataSchema = new DataSchema(columns, columnDataTypes);
    }
    brokerResponseNative.setResultTable(new ResultTable(finalDataSchema, rows));
  }

  /**
   * Generate index mapping based on selection expression to DataTable schema, which is groupBy columns,
   * then aggregation functions.
   *
   * @return a mapping from final schema idx to corresponding idx in data table schema.
   */
  private int[] getFinalSchemaMapIdx() {
    int[] finalSchemaMapIdx = new int[_sqlSelectionList.size()];
    int nextAggregationIdx = _numGroupBy;
    for (int i = 0; i < _sqlSelectionList.size(); i++) {
      finalSchemaMapIdx[i] = getExpressionMapIdx(_sqlSelectionList.get(i), nextAggregationIdx);
      if (finalSchemaMapIdx[i] == nextAggregationIdx) {
        nextAggregationIdx++;
      }
    }
    return finalSchemaMapIdx;
  }

  private int getExpressionMapIdx(Expression expression, int nextAggregationIdx) {
    // Check if expression matches groupBy list.
    int idxFromGroupByList = getGroupByIdx(_groupByList, expression);
    if (idxFromGroupByList != -1) {
      return idxFromGroupByList;
    }
    // Handle all functions
    if (expression.getFunctionCall() != null) {
      // handle AS
      if (expression.getFunctionCall().getOperator().equalsIgnoreCase(SqlKind.AS.toString())) {
        return getExpressionMapIdx(expression.getFunctionCall().getOperands().get(0), nextAggregationIdx);
      }
      // Return next aggregation idx.
      return nextAggregationIdx;
    }
    // Shouldn't reach here.
    throw new IllegalArgumentException(
        "Failed to get index from GroupBy Clause for selected expression - " + RequestUtils.prettyPrint(expression));
  }

  /**
   * Trying to match an expression based on given groupByList.
   *
   * @param groupByList
   * @param expression
   * @return matched idx from groupByList
   */
  private int getGroupByIdx(List<Expression> groupByList, Expression expression) {
    for (int i = 0; i < groupByList.size(); i++) {
      Expression groupByExpr = groupByList.get(i);
      if (groupByExpr.equals(expression)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Constructs the final result table schema for sql mode execution
   * The data type for the aggregations needs to be taken from the final result's data type
   */
  private DataSchema getSQLResultTableSchema(DataSchema dataSchema) {
    String[] columns = dataSchema.getColumnNames();
    DataSchema.ColumnDataType[] finalColumnDataTypes = new DataSchema.ColumnDataType[_numColumns];
    int aggIdx = 0;
    for (int i = 0; i < _numColumns; i++) {
      if (i < _numGroupBy) {
        finalColumnDataTypes[i] = dataSchema.getColumnDataType(i);
      } else {
        finalColumnDataTypes[i] = _aggregationFunctions[aggIdx].getFinalResultColumnType();
        aggIdx++;
      }
    }
    return new DataSchema(columns, finalColumnDataTypes);
  }

  private IndexedTable getIndexedTable(DataSchema dataSchema, Collection<DataTable> dataTables) {

    int indexedTableCapacity = GroupByUtils.getTableCapacity(_groupBy, _orderBy);
    IndexedTable indexedTable =
        new ConcurrentIndexedTable(dataSchema, _aggregationInfos, _orderBy, indexedTableCapacity);

    for (DataTable dataTable : dataTables) {
      BiFunction[] functions = new BiFunction[_numColumns];
      for (int i = 0; i < _numColumns; i++) {
        DataSchema.ColumnDataType columnDataType = dataSchema.getColumnDataType(i);
        BiFunction<Integer, Integer, Object> function;
        switch (columnDataType) {
          case INT:
            function = dataTable::getInt;
            break;
          case LONG:
            function = dataTable::getLong;
            break;
          case FLOAT:
            function = dataTable::getFloat;
            break;
          case DOUBLE:
            function = dataTable::getDouble;
            break;
          case STRING:
            function = dataTable::getString;
            break;
          case BYTES:
            function = dataTable::getBytes;
            break;
          case OBJECT:
            function = dataTable::getObject;
            break;
          // Add other aggregation intermediate result / group-by column type supports here
          default:
            throw new IllegalStateException();
        }
        functions[i] = function;
      }

      for (int row = 0; row < dataTable.getNumberOfRows(); row++) {
        Object[] columns = new Object[_numColumns];
        for (int col = 0; col < _numColumns; col++) {
          columns[col] = functions[col].apply(row, col);
        }
        Record record = new Record(columns);
        indexedTable.upsert(record);
      }
    }
    indexedTable.finish(true);
    return indexedTable;
  }

  /**
   * Extract the results of group by order by into a List of {@link AggregationResult}
   * There will be 1 aggregation result per aggregation. The group by keys will be the same across all aggregations
   * @param brokerResponseNative broker response
   * @param dataSchema data schema
   * @param dataTables Collection of data tables
   */
  private void setSQLGroupByInAggregationResults(BrokerResponseNative brokerResponseNative, DataSchema dataSchema,
      Collection<DataTable> dataTables) {

    List<String> groupByColumns = new ArrayList<>(_numGroupBy);
    int idx = 0;
    while (idx < _numGroupBy) {
      groupByColumns.add(dataSchema.getColumnName(idx));
      idx++;
    }

    List<String> aggregationColumns = new ArrayList<>(_numAggregationFunctions);
    List<List<GroupByResult>> groupByResults = new ArrayList<>(_numAggregationFunctions);
    while (idx < _numColumns) {
      aggregationColumns.add(dataSchema.getColumnName(idx));
      groupByResults.add(new ArrayList<>());
      idx++;
    }

    if (!dataTables.isEmpty()) {
      IndexedTable indexedTable = getIndexedTable(dataSchema, dataTables);

      Iterator<Record> sortedIterator = indexedTable.iterator();
      int numRows = 0;
      while (numRows < _groupBy.getTopN() && sortedIterator.hasNext()) {

        Record nextRecord = sortedIterator.next();
        Object[] values = nextRecord.getValues();

        int index = 0;
        List<String> group = new ArrayList<>(_numGroupBy);
        while (index < _numGroupBy) {
          group.add(values[index].toString());
          index++;
        }

        int aggNum = 0;
        while (index < _numColumns) {
          Serializable serializableValue =
              getSerializableValue(_aggregationFunctions[aggNum].extractFinalResult(values[index]));
          if (!_preserveType) {
            serializableValue = AggregationFunctionUtils.formatValue(serializableValue);
          }
          GroupByResult groupByResult = new GroupByResult();
          groupByResult.setGroup(group);
          groupByResult.setValue(serializableValue);
          groupByResults.get(aggNum).add(groupByResult);
          index++;
          aggNum++;
        }
        numRows++;
      }
    }

    List<AggregationResult> aggregationResults = new ArrayList<>(_numAggregationFunctions);
    for (int i = 0; i < _numAggregationFunctions; i++) {
      AggregationResult aggregationResult =
          new AggregationResult(groupByResults.get(i), groupByColumns, aggregationColumns.get(i));
      aggregationResults.add(aggregationResult);
    }
    brokerResponseNative.setAggregationResults(aggregationResults);
  }

  private Serializable getSerializableValue(Object value) {
    if (value instanceof Number) {
      return (Number) value;
    } else {
      return value.toString();
    }
  }

  /**
   * Constructs the final result table schema for PQL execution mode
   */
  private DataSchema getPQLResultTableSchema(String finalAggregationName, AggregationFunction aggregationFunction) {
    String[] finalDataSchemaColumns = new String[_numColumns];
    DataSchema.ColumnDataType[] finalColumnDataTypes = new DataSchema.ColumnDataType[_numColumns];
    int i;
    List<String> groupByExpressions = _groupBy.getExpressions();
    for (i = 0; i < _numGroupBy; i++) {
      finalDataSchemaColumns[i] = groupByExpressions.get(i);
      finalColumnDataTypes[i] = DataSchema.ColumnDataType.STRING;
    }
    finalDataSchemaColumns[i] = finalAggregationName;
    finalColumnDataTypes[i] = aggregationFunction.getFinalResultColumnType();
    return new DataSchema(finalDataSchemaColumns, finalColumnDataTypes);
  }

  /**
   * Reduce group-by results from multiple servers and set them into BrokerResponseNative passed in.
   *
   * @param brokerResponseNative broker response.
   * @param dataTables Collection of data tables
   * @param havingFilterQuery having filter query
   * @param havingFilterQueryMap having filter query map
   */
  @SuppressWarnings("unchecked")
  private void setGroupByHavingResults(BrokerResponseNative brokerResponseNative,
      boolean[] aggregationFunctionsSelectStatus, Collection<DataTable> dataTables, HavingFilterQuery havingFilterQuery,
      HavingFilterQueryMap havingFilterQueryMap) {

    // Merge results from all data tables.
    String[] columnNames = new String[_numAggregationFunctions];
    Map<String, Object>[] intermediateResultMaps = new Map[_numAggregationFunctions];
    for (DataTable dataTable : dataTables) {
      for (int i = 0; i < _numAggregationFunctions; i++) {
        if (columnNames[i] == null) {
          columnNames[i] = dataTable.getString(i, 0);
          intermediateResultMaps[i] = dataTable.getObject(i, 1);
        } else {
          Map<String, Object> mergedIntermediateResultMap = intermediateResultMaps[i];
          Map<String, Object> intermediateResultMapToMerge = dataTable.getObject(i, 1);
          for (Map.Entry<String, Object> entry : intermediateResultMapToMerge.entrySet()) {
            String groupKey = entry.getKey();
            Object intermediateResultToMerge = entry.getValue();
            if (mergedIntermediateResultMap.containsKey(groupKey)) {
              Object mergedIntermediateResult = mergedIntermediateResultMap.get(groupKey);
              mergedIntermediateResultMap
                  .put(groupKey, _aggregationFunctions[i].merge(mergedIntermediateResult, intermediateResultToMerge));
            } else {
              mergedIntermediateResultMap.put(groupKey, intermediateResultToMerge);
            }
          }
        }
      }
    }

    // Extract final result maps from the merged intermediate result maps.
    Map<String, Comparable>[] finalResultMaps = new Map[_numAggregationFunctions];
    for (int i = 0; i < _numAggregationFunctions; i++) {
      Map<String, Object> intermediateResultMap = intermediateResultMaps[i];
      Map<String, Comparable> finalResultMap = new HashMap<>();
      for (String groupKey : intermediateResultMap.keySet()) {
        Object intermediateResult = intermediateResultMap.get(groupKey);
        finalResultMap.put(groupKey, _aggregationFunctions[i].extractFinalResult(intermediateResult));
      }
      finalResultMaps[i] = finalResultMap;
    }
    //If HAVING clause is set, we further filter the group by results based on the HAVING predicate
    if (havingFilterQuery != null) {
      HavingClauseComparisonTree havingClauseComparisonTree =
          HavingClauseComparisonTree.buildHavingClauseComparisonTree(havingFilterQuery, havingFilterQueryMap);
      //Applying close policy
      //We just keep those groups (from different aggregation functions) that are exist in the result set of all aggregation functions.
      //In other words, we just keep intersection of groups of different aggregation functions.
      //Here we calculate the intersection of group key sets of different aggregation functions
      Set<String> intersectionOfKeySets = finalResultMaps[0].keySet();
      for (int i = 1; i < _numAggregationFunctions; i++) {
        intersectionOfKeySets.retainAll(finalResultMaps[i].keySet());
      }

      //Now it is time to remove those groups that do not validate HAVING clause predicate
      //We use TreeMap which supports CASE_INSENSITIVE_ORDER
      Map<String, Comparable> singleGroupAggResults = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      Map<String, Comparable>[] finalFilteredResultMaps = new Map[_numAggregationFunctions];
      for (int i = 0; i < _numAggregationFunctions; i++) {
        finalFilteredResultMaps[i] = new HashMap<>();
      }

      for (String groupKey : intersectionOfKeySets) {
        for (int i = 0; i < _numAggregationFunctions; i++) {
          singleGroupAggResults.put(columnNames[i], finalResultMaps[i].get(groupKey));
        }
        //if this group validate HAVING predicate keep it in the new map
        if (havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults)) {
          for (int i = 0; i < _numAggregationFunctions; i++) {
            finalFilteredResultMaps[i].put(groupKey, singleGroupAggResults.get(columnNames[i]));
          }
        }
      }
      //update the final results
      finalResultMaps = finalFilteredResultMaps;
    }

    int aggregationNumsInFinalResult = 0;
    for (int i = 0; i < _numAggregationFunctions; i++) {
      if (aggregationFunctionsSelectStatus[i]) {
        aggregationNumsInFinalResult++;
      }
    }

    if (aggregationNumsInFinalResult > 0) {
      String[] finalColumnNames = new String[aggregationNumsInFinalResult];
      Map<String, Comparable>[] finalOutResultMaps = new Map[aggregationNumsInFinalResult];
      String[] finalResultTableAggNames = new String[aggregationNumsInFinalResult];
      AggregationFunction[] finalAggregationFunctions = new AggregationFunction[aggregationNumsInFinalResult];
      int count = 0;
      for (int i = 0; i < _numAggregationFunctions; i++) {
        if (aggregationFunctionsSelectStatus[i]) {
          finalColumnNames[count] = columnNames[i];
          finalOutResultMaps[count] = finalResultMaps[i];
          finalResultTableAggNames[count] = _aggregationFunctionContexts[i].getResultColumnName();
          finalAggregationFunctions[count] = _aggregationFunctions[i];
          count++;
        }
      }
      // Trim the final result maps to topN and set them into the broker response.
      AggregationGroupByTrimmingService aggregationGroupByTrimmingService =
          new AggregationGroupByTrimmingService(finalAggregationFunctions, (int) _groupBy.getTopN());
      List<GroupByResult>[] groupByResultLists = aggregationGroupByTrimmingService.trimFinalResults(finalOutResultMaps);

      if (_responseFormatSql) {
        assert aggregationNumsInFinalResult == 1;
        List<GroupByResult> groupByResultList = groupByResultLists[0];
        List<Object[]> rows = new ArrayList<>();
        for (GroupByResult groupByResult : groupByResultList) {
          Object[] row = new Object[_numColumns];
          int i = 0;
          for (String column : groupByResult.getGroup()) {
            row[i++] = column;
          }
          row[i] = groupByResult.getValue();
          rows.add(row);
        }
        DataSchema finalDataSchema = getPQLResultTableSchema(finalResultTableAggNames[0], finalAggregationFunctions[0]);
        brokerResponseNative.setResultTable(new ResultTable(finalDataSchema, rows));
      } else {
        // Format the value into string if required
        if (!_preserveType) {
          for (List<GroupByResult> groupByResultList : groupByResultLists) {
            for (GroupByResult groupByResult : groupByResultList) {
              groupByResult.setValue(AggregationFunctionUtils.formatValue(groupByResult.getValue()));
            }
          }
        }
        List<AggregationResult> aggregationResults = new ArrayList<>(count);
        for (int i = 0; i < aggregationNumsInFinalResult; i++) {
          List<GroupByResult> groupByResultList = groupByResultLists[i];
          aggregationResults
              .add(new AggregationResult(groupByResultList, _groupBy.getExpressions(), finalColumnNames[i]));
        }
        brokerResponseNative.setAggregationResults(aggregationResults);
      }
    } else {
      throw new IllegalStateException(
          "There should be minimum one aggregation function in the select list of a Group by query");
    }
  }
}
