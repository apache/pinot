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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.HavingFilterQuery;
import org.apache.pinot.common.request.HavingFilterQueryMap;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByTrimmingService;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GroupByUtils;


public class GroupByResultSetter extends ResultSetter {

  public GroupByResultSetter(String tableName, BrokerRequest brokerRequest, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      BrokerMetrics brokerMetrics) {
    super(tableName, brokerRequest, dataSchema, dataTableMap, brokerResponseNative, brokerMetrics);
  }

  public void setAggregationGroupByResults() {
    if (_dataTableMap.isEmpty()) {
      return;
    }

    assert _dataSchema != null;
    int resultSize = 0;

    if (_groupByModeSql) {
      // sql + order by

      // if RESPONSE_FORMAT is SQL, return results in {@link ResultTable}
      if (_responseFormatSql) {
        setSQLGroupByOrderByResults(_brokerRequest.getAggregationsInfo(), _brokerRequest.getGroupBy(),
            _brokerRequest.getOrderBy());
        resultSize = _brokerResponseNative.getResultTable().getRows().size();
      } else {
        setPQLGroupByOrderByResults(_brokerRequest.getAggregationsInfo(), _brokerRequest.getGroupBy(),
            _brokerRequest.getOrderBy());
        if (!_brokerResponseNative.getAggregationResults().isEmpty()) {
          resultSize = _brokerResponseNative.getAggregationResults().get(0).getGroupByResult().size();
        }
      }
    } else {

      if (_responseFormatSql && _brokerRequest.getAggregationsInfoSize() > 1) {
        _brokerResponseNative.addToExceptions(new QueryProcessingException(QueryException.MERGE_RESPONSE_ERROR_CODE,
            "Response format sql unsupported for pql execution mode with multiple aggregations"));
      } else {

        setGroupByHavingResults(_brokerRequest.getAggregationsInfo(), _brokerRequest.getGroupBy(),
            _brokerRequest.getHavingFilterQuery(), _brokerRequest.getHavingFilterSubQueryMap());
        if (_responseFormatSql) {
          resultSize = _brokerResponseNative.getResultTable().getRows().size();
        } else {
          if (!_brokerResponseNative.getAggregationResults().isEmpty()) {
            resultSize = _brokerResponseNative.getAggregationResults().get(0).getGroupByResult().size();
          }
        }
      }
    }

    if (_brokerMetrics != null && resultSize > 0) {
      _brokerMetrics.addMeteredQueryValue(_brokerRequest, BrokerMeter.GROUP_BY_SIZE, resultSize);
    }
  }

  /**
   * Extract group by order by results and set into {@link ResultTable}
   * @param aggregationInfos aggregations info
   * @param groupBy group by info
   * @param orderBy order by info
   */
  private void setSQLGroupByOrderByResults(List<AggregationInfo> aggregationInfos, GroupBy groupBy,
      List<SelectionSort> orderBy) {

    List<String> columnNames = new ArrayList<>(_dataSchema.size());
    for (int i = 0; i < _dataSchema.size(); i++) {
      columnNames.add(_dataSchema.getColumnName(i));
    }

    int numGroupBy = groupBy.getExpressionsSize();
    int numAggregations = aggregationInfos.size();
    int numColumns = columnNames.size();

    IndexedTable indexedTable = getIndexedTable(groupBy, aggregationInfos, orderBy);

    AggregationFunction[] aggregationFunctions = new AggregationFunction[numAggregations];
    DataSchema.ColumnDataType[] finalColumnDataTypes = new DataSchema.ColumnDataType[numColumns];
    for (int i = 0; i < numAggregations; i++) {
      aggregationFunctions[i] =
          AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfos.get(i)).getAggregationFunction();
      finalColumnDataTypes[i] = aggregationFunctions[i].getFinalResultColumnType();
    }

    List<Object[]> rows = new ArrayList<>();
    Iterator<Record> sortedIterator = indexedTable.iterator();
    int numRows = 0;
    while (numRows < groupBy.getTopN() && sortedIterator.hasNext()) {

      Record nextRecord = sortedIterator.next();
      Object[] values = nextRecord.getValues();

      int index = numGroupBy;
      int aggNum = 0;
      while (index < numColumns) {
        values[index] = aggregationFunctions[aggNum++].extractFinalResult(values[index]);
        index++;
      }
      rows.add(values);
      numRows++;
    }

    DataSchema finalDataSchema = new DataSchema(_dataSchema.getColumnNames(), finalColumnDataTypes);
    _brokerResponseNative.setResultTable(new ResultTable(finalDataSchema, rows));
  }

  private IndexedTable getIndexedTable(GroupBy groupBy, List<AggregationInfo> aggregationInfos,
      List<SelectionSort> orderBy) {

    int numColumns = _dataSchema.size();
    int indexedTableCapacity = GroupByUtils.getTableCapacity(groupBy, orderBy);
    IndexedTable indexedTable =
        new ConcurrentIndexedTable(_dataSchema, aggregationInfos, orderBy, indexedTableCapacity);

    for (DataTable dataTable : _dataTables) {
      BiFunction[] functions = new BiFunction[numColumns];
      for (int i = 0; i < numColumns; i++) {
        DataSchema.ColumnDataType columnDataType = _dataSchema.getColumnDataType(i);
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
            // FIXME: support BYTES in DataTable instead of converting to string
            function = (row, col) -> BytesUtils.toByteArray(dataTable.getString(row, col));
            break;
          default:
            function = dataTable::getObject;
        }
        functions[i] = function;
      }

      for (int row = 0; row < dataTable.getNumberOfRows(); row++) {
        Object[] columns = new Object[numColumns];
        for (int col = 0; col < numColumns; col++) {
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
   * @param aggregationInfos aggregations info
   * @param groupBy group by info
   * @param orderBy order by info
   */
  private void setPQLGroupByOrderByResults(List<AggregationInfo> aggregationInfos, GroupBy groupBy,
      List<SelectionSort> orderBy) {

    int numGroupBy = groupBy.getExpressionsSize();
    int numAggregations = aggregationInfos.size();
    int numColumns = numGroupBy + numAggregations;

    List<String> groupByColumns = new ArrayList<>(numGroupBy);
    int idx = 0;
    while (idx < numGroupBy) {
      groupByColumns.add(_dataSchema.getColumnName(idx));
      idx++;
    }

    List<String> aggregationColumns = new ArrayList<>(numAggregations);
    AggregationFunction[] aggregationFunctions = new AggregationFunction[aggregationInfos.size()];
    List<List<GroupByResult>> groupByResults = new ArrayList<>(numAggregations);
    int aggIdx = 0;
    while (idx < numColumns) {
      aggregationColumns.add(_dataSchema.getColumnName(idx));
      aggregationFunctions[aggIdx] =
          AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfos.get(aggIdx)).getAggregationFunction();
      groupByResults.add(new ArrayList<>());
      idx++;
      aggIdx++;
    }

    if (!_dataTables.isEmpty()) {
      IndexedTable indexedTable = getIndexedTable(groupBy, aggregationInfos, orderBy);

      Iterator<Record> sortedIterator = indexedTable.iterator();
      int numRows = 0;
      while (numRows < groupBy.getTopN() && sortedIterator.hasNext()) {

        Record nextRecord = sortedIterator.next();
        Object[] values = nextRecord.getValues();

        int index = 0;
        List<String> group = new ArrayList<>(numGroupBy);
        while (index < numGroupBy) {
          group.add(values[index].toString());
          index++;
        }

        int aggNum = 0;
        while (index < numColumns) {
          Serializable serializableValue =
              getSerializableValue(aggregationFunctions[aggNum].extractFinalResult(values[index]));
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

    List<AggregationResult> aggregationResults = new ArrayList<>(numAggregations);
    for (int i = 0; i < numAggregations; i++) {
      AggregationResult aggregationResult =
          new AggregationResult(groupByResults.get(i), groupByColumns, aggregationColumns.get(i));
      aggregationResults.add(aggregationResult);
    }
    _brokerResponseNative.setAggregationResults(aggregationResults);
  }

  private Serializable getSerializableValue(Object value) {
    if (value instanceof Number) {
      return (Number) value;
    } else {
      return value.toString();
    }
  }

  /**
   * Reduce group-by results from multiple servers and set them into BrokerResponseNative passed in.
   *
   * @param aggregationsInfo aggregations info
   * @param groupBy group-by information.
   * @param havingFilterQuery having filter query
   * @param havingFilterQueryMap having filter query map
   */
  @SuppressWarnings("unchecked")
  private void setGroupByHavingResults(List<AggregationInfo> aggregationsInfo, GroupBy groupBy,
      HavingFilterQuery havingFilterQuery, HavingFilterQueryMap havingFilterQueryMap) {
    AggregationFunction[] aggregationFunctions = AggregationFunctionUtils.getAggregationFunctions(_brokerRequest);
    int numAggregationFunctions = aggregationFunctions.length;

    // Merge results from all data tables.
    String[] columnNames = new String[numAggregationFunctions];
    Map<String, Object>[] intermediateResultMaps = new Map[numAggregationFunctions];
    for (DataTable dataTable : _dataTables) {
      for (int i = 0; i < numAggregationFunctions; i++) {
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
                  .put(groupKey, aggregationFunctions[i].merge(mergedIntermediateResult, intermediateResultToMerge));
            } else {
              mergedIntermediateResultMap.put(groupKey, intermediateResultToMerge);
            }
          }
        }
      }
    }

    // Extract final result maps from the merged intermediate result maps.
    Map<String, Comparable>[] finalResultMaps = new Map[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      Map<String, Object> intermediateResultMap = intermediateResultMaps[i];
      Map<String, Comparable> finalResultMap = new HashMap<>();
      for (String groupKey : intermediateResultMap.keySet()) {
        Object intermediateResult = intermediateResultMap.get(groupKey);
        finalResultMap.put(groupKey, aggregationFunctions[i].extractFinalResult(intermediateResult));
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
      for (int i = 1; i < numAggregationFunctions; i++) {
        intersectionOfKeySets.retainAll(finalResultMaps[i].keySet());
      }

      //Now it is time to remove those groups that do not validate HAVING clause predicate
      //We use TreeMap which supports CASE_INSENSITIVE_ORDER
      Map<String, Comparable> singleGroupAggResults = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      Map<String, Comparable>[] finalFilteredResultMaps = new Map[numAggregationFunctions];
      for (int i = 0; i < numAggregationFunctions; i++) {
        finalFilteredResultMaps[i] = new HashMap<>();
      }

      for (String groupKey : intersectionOfKeySets) {
        for (int i = 0; i < numAggregationFunctions; i++) {
          singleGroupAggResults.put(columnNames[i], finalResultMaps[i].get(groupKey));
        }
        //if this group validate HAVING predicate keep it in the new map
        if (havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults)) {
          for (int i = 0; i < numAggregationFunctions; i++) {
            finalFilteredResultMaps[i].put(groupKey, singleGroupAggResults.get(columnNames[i]));
          }
        }
      }
      //update the final results
      finalResultMaps = finalFilteredResultMaps;
    }

    int aggregationNumsInFinalResult = 0;
    boolean[] aggregationFunctionsSelectStatus =
        AggregationFunctionUtils.getAggregationFunctionsSelectStatus(_brokerRequest.getAggregationsInfo());
    for (int i = 0; i < numAggregationFunctions; i++) {
      if (aggregationFunctionsSelectStatus[i]) {
        aggregationNumsInFinalResult++;
      }
    }

    if (aggregationNumsInFinalResult > 0) {
      String[] finalColumnNames = new String[aggregationNumsInFinalResult];
      String[] finalResultTableColumnNames = new String[aggregationNumsInFinalResult];
      Map<String, Comparable>[] finalOutResultMaps = new Map[aggregationNumsInFinalResult];
      AggregationFunction[] finalAggregationFunctions = new AggregationFunction[aggregationNumsInFinalResult];
      int count = 0;
      for (int i = 0; i < numAggregationFunctions; i++) {
        if (aggregationFunctionsSelectStatus[i]) {
          finalColumnNames[count] = columnNames[i];
          finalResultTableColumnNames[count] =
              AggregationFunctionUtils.getAggregationColumnName(aggregationsInfo.get(i));
          finalOutResultMaps[count] = finalResultMaps[i];
          finalAggregationFunctions[count] = aggregationFunctions[i];
          count++;
        }
      }
      // Trim the final result maps to topN and set them into the broker response.
      AggregationGroupByTrimmingService aggregationGroupByTrimmingService =
          new AggregationGroupByTrimmingService(finalAggregationFunctions, (int) groupBy.getTopN());
      List<GroupByResult>[] groupByResultLists = aggregationGroupByTrimmingService.trimFinalResults(finalOutResultMaps);

      // Format the value into string if required
      if (!_preserveType) {
        for (List<GroupByResult> groupByResultList : groupByResultLists) {
          for (GroupByResult groupByResult : groupByResultList) {
            groupByResult.setValue(AggregationFunctionUtils.formatValue(groupByResult.getValue()));
          }
        }
      }

      if (_responseFormatSql) {
        assert aggregationNumsInFinalResult == 1;
        List<GroupByResult> groupByResultList = groupByResultLists[0];
        int numGroupBy = groupBy.getExpressionsSize();
        int numColumns = numGroupBy + 1;

        List<Object[]> rows = new ArrayList<>();
        for (GroupByResult groupByResult : groupByResultList) {
          Object[] row = new Object[numColumns];
          int i = 0;
          for (String column : groupByResult.getGroup()) {
            row[i++] = column;
          }
          row[i] = groupByResult.getValue();
          rows.add(row);
        }
        String[] finalDataSchemaColumns = new String[numColumns];
        DataSchema.ColumnDataType[] finalColumnDataTypes = new DataSchema.ColumnDataType[numColumns];
        int i;
        List<String> groupByExpressions = groupBy.getExpressions();
        for (i = 0; i < numGroupBy; i++) {
          finalDataSchemaColumns[i] = groupByExpressions.get(i);
          finalColumnDataTypes[i] = DataSchema.ColumnDataType.STRING;
        }
        finalDataSchemaColumns[i] = finalResultTableColumnNames[0];
        finalColumnDataTypes[i] = aggregationFunctions[0].getFinalResultColumnType();
        DataSchema finalDataSchema = new DataSchema(finalDataSchemaColumns, finalColumnDataTypes);
        _brokerResponseNative.setResultTable(new ResultTable(finalDataSchema, rows));
      } else {

        List<AggregationResult> aggregationResults = new ArrayList<>(count);
        for (int i = 0; i < aggregationNumsInFinalResult; i++) {
          List<GroupByResult> groupByResultList = groupByResultLists[i];
          aggregationResults
              .add(new AggregationResult(groupByResultList, groupBy.getExpressions(), finalColumnNames[i]));
        }
        _brokerResponseNative.setAggregationResults(aggregationResults);
      }
    } else {
      throw new IllegalStateException(
          "There should be minimum one aggregation function in the select list of a Group by query");
    }
  }
}
