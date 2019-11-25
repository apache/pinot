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
package org.apache.pinot.core.query.reduce.resultsetter;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.DistinctTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.QueryOptions;


/**
 * Helper class to set results of distinct query into the BrokerResponseNative
 */
public class DistinctResultSetter implements ResultSetter {

  private final BrokerRequest _brokerRequest;
  private final AggregationFunction _aggregationFunction;

  DistinctResultSetter(BrokerRequest brokerRequest, AggregationFunction aggregationFunction, QueryOptions queryOptions) {
    _brokerRequest = brokerRequest;
    _aggregationFunction = aggregationFunction;
  }

  /**
   * Sets results of distinct into SelectionResults
   */
  @Override
  public void setResults(String tableName, DataSchema dataSchema, Map<ServerRoutingInstance, DataTable> dataTableMap,
      BrokerResponseNative brokerResponseNative, BrokerMetrics brokerMetrics) {

    if (dataTableMap.isEmpty()) {
      brokerResponseNative
          .setSelectionResults(new SelectionResults(Arrays.asList(getDistinctColumns()), new ArrayList<>(0)));
      return;
    }

    assert dataSchema != null;
    // DISTINCT is implemented as an aggregation function in the execution engine. Just like
    // other aggregation functions, DISTINCT returns its result as a single object
    // (of type DistinctTable) serialized by the server into the DataTable and deserialized
    // by the broker from the DataTable. So there should be exactly 1 row and 1 column and that
    // column value should be the serialized DistinctTable -- so essentially it is a DataTable
    // inside a DataTable
    Collection<DataTable> dataTables = dataTableMap.values();
    Preconditions.checkState(dataSchema.size() == 1, "DataTable from server for DISTINCT should have exactly one row");
    Preconditions.checkState(dataSchema.getColumnDataType(0) == DataSchema.ColumnDataType.OBJECT,
        "DistinctAggregationFunction should return result of type OBJECT");
    Object mergedIntermediateResult = null;
    // go over all the data tables from servers
    for (DataTable dataTable : dataTables) {
      Preconditions.checkState(dataTable.getNumberOfRows() == 1);
      // deserialize the DistinctTable
      Object intermediateResultToMerge = dataTable.getObject(0, 0);
      Preconditions.checkState(intermediateResultToMerge instanceof DistinctTable);
      DistinctTable distinctTable = (DistinctTable) intermediateResultToMerge;
      // since DistinctTable uses the Table interface and during deserialization, we didn't
      // have all the necessary information w.r.t ORDER BY, limit etc, we set it now
      // before merging so that resize/trimming/sorting happens correctly
      distinctTable.addLimitAndOrderByInfo(_brokerRequest);
      if (mergedIntermediateResult == null) {
        mergedIntermediateResult = intermediateResultToMerge;
      } else {
        _aggregationFunction.merge(mergedIntermediateResult, intermediateResultToMerge);
      }
    }

    DistinctTable distinctTable = (DistinctTable) mergedIntermediateResult;
    // finish the merging, sort (if ORDER BY), get iterator
    distinctTable.finish(true);

    List<Serializable[]> resultSet = new ArrayList<>(distinctTable.size());
    String[] columnNames = distinctTable.getDataSchema().getColumnNames();
    Iterator<Record> iterator = distinctTable.iterator();
    while (iterator.hasNext()) {
      Record record = iterator.next();
      Object[] columns = record.getValues();
      Serializable[] distinctRow = new Serializable[columns.length];
      for (int col = 0; col < columns.length; col++) {
        final Serializable columnValue = AggregationFunctionUtils.getSerializableValue(columns[col]);
        distinctRow[col] = columnValue;
      }
      resultSet.add(distinctRow);
    }

    // Up until now, we have treated DISTINCT similar to another aggregation function even in terms
    // of the result from function and merging results.
    // However, the DISTINCT query is just another SELECTION style query from the user's point
    // of view and will return one or records in the result table for the column(s) selected and so
    // for that reason, response from broker should be a selection query result.
    brokerResponseNative.setSelectionResults((new SelectionResults(Arrays.asList(columnNames), resultSet)));
  }

  private String[] getDistinctColumns() {
    return _brokerRequest.getAggregationsInfo().get(0).getAggregationParams().get("column").split(":");
  }
}
