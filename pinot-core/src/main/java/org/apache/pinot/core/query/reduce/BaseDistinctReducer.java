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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.QueryOptionsUtils;


/**
 * Helper class to reduce data tables and set results of distinct query into the BrokerResponseNative
 */
public abstract class BaseDistinctReducer {
  protected final DistinctAggregationFunction _distinctAggregationFunction;
  protected final boolean _responseFormatSql;

  // TODO: queryOptions.isPreserveType() is ignored for DISTINCT queries.
  public BaseDistinctReducer(QueryContext queryContext, DistinctAggregationFunction distinctAggregationFunction) {
    _distinctAggregationFunction = distinctAggregationFunction;
    _responseFormatSql = QueryOptionsUtils.isResponseFormatSQL(queryContext.getQueryOptions());
  }

  protected ResultTable reduceToResultTable(DistinctTable distinctTable) {
    List<Object[]> rows = new ArrayList<>(distinctTable.size());
    DataSchema dataSchema = distinctTable.getDataSchema();
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    Iterator<Record> iterator = distinctTable.getFinalResult();
    while (iterator.hasNext()) {
      Object[] values = iterator.next().getValues();
      Object[] row = new Object[numColumns];
      for (int i = 0; i < numColumns; i++) {
        row[i] = columnDataTypes[i].convertAndFormat(values[i]);
      }
      rows.add(row);
    }
    return new ResultTable(dataSchema, rows);
  }
}
