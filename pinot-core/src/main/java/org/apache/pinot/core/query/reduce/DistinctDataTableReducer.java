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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.distinct.table.BigDecimalDistinctTable;
import org.apache.pinot.core.query.distinct.table.BytesDistinctTable;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.distinct.table.DoubleDistinctTable;
import org.apache.pinot.core.query.distinct.table.FloatDistinctTable;
import org.apache.pinot.core.query.distinct.table.IntDistinctTable;
import org.apache.pinot.core.query.distinct.table.LongDistinctTable;
import org.apache.pinot.core.query.distinct.table.MultiColumnDistinctTable;
import org.apache.pinot.core.query.distinct.table.StringDistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.trace.Tracing;


/**
 * Helper class to reduce data tables and set results of distinct query into the BrokerResponseNative
 */
public class DistinctDataTableReducer implements DataTableReducer {
  private final QueryContext _queryContext;

  public DistinctDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
  }

  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, Collection<DataTable>> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    dataSchema = ReducerDataSchemaUtils.canonicalizeDataSchemaForDistinct(_queryContext, dataSchema);
    int limit = _queryContext.getLimit();
    if (dataTableMap.isEmpty() || limit == 0) {
      brokerResponseNative.setResultTable(new ResultTable(dataSchema, List.of()));
      return;
    }

    Collection<DataTable> dataTables = getFlatDataTables(dataTableMap);
    DistinctTable distinctTable = null;
    for (DataTable dataTable : dataTables) {
      Tracing.ThreadAccountantOps.sampleAndCheckInterruption();
      if (distinctTable == null) {
        distinctTable = createDistinctTable(dataSchema, dataTable);
        if (distinctTable.isSatisfied()) {
          break;
        }
      } else {
        if (distinctTable.mergeDataTable(dataTable)) {
          break;
        }
      }
    }
    brokerResponseNative.setResultTable(distinctTable.toResultTable());
  }

  private DistinctTable createDistinctTable(DataSchema dataSchema, DataTable dataTable) {
    int limit = _queryContext.getLimit();
    List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
    if (dataSchema.size() == 1) {
      OrderByExpressionContext orderByExpression = orderByExpressions != null ? orderByExpressions.get(0) : null;
      ColumnDataType columnDataType = dataSchema.getColumnDataType(0);
      switch (columnDataType.getStoredType()) {
        case INT:
          return new IntDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression,
              dataTable);
        case LONG:
          return new LongDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression,
              dataTable);
        case FLOAT:
          return new FloatDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression,
              dataTable);
        case DOUBLE:
          return new DoubleDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression,
              dataTable);
        case BIG_DECIMAL:
          return new BigDecimalDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(),
              orderByExpression, dataTable);
        case STRING:
          return new StringDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression,
              dataTable);
        case BYTES:
          return new BytesDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression,
              dataTable);
        default:
          throw new IllegalStateException("Unsupported data type: " + columnDataType);
      }
    } else {
      return new MultiColumnDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpressions,
          dataTable);
    }
  }
}
