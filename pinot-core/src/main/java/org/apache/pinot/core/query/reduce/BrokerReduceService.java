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

import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * The <code>BrokerReduceService</code> class provides service to reduce data tables gathered from multiple servers
 * to {@link BrokerResponseNative}.
 */
@ThreadSafe
public class BrokerReduceService extends BaseReduceService {
  public BrokerReduceService(PinotConfiguration config) {
    super(config);
  }

  public BrokerResponseNative reduceOnDataTable(BrokerRequest brokerRequest, BrokerRequest serverBrokerRequest,
      Map<ServerRoutingInstance, DataTable> dataTableMap, long reduceTimeOutMs, @Nullable BrokerMetrics brokerMetrics) {
    if (dataTableMap.isEmpty()) {
      // Empty response.
      return BrokerResponseNative.empty();
    }

    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    boolean enableTrace =
        queryOptions != null && Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE));

    ExecutionStatsAggregator aggregator = new ExecutionStatsAggregator(enableTrace);
    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();

    // Cache a data schema from data tables (try to cache one with data rows associated with it).
    DataSchema cachedDataSchema = null;

    // Process server response metadata.
    Iterator<Map.Entry<ServerRoutingInstance, DataTable>> iterator = dataTableMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ServerRoutingInstance, DataTable> entry = iterator.next();
      DataTable dataTable = entry.getValue();

      // aggregate metrics
      aggregator.aggregate(entry.getKey(), dataTable);

      // After processing the metadata, remove data tables without data rows inside.
      DataSchema dataSchema = dataTable.getDataSchema();
      if (dataSchema == null) {
        iterator.remove();
      } else {
        // Try to cache a data table with data rows inside, or cache one with data schema inside.
        if (dataTable.getNumberOfRows() == 0) {
          if (cachedDataSchema == null) {
            cachedDataSchema = dataSchema;
          }
          iterator.remove();
        } else {
          cachedDataSchema = dataSchema;
        }
      }
    }

    String tableName = serverBrokerRequest.getQuerySource().getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    // Set execution statistics and Update broker metrics.
    aggregator.setStats(rawTableName, brokerResponseNative, brokerMetrics);

    // NOTE: When there is no cached data schema, that means all servers encountered exception. In such case, return the
    //       response with metadata only.
    if (cachedDataSchema == null) {
      return brokerResponseNative;
    }

    QueryContext serverQueryContext = QueryContextConverterUtils.getQueryContext(serverBrokerRequest.getPinotQuery());
    DataTableReducer dataTableReducer = ResultReducerFactory.getResultReducer(serverQueryContext);
    try {
      dataTableReducer.reduceAndSetResults(rawTableName, cachedDataSchema, dataTableMap, brokerResponseNative,
          new DataTableReducerContext(_reduceExecutorService, _maxReduceThreadsPerQuery, reduceTimeOutMs,
              _groupByTrimThreshold), brokerMetrics);
    } catch (EarlyTerminationException e) {
      brokerResponseNative.addToExceptions(
          new QueryProcessingException(QueryException.QUERY_CANCELLATION_ERROR_CODE, e.toString()));
    }
    QueryContext queryContext;
    if (brokerRequest == serverBrokerRequest) {
      queryContext = serverQueryContext;
    } else {
      queryContext = QueryContextConverterUtils.getQueryContext(brokerRequest.getPinotQuery());
      GapfillUtils.GapfillType gapfillType = GapfillUtils.getGapfillType(queryContext);
      if (gapfillType == null) {
        throw new BadQueryRequestException("Nested query is not supported without gapfill");
      }
      BaseGapfillProcessor gapfillProcessor =
          GapfillProcessorFactory.getGapfillProcessor(queryContext, gapfillType);
      gapfillProcessor.process(brokerResponseNative);
    }

    if (!serverQueryContext.isExplain()) {
      updateAlias(queryContext, brokerResponseNative);
    }
    return brokerResponseNative;
  }

  public void shutDown() {
    _reduceExecutorService.shutdownNow();
  }
}
