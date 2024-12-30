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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>BrokerReduceService</code> class provides service to reduce data tables gathered from multiple servers
 * to {@link BrokerResponseNative}.
 */
@ThreadSafe
public class BrokerReduceService extends BaseReduceService {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerReduceService.class);

  public BrokerReduceService(PinotConfiguration config) {
    super(config);
  }

  public BrokerResponseNative reduceOnDataTable(
      BrokerRequest brokerRequest,
      BrokerRequest serverBrokerRequest,
      Map<ServerRoutingInstance, DataTable> dataTableMap,
      long reduceTimeOutMs,
      BrokerMetrics brokerMetrics) {
    if (dataTableMap.isEmpty()) {
      // Empty response.
      return BrokerResponseNative.empty();
    }

    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    boolean enableTrace =
        queryOptions != null && Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE));

    ExecutionStatsAggregator aggregator = new ExecutionStatsAggregator(enableTrace);
    BrokerResponseNative response = new BrokerResponseNative();

    // Cache a data schema from data tables (try to cache one with data rows associated with it).
    DataSchema schemaOfEmptyTable = null;
    DataSchema schemaOfNonEmptyTable = null;
    List<ServerRoutingInstance> serversWithConflictingSchema = new ArrayList<>();

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
          if (schemaOfEmptyTable == null) {
            schemaOfEmptyTable = dataSchema;
          }
          iterator.remove();
        } else {
          if (schemaOfNonEmptyTable == null) {
            schemaOfNonEmptyTable = dataSchema;
          } else {
            // Remove data tables with conflicting data schema.
            // NOTE: Only compare the column data types, since the column names (string representation of expression)
            //       can change across different versions.
            if (!Arrays.equals(dataSchema.getColumnDataTypes(), schemaOfNonEmptyTable.getColumnDataTypes())) {
              serversWithConflictingSchema.add(entry.getKey());
              iterator.remove();
            }
          }
        }
      }
    }

    String tableName = serverBrokerRequest.getQuerySource().getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    // Set execution statistics and Update broker metrics.
    aggregator.setStats(rawTableName, response, brokerMetrics);

    // Report the servers with conflicting data schema.
    if (!serversWithConflictingSchema.isEmpty()) {
      String errorMessage = QueryException.MERGE_RESPONSE_ERROR.getMessage() + ": responses for table: " + tableName
          + " from servers: " + serversWithConflictingSchema + " got dropped due to data schema inconsistency.";
      LOGGER.warn(errorMessage);
      brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.RESPONSE_MERGE_EXCEPTIONS, 1);
      response.addException(new QueryProcessingException(QueryException.MERGE_RESPONSE_ERROR_CODE, errorMessage));
    }

    // NOTE: When there is no cached data schema, that means all servers encountered exception. In such case, return the
    //       response with metadata only.
    DataSchema cachedDataSchema =
        schemaOfNonEmptyTable != null ? schemaOfNonEmptyTable : schemaOfEmptyTable;
    if (cachedDataSchema == null) {
      return response;
    }

    Integer minGroupTrimSizeOpt = null;
    Integer groupTrimThresholdOpt = null;
    Integer minInitialIndexedTableCapOpt = null;

    if (queryOptions != null) {
      minGroupTrimSizeOpt = QueryOptionsUtils.getMinBrokerGroupTrimSize(queryOptions);
      groupTrimThresholdOpt = QueryOptionsUtils.getGroupTrimThreshold(queryOptions);
      minInitialIndexedTableCapOpt = QueryOptionsUtils.getMinInitialIndexedTableCapacity(queryOptions);
    }

    int minGroupTrimSize = minGroupTrimSizeOpt != null ? minGroupTrimSizeOpt : _minGroupTrimSize;
    int groupTrimThreshold = groupTrimThresholdOpt != null ? groupTrimThresholdOpt : _groupByTrimThreshold;
    int minInitialIndexedTableCapacity =
        minInitialIndexedTableCapOpt != null ? minInitialIndexedTableCapOpt : _minInitialIndexedTableCapacity;

    QueryContext serverQueryContext = QueryContextConverterUtils.getQueryContext(serverBrokerRequest.getPinotQuery());
    try {
      DataTableReducer reducer = ResultReducerFactory.getResultReducer(serverQueryContext);
      reducer.reduceAndSetResults(rawTableName, cachedDataSchema, dataTableMap, response,
          new DataTableReducerContext(_reduceExecutorService, _maxReduceThreadsPerQuery, reduceTimeOutMs,
              groupTrimThreshold, minGroupTrimSize, minInitialIndexedTableCapacity), brokerMetrics);
    } catch (EarlyTerminationException e) {
      response.addException(
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
      BaseGapfillProcessor gapfillProcessor = GapfillProcessorFactory.getGapfillProcessor(queryContext, gapfillType);
      gapfillProcessor.process(response);
    }

    if (!serverQueryContext.isExplain()) {
      updateAlias(queryContext, response);
    }
    return response;
  }

  public void shutDown() {
    _reduceExecutorService.shutdownNow();
  }
}
