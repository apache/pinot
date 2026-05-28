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

import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.transport.ServerRoutingInstance;


/**
 * Interface for data tables reducers of query results
 */
public interface DataTableReducer {

  /**
   * Reduces data tables and sets the results of the query into the BrokerResponseNative
   * @param tableName table name
   * @param dataSchema schema from broker reduce service
   * @param dataTableMap map of servers to data tables
   * @param brokerResponseNative broker response
   * @param reducerContext DataTableReducer context
   * @param brokerMetrics broker metrics
   */
  void reduceAndSetResults(String tableName, DataSchema dataSchema, Map<ServerRoutingInstance, DataTable> dataTableMap,
      BrokerResponseNative brokerResponseNative, DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics);

  /**
   * Merges per-server data tables into a single <em>intermediate</em> {@link DataTable} WITHOUT
   * finalizing (no {@code extractFinalResult} / result formatting). The returned DataTable carries
   * intermediate state byte-shape identical to a single server's partial response, so a consumer can
   * intercept the merged intermediate results and custom handle it. It is expected that the merged
   * intermediate result can be reinjected in the normal reduce path.
   *
   * <p><b>WARNING:</b> this performs a full cross-server merge and re-serializes the result —
   * heavyweight work that must be run asynchronously, decoupled from request serving. Invoking it
   * inline while a query is being served adds that cost to the query and can severely degrade its
   * latency.
   *
   * <p>Reducers that cannot produce a re-mergeable intermediate (e.g. explain-plan) leave this default
   * implementation, which throws {@link UnsupportedOperationException}. Aggregation and group-by
   * reducers also throw when the query is configured for server-side final-result return
   * ({@code server.returnFinalResult}) because the inputs are then finalized, not intermediate.
   *
   * @param tableName table name
   * @param dataSchema schema from broker reduce service
   * @param dataTableMap map of servers to data tables
   * @param reducerContext DataTableReducer context
   * @param brokerMetrics broker metrics
   * @return the merged intermediate DataTable (intermediate, non-finalized state)
   */
  default DataTable mergeDataTablesOnly(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, DataTableReducerContext reducerContext,
      BrokerMetrics brokerMetrics) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support merge-only reduction");
  }
}
