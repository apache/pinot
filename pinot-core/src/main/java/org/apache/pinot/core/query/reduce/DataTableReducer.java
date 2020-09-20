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
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
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
}
