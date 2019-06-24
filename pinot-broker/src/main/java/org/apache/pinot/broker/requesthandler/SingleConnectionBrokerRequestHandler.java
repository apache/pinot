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
package org.apache.pinot.broker.requesthandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.broker.api.RequestStatistics;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.RoutingTable;
import org.apache.pinot.broker.routing.TimeBoundaryService;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.ServerInstance;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.transport.AsyncQueryResponse;
import org.apache.pinot.core.transport.QueryRouter;
import org.apache.pinot.core.transport.Server;
import org.apache.pinot.core.transport.ServerResponse;


/**
 * The <code>SingleConnectionBrokerRequestHandler</code> class is a thread-safe broker request handler using a single
 * connection per server to route the queries.
 */
@ThreadSafe
public class SingleConnectionBrokerRequestHandler extends BaseBrokerRequestHandler {
  private final QueryRouter _queryRouter;

  public SingleConnectionBrokerRequestHandler(Configuration config, RoutingTable routingTable,
      TimeBoundaryService timeBoundaryService, AccessControlFactory accessControlFactory,
      QueryQuotaManager queryQuotaManager, LazyTableConfigCache cache, BrokerMetrics brokerMetrics) {
    super(config, routingTable, timeBoundaryService, accessControlFactory, queryQuotaManager, cache, brokerMetrics);
    _queryRouter = new QueryRouter(_brokerId, brokerMetrics);
  }

  @Override
  public void start() {
  }

  @Override
  public synchronized void shutDown() {
    _queryRouter.shutDown();
  }

  @Override
  protected BrokerResponse processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      @Nullable BrokerRequest offlineBrokerRequest, @Nullable Map<String, List<String>> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest, @Nullable Map<String, List<String>> realtimeRoutingTable,
      long timeoutMs, ServerStats serverStats, RequestStatistics requestStatistics)
      throws Exception {
    assert offlineBrokerRequest != null || realtimeBrokerRequest != null;

    String rawTableName = TableNameBuilder.extractRawTableName(originalBrokerRequest.getQuerySource().getTableName());
    long scatterGatherStartTimeNs = System.nanoTime();
    AsyncQueryResponse asyncQueryResponse = _queryRouter
        .submitQuery(requestId, rawTableName, offlineBrokerRequest, offlineRoutingTable, realtimeBrokerRequest,
            realtimeRoutingTable, timeoutMs);
    Map<Server, ServerResponse> response = asyncQueryResponse.getResponse();
    _brokerMetrics
        .addPhaseTiming(rawTableName, BrokerQueryPhase.SCATTER_GATHER, System.nanoTime() - scatterGatherStartTimeNs);
    // TODO Use scatterGatherStats as serverStats
    serverStats.setServerStats(asyncQueryResponse.getStats());

    // TODO: do not convert Server to ServerInstance
    int numServersQueried = response.size();
    long totalResponseSize = 0;
    Map<ServerInstance, DataTable> dataTableMap = new HashMap<>(numServersQueried);
    for (Map.Entry<Server, ServerResponse> entry : response.entrySet()) {
      ServerResponse serverResponse = entry.getValue();
      DataTable dataTable = serverResponse.getDataTable();
      if (dataTable != null) {
        Server server = entry.getKey();
        if (server.getTableType() == TableType.OFFLINE) {
          dataTableMap.put(new ServerInstance(server.getHostName(), server.getPort(), 0), dataTable);
        } else {
          dataTableMap.put(new ServerInstance(server.getHostName(), server.getPort(), 1), dataTable);
        }
        totalResponseSize += serverResponse.getResponseSize();
      }
    }
    int numServersResponded = dataTableMap.size();

    long reduceStartTimeNs = System.nanoTime();
    BrokerResponseNative brokerResponse =
        _brokerReduceService.reduceOnDataTable(originalBrokerRequest, dataTableMap, _brokerMetrics);
    final long reduceTimeNanos = System.nanoTime() - reduceStartTimeNs;
    requestStatistics.setReduceTimeNanos(reduceTimeNanos);
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REDUCE, reduceTimeNanos);

    brokerResponse.setNumServersQueried(numServersQueried);
    brokerResponse.setNumServersResponded(numServersResponded);

    if (brokerResponse.getExceptionsSize() > 0) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS, 1);
    }
    if (numServersQueried > numServersResponded) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED, 1);
    }
    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.TOTAL_SERVER_RESPONSE_SIZE, totalResponseSize);

    return brokerResponse;
  }
}
