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
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.broker.api.RequestStatistics;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.core.transport.AsyncQueryResponse;
import org.apache.pinot.core.transport.QueryRouter;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.TlsConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static java.util.stream.Collectors.toList;


/**
 * The <code>SingleConnectionBrokerRequestHandler</code> class is a thread-safe broker request handler using a single
 * connection per server to route the queries.
 */
@ThreadSafe
public class SingleConnectionBrokerRequestHandler extends BaseBrokerRequestHandler {
  private final QueryRouter _queryRouter;

  public SingleConnectionBrokerRequestHandler(PinotConfiguration config, RoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      BrokerMetrics brokerMetrics, TlsConfig tlsConfig) {
    super(config, routingManager, accessControlFactory, queryQuotaManager, tableCache, brokerMetrics);
    _queryRouter = new QueryRouter(_brokerId, brokerMetrics, tlsConfig);
  }

  @Override
  public void start() {
  }

  @Override
  public synchronized void shutDown() {
    _queryRouter.shutDown();
    _brokerReduceService.shutDown();
  }

  @Override
  protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      @Nullable BrokerRequest offlineBrokerRequest, @Nullable Map<ServerInstance, List<String>> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest, @Nullable Map<ServerInstance, List<String>> realtimeRoutingTable,
      long timeoutMs, ServerStats serverStats, RequestStatistics requestStatistics)
      throws Exception {
    assert offlineBrokerRequest != null || realtimeBrokerRequest != null;

    String rawTableName = TableNameBuilder.extractRawTableName(originalBrokerRequest.getQuerySource().getTableName());
    long scatterGatherStartTimeNs = System.nanoTime();
    AsyncQueryResponse asyncQueryResponse = _queryRouter
        .submitQuery(requestId, rawTableName, offlineBrokerRequest, offlineRoutingTable, realtimeBrokerRequest,
            realtimeRoutingTable, timeoutMs);
    Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getResponse();
    _brokerMetrics
        .addPhaseTiming(rawTableName, BrokerQueryPhase.SCATTER_GATHER, System.nanoTime() - scatterGatherStartTimeNs);
    // TODO Use scatterGatherStats as serverStats
    serverStats.setServerStats(asyncQueryResponse.getStats());

    int numServersQueried = response.size();
    long totalResponseSize = 0;
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>(HashUtil.getHashMapCapacity(numServersQueried));
    for (Map.Entry<ServerRoutingInstance, ServerResponse> entry : response.entrySet()) {
      ServerResponse serverResponse = entry.getValue();
      DataTable dataTable = serverResponse.getDataTable();
      if (dataTable != null) {
        dataTableMap.put(entry.getKey(), dataTable);
        totalResponseSize += serverResponse.getResponseSize();
      }
    }
    int numServersResponded = dataTableMap.size();

    long reduceStartTimeNs = System.nanoTime();
    long reduceTimeOutMs = timeoutMs - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - scatterGatherStartTimeNs);
    BrokerResponseNative brokerResponse =
        _brokerReduceService.reduceOnDataTable(originalBrokerRequest, dataTableMap, reduceTimeOutMs, _brokerMetrics);
    final long reduceTimeNanos = System.nanoTime() - reduceStartTimeNs;
    requestStatistics.setReduceTimeNanos(reduceTimeNanos);
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REDUCE, reduceTimeNanos);

    brokerResponse.setNumServersQueried(numServersQueried);
    brokerResponse.setNumServersResponded(numServersResponded);

    Exception brokerRequestSendException = asyncQueryResponse.getBrokerRequestSendException();
    if (brokerRequestSendException != null) {
      String errorMsg = QueryException.getTruncatedStackTrace(brokerRequestSendException);
      brokerResponse
          .addToExceptions(new QueryProcessingException(QueryException.BROKER_REQUEST_SEND_ERROR_CODE, errorMsg));
    }
    if (brokerResponse.getExceptionsSize() > 0) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS, 1);
    }
    if (numServersQueried > numServersResponded) {
      //get list of servers that did not respond
      List<String> unresponsiveServers = dataTableMap
          .keySet().stream().filter(a -> !response.containsKey(a)).map(ServerRoutingInstance::getShortName)
          .collect(toList());
      String errorMessage = String.format("%d servers did not respond: %s.", numServersQueried - numServersResponded,
          unresponsiveServers);
      brokerResponse
          .addToExceptions(new QueryProcessingException(QueryException.SERVER_NOT_RESPONDING_ERROR_CODE, errorMessage));
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED, 1);
    }
    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.TOTAL_SERVER_RESPONSE_SIZE, totalResponseSize);

    return brokerResponse;
  }
}
