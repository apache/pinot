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
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.api.RequestStatistics;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.core.transport.AsyncQueryResponse;
import org.apache.pinot.core.transport.QueryRouter;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.TlsConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * The <code>SingleConnectionBrokerRequestHandler</code> class is a thread-safe broker request handler using a single
 * connection per server to route the queries.
 */
@ThreadSafe
public class GrpcBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final int DEFAULT_MAXIMUM_REQUEST_ATTEMPT = 10;
  private final Queue<QueryRouter> _queryRouters = new ConcurrentLinkedQueue<>();
  private final Map<String, AtomicInteger> _concurrentQueriesCountMap = new ConcurrentHashMap<>();
  private final int _maxBacklogPerServer;
  private final int _grpcBrokerThreadpoolSize;

  public GrpcBrokerRequestHandler(PinotConfiguration config, RoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      BrokerMetrics brokerMetrics, TlsConfig tlsConfig) {
    super(config, routingManager, accessControlFactory, queryQuotaManager, tableCache, brokerMetrics);

    // parse and fill constants
    _maxBacklogPerServer = Integer.parseInt(_config.getProperty("MAXIMUM_BACKLOG_PER_SERVER", "1"));
    _grpcBrokerThreadpoolSize = Integer.parseInt(_config.getProperty("GRPC_BROKER_THREADPOOL_SIZE", "1"));

    // Setup QueryRouters
    for (int i = 0; i < _grpcBrokerThreadpoolSize; i++) {
      _queryRouters.add(new QueryRouter(String.format("%s-%d", _brokerId, i), _brokerMetrics, tlsConfig));
    }
  }

  @Override
  public void start() {
  }

  @Override
  public synchronized void shutDown() {
    while (_queryRouters.peek() != null) {
      _queryRouters.poll().shutDown();
    }
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

    // Currently we directly returns the
    Map<ServerRoutingInstance, DataTable> dataTableMap =
        queryPinotServerForDataTable(requestId, _brokerId, offlineBrokerRequest, offlineRoutingTable, timeoutMs,
            true, 1);

    long reduceStartTimeNs = System.nanoTime();
    long reduceTimeOutMs = timeoutMs - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - scatterGatherStartTimeNs);
    BrokerResponseNative brokerResponse =
        _brokerReduceService.reduceOnDataTable(originalBrokerRequest, dataTableMap, reduceTimeOutMs, _brokerMetrics);
    final long reduceTimeNanos = System.nanoTime() - reduceStartTimeNs;
    requestStatistics.setReduceTimeNanos(reduceTimeNanos);
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REDUCE, reduceTimeNanos);

    return brokerResponse;
  }

  private static <T> T doWithRetries(int retries, Function<Integer, T> caller)
      throws RuntimeException {
    ProcessingException firstError = null;
    for (int i = 0; i < retries; i++) {
      try {
        return caller.apply(retries);
      } catch (Exception e) {
        if (e.getCause() instanceof ProcessingException
            && isRetriable(((ProcessingException) e.getCause()).getErrorCode())) {
          if (firstError == null) {
            firstError = (ProcessingException) e.getCause();
          }
        } else {
          throw e;
        }
      }
    }
    throw new RuntimeException(firstError);
  }

  // return false for now on all exception.
  private static boolean isRetriable(int errorCode) {
    return false;
  }

  /**
   * Query pinot server for data table.
   */
  public Map<ServerRoutingInstance, DataTable> queryPinotServerForDataTable(
      final long requestId,
      final String brokerHost,
      BrokerRequest brokerRequest,
      Map<ServerInstance, List<String>> routingTable,
      long connectionTimeoutInMillis,
      boolean ignoreEmptyResponses,
      int pinotRetryCount) {
    // Unfortunately the retries will all hit the same server because the routing decision has already been made by
    // the pinot broker
    Map<ServerRoutingInstance, DataTable> serverResponseMap = doWithRetries(pinotRetryCount, (requestAttemptId) -> {
      String rawTableName = TableNameBuilder.extractRawTableName(brokerRequest.getQuerySource().getTableName());
      if (!_concurrentQueriesCountMap.containsKey(brokerHost)) {
        _concurrentQueriesCountMap.put(brokerHost, new AtomicInteger(0));
      }
      int concurrentQueryNum = _concurrentQueriesCountMap.get(brokerHost).get();
      if (concurrentQueryNum > _maxBacklogPerServer) {
        ProcessingException processingException = new ProcessingException(QueryException.UNKNOWN_ERROR_CODE);
        processingException.setMessage("Reaching server query max backlog size is - " + _maxBacklogPerServer);
        throw new RuntimeException(processingException);
      }
      _concurrentQueriesCountMap.get(brokerHost).incrementAndGet();
      AsyncQueryResponse asyncQueryResponse;
      QueryRouter nextAvailableQueryRouter = getNextAvailableQueryRouter();
      if (TableNameBuilder.getTableTypeFromTableName(brokerRequest.getQuerySource().getTableName())
          == TableType.REALTIME) {
        asyncQueryResponse = nextAvailableQueryRouter.submitQuery(requestId, rawTableName, null, null, brokerRequest,
            routingTable, connectionTimeoutInMillis);
      } else {
        asyncQueryResponse = nextAvailableQueryRouter
            .submitQuery(makeGrpcRequestId(requestId, requestAttemptId), rawTableName, brokerRequest, routingTable,
                null, null, connectionTimeoutInMillis);
      }
      Map<ServerRoutingInstance, DataTable> serverInstanceDataTableMap = gatherServerResponses(
          ignoreEmptyResponses,
          routingTable,
          asyncQueryResponse,
          brokerRequest.getQuerySource().getTableName());
      _queryRouters.offer(nextAvailableQueryRouter);
      _concurrentQueriesCountMap.get(brokerHost).decrementAndGet();
      return serverInstanceDataTableMap;
    });
    return serverResponseMap;
  }

  private static long makeGrpcRequestId(long requestId, int requestAttemptId) {
    return requestId * DEFAULT_MAXIMUM_REQUEST_ATTEMPT + requestAttemptId;
  }

  private QueryRouter getNextAvailableQueryRouter() {
    QueryRouter queryRouter = _queryRouters.poll();
    while (queryRouter == null) {
      try {
        Thread.sleep(200L);
      } catch (InterruptedException e) {
        // Swallow the exception
      }
      queryRouter = _queryRouters.poll();
    }
    return queryRouter;
  }

  private Map<ServerRoutingInstance, DataTable> gatherServerResponses(boolean ignoreEmptyResponses,
      Map<ServerInstance, List<String>> routingTable,
      AsyncQueryResponse asyncQueryResponse,
      String tableNameWithType) {
    try {
      Map<ServerRoutingInstance, ServerResponse> queryResponses = asyncQueryResponse.getResponse();
      if (!ignoreEmptyResponses) {
        if (queryResponses.size() != routingTable.size()) {
          Map<String, String> routingTableForLogging = new HashMap<>();
          routingTable.entrySet().forEach(entry -> {
            String valueToPrint = entry.getValue().size() > 10 ? String.format("%d segments", entry.getValue().size())
                : entry.getValue().toString();
            routingTableForLogging.put(entry.getKey().toString(), valueToPrint);
          });
          ProcessingException processingException = new ProcessingException(QueryException.UNKNOWN_ERROR_CODE);
          processingException.setMessage(String.format(
              "%d of %d servers responded with routing table servers: %s, query stats: %s",
              queryResponses.size(), routingTable.size(), routingTableForLogging, asyncQueryResponse.getStats()));
          throw new RuntimeException(processingException);
        }
      }
      Map<ServerRoutingInstance, DataTable> serverResponseMap = new HashMap<>();
      queryResponses.entrySet().forEach(entry -> serverResponseMap.put(
          new ServerInstance(new InstanceConfig(
              String.format("Server_%s_%d", entry.getKey().getHostname(), entry.getKey().getPort())))
              .toServerRoutingInstance(TableType.OFFLINE),
          entry.getValue().getDataTable()));
      return serverResponseMap;
    } catch (InterruptedException e) {
      ProcessingException processingException = new ProcessingException(QueryException.UNKNOWN_ERROR_CODE);
      processingException.setMessage(
          String.format("Caught exception while fetching responses for table: %s\n%s", tableNameWithType, e));
      throw new RuntimeException(processingException);
    }
  }
}
