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
package org.apache.pinot.connector.presto;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.transport.AsyncQueryResponse;
import org.apache.pinot.core.transport.QueryRouter;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.TlsConfig;
import org.apache.pinot.pql.parsers.Pql2CompilationException;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class PinotScatterGatherQueryClient {
  private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();
  private static final String PRESTO_HOST_PREFIX = "presto-pinot-";

  private final String _prestoHostId;
  private final BrokerMetrics _brokerMetrics;
  private final Queue<QueryRouter> _queryRouters = new ConcurrentLinkedQueue<>();
  private final Config _config;
  private final Map<String, AtomicInteger> _concurrentQueriesCountMap = new ConcurrentHashMap<>();

  public enum ErrorCode {
    PINOT_INSUFFICIENT_SERVER_RESPONSE(true),
    PINOT_INVALID_PQL_GENERATED(false),
    PINOT_UNCLASSIFIED_ERROR(false),
    PINOT_QUERY_BACKLOG_FULL(false);

    private final boolean _retriable;

    ErrorCode(boolean retriable) {
      _retriable = retriable;
    }

    public boolean isRetriable() {
      return _retriable;
    }
  }

  public static class PinotException
      extends RuntimeException {
    private final ErrorCode _errorCode;

    public PinotException(ErrorCode errorCode, String message, Throwable t) {
      super(message, t);
      _errorCode = errorCode;
    }

    public PinotException(ErrorCode errorCode, String message) {
      this(errorCode, message, null);
    }

    public ErrorCode getErrorCode() {
      return _errorCode;
    }
  }

  public static class Config {
    private final int _threadPoolSize;

    private final int _maxBacklogPerServer;

    private final boolean _clientAuthEnabled;

    private final String _trustStorePath;

    private final String _trustStorePassword;

    private final String _keyStorePath;

    private final String _keyStorePassword;

    @Deprecated
    private final long _idleTimeoutMillis;
    @Deprecated
    private final int _minConnectionsPerServer;
    @Deprecated
    private final int _maxConnectionsPerServer;

    public Config(Map<String, Object> pinotConfigs) {
      _idleTimeoutMillis = Long.parseLong(pinotConfigs.get("idleTimeoutMillis").toString());
      _threadPoolSize = Integer.parseInt(pinotConfigs.get("threadPoolSize").toString());
      _minConnectionsPerServer = Integer.parseInt(pinotConfigs.get("minConnectionsPerServer").toString());
      _maxBacklogPerServer = Integer.parseInt(pinotConfigs.get("maxBacklogPerServer").toString());
      _maxConnectionsPerServer = Integer.parseInt(pinotConfigs.get("maxConnectionsPerServer").toString());
      _clientAuthEnabled = Boolean.parseBoolean(pinotConfigs.get("isClientAuthEnabled").toString());
      _trustStorePath = pinotConfigs.get("trustStorePath").toString();
      _trustStorePassword = pinotConfigs.get("trustStorePassword").toString();
      _keyStorePath = pinotConfigs.get("keyStorePath").toString();
      _keyStorePassword = pinotConfigs.get("keyStorePassword").toString();
    }

    public Config(long idleTimeoutMillis, int threadPoolSize, int minConnectionsPerServer, int maxBacklogPerServer,
        int maxConnectionsPerServer) {
      _idleTimeoutMillis = idleTimeoutMillis;
      _threadPoolSize = threadPoolSize;
      _minConnectionsPerServer = minConnectionsPerServer;
      _maxBacklogPerServer = maxBacklogPerServer;
      _maxConnectionsPerServer = maxConnectionsPerServer;
      _clientAuthEnabled = false;
      _trustStorePath = null;
      _trustStorePassword = null;
      _keyStorePath = null;
      _keyStorePassword = null;
    }

    public int getThreadPoolSize() {
      return _threadPoolSize;
    }

    public int getMaxBacklogPerServer() {
      return _maxBacklogPerServer;
    }

    @Deprecated
    public long getIdleTimeoutMillis() {
      return _idleTimeoutMillis;
    }

    @Deprecated
    public int getMinConnectionsPerServer() {
      return _minConnectionsPerServer;
    }

    @Deprecated
    public int getMaxConnectionsPerServer() {
      return _maxConnectionsPerServer;
    }

    public boolean isClientAuthEnabled() {
      return _clientAuthEnabled;
    }

    public String getTrustStorePath() {
      return _trustStorePath;
    }

    public String getTrustStorePassword() {
      return _trustStorePassword;
    }

    public String getKeyStorePath() {
      return _keyStorePath;
    }

    public String getKeyStorePassword() {
      return _keyStorePassword;
    }
  }

  public PinotScatterGatherQueryClient(Config pinotConfig) {
    _prestoHostId = getDefaultPrestoId();
    _brokerMetrics = new BrokerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _brokerMetrics.initializeGlobalMeters();
    TlsConfig tlsConfig = getTlsConfig(pinotConfig);

    // Setup QueryRouters
    for (int i = 0; i < pinotConfig.getThreadPoolSize(); i++) {
      _queryRouters.add(new QueryRouter(String.format("%s-%d", _prestoHostId, i), _brokerMetrics, tlsConfig));
    }

    _config = pinotConfig;
  }

  private TlsConfig getTlsConfig(Config pinotConfig) {
    TlsConfig tlsConfig = new TlsConfig();
    tlsConfig.setClientAuthEnabled(pinotConfig.isClientAuthEnabled());
    tlsConfig.setTrustStorePath(pinotConfig.getTrustStorePath());
    tlsConfig.setTrustStorePassword(pinotConfig.getTrustStorePassword());
    tlsConfig.setKeyStorePath(pinotConfig.getKeyStorePath());
    tlsConfig.setKeyStorePassword(pinotConfig.getKeyStorePassword());
    return tlsConfig;
  }

  private static <T> T doWithRetries(int retries, Function<Integer, T> caller) {
    PinotException firstError = null;
    for (int i = 0; i < retries; i++) {
      try {
        return caller.apply(i);
      } catch (PinotException e) {
        if (firstError == null) {
          firstError = e;
        }
        if (!e.getErrorCode().isRetriable()) {
          throw e;
        }
      }
    }
    throw firstError;
  }

  private String getDefaultPrestoId() {
    String defaultBrokerId;
    try {
      defaultBrokerId = PRESTO_HOST_PREFIX + InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      defaultBrokerId = PRESTO_HOST_PREFIX;
    }
    return defaultBrokerId;
  }

  public Map<ServerInstance, DataTable> queryPinotServerForDataTable(
      String pql,
      String serverHost,
      List<String> segments,
      long connectionTimeoutInMillis,
      boolean ignoreEmptyResponses,
      int pinotRetryCount) {
    BrokerRequest brokerRequest;
    try {
      brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(pql);
    } catch (Pql2CompilationException e) {
      throw new PinotException(ErrorCode.PINOT_INVALID_PQL_GENERATED,
          String.format("Parsing error with on %s, Error = %s", serverHost, e.getMessage()), e);
    }

    Map<org.apache.pinot.core.transport.ServerInstance, List<String>> routingTable = new HashMap<>();
    routingTable.put(new org.apache.pinot.core.transport.ServerInstance(new InstanceConfig(serverHost)),
        new ArrayList<>(segments));

    // Unfortunately the retries will all hit the same server because the routing decision has already been made by
      // the pinot broker
    Map<ServerInstance, DataTable> serverResponseMap = doWithRetries(pinotRetryCount, (requestId) -> {
      String rawTableName = TableNameBuilder.extractRawTableName(brokerRequest.getQuerySource().getTableName());
      if (!_concurrentQueriesCountMap.containsKey(serverHost)) {
        _concurrentQueriesCountMap.put(serverHost, new AtomicInteger(0));
      }
      int concurrentQueryNum = _concurrentQueriesCountMap.get(serverHost).get();
      if (concurrentQueryNum > _config.getMaxBacklogPerServer()) {
        throw new PinotException(ErrorCode.PINOT_QUERY_BACKLOG_FULL,
            "Reaching server query max backlog size is - " + _config.getMaxBacklogPerServer());
      }
      _concurrentQueriesCountMap.get(serverHost).incrementAndGet();
      AsyncQueryResponse asyncQueryResponse;
      QueryRouter nextAvailableQueryRouter = getNextAvailableQueryRouter();
      if (TableNameBuilder.getTableTypeFromTableName(brokerRequest.getQuerySource().getTableName())
          == TableType.REALTIME) {
        asyncQueryResponse = nextAvailableQueryRouter.submitQuery(requestId, rawTableName, null, null, brokerRequest,
            routingTable, connectionTimeoutInMillis);
      } else {
        asyncQueryResponse = nextAvailableQueryRouter
            .submitQuery(requestId, rawTableName, brokerRequest, routingTable, null, null, connectionTimeoutInMillis);
      }
      Map<ServerInstance, DataTable> serverInstanceDataTableMap = gatherServerResponses(
          ignoreEmptyResponses,
          routingTable,
          asyncQueryResponse,
          brokerRequest.getQuerySource().getTableName());
      _queryRouters.offer(nextAvailableQueryRouter);
      _concurrentQueriesCountMap.get(serverHost).decrementAndGet();
      return serverInstanceDataTableMap;
    });
    return serverResponseMap;
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

  private Map<ServerInstance, DataTable> gatherServerResponses(boolean ignoreEmptyResponses,
      Map<org.apache.pinot.core.transport.ServerInstance, List<String>> routingTable,
      AsyncQueryResponse asyncQueryResponse, String tableNameWithType) {
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
          throw new PinotException(ErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE, String
              .format("%d of %d servers responded with routing table servers: %s, query stats: %s",
                  queryResponses.size(), routingTable.size(), routingTableForLogging, asyncQueryResponse.getStats()));
        }
      }
      Map<ServerInstance, DataTable> serverResponseMap = new HashMap<>();
      queryResponses.entrySet().forEach(entry -> serverResponseMap.put(
          new ServerInstance(new InstanceConfig(
              String.format("Server_%s_%d", entry.getKey().getHostname(), entry.getKey().getPort()))),
          entry.getValue().getDataTable()));
      return serverResponseMap;
    } catch (InterruptedException e) {
      throw new PinotException(ErrorCode.PINOT_UNCLASSIFIED_ERROR,
          String.format("Caught exception while fetching responses for table: %s", tableNameWithType), e);
    }
  }
}
