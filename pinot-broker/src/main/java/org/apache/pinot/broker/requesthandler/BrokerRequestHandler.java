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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryProgressStats;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.RequestScope;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


@ThreadSafe
public interface BrokerRequestHandler {

  void start();

  void shutDown();

  /// Warms this handler's data plane so the first real query does not pay for it, and reports whether the
  /// handler reached its warmth floor. Called during startup after Helix convergence, before readiness is
  /// granted.
  ///
  /// Implementations must honour three contracts, because the caller depends on each:
  ///   - **Bounded — must return by `deadlineMs`.** This is the load-bearing clause of the feature: a
  ///     warmup that runs past the deadline stalls a rolling restart. `deadlineMs` is an absolute
  ///     [System#currentTimeMillis] value, and every wait an implementation performs (each probe, each
  ///     blocking get) must be bounded by the remaining budget, not a fixed constant. Time already spent
  ///     waiting for the cluster view to converge counts against the same budget, so the rolling-restart
  ///     cost stays bounded by one number.
  ///   - **Must never throw.** The caller opens the readiness gate off this call; an exception must be
  ///     swallowed and treated as "did not reach the floor", never propagated.
  ///   - **Safe to run against a handler that is already serving.** Nothing stops this being invoked on a
  ///     started, traffic-serving handler -- [org.apache.pinot.integration.tests] does exactly that, and an
  ///     admin endpoint wiring it later would too -- so an implementation must not mutate shared serving
  ///     state or assume it is the only in-flight work.
  ///
  /// This gate composes with any other startup gate the broker registers (e.g. [#preConnectServers(long)]):
  /// each gate is independent with its own budget, and readiness is granted only once **all** are satisfied.
  ///
  /// @return `true` if the handler reached its warmth floor, `false` if the deadline passed first. Either
  ///         way the caller proceeds; the result is for logging and metrics.
  default boolean warmUp(BrokerWarmupConfig config, long deadlineMs) {
    return true;
  }

  /// Opens broker-to-server channels ahead of traffic so the first real query does not pay the blocking
  /// connect -- and, when broker-to-server TLS is on, the handshake -- on its critical path. Called once
  /// at startup after Helix has converged, when `pinot.broker.startup.preconnect.enabled` is set.
  ///
  /// Only the single-connection SSE handler opens Netty channels, so the default is a no-op. Returns the
  /// number of channels connected before `deadlineMs` (an absolute [System#currentTimeMillis] value).
  default int preConnectServers(long deadlineMs) {
    return 0;
  }

  BrokerResponse handleRequest(JsonNode request, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext, @Nullable HttpHeaders httpHeaders)
      throws Exception;

  @VisibleForTesting
  default BrokerResponse handleRequest(String sql)
      throws Exception {
    ObjectNode request = JsonUtils.newObjectNode();
    request.put(Request.SQL, sql);
    try (RequestScope requestContext = Tracing.getTracer().createRequestScope()) {
      requestContext.setRequestArrivalTimeMillis(System.currentTimeMillis());
      return handleRequest(request, null, null, requestContext, null);
    }
  }

  /// Run a query and use the time-series engine.
  default TimeSeriesBlock handleTimeSeriesRequest(String lang, String rawQueryParamString,
      Map<String, String> queryParams, RequestContext requestContext, @Nullable RequesterIdentity requesterIdentity,
      HttpHeaders httpHeaders) throws QueryException {
    throw new UnsupportedOperationException("Handler does not support Time Series requests");
  }

  /// Handle an explain request for time-series queries.
  /// Returns a BrokerResponse containing the logical explain plan.
  default BrokerResponse handleExplainTimeSeriesRequest(String lang, String rawQueryParamString,
      Map<String, String> queryParams) {
    throw new UnsupportedOperationException("Handler does not support Time Series explain requests");
  }

  Map<Long, String> getRunningQueries();

  /// Returns progress for a running query, or `null` if the query is not running or progress is disabled. Single-stage
  /// progress uses selected segments as work units. Multi-stage progress also includes stage op-chains and may contain
  /// component detail rows.
  @Nullable
  QueryProgressStats getQueryProgressStats(long queryId, int timeoutMs, Executor executor,
      HttpClientConnectionManager connMgr)
      throws Exception;

  /// Cancel a query as identified by the queryId. This method is non-blocking so the query may still run for a while
  /// after calling this method. This cancel method can be called multiple times.
  /// @param queryId the unique Id assigned to the query by the broker
  /// @param timeoutMs timeout to wait for servers to respond the cancel requests
  /// @param executor to send cancel requests to servers in parallel
  /// @param connMgr to provide the http connections
  /// @param serverResponses to collect cancel responses from all servers if a map is provided
  /// @return true if there is a running query for the given queryId.
  boolean cancelQuery(long queryId, int timeoutMs, Executor executor, HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses)
      throws Exception;

  /// Cancel a query as identified by the clientQueryId provided externally. This method is non-blocking so the query
  /// may still run for a while after calling this method. This cancel method can be called multiple times.
  /// @param clientQueryId the Id assigned to the query by the client
  /// @param timeoutMs timeout to wait for servers to respond the cancel requests
  /// @param executor to send cancel requests to servers in parallel
  /// @param connMgr to provide the http connections
  /// @param serverResponses to collect cancel responses from all servers if a map is provided
  /// @return true if there is a running query for the given clientQueryId.
  boolean cancelQueryByClientId(String clientQueryId, int timeoutMs, Executor executor,
      HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses)
      throws Exception;

  /// Returns the request ID for the given client query ID.
  OptionalLong getRequestIdByClientId(String clientQueryId);
}
