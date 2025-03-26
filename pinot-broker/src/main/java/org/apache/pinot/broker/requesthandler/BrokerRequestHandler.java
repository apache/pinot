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
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.PinotBrokerTimeSeriesResponse;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.RequestScope;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;


@ThreadSafe
public interface BrokerRequestHandler {

  void start();

  void shutDown();

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

  /**
   * Run a query and use the time-series engine.
   */
  default PinotBrokerTimeSeriesResponse handleTimeSeriesRequest(String lang, String rawQueryParamString,
      RequestContext requestContext) {
    throw new UnsupportedOperationException("Handler does not support Time Series requests");
  }

  Map<Long, String> getRunningQueries();

  /**
   * Cancel a query as identified by the queryId. This method is non-blocking so the query may still run for a while
   * after calling this method. This cancel method can be called multiple times.
   * @param queryId the unique Id assigned to the query by the broker
   * @param timeoutMs timeout to wait for servers to respond the cancel requests
   * @param executor to send cancel requests to servers in parallel
   * @param connMgr to provide the http connections
   * @param serverResponses to collect cancel responses from all servers if a map is provided
   * @return true if there is a running query for the given queryId.
   */
  boolean cancelQuery(long queryId, int timeoutMs, Executor executor, HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses)
      throws Exception;

  /**
   * Cancel a query as identified by the clientQueryId provided externally. This method is non-blocking so the query may
   * still run for a while after calling this method. This cancel method can be called multiple times.
   * @param clientQueryId the Id assigned to the query by the client
   * @param timeoutMs timeout to wait for servers to respond the cancel requests
   * @param executor to send cancel requests to servers in parallel
   * @param connMgr to provide the http connections
   * @param serverResponses to collect cancel responses from all servers if a map is provided
   * @return true if there is a running query for the given clientQueryId.
   */
  boolean cancelQueryByClientId(String clientQueryId, int timeoutMs, Executor executor,
      HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses)
      throws Exception;

  /// Returns the request ID for the given client query ID.
  OptionalLong getRequestIdByClientId(String clientQueryId);
}
