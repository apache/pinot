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

package org.apache.pinot.controller.util;

import com.google.common.collect.BiMap;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.pinot.common.http.MultiHttpRequest;
import org.apache.pinot.common.http.MultiHttpRequestBufferedResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a helper class that can be used to make HttpGet (MultiGet) calls and get the responses back.
 * The responses are returned as a string.
 *
 * The helper also records number of failed responses so that the caller knows if any of the calls
 * failed to respond. The failed instance is logged for debugging.
 */
public class CompletionServiceHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompletionServiceHelper.class);

  private final Executor _executor;
  private final HttpClientConnectionManager _httpConnectionManager;
  private final BiMap<String, String> _endpointsToServers;

  public CompletionServiceHelper(Executor executor, HttpClientConnectionManager httpConnectionManager,
      BiMap<String, String> endpointsToServers) {
    _executor = executor;
    _httpConnectionManager = httpConnectionManager;
    _endpointsToServers = endpointsToServers;
  }

  public CompletionServiceResponse doMultiGetRequest(List<String> serverURLs, String tableNameWithType,
      boolean multiRequestPerServer, int timeoutMs) {
    return doMultiGetRequest(serverURLs, tableNameWithType, multiRequestPerServer, null, timeoutMs, null);
  }

  /**
   * This method makes a MultiGet call to all given URLs.
   * @param serverURLs server urls to send GET request.
   * @param tableNameWithType table name with type suffix
   * @param multiRequestPerServer it's possible that need to send multiple requests to a same server.
   *                              If multiRequestPerServer is set as false, return as long as one of the requests get
   *                              response; If multiRequestPerServer is set as true, wait until all requests
   *                              get response.
   * @param requestHeaders Headers to be set when making the http calls.
   * @param timeoutMs timeout in milliseconds to wait per request.
   * @param useCase the use case initiating the multi-get request. If not null and an exception is thrown, only the
   *                error message and the use case are logged instead of the full stack trace.
   * @return CompletionServiceResponse Map of the endpoint(server instance, or full request path if
   * multiRequestPerServer is true) to the response from that endpoint.
   */
  public CompletionServiceResponse doMultiGetRequest(List<String> serverURLs, String tableNameWithType,
      boolean multiRequestPerServer, @Nullable Map<String, String> requestHeaders, int timeoutMs,
      @Nullable String useCase) {
    MultiHttpRequest.BufferedRequestExecution execution =
        new MultiHttpRequest(_executor, _httpConnectionManager).executeGetBuffered(serverURLs, requestHeaders,
            timeoutMs);
    return collectBufferedResponse(tableNameWithType, serverURLs.size(), execution, multiRequestPerServer, useCase);
  }

  /// Makes buffered, cancellable GET requests and returns once all complete or the deadline from the
  /// [System#nanoTime()] clock is reached.
  public CompletionServiceResponse doMultiGetRequestUntil(List<String> serverURLs, String tableNameWithType,
      boolean multiRequestPerServer, int timeoutMs, long nanoTimeDeadline, @Nullable String useCase) {
    MultiHttpRequest.BufferedRequestExecution execution =
        new MultiHttpRequest(_executor, _httpConnectionManager).executeGetBuffered(serverURLs, null, timeoutMs);
    return collectBufferedResponseUntil(tableNameWithType, serverURLs.size(), execution, multiRequestPerServer, useCase,
        nanoTimeDeadline);
  }

  /**
   * This method makes a MultiPost call to all given URLs and its corresponding bodies.
   * @param serverURLsAndRequestBodies server urls to send POST request.
   * @param tableNameWithType table name with type suffix
   * @param multiRequestPerServer it's possible that need to send multiple requests to a same server.
   *                              If multiRequestPerServer is set as false, return as long as one of the requests return
   *                              response; If multiRequestPerServer is set as true, wait until all requests
   *                              return response.
   * @param requestHeaders Headers to be set when making the http calls.
   * @param timeoutMs timeout in milliseconds to wait per request.
   * @param useCase the use case initiating the multi-post request. If not null and an exception is thrown, only the
   *                error message and the use case are logged instead of the full stack trace.
   * @return CompletionServiceResponse Map of the endpoint(server instance, or full request path if
   * multiRequestPerServer is true) to the response from that endpoint.
   */
  public CompletionServiceResponse doMultiPostRequest(List<Pair<String, String>> serverURLsAndRequestBodies,
      String tableNameWithType, boolean multiRequestPerServer, @Nullable Map<String, String> requestHeaders,
      int timeoutMs, @Nullable String useCase) {
    MultiHttpRequest.BufferedRequestExecution execution =
        new MultiHttpRequest(_executor, _httpConnectionManager).createBufferedExecution(timeoutMs);
    try {
      for (Pair<String, String> requestAndBody : serverURLsAndRequestBodies) {
        HttpPost request = new HttpPost(requestAndBody.getLeft());
        request.setEntity(new StringEntity(requestAndBody.getRight()));
        if (requestHeaders != null) {
          requestHeaders.forEach(request::setHeader);
        }
        execution.submit(request);
      }
    } catch (RuntimeException | Error e) {
      execution.close();
      throw e;
    }
    return collectBufferedResponse(tableNameWithType, serverURLsAndRequestBodies.size(), execution,
        multiRequestPerServer, useCase);
  }

  private CompletionServiceResponse collectBufferedResponse(String tableNameWithType, int size,
      MultiHttpRequest.BufferedRequestExecution execution, boolean multiRequestPerServer, @Nullable String useCase) {
    return collectBufferedResponse(tableNameWithType, size, execution, multiRequestPerServer, useCase, 0, false);
  }

  private CompletionServiceResponse collectBufferedResponseUntil(String tableNameWithType, int size,
      MultiHttpRequest.BufferedRequestExecution execution, boolean multiRequestPerServer, @Nullable String useCase,
      long nanoTimeDeadline) {
    return collectBufferedResponse(tableNameWithType, size, execution, multiRequestPerServer, useCase, nanoTimeDeadline,
        true);
  }

  private CompletionServiceResponse collectBufferedResponse(String tableNameWithType, int size,
      MultiHttpRequest.BufferedRequestExecution execution, boolean multiRequestPerServer, @Nullable String useCase,
      long nanoTimeDeadline, boolean enforceDeadline) {
    CompletionServiceResponse completionServiceResponse = new CompletionServiceResponse();
    try (execution) {
      for (int i = 0; i < size; i++) {
        try {
          Future<MultiHttpRequestBufferedResponse> completedFuture;
          if (enforceDeadline) {
            long remainingNanos = nanoTimeDeadline - System.nanoTime();
            if (remainingNanos <= 0) {
              completionServiceResponse._failedResponseCount += size - i;
              break;
            }
            completedFuture = execution.poll(remainingNanos, TimeUnit.NANOSECONDS);
            if (completedFuture == null) {
              completionServiceResponse._failedResponseCount += size - i;
              break;
            }
          } else {
            completedFuture = execution.take();
          }
          MultiHttpRequestBufferedResponse response = completedFuture.get();
          addResponse(completionServiceResponse, response.getURI(), response.getStatusCode(),
              response.getReasonPhrase(), response.getResponseBody(), multiRequestPerServer);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          completionServiceResponse._failedResponseCount += size - i;
          break;
        } catch (Exception e) {
          recordConnectionError(completionServiceResponse, useCase, e);
        }
      }
    }
    return finishResponse(tableNameWithType, size, completionServiceResponse);
  }

  private void addResponse(CompletionServiceResponse response, URI uri, int statusCode,
      @Nullable String reasonPhrase, @Nullable String responseBody, boolean multiRequestPerServer) {
    String instance =
        _endpointsToServers.get(String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort()));
    if (statusCode >= 300) {
      LOGGER.error("Server: {} returned error: {}, reason: {} for uri: {}", instance, statusCode, reasonPhrase, uri);
      response._failedResponseCount++;
      return;
    }
    String key = multiRequestPerServer ? uri.toString() : instance;
    // Multiple requests can use the same URI with different POST bodies, so preserve every response with a suffix.
    if (multiRequestPerServer) {
      int count = response._instanceToRequestCount.compute(key, (k, v) -> v == null ? 1 : v + 1);
      key = key + "__" + count;
    }
    response._httpResponses.put(key, responseBody != null ? responseBody : "");
  }

  private static void recordConnectionError(CompletionServiceResponse response, @Nullable String useCase, Exception e) {
    String reason = useCase == null ? "" : String.format(" in '%s'", useCase);
    LOGGER.error("Connection error {}. Details: {}", reason, e.getMessage());
    response._failedResponseCount++;
  }

  private static CompletionServiceResponse finishResponse(String tableNameWithType, int size,
      CompletionServiceResponse completionServiceResponse) {
    int numServerRequestsResponded = completionServiceResponse._httpResponses.size();
    if (numServerRequestsResponded != size) {
      LOGGER.warn("Finished reading information for table: {} with {}/{} server-request responses", tableNameWithType,
          numServerRequestsResponded, size);
    } else {
      LOGGER.info("Finished reading information for table: {}", tableNameWithType);
    }
    return completionServiceResponse;
  }

  public CompletionServiceResponse doMultiGetRequest(List<String> serverURLs, String tableNameWithType,
      boolean multiRequestPerServer, int timeoutMs, @Nullable String useCase) {
    return doMultiGetRequest(serverURLs, tableNameWithType, multiRequestPerServer, null, timeoutMs, useCase);
  }

  public CompletionServiceResponse doMultiGetRequest(List<String> serverURLs, String tableNameWithType,
      boolean multiRequestPerServer, @Nullable Map<String, String> requestHeaders, int timeoutMs) {
    return doMultiGetRequest(serverURLs, tableNameWithType, multiRequestPerServer, requestHeaders, timeoutMs, null);
  }


  /**
   * Helper class to maintain the completion service response to be sent back to the caller.
   */
  static public class CompletionServiceResponse {
    // Map of the server instance to the response from that server
    public Map<String, String> _httpResponses;
    // Number of failures encountered when requesting
    public int _failedResponseCount;
    // Map of instance to count of requests
    public Map<String, Integer> _instanceToRequestCount;

    public CompletionServiceResponse() {
      _httpResponses = new HashMap<>();
      _failedResponseCount = 0;
      _instanceToRequestCount = new HashMap<>();
    }
  }
}
