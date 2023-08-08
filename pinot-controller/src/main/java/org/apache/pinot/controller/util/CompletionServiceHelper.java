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
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.common.http.MultiHttpRequest;
import org.apache.pinot.common.http.MultiHttpRequestResponse;
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
    CompletionServiceResponse completionServiceResponse = new CompletionServiceResponse();

    // TODO: use some service other than completion service so that we know which server encounters the error
    CompletionService<MultiHttpRequestResponse> completionService =
        new MultiHttpRequest(_executor, _httpConnectionManager).execute(serverURLs, requestHeaders, timeoutMs);
    for (int i = 0; i < serverURLs.size(); i++) {
      MultiHttpRequestResponse multiHttpRequestResponse = null;
      try {
        multiHttpRequestResponse = completionService.take().get();
        URI uri = multiHttpRequestResponse.getURI();
        CloseableHttpResponse response = multiHttpRequestResponse.getResponse();
        String instance =
            _endpointsToServers.get(String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort()));
        if (response.getStatusLine().getStatusCode() >= 300) {
          LOGGER.error("Server: {} returned error: {}", instance, response.getStatusLine().getStatusCode());
          completionServiceResponse._failedResponseCount++;
          continue;
        }
        String responseString = EntityUtils.toString(response.getEntity());
        completionServiceResponse._httpResponses
            .put(multiRequestPerServer ? uri.toString() : instance, responseString);
      } catch (Exception e) {
        String reason = useCase == null ? "" : String.format(" in '%s'", useCase);
        LOGGER.error("Connection error{}. Details: {}", reason, e.getMessage());
        completionServiceResponse._failedResponseCount++;
      } finally {
        if (multiHttpRequestResponse != null) {
          try {
            multiHttpRequestResponse.getResponse().close();
          } catch (IOException e) {
            LOGGER.error("Connection close error. Details: {}", e.getMessage());
          }
        }
      }
    }

    int numServersResponded = completionServiceResponse._httpResponses.size();
    if (numServersResponded != serverURLs.size()) {
      LOGGER.warn("Finished reading information for table: {} with {}/{} server responses", tableNameWithType,
          numServersResponded, serverURLs.size());
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

    public CompletionServiceResponse() {
      _httpResponses = new HashMap<>();
      _failedResponseCount = 0;
    }
  }
}
