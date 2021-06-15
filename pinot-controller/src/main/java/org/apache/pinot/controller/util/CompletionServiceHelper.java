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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.pinot.common.http.MultiGetRequest;
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
  private final HttpConnectionManager _httpConnectionManager;
  private final BiMap<String, String> _endpointsToServers;

  public CompletionServiceHelper(Executor executor, HttpConnectionManager httpConnectionManager,
                                 BiMap<String, String> endpointsToServers) {
    _executor = executor;
    _httpConnectionManager = httpConnectionManager;
    _endpointsToServers = endpointsToServers;
  }

  public CompletionServiceResponse doMultiGetRequest(List<String> serverURLs, String tableNameWithType, int timeoutMs) {
    CompletionServiceResponse completionServiceResponse = new CompletionServiceResponse();

    // TODO: use some service other than completion service so that we know which server encounters the error
    CompletionService<GetMethod> completionService =
        new MultiGetRequest(_executor, _httpConnectionManager).execute(serverURLs, timeoutMs);
    for (int i = 0; i < serverURLs.size(); i++) {
      GetMethod getMethod = null;
      try {
        getMethod = completionService.take().get();
        URI uri = getMethod.getURI();
        String instance = _endpointsToServers.get(
            String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort()));
        if (getMethod.getStatusCode() >= 300) {
          LOGGER.error("Server: {} returned error: {}", instance, getMethod.getStatusCode());
          completionServiceResponse._failedResponseCount++;
          continue;
        }
        completionServiceResponse._httpResponses.put(uri.toString(), getMethod.getResponseBodyAsString());
      } catch (Exception e) {
        LOGGER.error("Connection error", e);
        completionServiceResponse._failedResponseCount++;
      } finally {
        if (getMethod != null) {
          getMethod.releaseConnection();
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
