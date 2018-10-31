/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.resources;

import com.google.common.collect.BiMap;
import com.linkedin.pinot.common.http.MultiGetRequest;
import com.linkedin.pinot.common.restlet.resources.SegmentSizeInfo;
import com.linkedin.pinot.common.restlet.resources.TableSizeInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.GetMethod;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Get the size information details from the server. Only the servers returning success are returned by the method
 * For servers returning errors (http error or otherwise), no entry is created in the return map
 */
public class ServerTableSizeReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerTableSizeReader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Executor _executor;
  private final HttpConnectionManager _connectionManager;

  public ServerTableSizeReader(Executor executor, HttpConnectionManager connectionManager) {
    _executor = executor;
    _connectionManager = connectionManager;
  }

  public Map<String, List<SegmentSizeInfo>> getSegmentSizeInfoFromServers(BiMap<String, String> serverEndPoints,
      String tableNameWithType, int timeoutMs) {
    int numServers = serverEndPoints.size();
    LOGGER.info("Reading segment sizes from {} servers for table: {} with timeout: {}ms", numServers, tableNameWithType,
        timeoutMs);

    List<String> serverUrls = new ArrayList<>(numServers);
    BiMap<String, String> endpointsToServers = serverEndPoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String tableSizeUri = "http://" + endpoint + "/table/" + tableNameWithType + "/size";
      serverUrls.add(tableSizeUri);
    }

    // TODO: use some service other than completion service so that we know which server encounters the error
    CompletionService<GetMethod> completionService =
        new MultiGetRequest(_executor, _connectionManager).execute(serverUrls, timeoutMs);
    Map<String, List<SegmentSizeInfo>> serverToSegmentSizeInfoListMap = new HashMap<>();

    for (int i = 0; i < numServers; i++) {
      GetMethod getMethod = null;
      try {
        getMethod = completionService.take().get();
        URI uri = getMethod.getURI();
        String instance = endpointsToServers.get(uri.getHost() + ":" + uri.getPort());
        if (getMethod.getStatusCode() >= 300) {
          LOGGER.error("Server: {} returned error: {}", instance, getMethod.getStatusCode());
          continue;
        }
        TableSizeInfo tableSizeInfo = OBJECT_MAPPER.readValue(getMethod.getResponseBodyAsStream(), TableSizeInfo.class);
        serverToSegmentSizeInfoListMap.put(instance, tableSizeInfo.segments);
      } catch (Exception e) {
        // Ignore individual exceptions because the exception has been logged in MultiGetRequest
        // Log the number of failed servers after gathering all responses
      } finally {
        if (getMethod != null) {
          getMethod.releaseConnection();
        }
      }
    }

    int numServersResponded = serverToSegmentSizeInfoListMap.size();
    if (numServersResponded != numServers) {
      LOGGER.warn("Finish reading segment sizes for table: {} with {}/{} servers responded", tableNameWithType,
          numServersResponded, numServers);
    } else {
      LOGGER.info("Finish reading segment sizes for table: {}", tableNameWithType);
    }
    return serverToSegmentSizeInfoListMap;
  }
}
