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
package org.apache.pinot.controller.api.resources;

import com.google.common.collect.BiMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Get the size information details from the server. Only the servers returning success are returned by the method
 * For servers returning errors (http error or otherwise), no entry is created in the return map
 */
public class ServerTableSizeReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerTableSizeReader.class);

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
      String tableSizeUri = endpoint + "/table/" + tableNameWithType + "/size";
      serverUrls.add(tableSizeUri);
    }

    // Helper service to run a httpget call to the server
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, tableNameWithType, false, timeoutMs,
            "get segment size info from servers");
    Map<String, List<SegmentSizeInfo>> serverToSegmentSizeInfoListMap = new HashMap<>();
    int failedParses = 0;
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        TableSizeInfo tableSizeInfo = JsonUtils.stringToObject(streamResponse.getValue(), TableSizeInfo.class);
        serverToSegmentSizeInfoListMap.put(streamResponse.getKey(), tableSizeInfo.getSegments());
      } catch (IOException e) {
        failedParses++;
        LOGGER.error("Unable to parse server {} response due to an error: ", streamResponse.getKey(), e);
      }
    }
    if (failedParses != 0) {
      LOGGER.warn("Failed to parse {} / {} segment size info responses from servers.", failedParses, serverUrls.size());
    }
    return serverToSegmentSizeInfoListMap;
  }
}
