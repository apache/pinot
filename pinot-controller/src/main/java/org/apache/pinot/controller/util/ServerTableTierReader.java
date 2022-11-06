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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.pinot.common.restlet.resources.TableTierInfo;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Get the storage tier details from multi servers in parallel. Only servers returning success are returned by the
 * method. For those returning errors (http error or otherwise), no entry is created in the return map.
 */
public class ServerTableTierReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerTableTierReader.class);

  private final Executor _executor;
  private final HttpConnectionManager _connectionManager;

  public ServerTableTierReader(Executor executor, HttpConnectionManager connectionManager) {
    _executor = executor;
    _connectionManager = connectionManager;
  }

  public Map<String, TableTierInfo> getTableTierInfoFromServers(BiMap<String, String> serverEndPoints,
      String tableNameWithType, @Nullable String segmentName, int timeoutMs) {
    int numServers = serverEndPoints.size();
    LOGGER.info("Getting segment storage tiers from {} servers for table: {} with timeout: {}ms", numServers,
        tableNameWithType, timeoutMs);
    List<String> serverUrls = new ArrayList<>(numServers);
    BiMap<String, String> endpointsToServers = serverEndPoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String tierUri = endpoint;
      if (segmentName == null) {
        tierUri += "/tables/" + tableNameWithType + "/tiers";
      } else {
        tierUri += "/segments/" + tableNameWithType + "/" + segmentName + "/tiers";
      }
      serverUrls.add(tierUri);
    }
    LOGGER.debug("Getting table tier info with serverUrls: {}", serverUrls);
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, tableNameWithType, false, timeoutMs);
    Map<String, TableTierInfo> serverToTableTierInfoMap = new HashMap<>();
    int failedParses = 0;
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        TableTierInfo tableTierInfo = JsonUtils.stringToObject(streamResponse.getValue(), TableTierInfo.class);
        serverToTableTierInfoMap.put(streamResponse.getKey(), tableTierInfo);
      } catch (IOException e) {
        failedParses++;
        LOGGER.error("Failed to parse server {} response", streamResponse.getKey(), e);
      }
    }
    if (failedParses != 0) {
      LOGGER.warn("Failed to parse {} / {} TableTierInfo responses from servers.", failedParses, serverUrls.size());
    }
    return serverToTableTierInfoMap;
  }
}
