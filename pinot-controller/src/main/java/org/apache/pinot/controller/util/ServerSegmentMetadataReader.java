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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a helper class that calls the server API endpoints to fetch server metadata and the segment reload status
 * Only the servers returning success are returned by the method. For servers returning errors (http error or otherwise),
 * no entry is created in the return list
 */
public class ServerSegmentMetadataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerSegmentMetadataReader.class);

  private final Executor _executor;
  private final HttpConnectionManager _connectionManager;

  public ServerSegmentMetadataReader(Executor executor, HttpConnectionManager connectionManager) {
    _executor = executor;
    _connectionManager = connectionManager;
  }

  /**
   * This method is called when the API request is to fetch segment metadata for all segments of the table.
   * This method makes a MultiGet call to all servers that host their respective segments and gets the results.
   * This method accept a list of column names as filter, and will return column metadata for the column in the
   * list.
   * @return list of segments and their metadata as a JSON string
   */
  public List<String> getSegmentMetadataFromServer(String tableNameWithType,
      Map<String, List<String>> serversToSegmentsMap, BiMap<String, String> endpoints, List<String> columns,
      int timeoutMs) {
    LOGGER.debug("Reading segment metadata from servers for table {}.", tableNameWithType);
    List<String> serverURLs = new ArrayList<>();
    for (Map.Entry<String, List<String>> serverToSegments : serversToSegmentsMap.entrySet()) {
      List<String> segments = serverToSegments.getValue();
      for (String segment : segments) {
        serverURLs.add(generateSegmentMetadataServerURL(tableNameWithType, segment, columns,
            endpoints.get(serverToSegments.getKey())));
      }
    }
    BiMap<String, String> endpointsToServers = endpoints.inverse();
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverURLs, tableNameWithType, true, timeoutMs);
    List<String> segmentsMetadata = new ArrayList<>();

    int failedParses = 0;
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        String segmentMetadata = streamResponse.getValue();
        segmentsMetadata.add(segmentMetadata);
      } catch (Exception e) {
        failedParses++;
        LOGGER.error("Unable to parse server {} response due to an error: ", streamResponse.getKey(), e);
      }
    }
    if (failedParses != 0) {
      LOGGER.error("Unable to parse server {} / {} response due to an error: ", failedParses, serverURLs.size());
    }

    LOGGER.debug("Retrieved segment metadata from servers.");
    return segmentsMetadata;
  }

  private String generateSegmentMetadataServerURL(String tableNameWithType, String segmentName, List<String> columns,
      String endpoint) {
    try {
      tableNameWithType = URLEncoder.encode(tableNameWithType, StandardCharsets.UTF_8.name());
      segmentName = URLEncoder.encode(segmentName, StandardCharsets.UTF_8.name());
      String paramsStr = "";
      if (columns != null) {
        List<String> params = new ArrayList<>(columns.size());
        for (String column : columns) {
          params.add(String.format("columns=%s", column));
        }
        paramsStr = String.join("&", params);
      }
      return String.format("%s/tables/%s/segments/%s/metadata?%s", endpoint, tableNameWithType, segmentName, paramsStr);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e.getCause());
    }
  }
}
