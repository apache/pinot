/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.linkedin.pinot.common.http.MultiGetRequest;
import com.linkedin.pinot.common.restlet.resources.SegmentSizeInfo;
import com.linkedin.pinot.common.restlet.resources.TableSizeInfo;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.ConnectionPoolTimeoutException;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(
      ServerTableSizeReader.class);

  private final Executor executor;
  private final HttpConnectionManager connectionManager;

  public ServerTableSizeReader(Executor executor, HttpConnectionManager connectionManager) {
    this.executor = executor;
    this.connectionManager = connectionManager;
  }

  public Map<String, List<SegmentSizeInfo>> getSizeDetailsFromServers(BiMap<String, String> serverEndPoints,
      String table, int timeoutMsec) {

    List<String> serverUrls = new ArrayList<>(serverEndPoints.size());
    BiMap<String, String> endpointsToServers = serverEndPoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String tableSizeUri = "http://" + endpoint + "/table/" + table + "/size";
      serverUrls.add(tableSizeUri);
    }

    MultiGetRequest mget = new MultiGetRequest(executor, connectionManager);
    LOGGER.info("Reading segment sizes from servers for table: {}, timeoutMsec: {}", table, timeoutMsec);
    CompletionService<GetMethod> completionService = mget.execute(serverUrls, timeoutMsec);

    Map<String, List<SegmentSizeInfo>> serverSegmentSizes = new HashMap<>(serverEndPoints.size());

    for (int i = 0; i < serverUrls.size(); i++) {
      GetMethod getMethod = null;
      try {
        getMethod = completionService.take().get();
        URI uri = getMethod.getURI();
        String instance = endpointsToServers.get(uri.getHost() + ":" + uri.getPort());
        if (getMethod.getStatusCode() >= 300) {
          LOGGER.error("Server: {} returned error: {}", instance, getMethod.getStatusCode());
          continue;
        }
        TableSizeInfo tableSizeInfo = new ObjectMapper().readValue(getMethod.getResponseBodyAsString(), TableSizeInfo.class);
        serverSegmentSizes.put(instance, tableSizeInfo.segments);
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupted exception while reading segment size for table: {}", table, e);
      } catch (ExecutionException e) {
        if (Throwables.getRootCause(e) instanceof SocketTimeoutException) {
          LOGGER.warn("Server request to read table size was timed out for table: {}", table, e);
        } else if (Throwables.getRootCause(e) instanceof ConnectTimeoutException) {
          LOGGER.warn("Server request to read table size timed out waiting for connection. table: {}", table, e);
        } else if (Throwables.getRootCause(e) instanceof ConnectionPoolTimeoutException) {
          LOGGER.warn("Server request to read table size timed out on getting a connection from pool, table: {}", table, e);
        } else {
          LOGGER.warn("Execution exception while reading segment sizes for table: {}", table, e);
        }
      } catch(Exception e) {
        LOGGER.warn("Error while reading segment sizes for table: {}", table);
      } finally {
        if (getMethod != null) {
          getMethod.releaseConnection();
        }
      }
    }
    LOGGER.info("Finished reading segment sizes for table: {}", table);
    return serverSegmentSizes;
  }
}
