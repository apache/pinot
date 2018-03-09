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
package com.linkedin.pinot.controller.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.linkedin.pinot.common.http.MultiGetRequest;
import com.linkedin.pinot.common.restlet.resources.ServerPerfMetrics;
import com.linkedin.pinot.common.restlet.resources.ServerSegmentInfo;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.ConnectionPoolTimeoutException;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerPerfMetricsReader {
  private static Logger LOGGER = LoggerFactory.getLogger(TableSizeReader.class);
  private Executor _executor;
  private HttpConnectionManager _connectionManager;
  private PinotHelixResourceManager _helixResourceManager;

  public ServerPerfMetricsReader(Executor executor, HttpConnectionManager connectionManager,
      PinotHelixResourceManager helixResourceManager) {
    _executor = executor;
    _connectionManager = connectionManager;
    _helixResourceManager = helixResourceManager;
  }

  public @Nullable
  ServerSegmentInfo getServerPerfMetrics(@Nonnull String serverNameOrEndpoint, boolean isThisServerName, @Nonnegative int timeoutMsec) {

    Preconditions.checkNotNull(serverNameOrEndpoint, "Server Name Or Endpoint name should not be null");
    Preconditions.checkArgument(timeoutMsec > 0, "Timeout value must be greater than 0");
    if (isThisServerName) {
      Preconditions.checkNotNull(_helixResourceManager, "_helixResourceManager should not be null");
    }

    ServerSegmentInfo serverSegmentInfo = new ServerSegmentInfo(serverNameOrEndpoint);

    String serverPerfUrl;

    if (isThisServerName) {
      BiMap<String, String> endpoint = _helixResourceManager.getDataInstanceAdminEndpoints(
          Collections.singleton(serverNameOrEndpoint));
      Preconditions.checkNotNull(endpoint, "Server endpoint should not be null");
      Iterator<String> iterator = endpoint.values().iterator();
      serverPerfUrl = "http://" + iterator.next() + CommonConstants.Helix.ServerPerfMetricUris.SERVER_SEGMENT_INFO_URI;
    } else {
      serverPerfUrl = "http://" + serverNameOrEndpoint + CommonConstants.Helix.ServerPerfMetricUris.SERVER_SEGMENT_INFO_URI;
    }

    List<String> serverUrl = new ArrayList<>(1);
    serverUrl.add(serverPerfUrl);
    MultiGetRequest mget = new MultiGetRequest(_executor, _connectionManager);
    LOGGER.info("Reading segments size for server: {}, timeoutMsec: {}", serverNameOrEndpoint, timeoutMsec);
    CompletionService<GetMethod> completionService = mget.execute(serverUrl, timeoutMsec);
    GetMethod getMethod = null;
    try {
      getMethod = completionService.take().get();
      if (getMethod.getStatusCode() >= timeoutMsec) {
        LOGGER.error("Server: {} returned error: {}", serverNameOrEndpoint, getMethod.getStatusCode());
        return serverSegmentInfo;
      }
      ServerPerfMetrics serverPerfMetrics =
          new ObjectMapper().readValue(getMethod.getResponseBodyAsString(), ServerPerfMetrics.class);
      serverSegmentInfo.setSegmentCount(serverPerfMetrics.segmentCount);
      serverSegmentInfo.setSegmentSizeInBytes(serverPerfMetrics.segmentDiskSizeInBytes);
      //serverSegmentInfo.setSegmentList(serverPerfMetrics.segmentList);
      serverSegmentInfo.setSegmentCPULoad(serverPerfMetrics.segmentCPULoad);

      serverSegmentInfo.setTableList(serverPerfMetrics.tableList);
      serverSegmentInfo.setSegmentTimeInfo(serverPerfMetrics.segmentTimeInfo);

    } catch (InterruptedException e) {
      LOGGER.warn("Interrupted exception while reading segments size for server: {}", serverNameOrEndpoint, e);
    } catch (ExecutionException e) {
      if (Throwables.getRootCause(e) instanceof SocketTimeoutException) {
        LOGGER.warn("Server request to read segments info was timed out for table: {}", serverNameOrEndpoint, e);
      } else if (Throwables.getRootCause(e) instanceof ConnectTimeoutException) {
        LOGGER.warn("Server request to read segments info timed out waiting for connection. table: {}",
            serverNameOrEndpoint, e);
      } else if (Throwables.getRootCause(e) instanceof ConnectionPoolTimeoutException) {
        LOGGER.warn("Server request to read segments info timed out on getting a connection from pool, table: {}",
            serverNameOrEndpoint, e);
      } else {
        LOGGER.warn("Execution exception while reading segment info for server: {}", serverNameOrEndpoint, e);
      }
    } catch (Exception e) {
      LOGGER.warn("Error while reading segment info for server: {}", serverNameOrEndpoint);
    } finally {
      if (getMethod != null) {
        getMethod.releaseConnection();
      }
    }
    return serverSegmentInfo;
  }
}
