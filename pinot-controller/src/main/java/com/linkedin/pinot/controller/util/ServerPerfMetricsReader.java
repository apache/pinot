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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.linkedin.pinot.common.http.MultiGetRequest;
import com.linkedin.pinot.common.restlet.resources.ServerPerfMetrics;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
    this._executor = executor;
    this._connectionManager = connectionManager;
    this._helixResourceManager = helixResourceManager;
  }

  public @Nullable
  ServerPerfMetricsReader.ServerSegmentsInfo getServerPerfMetrics(@Nonnull String serverName,
      @Nonnegative int timeoutMsec) {
    Preconditions.checkNotNull(serverName, "Server name should not be null");
    Preconditions.checkArgument(timeoutMsec > 0, "Timeout value must be greater than 0");

    ServerPerfMetricsReader.ServerSegmentsInfo serverSegmentsInfo =
        new ServerPerfMetricsReader.ServerSegmentsInfo(serverName);
    Set<String> singleServerList = new HashSet<String>();
    singleServerList.add(serverName);
    BiMap<String, String> endpoint = _helixResourceManager.getDataInstanceAdminEndpoints(singleServerList);
    Preconditions.checkNotNull(endpoint, "Server endpoint should not be null");
    BiMap<String, String> endpointInverse = endpoint.inverse();
    List<String> serverUrl = new ArrayList<>(1);
    Iterator<String> iterator = endpointInverse.keySet().iterator();
    String serverPerfUrl = "http://" + iterator.next() + "/ServerPerfMetrics/SegmentsInfo";
    serverUrl.add(serverPerfUrl);
    MultiGetRequest mget = new MultiGetRequest(_executor, _connectionManager);
    LOGGER.info("Reading segments size for server: {}, timeoutMsec: {}", serverName, timeoutMsec);
    CompletionService<GetMethod> completionService = mget.execute(serverUrl, timeoutMsec);
    GetMethod getMethod = null;
    try {
      getMethod = completionService.take().get();
      if (getMethod.getStatusCode() >= timeoutMsec) {
        LOGGER.error("Server: {} returned error: {}", serverName, getMethod.getStatusCode());
        return serverSegmentsInfo;
      }
      ServerPerfMetrics serverPerfMetrics =
          new ObjectMapper().readValue(getMethod.getResponseBodyAsString(), ServerPerfMetrics.class);
      serverSegmentsInfo.setReportedNumOfSegments(serverPerfMetrics.numOfSegments);
      serverSegmentsInfo.setReportedSegmentsSizeInBytes(serverPerfMetrics.segmentsDiskSizeInBytes);
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupted exception while reading segments size for server: {}", serverName, e);
    } catch (ExecutionException e) {
      if (Throwables.getRootCause(e) instanceof SocketTimeoutException) {
        LOGGER.warn("Server request to read segments info was timed out for table: {}", serverName, e);
      } else if (Throwables.getRootCause(e) instanceof ConnectTimeoutException) {
        LOGGER.warn("Server request to read segments info timed out waiting for connection. table: {}", serverName, e);
      } else if (Throwables.getRootCause(e) instanceof ConnectionPoolTimeoutException) {
        LOGGER.warn("Server request to read segments info timed out on getting a connection from pool, table: {}",
            serverName, e);
      } else {
        LOGGER.warn("Execution exception while reading segment info for server: {}", serverName, e);
      }
    } catch (Exception e) {
      LOGGER.warn("Error while reading segment info for server: {}", serverName);
    } finally {
      if (getMethod != null) {
        getMethod.releaseConnection();
      }
    }
    return serverSegmentsInfo;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class ServerSegmentsInfo {
    private String _serverName;
    private long _reportedNumOfSegments;
    private long _reportedSegmentsSizeInBytes;

    public ServerSegmentsInfo(String serverName) {
      this._serverName = serverName;
      //We set default value to -1 as indication of error in returning reported info from a Pinot server
      _reportedNumOfSegments = -1;
      _reportedSegmentsSizeInBytes = -1;
    }

    public String getServerName(){
      return _serverName;
    }
    public long getReportedNumOfSegments() {
      return _reportedNumOfSegments;
    }

    public void setReportedNumOfSegments(long reportedNumOfSegments) {
      this._reportedNumOfSegments = reportedNumOfSegments;
    }

    public long getReportedSegmentsSizeInBytes() {
      return _reportedSegmentsSizeInBytes;
    }

    public void setReportedSegmentsSizeInBytes(long reportedSegmentsSizeInBytes) {
      this._reportedSegmentsSizeInBytes = reportedSegmentsSizeInBytes;
    }
  }
}
