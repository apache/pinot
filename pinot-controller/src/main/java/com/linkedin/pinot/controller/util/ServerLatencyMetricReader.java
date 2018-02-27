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
import com.linkedin.pinot.common.http.MultiGetRequest;
import com.linkedin.pinot.common.restlet.resources.ServerLatencyMetric;
import com.linkedin.pinot.common.restlet.resources.ServerLoadMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;

import java.io.*;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.sharding.ServerLoadMetric;
import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.ConnectionPoolTimeoutException;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.helix.HelixAdmin;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerLatencyMetricReader {
    private static Logger LOGGER = LoggerFactory.getLogger(TableSizeReader.class);
    private Executor _executor;
    private HttpConnectionManager _connectionManager;
    private PinotHelixResourceManager _helixResourceManager;

    public ServerLatencyMetricReader(Executor executor, HttpConnectionManager connectionManager,
                                     PinotHelixResourceManager helixResourceManager) {
        _executor = executor;
        _connectionManager = connectionManager;
        _helixResourceManager = helixResourceManager;
    }

    public @Nullable
    ServerLoadMetrics getServerLatencyMetrics(@Nonnull String serverNameOrEndpoint, @Nonnull String tableName, boolean isThisServerName,
                                                      @Nonnegative int timeoutMsec) {
        Preconditions.checkNotNull(serverNameOrEndpoint, "Server Name Or Endpoint name should not be null");
        Preconditions.checkArgument(timeoutMsec > 0, "Timeout value must be greater than 0");
        if (isThisServerName) {
            Preconditions.checkNotNull(_helixResourceManager, "_helixResourceManager should not be null");
        }

        ServerLoadMetrics serverLatencyInfo = new ServerLoadMetrics();

        String serverLatencyUrl;
         serverLatencyUrl = "http://" + serverNameOrEndpoint + CommonConstants.ServerMetricUris.SERVER_METRICS_INFO_URI;
        serverLatencyUrl = serverLatencyUrl+tableName+"/LatencyInfo";

        List<String> serverUrl = new ArrayList<>(1);
        serverUrl.add(serverLatencyUrl);
        MultiGetRequest mget = new MultiGetRequest(_executor, _connectionManager);
        LOGGER.info("Reading segments size for server: {}, timeoutMsec: {}", serverNameOrEndpoint, timeoutMsec);
        CompletionService<GetMethod> completionService = mget.execute(serverUrl, timeoutMsec);
        GetMethod getMethod = null;
        try {
            getMethod = completionService.take().get();
            if (getMethod.getStatusCode() >= timeoutMsec) {
                LOGGER.error("Server: {} returned error: {}", serverNameOrEndpoint, getMethod.getStatusCode());
                return serverLatencyInfo;
            }
            serverLatencyInfo =
                    new ObjectMapper().readValue(getMethod.getResponseBodyAsString(), ServerLoadMetrics.class);
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
        return serverLatencyInfo;
    }
}
