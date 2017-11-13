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
    private HelixAdmin _helixAdmin;

    public ServerLatencyMetricReader(Executor executor, HttpConnectionManager connectionManager,
                                     HelixAdmin helixAdmin) {
        _executor = executor;
        _connectionManager = connectionManager;
        _helixAdmin = helixAdmin;
    }

    public @Nullable
    ServerLoadMetrics getServerLatencyMetrics(@Nonnull String serverNameOrEndpoint, @Nonnull String tableName, boolean isThisServerName,
                                                      @Nonnegative int timeoutMsec) {
        Preconditions.checkNotNull(serverNameOrEndpoint, "Server Name Or Endpoint name should not be null");
        Preconditions.checkArgument(timeoutMsec > 0, "Timeout value must be greater than 0");
        if (isThisServerName) {
            Preconditions.checkNotNull(_helixAdmin, "_helixAdmin should not be null");
        }

        ServerLoadMetrics serverLatencyInfo = new ServerLoadMetrics();

        String serverLatencyUrl;
         serverLatencyUrl = "http://" + serverNameOrEndpoint + CommonConstants.Helix.ServerMetricUris.SERVER_METRICS_INFO_URI;
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

    public static void writeToFile(String inputDirLoc) {
        String outputLoc = "target/training";
        String expectedOutputFileName = "";
        File inputDir = new File(inputDirLoc);
        File outputDir = new File(outputLoc);

        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                throw new RuntimeException("Failed to create output directory: " + outputDir);
            }
        }

        String[] inputFileNames = inputDir.list();
        assert inputFileNames != null;
        BufferedWriter writer = null;
        for (String inputFileName : inputFileNames) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(new File(inputDir, inputFileName)));
                writer = new BufferedWriter(new FileWriter(new File(outputDir, inputFileName)));
                assert writer != null;
                String line;
                while ((line = reader.readLine()) != null) {
                    writer.write(line);
                    writer.newLine();
                }
                reader.close();
                if (writer != null) {
                    writer.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
    public static void writeJSONToFile(ServerLoadMetrics loadMetrics){
        ObjectMapper mapper = new ObjectMapper();
        try {
            File file = new File("target/loadmetrics.json");
            mapper.writeValue(file, loadMetrics);
            System.out.println(file.getAbsolutePath());
            //Convert object to JSON string
            String jsonInString = mapper.writeValueAsString(loadMetrics);
            System.out.println(jsonInString);
            //Convert object to JSON string and pretty print
            jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(loadMetrics);
            System.out.println(jsonInString);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void main(String args[]){
        ServerLoadMetrics metrics = new ServerLoadMetrics();
        ServerLatencyMetric metric = new ServerLatencyMetric(300,20.0,40.0);
        ServerLatencyMetric metric1 = new ServerLatencyMetric(300,20.0,40.0);
        ServerLatencyMetric metric2 = new ServerLatencyMetric(300,20.0,40.0);
        ServerLatencyMetric metric3 = new ServerLatencyMetric(300,20.0,40.0);
        List list = new ArrayList();
        list.add(metric);
        list.add(metric1);
        list.add(metric2);
        list.add(metric3);

        metrics.set_latencies(list);
        writeToFile("target/workloadData/");
        //writeJSONToFile(metrics);
    }
}
