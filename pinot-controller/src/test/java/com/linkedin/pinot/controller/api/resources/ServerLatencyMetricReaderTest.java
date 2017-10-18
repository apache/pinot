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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.restlet.resources.ServerLatencyInfo;
import com.linkedin.pinot.common.restlet.resources.ServerPerfMetrics;
import com.linkedin.pinot.common.restlet.resources.ServerSegmentInfo;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.util.ServerLatencyMetricReader;
import com.linkedin.pinot.controller.util.ServerPerfMetricsReader;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ServerLatencyMetricReaderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerLatencyMetricReaderTest.class);
    final int _timeoutMsec = 5000;
    final int _serverCount = 3;
    private final ExecutorService _executor = Executors.newFixedThreadPool(3);
    private final HttpConnectionManager _httpConnectionManager = new MultiThreadedHttpConnectionManager();
    private final int _serverPortStart = 10000;
    private final List<HttpServer> _servers = new ArrayList<>();
    private final List<String> _serverList = new ArrayList<>();
    private final List<String> _tableList = new ArrayList<>();
    private final List<String> _endpointList = new ArrayList<>();
    private List<Double> _server1latencies;
    private List<Double> _server2Latencies;
    private List<Double> _server3Latencies;
    private ServerLatencyInfo _server1LatencyInfo;
    private ServerLatencyInfo _server2LatencyInfo;
    private ServerLatencyInfo _server3LatencyInfo;

    @BeforeClass
    public void setUp() throws IOException {
        for (int i = 0; i < _serverCount; i++) {
            _serverList.add("server_" + i);
            _tableList.add("table_" + i);
            _endpointList.add("localhost:" + (_serverPortStart + i));
        }
        _server1latencies = Arrays.asList(1.0, 3.0, 5.0);
        _server2Latencies = Arrays.asList(2.0, 3.0);
        _server3Latencies = Arrays.asList();
        _server1LatencyInfo = createSegmentsInfo(_serverList.get(0), _tableList.get(0), _server1latencies);
        _server2LatencyInfo = createSegmentsInfo(_serverList.get(1), _tableList.get(1), _server2Latencies);
        _server3LatencyInfo = createSegmentsInfo(_serverList.get(2), _tableList.get(2), _server3Latencies);
        _servers.add(startServer(_serverPortStart,_tableList.get(0), createHandler(200, _server1LatencyInfo, 0)));
        _servers.add(startServer(_serverPortStart + 1,_tableList.get(0), createHandler(200, _server2LatencyInfo, 0)));
        _servers.add(startServer(_serverPortStart + 2,_tableList.get(0), createHandler(200, _server3LatencyInfo, 0)));
    }

    @AfterClass
    public void tearDown() {
        for (HttpServer server : _servers) {
            if (server != null) {
                server.stop(0);
            }
        }
    }

    private ServerLatencyInfo createSegmentsInfo(String serverNameOrEndpoint, String tableName, List<Double> latencies) {
        ServerLatencyInfo serverLatencyInfo = new ServerLatencyInfo();
        serverLatencyInfo.set_serverName(serverNameOrEndpoint);
        serverLatencyInfo.set_tableName(tableName);
        serverLatencyInfo.set_segmentLatencyInSecs(latencies);
        return serverLatencyInfo;
    }

    private long segmentIndexToSize(int index) {
        return 100 + index * 100;
    }

    private HttpHandler createHandler(final int status, final ServerLatencyInfo serverLatencyInfo,
                                      final int sleepTimeMs) {
        return new HttpHandler() {
            @Override
            public void handle(HttpExchange httpExchange) throws IOException {
                if (sleepTimeMs > 0) {
                    try {
                        Thread.sleep(sleepTimeMs);
                    } catch (InterruptedException e) {
                        LOGGER.info("Handler interrupted during sleep");
                    }
                }
                String json = new ObjectMapper().writeValueAsString(serverLatencyInfo);
                httpExchange.sendResponseHeaders(status, json.length());
                OutputStream responseBody = httpExchange.getResponseBody();
                responseBody.write(json.getBytes());
                responseBody.close();
            }
        };
    }

    private HttpServer startServer(int port,String table, HttpHandler handler) throws IOException {
        final HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext(CommonConstants.Helix.ServerMetricUris.SERVER_METRICS_INFO_URI+table+"/LatencyInfo", handler);
        new Thread(new Runnable() {
            @Override
            public void run() {
                server.start();
            }
        }).start();
        return server;
    }

    @Test
    public void testServerLatencyMetricsReader() {
        ServerLatencyMetricReader  serverLatencyMetricReader =
                new ServerLatencyMetricReader(_executor, _httpConnectionManager, null);
        ServerLatencyInfo serverLatencyInfo;
        //Check metric for server1
        serverLatencyInfo = serverLatencyMetricReader.getServerLatencyMetrics(_endpointList.get(0),_tableList.get(0), false, _timeoutMsec);
        Assert.assertEquals(serverLatencyInfo.get_serverName(), _server1LatencyInfo.get_serverName());
        Assert.assertEquals(serverLatencyInfo.get_tableName(),
                _server1LatencyInfo.get_tableName());
        Assert.assertEquals(serverLatencyInfo.get_segmentLatencyInSecs(),
                _server1LatencyInfo.get_segmentLatencyInSecs());
        //Check metrics for server2
//        serverLatencyInfo = serverLatencyMetricReader.getServerLatencyMetrics(_endpointList.get(1),_tableList.get(1), false, _timeoutMsec);
//        Assert.assertEquals(serverLatencyInfo.get_serverName(), _server2LatencyInfo.get_serverName());
//        Assert.assertEquals(serverLatencyInfo.get_tableName(),
//                _server2LatencyInfo.get_tableName());
//        Assert.assertEquals(serverLatencyInfo.get_segmentLatencyInSecs(),
//                _server2LatencyInfo.get_segmentLatencyInSecs());
//        //Check metrics for server3
//        serverLatencyInfo = serverLatencyMetricReader.getServerLatencyMetrics(_endpointList.get(1),_tableList.get(2), false, _timeoutMsec);
//        Assert.assertEquals(serverLatencyInfo.get_serverName(), _server3LatencyInfo.get_serverName());
//        Assert.assertEquals(serverLatencyInfo.get_tableName(),
//                _server3LatencyInfo.get_tableName());
//        Assert.assertEquals(serverLatencyInfo.get_segmentLatencyInSecs(),
//                _server3LatencyInfo.get_segmentLatencyInSecs());
    }

    @Test
    public void testServerSizesErrors() {
        ServerLatencyMetricReader serverLatencyMetricReader =
                new ServerLatencyMetricReader(_executor, _httpConnectionManager, null);
        ServerLatencyInfo serverLatencyInfo;
        serverLatencyInfo = serverLatencyMetricReader.getServerLatencyMetrics("FakeEndPoint","hell",false, _timeoutMsec);
        Assert.assertEquals(serverLatencyInfo.get_tableName(), null);
    }
}
