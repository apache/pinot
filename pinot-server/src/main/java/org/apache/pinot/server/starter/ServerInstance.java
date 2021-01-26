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
package org.apache.pinot.server.starter;

import com.google.common.base.Preconditions;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.metrics.MetricsHelper;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.query.scheduler.QuerySchedulerFactory;
import org.apache.pinot.core.transport.QueryServer;
import org.apache.pinot.core.transport.TlsConfig;
import org.apache.pinot.core.transport.grpc.GrpcQueryServer;
import org.apache.pinot.core.util.TlsUtils;
import org.apache.pinot.server.conf.ServerConf;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A standalone server which will listen on a port and serve queries based on the given configuration. Cluster
 * management is maintained outside of this class.
 */
public class ServerInstance {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerInstance.class);

  private final ServerMetrics _serverMetrics;
  private final InstanceDataManager _instanceDataManager;
  private final QueryExecutor _queryExecutor;
  private final LongAccumulator _latestQueryTime;
  private final QueryScheduler _queryScheduler;
  private final QueryServer _nettyQueryServer;
  private final QueryServer _nettyTlsQueryServer;
  private final GrpcQueryServer _grpcQueryServer;

  private boolean _started = false;

  public ServerInstance(ServerConf serverConf, HelixManager helixManager)
      throws Exception {
    LOGGER.info("Initializing server instance");

    LOGGER.info("Initializing server metrics");
    MetricsHelper.initializeMetrics(serverConf.getMetricsConfig());
    MetricsRegistry metricsRegistry = new MetricsRegistry();
    MetricsHelper.registerMetricsRegistry(metricsRegistry);
    _serverMetrics =
        new ServerMetrics(serverConf.getMetricsPrefix(), metricsRegistry, serverConf.emitTableLevelMetrics(),
            serverConf.getAllowedTablesForEmittingMetrics());
    _serverMetrics.initializeGlobalMeters();

    String instanceDataManagerClassName = serverConf.getInstanceDataManagerClassName();
    LOGGER.info("Initializing instance data manager of class: {}", instanceDataManagerClassName);
    _instanceDataManager = (InstanceDataManager) Class.forName(instanceDataManagerClassName).newInstance();
    _instanceDataManager.init(serverConf.getInstanceDataManagerConfig(), helixManager, _serverMetrics);

    // Initialize FunctionRegistry before starting the query executor
    FunctionRegistry.init();
    String queryExecutorClassName = serverConf.getQueryExecutorClassName();
    LOGGER.info("Initializing query executor of class: {}", queryExecutorClassName);
    _queryExecutor = (QueryExecutor) Class.forName(queryExecutorClassName).newInstance();
    PinotConfiguration queryExecutorConfig = serverConf.getQueryExecutorConfig();
    _queryExecutor.init(queryExecutorConfig, _instanceDataManager, _serverMetrics);

    LOGGER.info("Initializing query scheduler");
    _latestQueryTime = new LongAccumulator(Long::max, 0);
    _queryScheduler =
        QuerySchedulerFactory.create(serverConf.getSchedulerConfig(), _queryExecutor, _serverMetrics, _latestQueryTime);

    TlsConfig tlsConfig = TlsUtils.extractTlsConfig(serverConf.getPinotConfig(), CommonConstants.Server.SERVER_TLS_PREFIX);

    if (serverConf.isNettyServerEnabled()) {
      int nettyPort = serverConf.getNettyPort();
      LOGGER.info("Initializing Netty query server on port: {}", nettyPort);
      _nettyQueryServer = new QueryServer(nettyPort, _queryScheduler, _serverMetrics);
    } else {
      _nettyQueryServer = null;
    }

    if (serverConf.isNettyTlsServerEnabled()) {
      int nettySecPort = serverConf.getNettyTlsPort();
      LOGGER.info("Initializing TLS-secured Netty query server on port: {}", nettySecPort);
      _nettyTlsQueryServer = new QueryServer(nettySecPort, _queryScheduler, _serverMetrics, tlsConfig);
    } else {
      _nettyTlsQueryServer = null;
    }

    if (serverConf.isEnableGrpcServer()) {
      if (tlsConfig.isCustomized()) {
        LOGGER.warn("gRPC query server does not support TLS yet");
      }

      int grpcPort = serverConf.getGrpcPort();
      LOGGER.info("Initializing gRPC query server on port: {}", grpcPort);
      _grpcQueryServer = new GrpcQueryServer(grpcPort, _queryExecutor, _serverMetrics);
    } else {
      _grpcQueryServer = null;
    }

    LOGGER.info("Initializing transform functions");
    Set<Class<TransformFunction>> transformFunctionClasses = new HashSet<>();
    for (String transformFunctionClassName : serverConf.getTransformFunctions()) {
      try {
        //noinspection unchecked
        transformFunctionClasses.add((Class<TransformFunction>) Class.forName(transformFunctionClassName));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Failed to find transform function class: " + transformFunctionClassName);
      }
    }
    TransformFunctionFactory.init(transformFunctionClasses);

    LOGGER.info("Finish initializing server instance");
  }

  public synchronized void start() {
    // This method is called when Helix starts a new ZK session, and can be called multiple times. We only need to start
    // the server instance once, and simply ignore the following invocations.
    if (_started) {
      LOGGER.info("Server instance is already running, skipping the start");
      return;
    }

    LOGGER.info("Starting server instance");

    LOGGER.info("Starting instance data manager");
    _instanceDataManager.start();
    LOGGER.info("Starting query executor");
    _queryExecutor.start();
    LOGGER.info("Starting query scheduler");
    _queryScheduler.start();
    if (_nettyQueryServer != null) {
      LOGGER.info("Starting Netty query server");
      _nettyQueryServer.start();
    }
    if (_nettyTlsQueryServer != null) {
      LOGGER.info("Starting TLS-secured Netty query server");
      _nettyTlsQueryServer.start();
    }
    if (_grpcQueryServer != null) {
      LOGGER.info("Starting gRPC query server");
      _grpcQueryServer.start();
    }

    _started = true;
    LOGGER.info("Finish starting server instance");
  }

  public synchronized void shutDown() {
    Preconditions.checkState(_started, "Server instance is not running");
    LOGGER.info("Shutting down server instance");

    if (_nettyTlsQueryServer != null) {
      LOGGER.info("Shutting down TLS-secured Netty query server");
      _nettyTlsQueryServer.shutDown();
    }
    if (_grpcQueryServer != null) {
      LOGGER.info("Shutting down gRPC query server");
      _grpcQueryServer.shutdown();
    }
    if (_nettyQueryServer != null) {
      LOGGER.info("Shutting down Netty query server");
      _nettyQueryServer.shutDown();
    }
    LOGGER.info("Shutting down query scheduler");
    _queryScheduler.stop();
    LOGGER.info("Shutting down query executor");
    _queryExecutor.shutDown();
    LOGGER.info("Shutting down instance data manager");
    _instanceDataManager.shutDown();
    LOGGER.info("Shutting down metrics registry");
    _serverMetrics.getMetricsRegistry().shutdown();
    _started = false;
    LOGGER.info("Finish shutting down server instance");
  }

  public ServerMetrics getServerMetrics() {
    return _serverMetrics;
  }

  public InstanceDataManager getInstanceDataManager() {
    return _instanceDataManager;
  }

  public long getLatestQueryTime() {
    return _latestQueryTime.get();
  }
}
