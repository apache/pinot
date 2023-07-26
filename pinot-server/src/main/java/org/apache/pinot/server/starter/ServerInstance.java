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
import io.netty.channel.ChannelHandler;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.query.scheduler.QuerySchedulerFactory;
import org.apache.pinot.core.transport.ChannelHandlerFactory;
import org.apache.pinot.core.transport.InstanceRequestHandler;
import org.apache.pinot.core.transport.QueryServer;
import org.apache.pinot.core.transport.grpc.GrpcQueryServer;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.server.access.AccessControlFactory;
import org.apache.pinot.server.access.AllowAllAccessFactory;
import org.apache.pinot.server.conf.ServerConf;
import org.apache.pinot.server.worker.WorkerQueryServer;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.CommonConstants;
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
  private final AccessControl _accessControl;
  private final HelixManager _helixManager;

  private final WorkerQueryServer _workerQueryServer;
  private ChannelHandler _instanceRequestHandler;

  private boolean _dataManagerStarted = false;
  private boolean _queryServerStarted = false;

  public ServerInstance(ServerConf serverConf, HelixManager helixManager, AccessControlFactory accessControlFactory)
      throws Exception {
    LOGGER.info("Initializing server instance");
    _helixManager = helixManager;

    LOGGER.info("Initializing server metrics");
    PinotMetricsRegistry metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry(serverConf.getMetricsConfig());
    _serverMetrics =
        new ServerMetrics(serverConf.getMetricsPrefix(), metricsRegistry, serverConf.emitTableLevelMetrics(),
            serverConf.getAllowedTablesForEmittingMetrics());
    _serverMetrics.initializeGlobalMeters();
    _serverMetrics.setValueOfGlobalGauge(ServerGauge.VERSION, PinotVersion.VERSION_METRIC_NAME, 1);
    ServerMetrics.register(_serverMetrics);

    String instanceDataManagerClassName = serverConf.getInstanceDataManagerClassName();
    LOGGER.info("Initializing instance data manager of class: {}", instanceDataManagerClassName);
    _instanceDataManager = PluginManager.get().createInstance(instanceDataManagerClassName);
    _instanceDataManager.init(serverConf.getInstanceDataManagerConfig(), helixManager, _serverMetrics);

    // Initialize FunctionRegistry before starting the query executor
    FunctionRegistry.init();
    String queryExecutorClassName = serverConf.getQueryExecutorClassName();
    LOGGER.info("Initializing query executor of class: {}", queryExecutorClassName);
    _queryExecutor = PluginManager.get().createInstance(queryExecutorClassName);
    PinotConfiguration queryExecutorConfig = serverConf.getQueryExecutorConfig();
    _queryExecutor.init(queryExecutorConfig, _instanceDataManager, _serverMetrics);

    LOGGER.info("Initializing query scheduler");
    _latestQueryTime = new LongAccumulator(Long::max, 0);
    _queryScheduler =
        QuerySchedulerFactory.create(serverConf.getSchedulerConfig(), _queryExecutor, _serverMetrics, _latestQueryTime);

    TlsConfig tlsConfig =
        TlsUtils.extractTlsConfig(serverConf.getPinotConfig(), CommonConstants.Server.SERVER_TLS_PREFIX);
    NettyConfig nettyConfig =
        NettyConfig.extractNettyConfig(serverConf.getPinotConfig(), CommonConstants.Server.SERVER_NETTY_PREFIX);
    accessControlFactory
        .init(serverConf.getPinotConfig().subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_ACCESS_CONTROL),
            helixManager);
    _accessControl = accessControlFactory.create();

    if (serverConf.isMultiStageServerEnabled()) {
      LOGGER.info("Initializing Multi-stage query engine");
      _workerQueryServer = new WorkerQueryServer(serverConf.getPinotConfig(), _instanceDataManager, helixManager,
          _serverMetrics);
    } else {
      _workerQueryServer = null;
    }

    if (serverConf.isNettyServerEnabled()) {
      int nettyPort = serverConf.getNettyPort();
      LOGGER.info("Initializing Netty query server on port: {}", nettyPort);
      _instanceRequestHandler = ChannelHandlerFactory
          .getInstanceRequestHandler(helixManager.getInstanceName(), serverConf.getPinotConfig(), _queryScheduler,
              _serverMetrics, new AllowAllAccessFactory().create());
      _nettyQueryServer = new QueryServer(nettyPort, nettyConfig, _instanceRequestHandler);
    } else {
      _nettyQueryServer = null;
    }

    if (serverConf.isNettyTlsServerEnabled()) {
      int nettySecPort = serverConf.getNettyTlsPort();
      LOGGER.info("Initializing TLS-secured Netty query server on port: {}", nettySecPort);
      _instanceRequestHandler = ChannelHandlerFactory
          .getInstanceRequestHandler(helixManager.getInstanceName(), serverConf.getPinotConfig(), _queryScheduler,
              _serverMetrics, _accessControl);
      _nettyTlsQueryServer = new QueryServer(nettySecPort, nettyConfig, tlsConfig, _instanceRequestHandler);
    } else {
      _nettyTlsQueryServer = null;
    }
    if (serverConf.isEnableGrpcServer()) {
      int grpcPort = serverConf.getGrpcPort();
      LOGGER.info("Initializing gRPC query server on port: {}", grpcPort);
      _grpcQueryServer = new GrpcQueryServer(grpcPort, GrpcConfig.buildGrpcQueryConfig(serverConf.getPinotConfig()),
          serverConf.isGrpcTlsServerEnabled() ? TlsUtils
              .extractTlsConfig(serverConf.getPinotConfig(), CommonConstants.Server.SERVER_GRPCTLS_PREFIX) : null,
          _queryExecutor, _serverMetrics, _accessControl);
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

  public synchronized void startDataManager() {
    // This method is called when Helix starts a new ZK session, and can be called multiple times. We only need to start
    // the data manager once, and simply ignore the following invocations.
    if (_dataManagerStarted) {
      LOGGER.info("Data manager is already running, skipping the start");
      return;
    }

    LOGGER.info("Starting data manager");
    _instanceDataManager.start();
    _dataManagerStarted = true;
    LOGGER.info("Finish starting data manager");
  }

  public synchronized void startQueryServer() {
    if (_queryServerStarted) {
      LOGGER.warn("Query server is already running, skipping the start");
      return;
    }

    LOGGER.info("Starting query server");

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
    if (_workerQueryServer != null) {
      LOGGER.info("Starting worker query server");
      _workerQueryServer.start();
    }

    _queryServerStarted = true;
    LOGGER.info("Finish starting query server");
  }

  public synchronized void shutDown() {
    if (!_dataManagerStarted) {
      LOGGER.warn("Server instance is not running, skipping the shut down");
      return;
    }

    LOGGER.info("Shutting down server instance");

    if (_queryServerStarted) {
      LOGGER.info("Shutting down query server");

      if (_nettyQueryServer != null) {
        LOGGER.info("Shutting down Netty query server");
        _nettyQueryServer.shutDown();
      }
      if (_nettyTlsQueryServer != null) {
        LOGGER.info("Shutting down TLS-secured Netty query server");
        _nettyTlsQueryServer.shutDown();
      }
      if (_grpcQueryServer != null) {
        LOGGER.info("Shutting down gRPC query server");
        _grpcQueryServer.shutdown();
      }
      if (_workerQueryServer != null) {
        LOGGER.info("Shutting down worker query server");
        _workerQueryServer.shutDown();
      }
      LOGGER.info("Shutting down query scheduler");
      _queryScheduler.stop();
      LOGGER.info("Shutting down query executor");
      _queryExecutor.shutDown();

      _queryServerStarted = false;
      LOGGER.info("Finish shutting down query server");
    }

    LOGGER.info("Shutting down data manager");
    _instanceDataManager.shutDown();
    LOGGER.info("Shutting down metrics registry");
    _serverMetrics.getMetricsRegistry().shutdown();

    _dataManagerStarted = false;
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

  public InstanceRequestHandler getInstanceRequestHandler() {
    Preconditions.checkState(_instanceRequestHandler instanceof InstanceRequestHandler,
        "Unexpected type of instance request handler");
    return (InstanceRequestHandler) _instanceRequestHandler;
  }

  public HelixManager getHelixManager() {
    return _helixManager;
  }
}
