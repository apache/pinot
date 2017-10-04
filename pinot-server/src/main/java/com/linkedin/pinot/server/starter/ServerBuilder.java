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
package com.linkedin.pinot.server.starter;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
import com.linkedin.pinot.core.operator.transform.TransformUtils;
import com.linkedin.pinot.core.operator.transform.function.TransformFunctionFactory;
import com.linkedin.pinot.core.query.scheduler.QueryScheduler;
import com.linkedin.pinot.core.query.scheduler.QuerySchedulerFactory;
import com.linkedin.pinot.server.conf.NettyServerConfig;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.transport.netty.NettyServer;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;
import com.linkedin.pinot.transport.netty.NettyTCPServer;
import com.yammer.metrics.core.MetricsRegistry;
import java.lang.reflect.InvocationTargetException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Initialize a ServerBuilder with serverConf file.
 */
public class ServerBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerBuilder.class);

  private final ServerConf _serverConf;

  private ServerMetrics _serverMetrics;

  private MetricsRegistry metricsRegistry;

  public ServerConf getConfiguration() {
    return _serverConf;
  }

  /**
   * Initialize with ServerConf object
   * @param serverConf object
   * @param metricsRegistry
   */
  public ServerBuilder(ServerConf serverConf, MetricsRegistry metricsRegistry) {
    _serverConf = serverConf;
    this.metricsRegistry = metricsRegistry;
    initMetrics();
  }

  public DataManager buildInstanceDataManager() throws InstantiationException, IllegalAccessException,
      ClassNotFoundException {
    String className = _serverConf.getInstanceDataManagerClassName();
    LOGGER.info("Trying to Load Instance DataManager by Class : " + className);
    DataManager instanceDataManager = (DataManager) Class.forName(className).newInstance();
    instanceDataManager.init(_serverConf.getInstanceDataManagerConfig());
    return instanceDataManager;
  }

  public QueryExecutor buildQueryExecutor(DataManager instanceDataManager) throws InstantiationException,
      IllegalAccessException, ClassNotFoundException, ConfigurationException {
    String className = _serverConf.getQueryExecutorClassName();
    LOGGER.info("Trying to Load Query Executor by Class : " + className);
    QueryExecutor queryExecutor = (QueryExecutor) Class.forName(className).newInstance();
    queryExecutor.init(_serverConf.getQueryExecutorConfig(), instanceDataManager, _serverMetrics);
    return queryExecutor;
  }

  public QueryScheduler buildQueryScheduler(QueryExecutor queryExecutor)
      throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
             InstantiationException {
    Configuration schedulerConfig = _serverConf.getSchedulerConfig();
    return QuerySchedulerFactory.create(schedulerConfig, queryExecutor, _serverMetrics);
  }


  /**
   * This method initializes the transform factory containing built-in functions
   * as well as functions specified in the server configuration.
   *
   * @param serverConf Server configuration
   */
  public static void init(ServerConf serverConf) {
    TransformFunctionFactory.init(
        ArrayUtils.addAll(TransformUtils.getBuiltInTransform(), serverConf.getTransformFunctions()));
  }

  public NettyServer buildNettyServer(NettyServerConfig nettyServerConfig, RequestHandlerFactory requestHandlerFactory) {
    LOGGER.info("Trying to build NettyTCPServer with port : " + nettyServerConfig.getPort());
    NettyServer nettyServer = new NettyTCPServer(nettyServerConfig.getPort(), requestHandlerFactory, null);
    return nettyServer;
  }

  private void initMetrics() {
    MetricsHelper.initializeMetrics(_serverConf.getMetricsConfig());
    MetricsHelper.registerMetricsRegistry(metricsRegistry);
    _serverMetrics = new ServerMetrics(metricsRegistry, !_serverConf.emitTableLevelMetrics());
    _serverMetrics.initializeGlobalMeters();

    TableDataManagerProvider.setServerMetrics(_serverMetrics);
  }

  public ServerMetrics getServerMetrics() {
    return _serverMetrics;
  }
}
