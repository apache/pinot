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

import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.operator.transform.TransformUtils;
import com.linkedin.pinot.core.operator.transform.function.TransformFunctionFactory;
import com.linkedin.pinot.core.query.scheduler.QueryScheduler;
import com.linkedin.pinot.core.query.scheduler.QuerySchedulerFactory;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.transport.netty.NettyServer;
import com.linkedin.pinot.transport.netty.NettyTCPServer;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>ServerBuilder</code> class provides methods to build components for a server instance.
 */
public class ServerBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerBuilder.class);

  private final ServerConf _serverConf;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private ServerMetrics _serverMetrics;

  /**
   * Construct with ServerConf object.
   *
   * @param serverConf Server config
   */
  public ServerBuilder(ServerConf serverConf, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _serverConf = serverConf;
    _propertyStore = propertyStore;
    init();
  }

  private void init() {
    initMetrics();
    initTransformFunctions();
  }

  private void initMetrics() {
    MetricsHelper.initializeMetrics(_serverConf.getMetricsConfig());
    MetricsRegistry metricsRegistry = new MetricsRegistry();
    MetricsHelper.registerMetricsRegistry(metricsRegistry);
    _serverMetrics = new ServerMetrics(metricsRegistry, !_serverConf.emitTableLevelMetrics());
    _serverMetrics.initializeGlobalMeters();
  }

  private void initTransformFunctions() {
    TransformFunctionFactory.init(
        ArrayUtils.addAll(TransformUtils.getBuiltInTransform(), _serverConf.getTransformFunctions()));
  }

  public ServerMetrics getServerMetrics() {
    return _serverMetrics;
  }

  public InstanceDataManager buildInstanceDataManager()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    String className = _serverConf.getInstanceDataManagerClassName();
    LOGGER.info("Building instance data manager of class: {}", className);
    InstanceDataManager instanceDataManager = (InstanceDataManager) Class.forName(className).newInstance();
    instanceDataManager.init(_serverConf.getInstanceDataManagerConfig(), _propertyStore, _serverMetrics);
    return instanceDataManager;
  }

  public QueryExecutor buildQueryExecutor(InstanceDataManager instanceDataManager)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException, ConfigurationException {
    String className = _serverConf.getQueryExecutorClassName();
    LOGGER.info("Building query scheduler of class: {}", className);
    QueryExecutor queryExecutor = (QueryExecutor) Class.forName(className).newInstance();
    queryExecutor.init(_serverConf.getQueryExecutorConfig(), instanceDataManager, _serverMetrics);
    return queryExecutor;
  }

  public QueryScheduler buildQueryScheduler(QueryExecutor queryExecutor) {
    return QuerySchedulerFactory.create(_serverConf.getSchedulerConfig(), queryExecutor, _serverMetrics);
  }

  public NettyServer buildNettyServer(NettyServer.RequestHandlerFactory requestHandlerFactory)
      throws ConfigurationException {
    int nettyPort = _serverConf.getNettyConfig().getPort();
    LOGGER.info("Building netty TCP server with port: {}", nettyPort);
    return new NettyTCPServer(nettyPort, requestHandlerFactory, null);
  }
}
