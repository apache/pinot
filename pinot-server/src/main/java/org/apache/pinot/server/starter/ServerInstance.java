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

import java.util.concurrent.atomic.LongAccumulator;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.query.scheduler.QuerySchedulerFactory;
import org.apache.pinot.server.conf.ServerConf;
import org.apache.pinot.server.request.ScheduledRequestHandler;
import org.apache.pinot.transport.netty.NettyServer;
import org.apache.pinot.transport.netty.NettyServer.RequestHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A standalone server which will listen on a port and serve queries based on the given configuration. Cluster
 * management is maintained outside of this class.
 */
public class ServerInstance {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerInstance.class);

  private ServerConf _serverConf;
  private ServerMetrics _serverMetrics;
  private InstanceDataManager _instanceDataManager;
  private QueryExecutor _queryExecutor;
  private QueryScheduler _queryScheduler;
  private ScheduledRequestHandler _requestHandler;
  private NettyServer _nettyServer;
  private LongAccumulator _latestQueryTime;

  private boolean _started = false;

  public void init(@Nonnull ServerConf serverConf, @Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore)
      throws Exception {
    LOGGER.info("Initializing server instance");

    _serverConf = serverConf;
    ServerBuilder serverBuilder = new ServerBuilder(_serverConf, propertyStore);
    _serverMetrics = serverBuilder.getServerMetrics();
    _instanceDataManager = serverBuilder.buildInstanceDataManager();
    _queryExecutor = serverBuilder.buildQueryExecutor(_instanceDataManager);
    _latestQueryTime = new LongAccumulator(Long::max, 0);
    _queryScheduler = serverBuilder.buildQueryScheduler(_queryExecutor, _latestQueryTime);
    _requestHandler = new ScheduledRequestHandler(_queryScheduler, _serverMetrics);
    _nettyServer = serverBuilder.buildNettyServer(new RequestHandlerFactory() {
      @Override
      public NettyServer.RequestHandler createNewRequestHandler() {
        return _requestHandler;
      }
    });

    LOGGER.info("Finish initializing server instance");
  }

  public void start() {
    LOGGER.info("Starting server instance");
    if (_started) {
      LOGGER.info("Server instance is already started");
      return;
    }

    LOGGER.info("Starting instance data manager");
    _instanceDataManager.start();
    LOGGER.info("Starting query executor");
    _queryExecutor.start();
    LOGGER.info("Starting query scheduler");
    _queryScheduler.start();
    LOGGER.info("Starting netty server");
    new Thread(_nettyServer).start();

    _started = true;
    LOGGER.info("Finish starting server instance");
  }

  public void shutDown() {
    LOGGER.info("Shutting down server instance");
    if (!_started) {
      LOGGER.info("Server instance is not running");
      return;
    }

    LOGGER.info("Shutting down netty server");
    _nettyServer.shutdownGracefully();
    LOGGER.info("Shutting down query scheduler");
    _queryScheduler.stop();
    LOGGER.info("Shutting down query executor");
    _queryExecutor.shutDown();
    LOGGER.info("Shutting down instance data manager");
    _instanceDataManager.shutDown();

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

  public void resetQueryScheduler(String schedulerName) {
    Configuration schedulerConfig = _serverConf.getSchedulerConfig();
    schedulerConfig.setProperty(QuerySchedulerFactory.ALGORITHM_NAME_CONFIG_KEY, schedulerName);
    _queryScheduler = QuerySchedulerFactory.create(schedulerConfig, _queryExecutor, _serverMetrics, _latestQueryTime);
    _queryScheduler.start();
    _requestHandler.setScheduler(_queryScheduler);
  }
}
