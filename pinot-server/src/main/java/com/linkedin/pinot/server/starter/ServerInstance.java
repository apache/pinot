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

import com.linkedin.pinot.core.query.scheduler.QueryScheduler;
import com.yammer.metrics.core.MetricsRegistry;
import java.lang.reflect.InvocationTargetException;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.transport.netty.NettyServer;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;


/**
*
* A Standalone Server which will run on the port configured in the properties file.
* and accept queries. All configurations needed to run the server is provided
* in the config file passed. No external cluster manager integration available (yet)
*
*/
public class ServerInstance {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerInstance.class);

  private ServerConf _serverConf;
  private DataManager _instanceDataManager;
  private QueryExecutor _queryExecutor;
  private RequestHandlerFactory _requestHandlerFactory;
  private NettyServer _nettyServer;
  private ServerMetrics _serverMetrics;
  private QueryScheduler _queryScheduler;
  private Thread _serverThread;

  private boolean _istarted = false;

  public ServerInstance() {

  }

  /**
   * Initialize ServerInstance with ServerConf
   * @param serverConf
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws ClassNotFoundException
   * @throws ConfigurationException
   */
  public void init(ServerConf serverConf, MetricsRegistry metricsRegistry)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException, ConfigurationException,
             NoSuchMethodException, InvocationTargetException {
    _serverConf = serverConf;
    LOGGER.info("Trying to build server config");
    ServerBuilder serverBuilder = new ServerBuilder(_serverConf, metricsRegistry);
    LOGGER.info("Trying to build InstanceDataManager");
    _instanceDataManager = serverBuilder.buildInstanceDataManager();
    LOGGER.info("Trying to build QueryExecutor");
    _queryExecutor = serverBuilder.buildQueryExecutor(_instanceDataManager);
    _queryScheduler = serverBuilder.buildQueryScheduler(_queryExecutor);

    LOGGER.info("Trying to build RequestHandlerFactory");
    setRequestHandlerFactory(serverBuilder.buildRequestHandlerFactory(_queryScheduler));
    LOGGER.info("Trying to build NettyServer");
    _nettyServer = serverBuilder.buildNettyServer(_serverConf.getNettyConfig(), _requestHandlerFactory);
    setServerThread(new Thread(_nettyServer));
    LOGGER.info("ServerInstance Initialization is Done!");
    _serverMetrics = serverBuilder.getServerMetrics();
  }

  /**
   * Start ServerInstance
   */
  public void start() {
    LOGGER.info("Trying to start InstanceDataManager");
    _instanceDataManager.start();
    LOGGER.info("Trying to start QueryExecutor");
    _queryExecutor.start();
    LOGGER.info("Trying to start ServerThread");
    _serverThread.start();
    _istarted = true;
    LOGGER.info("ServerInstance is Started!");
  }

  /**
   * ShutDown ServerInstance
   */
  public void shutDown() {
    if (isStarted()) {
      _queryExecutor.shutDown();
      _instanceDataManager.shutDown();
      _nettyServer.shutdownGracefully();
      _istarted = false;
      LOGGER.info("ServerInstance is ShutDown Completely!");
    } else {
      LOGGER.info("ServerInstance is already ShutDown! Won't do anything!");
    }
  }

  /**
   * @return instanceDataManager
   */
  public DataManager getInstanceDataManager() {
    return _instanceDataManager;
  }

  /**
   * @param instanceDataManager
   */
  public void setInstanceDataManager(DataManager instanceDataManager) {
    this._instanceDataManager = instanceDataManager;
  }

  /**
   * @return queryExecutor
   */
  public QueryExecutor getQueryExecutor() {
    return _queryExecutor;
  }

  /**
   * @param queryExecutor
   */
  public void setQueryExecutor(QueryExecutor queryExecutor) {
    this._queryExecutor = queryExecutor;
  }

  /**
   * @return requestHandlerFactory
   */
  public RequestHandlerFactory getRequestHandlerFactory() {
    return _requestHandlerFactory;
  }

  /**
   * @param requestHandlerFactory
   */
  public void setRequestHandlerFactory(RequestHandlerFactory requestHandlerFactory) {
    this._requestHandlerFactory = requestHandlerFactory;
  }

  public boolean isStarted() {
    return _istarted;
  }

  /**
   * @return serverThread
   */
  public Thread getServerThread() {
    return _serverThread;
  }

  /**
   * @param serverThread
   */
  public void setServerThread(Thread serverThread) {
    this._serverThread = serverThread;
  }

  /**
   * @return nettyServer
   */
  public NettyServer getNettyServer() {
    return _nettyServer;
  }

  /**
   * @param nettyServer
   */
  public void setNettyServer(NettyServer nettyServer) {
    this._nettyServer = nettyServer;
  }

  public ServerMetrics getServerMetrics() {
    return _serverMetrics;
  }
}
