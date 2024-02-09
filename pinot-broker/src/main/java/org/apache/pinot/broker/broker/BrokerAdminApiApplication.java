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
package org.apache.pinot.broker.broker;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.swagger.jaxrs.config.BeanConfig;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.helix.HelixManager;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.pinot.broker.api.resources.PinotBrokerHealthCheck;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.log.DummyLogFileServer;
import org.apache.pinot.common.utils.log.LocalLogFileServer;
import org.apache.pinot.common.utils.log.LogFileServer;
import org.apache.pinot.core.api.ServiceAutoDiscoveryFeature;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BrokerAdminApiApplication extends ResourceConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerAdminApiApplication.class);
  public static final String PINOT_CONFIGURATION = "pinotConfiguration";
  public static final String BROKER_INSTANCE_ID = "brokerInstanceId";

  private final String _brokerResourcePackages;
  private final boolean _useHttps;
  private final boolean _swaggerBrokerEnabled;
  private final ExecutorService _executorService;

  private HttpServer _httpServer;

  public BrokerAdminApiApplication(BrokerRoutingManager routingManager, BrokerRequestHandler brokerRequestHandler,
      BrokerMetrics brokerMetrics, PinotConfiguration brokerConf, SqlQueryExecutor sqlQueryExecutor,
      ServerRoutingStatsManager serverRoutingStatsManager, AccessControlFactory accessFactory,
      HelixManager helixManager, String instanceId) {
    _brokerResourcePackages = brokerConf.getProperty(CommonConstants.Broker.BROKER_RESOURCE_PACKAGES,
        CommonConstants.Broker.DEFAULT_BROKER_RESOURCE_PACKAGES);
    String[] pkgs = _brokerResourcePackages.split(",");
    packages(pkgs);
    property(PINOT_CONFIGURATION, brokerConf);
    _useHttps = Boolean.parseBoolean(brokerConf.getProperty(CommonConstants.Broker.CONFIG_OF_SWAGGER_USE_HTTPS));
    _swaggerBrokerEnabled = brokerConf.getProperty(CommonConstants.Broker.CONFIG_OF_SWAGGER_BROKER_ENABLED,
        CommonConstants.Broker.DEFAULT_SWAGGER_BROKER_ENABLED);
    if (brokerConf.getProperty(CommonConstants.Broker.BROKER_SERVICE_AUTO_DISCOVERY, false)) {
      register(ServiceAutoDiscoveryFeature.class);
    }
    _executorService =
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("async-task-thread-%d").build());
    PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager();
    int timeoutMs = (int) brokerConf
        .getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_TIMEOUT_MS,
            CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS);
    connMgr.setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(timeoutMs).build());
    PinotBrokerHealthCheck pinotBrokerHealthCheck =
        new PinotBrokerHealthCheck(Instant.now(), instanceId, brokerMetrics);
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(connMgr).to(HttpClientConnectionManager.class);
        bind(_executorService).to(Executor.class);
        bind(helixManager).to(HelixManager.class);
        bind(sqlQueryExecutor).to(SqlQueryExecutor.class);
        bind(routingManager).to(BrokerRoutingManager.class);
        bind(brokerRequestHandler).to(BrokerRequestHandler.class);
        bind(brokerMetrics).to(BrokerMetrics.class);
        String loggerRootDir = brokerConf.getProperty(CommonConstants.Broker.CONFIG_OF_LOGGER_ROOT_DIR);
        if (loggerRootDir != null) {
          bind(new LocalLogFileServer(loggerRootDir)).to(LogFileServer.class);
        } else {
          bind(new DummyLogFileServer()).to(LogFileServer.class);
        }
        bind(brokerConf.getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ID)).named(BROKER_INSTANCE_ID);
        bind(serverRoutingStatsManager).to(ServerRoutingStatsManager.class);
        bind(accessFactory).to(AccessControlFactory.class);
        bind(instanceId).named(BrokerAdminApiApplication.BROKER_INSTANCE_ID);
        bind(pinotBrokerHealthCheck).to(PinotBrokerHealthCheck.class);
      }
    });
    boolean enableBoundedJerseyThreadPoolExecutor = brokerConf
        .getProperty(CommonConstants.Broker.CONFIG_OF_ENABLE_BOUNDED_JERSEY_THREADPOOL_EXECUTOR,
            CommonConstants.Broker.DEFAULT_ENABLE_BOUNDED_JERSEY_THREADPOOL_EXECUTOR);
    if (enableBoundedJerseyThreadPoolExecutor) {
      register(buildBrokerManagedAsyncExecutorProvider(brokerConf, brokerMetrics));
    }
    register(JacksonFeature.class);
    registerClasses(io.swagger.jaxrs.listing.ApiListingResource.class);
    registerClasses(io.swagger.jaxrs.listing.SwaggerSerializers.class);
    register(AuthenticationFilter.class);
  }

  public void start(List<ListenerConfig> listenerConfigs) {
    _httpServer = ListenerConfigUtil.buildHttpServer(this, listenerConfigs);

    try {
      _httpServer.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start http server", e);
    }

    if (_swaggerBrokerEnabled) {
      PinotReflectionUtils.runWithLock(this::setupSwagger);
    } else {
      LOGGER.info("Hiding Swagger UI for Broker, by {}", CommonConstants.Broker.CONFIG_OF_SWAGGER_BROKER_ENABLED);
    }
  }

  private void setupSwagger() {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle("Pinot Broker API");
    beanConfig.setDescription("APIs for accessing Pinot broker information");
    beanConfig.setContact("https://github.com/apache/pinot");
    beanConfig.setVersion("1.0");
    beanConfig.setExpandSuperTypes(false);
    if (_useHttps) {
      beanConfig.setSchemes(new String[]{CommonConstants.HTTPS_PROTOCOL});
    } else {
      beanConfig.setSchemes(new String[]{CommonConstants.HTTP_PROTOCOL, CommonConstants.HTTPS_PROTOCOL});
    }
    beanConfig.setBasePath("/");
    beanConfig.setResourcePackage(_brokerResourcePackages);
    beanConfig.setScan(true);

    HttpHandler httpHandler = new CLStaticHttpHandler(BrokerAdminApiApplication.class.getClassLoader(), "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    _httpServer.getServerConfiguration().addHttpHandler(httpHandler, "/api/", "/help/");

    URL swaggerDistLocation =
        BrokerAdminApiApplication.class.getClassLoader().getResource(CommonConstants.CONFIG_OF_SWAGGER_RESOURCES_PATH);
    CLStaticHttpHandler swaggerDist = new CLStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}));
    _httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }

  private BrokerManagedAsyncExecutorProvider buildBrokerManagedAsyncExecutorProvider(PinotConfiguration brokerConf,
      BrokerMetrics brokerMetrics) {
    int corePoolSize = brokerConf
        .getProperty(CommonConstants.Broker.CONFIG_OF_JERSEY_THREADPOOL_EXECUTOR_CORE_POOL_SIZE,
            CommonConstants.Broker.DEFAULT_JERSEY_THREADPOOL_EXECUTOR_CORE_POOL_SIZE);
    int maximumPoolSize = brokerConf
        .getProperty(CommonConstants.Broker.CONFIG_OF_JERSEY_THREADPOOL_EXECUTOR_MAX_POOL_SIZE,
            CommonConstants.Broker.DEFAULT_JERSEY_THREADPOOL_EXECUTOR_MAX_POOL_SIZE);
    int queueSize = brokerConf.getProperty(CommonConstants.Broker.CONFIG_OF_JERSEY_THREADPOOL_EXECUTOR_QUEUE_SIZE,
        CommonConstants.Broker.DEFAULT_JERSEY_THREADPOOL_EXECUTOR_QUEUE_SIZE);
    return new BrokerManagedAsyncExecutorProvider(corePoolSize, maximumPoolSize, queueSize, brokerMetrics);
  }

  public void stop() {
    if (_httpServer != null) {
      LOGGER.info("Shutting down http server");
      _httpServer.shutdownNow();
    }
    LOGGER.info("Shutting down executor service");
    _executorService.shutdownNow();
  }
}
