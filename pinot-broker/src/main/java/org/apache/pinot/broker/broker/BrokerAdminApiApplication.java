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

import io.swagger.jaxrs.config.BeanConfig;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.api.ServiceAutoDiscoveryFeature;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.core.transport.ListenerConfig;
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


public class BrokerAdminApiApplication extends ResourceConfig {
  private static final String RESOURCE_PACKAGE = "org.apache.pinot.broker.api.resources";
  public static final String PINOT_CONFIGURATION = "pinotConfiguration";
  public static final String BROKER_INSTANCE_ID = "brokerInstanceId";
  private final boolean _useHttps;

  private HttpServer _httpServer;

  public BrokerAdminApiApplication(BrokerRoutingManager routingManager, BrokerRequestHandler brokerRequestHandler,
      BrokerMetrics brokerMetrics, PinotConfiguration brokerConf, SqlQueryExecutor sqlQueryExecutor) {
    packages(RESOURCE_PACKAGE);
    property(PINOT_CONFIGURATION, brokerConf);
    _useHttps = Boolean.parseBoolean(brokerConf.getProperty(CommonConstants.Broker.CONFIG_OF_SWAGGER_USE_HTTPS));
    if (brokerConf.getProperty(CommonConstants.Broker.BROKER_SERVICE_AUTO_DISCOVERY, false)) {
      register(ServiceAutoDiscoveryFeature.class);
    }
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(sqlQueryExecutor).to(SqlQueryExecutor.class);
        bind(routingManager).to(BrokerRoutingManager.class);
        bind(brokerRequestHandler).to(BrokerRequestHandler.class);
        bind(brokerMetrics).to(BrokerMetrics.class);
        bind(brokerConf.getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ID)).named(BROKER_INSTANCE_ID);
      }
    });
    register(JacksonFeature.class);
    registerClasses(io.swagger.jaxrs.listing.ApiListingResource.class);
    registerClasses(io.swagger.jaxrs.listing.SwaggerSerializers.class);
  }

  public void start(List<ListenerConfig> listenerConfigs) {
    _httpServer = ListenerConfigUtil.buildHttpServer(this, listenerConfigs);

    try {
      _httpServer.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start http server", e);
    }
    PinotReflectionUtils.runWithLock(this::setupSwagger);
  }

  private void setupSwagger() {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle("Pinot Broker API");
    beanConfig.setDescription("APIs for accessing Pinot broker information");
    beanConfig.setContact("https://github.com/apache/pinot");
    beanConfig.setVersion("1.0");
    if (_useHttps) {
      beanConfig.setSchemes(new String[]{CommonConstants.HTTPS_PROTOCOL});
    } else {
      beanConfig.setSchemes(new String[]{CommonConstants.HTTP_PROTOCOL, CommonConstants.HTTPS_PROTOCOL});
    }
    beanConfig.setBasePath("/");
    beanConfig.setResourcePackage(RESOURCE_PACKAGE);
    beanConfig.setScan(true);

    HttpHandler httpHandler = new CLStaticHttpHandler(BrokerAdminApiApplication.class.getClassLoader(), "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    _httpServer.getServerConfiguration().addHttpHandler(httpHandler, "/api/", "/help/");

    URL swaggerDistLocation =
        BrokerAdminApiApplication.class.getClassLoader().getResource("META-INF/resources/webjars/swagger-ui/3.23.11/");
    CLStaticHttpHandler swaggerDist = new CLStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}));
    _httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }

  public void stop() {
    if (_httpServer != null) {
      _httpServer.shutdownNow();
    }
  }
}
