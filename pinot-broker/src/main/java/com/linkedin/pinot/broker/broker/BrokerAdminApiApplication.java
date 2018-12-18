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
package com.linkedin.pinot.broker.broker;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.broker.requesthandler.BrokerRequestHandler;
import com.linkedin.pinot.broker.routing.RoutingTable;
import com.linkedin.pinot.broker.routing.TimeBoundaryService;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import io.swagger.jaxrs.config.BeanConfig;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.grizzly.ssl.SSLContextConfigurator;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;


public class BrokerAdminApiApplication extends ResourceConfig {
  private static final String RESOURCE_PACKAGE = "com.linkedin.pinot.broker.api.resources";

  private URI _baseUri;
  private HttpServer _httpServer;
  
  private String _keyStoreFile; // contains server keypair
  private String _keyStorePass;
  private String _trustStoreFile; // contains client certificate
  private String _trustStorePass;
  private boolean _useHTTPS = false;

  public BrokerAdminApiApplication(BrokerServerBuilder brokerServerBuilder) {
    packages(RESOURCE_PACKAGE);
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(brokerServerBuilder).to(BrokerServerBuilder.class);
        bind(brokerServerBuilder.getRoutingTable()).to(RoutingTable.class);
        bind(brokerServerBuilder.getTimeBoundaryService()).to(TimeBoundaryService.class);
        bind(brokerServerBuilder.getBrokerMetrics()).to(BrokerMetrics.class);
        bind(brokerServerBuilder.getBrokerRequestHandler()).to(BrokerRequestHandler.class);
      }
    });
    registerClasses(io.swagger.jaxrs.listing.ApiListingResource.class);
    registerClasses(io.swagger.jaxrs.listing.SwaggerSerializers.class);
  }

  public void setSSLConfigs(String keyStoreFile, String keyStorePass, String trustStoreFile, String trustStorePass) {
	  _useHTTPS = true;
      _keyStoreFile = keyStoreFile;
	  _keyStorePass = keyStorePass;
	  _trustStoreFile = trustStoreFile;
	  _trustStorePass = trustStorePass;
  }

  public void start(int httpPort) {
    Preconditions.checkArgument(httpPort > 0);

    if (_useHTTPS) {
      _baseUri = URI.create("https://0.0.0.0:" + httpPort + "/");
      SSLContextConfigurator sslContext = new SSLContextConfigurator();
          sslContext.setKeyStoreFile(_keyStoreFile);
	  sslContext.setKeyStorePass(_keyStorePass);
	  sslContext.setTrustStoreFile(_trustStoreFile);
	  sslContext.setTrustStorePass(_trustStorePass);
	  _httpServer = GrizzlyHttpServerFactory.createHttpServer(_baseUri, this, true, 
        new SSLEngineConfigurator(sslContext).setClientMode(false).setNeedClientAuth(false));
    } else {
      _baseUri = URI.create("http://0.0.0.0:" + httpPort + "/");
      _httpServer = GrizzlyHttpServerFactory.createHttpServer(_baseUri, this);
    }
    setupSwagger();
  }

  private void setupSwagger() {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle("Pinot Broker API");
    beanConfig.setDescription("APIs for accessing Pinot broker information");
    beanConfig.setContact("https://github.com/linkedin/pinot");
    beanConfig.setVersion("1.0");
    if (_useHTTPS) {
      beanConfig.setSchemes(new String[]{"https"});
    }
    else {
      beanConfig.setSchemes(new String[]{"http"});
    }
    beanConfig.setBasePath(_baseUri.getPath());
    beanConfig.setResourcePackage(RESOURCE_PACKAGE);
    beanConfig.setScan(true);

    HttpHandler httpHandler = new CLStaticHttpHandler(BrokerAdminApiApplication.class.getClassLoader(), "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    _httpServer.getServerConfiguration().addHttpHandler(httpHandler, "/api", "/help");

    URL swaggerDistLocation =
        BrokerAdminApiApplication.class.getClassLoader().getResource("META-INF/resources/webjars/swagger-ui/2.2.2/");
    CLStaticHttpHandler swaggerDist = new CLStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}));
    _httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }

  public void stop() {
    if (_httpServer != null) {
      _httpServer.shutdownNow();
    }
  }
}
