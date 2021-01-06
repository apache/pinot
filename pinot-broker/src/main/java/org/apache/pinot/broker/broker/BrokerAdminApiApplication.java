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

import com.google.common.base.Preconditions;
import io.swagger.jaxrs.config.BeanConfig;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.ssl.SSLContextConfigurator;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import static org.apache.pinot.common.utils.CommonConstants.Broker.*;
import static org.apache.pinot.common.utils.CommonConstants.HTTPS_PROTOCOL;


public class BrokerAdminApiApplication extends ResourceConfig {
  private static final String RESOURCE_PACKAGE = "org.apache.pinot.broker.api.resources";

  private URI _baseUri;
  private HttpServer _httpServer;

  public BrokerAdminApiApplication(RoutingManager routingManager, BrokerRequestHandler brokerRequestHandler,
      BrokerMetrics brokerMetrics) {
    packages(RESOURCE_PACKAGE);
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(routingManager).to(RoutingManager.class);
        bind(brokerRequestHandler).to(BrokerRequestHandler.class);
        bind(brokerMetrics).to(BrokerMetrics.class);
      }
    });
    register(JacksonFeature.class);
    registerClasses(io.swagger.jaxrs.listing.ApiListingResource.class);
    registerClasses(io.swagger.jaxrs.listing.SwaggerSerializers.class);
  }

  public void start(PinotConfiguration brokerConf) {
    int brokerQueryPort = brokerConf.getProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT,
        CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT);
    String brokerQueryProtocol = brokerConf.getProperty(CONFIG_OF_BROKER_CLIENT_PROTOCOL,
        DEFAULT_BROKER_CLIENT_PROTOCOL);

    Preconditions.checkArgument(brokerQueryPort > 0);
    _baseUri = URI.create(String.format("%s://0.0.0.0:%d/", brokerQueryProtocol, brokerQueryPort));

    _httpServer = buildHttpsServer(brokerConf);
    setupSwagger();
  }

  private HttpServer buildHttpsServer(PinotConfiguration brokerConf) {
    boolean isSecure = HTTPS_PROTOCOL.equals(brokerConf.getProperty(CONFIG_OF_BROKER_CLIENT_PROTOCOL));

    if (isSecure) {
      return GrizzlyHttpServerFactory.createHttpServer(_baseUri, this, true, buildSSLConfig(brokerConf));
    }

    return GrizzlyHttpServerFactory.createHttpServer(_baseUri, this);
  }

  private SSLEngineConfigurator buildSSLConfig(PinotConfiguration brokerConf) {
    SSLContextConfigurator sslContextConfigurator = new SSLContextConfigurator();

    sslContextConfigurator.setKeyStoreFile(brokerConf.getProperty(CONFIG_OF_BROKER_CLIENT_TLS_KEYSTORE_PATH));
    sslContextConfigurator.setKeyStorePass(brokerConf.getProperty(CONFIG_OF_BROKER_CLIENT_TLS_KEYSTORE_PASSWORD));
    sslContextConfigurator.setTrustStoreFile(brokerConf.getProperty(CONFIG_OF_BROKER_CLIENT_TLS_TRUSTSTORE_PATH));
    sslContextConfigurator.setTrustStorePass(brokerConf.getProperty(CONFIG_OF_BROKER_CLIENT_TLS_TRUSTSTORE_PASSWORD));

    boolean requiresClientAuth = brokerConf.getProperty(CONFIG_OF_BROKER_CLIENT_TLS_REQUIRES_CLIENT_AUTH,
        DEFAULT_BROKER_CLIENT_TLS_REQUIRES_CLIENT_AUTH);

    return new SSLEngineConfigurator(sslContextConfigurator).setClientMode(false)
        .setWantClientAuth(requiresClientAuth).setEnabledProtocols(new String[] { "TLSv1.2" });
  }

  private void setupSwagger() {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle("Pinot Broker API");
    beanConfig.setDescription("APIs for accessing Pinot broker information");
    beanConfig.setContact("https://github.com/apache/incubator-pinot");
    beanConfig.setVersion("1.0");
    beanConfig.setSchemes(new String[] { _baseUri.getScheme() });
    beanConfig.setBasePath(_baseUri.getPath());
    beanConfig.setResourcePackage(RESOURCE_PACKAGE);
    beanConfig.setScan(true);

    HttpHandler httpHandler = new CLStaticHttpHandler(BrokerAdminApiApplication.class.getClassLoader(), "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    _httpServer.getServerConfiguration().addHttpHandler(httpHandler, "/api/", "/help/");

    URL swaggerDistLocation =
        BrokerAdminApiApplication.class.getClassLoader().getResource("META-INF/resources/webjars/swagger-ui/3.18.2/");
    CLStaticHttpHandler swaggerDist = new CLStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}));
    _httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }

  public void stop() {
    if (_httpServer != null) {
      _httpServer.shutdownNow();
    }
  }
}
