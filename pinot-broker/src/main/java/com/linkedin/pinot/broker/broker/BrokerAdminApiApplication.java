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
package com.linkedin.pinot.broker.broker;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.broker.requesthandler.BrokerRequestHandler;
import com.linkedin.pinot.broker.routing.TimeBoundaryService;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import io.swagger.jaxrs.config.BeanConfig;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BrokerAdminApiApplication extends ResourceConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerAdminApiApplication.class);
  public static final String RESOURCE_PACKAGE = "com.linkedin.pinot.broker.api.resources";

  private URI baseUri;
  private boolean started = false;
  private HttpServer httpServer;
  private BrokerServerBuilder brokerServerBuilder;
  private BrokerMetrics brokerMetrics;
  private BrokerRequestHandler brokerRequestHandler;
  private TimeBoundaryService timeBoundaryService;

  public BrokerAdminApiApplication(final BrokerServerBuilder brokerServerBuilder, final BrokerMetrics brokerMetrics,
      final BrokerRequestHandler brokerRequestHandler, final TimeBoundaryService timeBoundaryService) {
    this.brokerServerBuilder = brokerServerBuilder;
    this.brokerMetrics = brokerMetrics;
    this.brokerRequestHandler = brokerRequestHandler;
    this.timeBoundaryService = timeBoundaryService;

    packages(RESOURCE_PACKAGE);
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(brokerServerBuilder).to(BrokerServerBuilder.class);
        bind(brokerMetrics).to(BrokerMetrics.class);
        bind(brokerRequestHandler).to(BrokerRequestHandler.class);
        bind(timeBoundaryService).to(TimeBoundaryService.class);
      }
    });

    registerClasses(io.swagger.jaxrs.listing.ApiListingResource.class);
    registerClasses(io.swagger.jaxrs.listing.SwaggerSerializers.class);
  }

  public boolean start(int httpPort) {
    Preconditions.checkArgument(httpPort > 0);
    baseUri = URI.create("http://0.0.0.0:" + Integer.toString(httpPort) + "/");
    httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, this);
    setupSwagger(httpServer);
    started = true;
    return true;
  }

  public URI getBaseUri() {
    return baseUri;
  }

  private void setupSwagger(HttpServer httpServer) {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle("Pinot Broker API");
    beanConfig.setDescription("APIs for accessing Pinot broker information");
    beanConfig.setContact("https://github.com/linkedin/pinot");
    beanConfig.setVersion("1.0");
    beanConfig.setSchemes(new String[]{"http"});
    beanConfig.setBasePath(baseUri.getPath());
    beanConfig.setResourcePackage(RESOURCE_PACKAGE);
    beanConfig.setScan(true);

    CLStaticHttpHandler staticHttpHandler =
        new CLStaticHttpHandler(BrokerAdminApiApplication.class.getClassLoader(), "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    httpServer.getServerConfiguration().addHttpHandler(staticHttpHandler, "/api");
    httpServer.getServerConfiguration().addHttpHandler(staticHttpHandler, "/help");

    URL swaggerDistLocation =
        BrokerAdminApiApplication.class.getClassLoader().getResource("META-INF/resources/webjars/swagger-ui/2.2.2/");
    CLStaticHttpHandler swaggerDist = new CLStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}));
    httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }

  public void stop() {
    if (!started) {
      return;
    }
    httpServer.shutdownNow();
  }
}
