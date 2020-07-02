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
package org.apache.pinot.server.starter.helix;

import io.swagger.jaxrs.config.BeanConfig;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import org.apache.pinot.server.api.access.AccessControlFactory;
import org.apache.pinot.server.starter.ServerInstance;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AdminApiApplication extends ResourceConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdminApiApplication.class);

  private final ServerInstance serverInstance;
  private final AccessControlFactory accessControlFactory;
  private URI baseUri;
  private boolean started = false;
  private HttpServer httpServer;
  public static final String RESOURCE_PACKAGE = "org.apache.pinot.server.api.resources";

  public AdminApiApplication(ServerInstance instance, AccessControlFactory accessControlFactory) {
    this.serverInstance = instance;
    this.accessControlFactory = accessControlFactory;
    packages(RESOURCE_PACKAGE);
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(serverInstance).to(ServerInstance.class);
        bind(accessControlFactory).to(AccessControlFactory.class);
      }
    });

    register(JacksonFeature.class);

    registerClasses(io.swagger.jaxrs.listing.ApiListingResource.class);
    registerClasses(io.swagger.jaxrs.listing.SwaggerSerializers.class);
    register(new ContainerResponseFilter() {
      @Override
      public void filter(ContainerRequestContext containerRequestContext,
          ContainerResponseContext containerResponseContext)
          throws IOException {
        containerResponseContext.getHeaders().add("Access-Control-Allow-Origin", "*");
      }
    });
  }

  public boolean start(int httpPort) {
    if (httpPort <= 0) {
      LOGGER.warn("Invalid admin API port: {}. Not starting admin service", httpPort);
      return false;
    }

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
    beanConfig.setTitle("Pinot Server API");
    beanConfig.setDescription("APIs for accessing Pinot server information");
    beanConfig.setContact("https://github.com/apache/incubator-pinot");
    beanConfig.setVersion("1.0");
    beanConfig.setSchemes(new String[]{"http"});
    beanConfig.setBasePath(baseUri.getPath());
    beanConfig.setResourcePackage(RESOURCE_PACKAGE);
    beanConfig.setScan(true);

    CLStaticHttpHandler staticHttpHandler =
        new CLStaticHttpHandler(AdminApiApplication.class.getClassLoader(), "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    httpServer.getServerConfiguration().addHttpHandler(staticHttpHandler, "/api/");
    httpServer.getServerConfiguration().addHttpHandler(staticHttpHandler, "/help/");

    URL swaggerDistLocation =
        AdminApiApplication.class.getClassLoader().getResource("META-INF/resources/webjars/swagger-ui/2.2.2/");
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
