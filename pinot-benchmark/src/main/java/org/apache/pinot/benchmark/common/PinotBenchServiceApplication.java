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
package org.apache.pinot.benchmark.common;

import com.google.common.base.Preconditions;
import io.swagger.jaxrs.config.BeanConfig;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import javax.ws.rs.container.ContainerResponseFilter;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotBenchServiceApplication extends ResourceConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotBenchServiceApplication.class);

  private HttpServer httpServer;
  private URI baseUri;
  private boolean _useHttps = false;
  private volatile boolean started = false;
  private static final String RESOURCE_PACKAGE = "org.apache.pinot.benchmark.api.resources";

  public PinotBenchServiceApplication() {
    super();
    packages(RESOURCE_PACKAGE);
    register(JacksonFeature.class);
    register(MultiPartFeature.class);
    registerClasses(io.swagger.jaxrs.listing.ApiListingResource.class);
    registerClasses(io.swagger.jaxrs.listing.SwaggerSerializers.class);
    register((ContainerResponseFilter) (containerRequestContext,
        containerResponseContext) -> containerResponseContext.getHeaders().add("Access-Control-Allow-Origin", "*"));
  }

  public boolean start(int httpPort) {
    Preconditions.checkArgument(httpPort > 0);
    baseUri = URI.create("http://0.0.0.0:" + httpPort + "/");
    httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, this);
    setupSwagger(httpServer);
    started = true;
    LOGGER.info("Start jersey admin API on port: {}", httpPort);
    return true;
  }

  public void stop() {
    if (!started) {
      return;
    }
    httpServer.shutdownNow();
  }

  public void registerBinder(AbstractBinder binder) {
    register(binder);
  }

  private void setupSwagger(HttpServer httpServer) {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle("Pinot bench service API");
    beanConfig.setDescription("APIs for running Pinot bench against pinot perf cluster");
    beanConfig.setContact("Pinot Team");
    beanConfig.setVersion("1.0");
    if (_useHttps) {
      beanConfig.setSchemes(new String[]{"https"});
    } else {
      beanConfig.setSchemes(new String[]{"http"});
    }
    beanConfig.setBasePath(baseUri.getPath());
    beanConfig.setResourcePackage(RESOURCE_PACKAGE);
    beanConfig.setScan(true);

    ClassLoader loader = this.getClass().getClassLoader();
    CLStaticHttpHandler apiStaticHttpHandler = new CLStaticHttpHandler(loader, "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    httpServer.getServerConfiguration().addHttpHandler(apiStaticHttpHandler, "/api/");

    URL swaggerDistLocation = loader.getResource("META-INF/resources/webjars/swagger-ui/2.2.2/");
    CLStaticHttpHandler swaggerDist = new CLStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}));
    httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }
}
