/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import io.swagger.jaxrs.config.BeanConfig;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;


public class ControllerAdminApiApplication extends ResourceConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerAdminApiApplication.class);

  private HttpServer httpServer;
  private URI baseUri;
  private boolean started = false;
  private static final String RESOURCE_PACKAGE = "com.linkedin.pinot.controller.api.resources";
  private static String CONSOLE_WEB_PATH;
  private final boolean _useHttps;

  public ControllerAdminApiApplication(String consoleWebPath, boolean useHttps) {
    super();
    CONSOLE_WEB_PATH = consoleWebPath;
    _useHttps = useHttps;
    if (!CONSOLE_WEB_PATH.endsWith("/")) {
      CONSOLE_WEB_PATH += "/";
    }
    packages(RESOURCE_PACKAGE);
    // TODO See ControllerResponseFilter
//    register(new LoggingFeature());
    register(JacksonFeature.class);
    register(MultiPartFeature.class);
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
    // property("jersey.config.server.tracing.type", "ALL");
    // property("jersey.config.server.tracing.threshold", "VERBOSE");
  }

  public void registerBinder(AbstractBinder binder) {
    register(binder);
  }

  public boolean start(int httpPort) {
    // ideally greater than reserved port but then port 80 is also valid
    Preconditions.checkArgument(httpPort > 0);
    baseUri = URI.create("http://0.0.0.0:" + Integer.toString(httpPort) + "/");
    httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, this);

    setupSwagger(httpServer);

    ClassLoader classLoader = ControllerAdminApiApplication.class.getClassLoader();

    // This is ugly from typical patterns to setup static resources but all our APIs are
    // at path "/". So, configuring static handler for path "/" does not work well.
    // Configuring this as a default servlet is an option but that is still ugly if we evolve
    // So, we setup specific handlers for static resource directory. index.html is served directly
    // by a jersey handler

    httpServer.getServerConfiguration().addHttpHandler(
        new CLStaticHttpHandler(classLoader,"/static/query/"), "/query");
    httpServer.getServerConfiguration().addHttpHandler(
        new CLStaticHttpHandler(classLoader, "/static/css/"), "/css");
    httpServer.getServerConfiguration().addHttpHandler(
        new CLStaticHttpHandler(classLoader, "/static/js/"), "/js");
    // without this explicit request to /index.html will not work
    httpServer.getServerConfiguration().addHttpHandler(
        new CLStaticHttpHandler(classLoader, "/static/"), "/index.html");

    started = true;
    LOGGER.info("Start jersey admin API on port: {}", httpPort);
    return true;
  }

  public URI getBaseUri() {
    return baseUri;
  }

  private void setupSwagger(HttpServer httpServer) {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle("Pinot Controller API");
    beanConfig.setDescription("APIs for accessing Pinot Controller information");
    beanConfig.setContact("https://github.com/linkedin/pinot");
    beanConfig.setVersion("1.0");
    if (_useHttps) {
      beanConfig.setSchemes(new String[]{"https"});
    } else {
      beanConfig.setSchemes(new String[]{"http"});
    }
    beanConfig.setBasePath(baseUri.getPath());
    beanConfig.setResourcePackage(RESOURCE_PACKAGE);
    beanConfig.setScan(true);

    CLStaticHttpHandler apiStaticHttpHandler = new CLStaticHttpHandler(ControllerAdminApiApplication.class.getClassLoader(),
        "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    httpServer.getServerConfiguration().addHttpHandler(apiStaticHttpHandler, "/api");
    httpServer.getServerConfiguration().addHttpHandler(apiStaticHttpHandler, "/help");

    URL swaggerDistLocation = ControllerAdminApiApplication.class.getClassLoader()
        .getResource("META-INF/resources/webjars/swagger-ui/2.2.2/");
    CLStaticHttpHandler swaggerDist = new CLStaticHttpHandler(
        new URLClassLoader(new URL[] {swaggerDistLocation}));
    httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }

   public void stop(){
    if (!started) {
      return;
    }
    httpServer.shutdownNow();
  }
}
