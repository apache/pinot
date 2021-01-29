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
package org.apache.pinot.controller.api;

import com.google.common.annotations.VisibleForTesting;
import io.swagger.jaxrs.config.BeanConfig;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ControllerAdminApiApplication extends ResourceConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerAdminApiApplication.class);

  private HttpServer _httpServer;
  private static final String RESOURCE_PACKAGE = "org.apache.pinot.controller.api.resources";

  public ControllerAdminApiApplication() {
    super();

    packages(RESOURCE_PACKAGE);
    // TODO See ControllerResponseFilter
//    register(new LoggingFeature());
    register(JacksonFeature.class);
    register(MultiPartFeature.class);
    registerClasses(io.swagger.jaxrs.listing.ApiListingResource.class);
    registerClasses(io.swagger.jaxrs.listing.SwaggerSerializers.class);
    register(new CorsFilter());
    register(AuthFilter.class);
    // property("jersey.config.server.tracing.type", "ALL");
    // property("jersey.config.server.tracing.threshold", "VERBOSE");
  }

  public void registerBinder(AbstractBinder binder) {
    register(binder);
  }

  public void start(List<ListenerConfig> listenerConfigs) {
    _httpServer = ListenerConfigUtil.buildHttpServer(this, listenerConfigs);

    try {
      _httpServer.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start http server", e);
    }

    setupSwagger(_httpServer);

    ClassLoader classLoader = ControllerAdminApiApplication.class.getClassLoader();

    // This is ugly from typical patterns to setup static resources but all our APIs are
    // at path "/". So, configuring static handler for path "/" does not work well.
    // Configuring this as a default servlet is an option but that is still ugly if we evolve
    // So, we setup specific handlers for static resource directory. index.html is served directly
    // by a jersey handler

    _httpServer.getServerConfiguration().addHttpHandler(new CLStaticHttpHandler(classLoader, "/webapp/"), "/index.html");
    _httpServer.getServerConfiguration().addHttpHandler(new CLStaticHttpHandler(classLoader, "/webapp/js/"), "/js/");
  }

  private void setupSwagger(HttpServer httpServer) {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle("Pinot Controller API");
    beanConfig.setDescription("APIs for accessing Pinot Controller information");
    beanConfig.setContact("https://github.com/apache/incubator-pinot");
    beanConfig.setVersion("1.0");
    beanConfig.setSchemes(new String[]{CommonConstants.HTTP_PROTOCOL, CommonConstants.HTTPS_PROTOCOL});
    beanConfig.setBasePath("/");
    beanConfig.setResourcePackage(RESOURCE_PACKAGE);
    beanConfig.setScan(true);

    ClassLoader loader = this.getClass().getClassLoader();
    CLStaticHttpHandler apiStaticHttpHandler = new CLStaticHttpHandler(loader, "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    httpServer.getServerConfiguration().addHttpHandler(apiStaticHttpHandler, "/api/");
    httpServer.getServerConfiguration().addHttpHandler(apiStaticHttpHandler, "/help/");

    URL swaggerDistLocation = loader.getResource("META-INF/resources/webjars/swagger-ui/3.18.2/");
    CLStaticHttpHandler swaggerDist = new CLStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}));
    httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }

  public void stop() {
    if (!_httpServer.isStarted()) {
      return;
    }
    _httpServer.shutdownNow();
  }

  private class CorsFilter implements ContainerResponseFilter {
    @Override
    public void filter(ContainerRequestContext containerRequestContext,
        ContainerResponseContext containerResponseContext)
        throws IOException {
      containerResponseContext.getHeaders().add("Access-Control-Allow-Origin", "*");
    }
  }

  @javax.ws.rs.ext.Provider
  public static class AuthFilter implements ContainerRequestFilter {

    @Inject
    Provider<Request> _requestProvider;

    @Inject
    AccessControlFactory _accessControlFactory;

    @Context
    ResourceInfo _resourceInfo;

    @Context
    HttpHeaders _httpHeaders;

    @Override
    public void filter(ContainerRequestContext requestContext)
        throws IOException {
      // check if authentication is required
      Method endpointMethod = _resourceInfo.getResourceMethod();
      if (endpointMethod.isAnnotationPresent(Authenticate.class)) {
        // Perform authentication:
        // Note that table name is extracted from "path parameters" or "query parameters" if it's defined as one of the
        // followings:
        //     - "tableName",
        //     - "tableNameWithType", or
        //     - "schemaName"
        // If table name is not available, it means the endpoint is not a table-level endpoint.
        AccessControlUtils accessControlUtils = new AccessControlUtils();
        AccessType accessType = endpointMethod.getAnnotation(Authenticate.class).value();
        String endpointUrl = _requestProvider.get().getRequestURL().toString();
        UriInfo uriInfo = requestContext.getUriInfo();
        Optional<String> tableName = extractTableName(uriInfo.getPathParameters(), uriInfo.getQueryParameters());
        accessControlUtils
            .validatePermission(tableName, accessType, _httpHeaders, endpointUrl, _accessControlFactory.create());
      }
    }

    @VisibleForTesting
    Optional<String> extractTableName(MultivaluedMap<String, String> pathParameters,
        MultivaluedMap<String, String> queryParameters) {
      Optional<String> tableName = extractTableName(pathParameters);
      if (tableName.isPresent()) {
        return tableName;
      }
      return extractTableName(queryParameters);
    }

    private Optional<String> extractTableName(MultivaluedMap<String, String> mmap) {
      String tableName = mmap.getFirst("tableName");
      if (tableName == null) {
        tableName = mmap.getFirst("tableNameWithType");
        if (tableName == null) {
          tableName = mmap.getFirst("schemaName");
        }
      }
      return Optional.ofNullable(tableName);
    }
  }
}
