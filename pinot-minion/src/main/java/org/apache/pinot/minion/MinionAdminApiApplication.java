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
package org.apache.pinot.minion;

import io.swagger.jaxrs.config.BeanConfig;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;


/**
 * Admin APP for Pinot Minion.
 * <ul>
 *   <li>Starts a http server.</li>
 *   <li>Sets up swagger.</li>
 * </ul>
 */
public class MinionAdminApiApplication extends ResourceConfig {
  private static final String RESOURCE_PACKAGE = "org.apache.pinot.minion.api.resources";
  public static final String PINOT_CONFIGURATION = "pinotConfiguration";

  private HttpServer _httpServer;

  public MinionAdminApiApplication(PinotConfiguration minionConf) {
    packages(RESOURCE_PACKAGE);
    property(PINOT_CONFIGURATION, minionConf);

    register(new AbstractBinder() {
      @Override
      protected void configure() {
        // TODO: Add bindings as needed in future.
      }
    });

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
    synchronized (PinotReflectionUtils.getReflectionLock()) {
      setupSwagger();
    }
  }

  private void setupSwagger() {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle("Pinot Minion API");
    beanConfig.setDescription("APIs for accessing Pinot Minion information");
    beanConfig.setContact("https://github.com/apache/incubator-pinot");
    beanConfig.setVersion("1.0");
    beanConfig.setSchemes(new String[]{CommonConstants.HTTP_PROTOCOL, CommonConstants.HTTPS_PROTOCOL});
    beanConfig.setBasePath("/");
    beanConfig.setResourcePackage(RESOURCE_PACKAGE);
    beanConfig.setScan(true);

    HttpHandler httpHandler = new CLStaticHttpHandler(MinionAdminApiApplication.class.getClassLoader(), "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    _httpServer.getServerConfiguration().addHttpHandler(httpHandler, "/api/", "/help/");

    URL swaggerDistLocation =
        MinionAdminApiApplication.class.getClassLoader().getResource("META-INF/resources/webjars/swagger-ui/3.18.2/");
    CLStaticHttpHandler swaggerDist = new CLStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}));
    _httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }

  public void stop() {
    if (_httpServer != null) {
      _httpServer.shutdownNow();
    }
  }
}
