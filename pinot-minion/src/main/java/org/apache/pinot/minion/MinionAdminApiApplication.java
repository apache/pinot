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
import java.time.Instant;
import java.util.List;
import org.apache.pinot.common.utils.log.DummyLogFileServer;
import org.apache.pinot.common.utils.log.LocalLogFileServer;
import org.apache.pinot.common.utils.log.LogFileServer;
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
  public static final String MINION_INSTANCE_ID = "minionInstanceId";

  public static final String START_TIME = "minionStartTime";

  private HttpServer _httpServer;
  private final boolean _useHttps;

  public MinionAdminApiApplication(String instanceId, PinotConfiguration minionConf) {
    packages(RESOURCE_PACKAGE);
    property(PINOT_CONFIGURATION, minionConf);
    _useHttps = Boolean.parseBoolean(minionConf.getProperty(CommonConstants.Minion.CONFIG_OF_SWAGGER_USE_HTTPS));
    Instant minionStartTime = Instant.now();
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        // TODO: Add bindings as needed in future.
        bind(instanceId).named(MINION_INSTANCE_ID);
        String loggerRootDir = minionConf.getProperty(CommonConstants.Minion.CONFIG_OF_LOGGER_ROOT_DIR);
        if (loggerRootDir != null) {
          bind(new LocalLogFileServer(loggerRootDir)).to(LogFileServer.class);
        } else {
          bind(new DummyLogFileServer()).to(LogFileServer.class);
        }
        bind(minionStartTime).named(START_TIME);
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
    PinotReflectionUtils.runWithLock(this::setupSwagger);
  }

  private void setupSwagger() {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle("Pinot Minion API");
    beanConfig.setDescription("APIs for accessing Pinot Minion information");
    beanConfig.setContact("https://github.com/apache/pinot");
    beanConfig.setVersion("1.0");
    beanConfig.setExpandSuperTypes(false);
    if (_useHttps) {
      beanConfig.setSchemes(new String[]{CommonConstants.HTTPS_PROTOCOL});
    } else {
      beanConfig.setSchemes(new String[]{CommonConstants.HTTP_PROTOCOL, CommonConstants.HTTPS_PROTOCOL});
    }
    beanConfig.setBasePath("/");
    beanConfig.setResourcePackage(RESOURCE_PACKAGE);
    beanConfig.setScan(true);

    HttpHandler httpHandler = new CLStaticHttpHandler(MinionAdminApiApplication.class.getClassLoader(), "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    _httpServer.getServerConfiguration().addHttpHandler(httpHandler, "/api/", "/help/");

    URL swaggerDistLocation =
        MinionAdminApiApplication.class.getClassLoader().getResource(CommonConstants.CONFIG_OF_SWAGGER_RESOURCES_PATH);
    CLStaticHttpHandler swaggerDist = new CLStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}));
    _httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }

  public void stop() {
    if (_httpServer != null) {
      _httpServer.shutdownNow();
    }
  }
}
