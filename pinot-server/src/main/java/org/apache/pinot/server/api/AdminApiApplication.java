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
package org.apache.pinot.server.api;

import io.swagger.jaxrs.config.BeanConfig;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.log.DummyLogFileServer;
import org.apache.pinot.common.utils.log.LocalLogFileServer;
import org.apache.pinot.common.utils.log.LogFileServer;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.server.access.AccessControlFactory;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AdminApiApplication extends ResourceConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdminApiApplication.class);
  public static final String PINOT_CONFIGURATION = "pinotConfiguration";
  public static final String SERVER_INSTANCE_ID = "serverInstanceId";

  private final AtomicBoolean _shutDownInProgress = new AtomicBoolean();
  private final ServerInstance _serverInstance;
  private HttpServer _httpServer;
  private final String _adminApiResourcePackages;


  public AdminApiApplication(ServerInstance instance, AccessControlFactory accessControlFactory,
      PinotConfiguration serverConf) {
    _serverInstance = instance;

    _adminApiResourcePackages = serverConf.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_RESOURCE_PACKAGES,
        CommonConstants.Server.DEFAULT_SERVER_RESOURCE_PACKAGES);
    packages(_adminApiResourcePackages);
    property(PINOT_CONFIGURATION, serverConf);

    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(_shutDownInProgress).to(AtomicBoolean.class);
        bind(_serverInstance).to(ServerInstance.class);
        bind(_serverInstance.getHelixManager()).to(HelixManager.class);
        bind(_serverInstance.getServerMetrics()).to(ServerMetrics.class);
        bind(accessControlFactory).to(AccessControlFactory.class);
        bind(serverConf.getProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_ID)).named(SERVER_INSTANCE_ID);
        String loggerRootDir = serverConf.getProperty(CommonConstants.Server.CONFIG_OF_LOGGER_ROOT_DIR);
        if (loggerRootDir != null) {
          bind(new LocalLogFileServer(loggerRootDir)).to(LogFileServer.class);
        } else {
          bind(new DummyLogFileServer()).to(LogFileServer.class);
        }
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

  public boolean start(List<ListenerConfig> listenerConfigs) {
    _httpServer = ListenerConfigUtil.buildHttpServer(this, listenerConfigs);

    try {
      _httpServer.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start http server", e);
    }

    PinotConfiguration pinotConfiguration = (PinotConfiguration) getProperties().get(PINOT_CONFIGURATION);
    // Allow optional start of the swagger as the Reflection lib has multi-thread access bug (issues/7271). It is not
    // always possible to pin the Reflection lib on 0.9.9. So this optional setting will disable the swagger because it
    // is NOT an essential part of Pinot servers.
    if (pinotConfiguration.getProperty(CommonConstants.Server.CONFIG_OF_SWAGGER_SERVER_ENABLED,
        CommonConstants.Server.DEFAULT_SWAGGER_SERVER_ENABLED)) {
      LOGGER.info("Starting swagger for the Pinot server.");
      PinotReflectionUtils.runWithLock(() -> setupSwagger(pinotConfiguration));
    }
    return true;
  }

  private void setupSwagger(PinotConfiguration pinotConfiguration) {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle("Pinot Server API");
    beanConfig.setDescription("APIs for accessing Pinot server information");
    beanConfig.setContact("https://github.com/apache/pinot");
    beanConfig.setVersion("1.0");
    beanConfig.setExpandSuperTypes(false);
    if (Boolean.parseBoolean(pinotConfiguration.getProperty(CommonConstants.Server.CONFIG_OF_SWAGGER_USE_HTTPS))) {
      beanConfig.setSchemes(new String[]{CommonConstants.HTTPS_PROTOCOL});
    } else {
      beanConfig.setSchemes(new String[]{CommonConstants.HTTP_PROTOCOL, CommonConstants.HTTPS_PROTOCOL});
    }
    beanConfig.setBasePath("/");
    beanConfig.setResourcePackage(_adminApiResourcePackages);
    beanConfig.setScan(true);
    try {
      beanConfig.setHost(InetAddress.getLocalHost().getHostName());
    } catch (UnknownHostException e) {
      throw new RuntimeException("Cannot get localhost name");
    }

    CLStaticHttpHandler staticHttpHandler =
        new CLStaticHttpHandler(AdminApiApplication.class.getClassLoader(), "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    _httpServer.getServerConfiguration().addHttpHandler(staticHttpHandler, "/api/");
    _httpServer.getServerConfiguration().addHttpHandler(staticHttpHandler, "/help/");

    URL swaggerDistLocation =
        AdminApiApplication.class.getClassLoader().getResource(CommonConstants.CONFIG_OF_SWAGGER_RESOURCES_PATH);
    CLStaticHttpHandler swaggerDist = new CLStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}));
    _httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }

  /**
   * Starts shutting down the HTTP server, which rejects all requests except for the liveness check.
   */
  public void startShuttingDown() {
    _shutDownInProgress.set(true);
  }

  /**
   * Stops the HTTP server.
   */
  public void stop() {
    _httpServer.shutdownNow();
  }
}
