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
package org.apache.pinot.plugin.stream.push;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.internal.guava.ThreadFactoryBuilder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.process.JerseyProcessingUncaughtExceptionHandler;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PushApiApplication extends ResourceConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(PushApiApplication.class);

  public static final String START_TIME = "serverStartTime";

  private static final String RESOURCE_PACKAGES = "org.apache.pinot.plugin.stream.push.resources";

  private final AtomicBoolean _shutDownInProgress = new AtomicBoolean();
  private HttpServer _httpServer;
  private final PushBasedIngestionBufferManager _bufferManager;

  public PushApiApplication(PushBasedIngestionBufferManager bufferManager) {
    _bufferManager = bufferManager;

    packages(RESOURCE_PACKAGES);
    Instant serverStartTime = Instant.now();

    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(_shutDownInProgress).to(AtomicBoolean.class);
        bind(_bufferManager).to(PushBasedIngestionBufferManager.class);
        bind(serverStartTime).named(START_TIME);
      }
    });

    register(JacksonFeature.class);

    register(new ContainerResponseFilter() {
      @Override
      public void filter(ContainerRequestContext containerRequestContext,
          ContainerResponseContext containerResponseContext)
          throws IOException {
        containerResponseContext.getHeaders().add("Access-Control-Allow-Origin", "*");
      }
    });
  }

  public boolean start(int port) {
    _httpServer = buildHttpServer(this, port);
    try {
      LOGGER.info("Starting push API on port {}", port);
      _httpServer.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start http server", e);
    }

    return true;
  }

  public static HttpServer buildHttpServer(ResourceConfig resConfig, int port) {
    // The URI is irrelevant since the default listener will be manually rewritten.
    HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(URI.create("http://0.0.0.0/"), resConfig, false);

    // Listeners cannot be configured with the factory. Manual overrides are required as instructed by Javadoc.
    // @see org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory.createHttpServer(java.net.URI, org
    // .glassfish.jersey.grizzly2.httpserver.GrizzlyHttpContainer, boolean, org.glassfish.grizzly.ssl
    // .SSLEngineConfigurator, boolean)
    httpServer.removeListener("grizzly");

    final NetworkListener listener = new NetworkListener("push-api-" + port, "0.0.0.0", port);

    listener.getTransport().getWorkerThreadPoolConfig().setThreadFactory(
            new ThreadFactoryBuilder().setNameFormat("grizzly-http-server-push-api-%d")
                .setUncaughtExceptionHandler(new JerseyProcessingUncaughtExceptionHandler()).build())
        .setCorePoolSize(10)
        .setMaxPoolSize(10);

//    if (CommonConstants.HTTPS_PROTOCOL.equals(listenerConfig.getProtocol())) {
//      listener.setSecure(true);
//      listener.setSSLEngineConfig(buildSSLEngineConfigurator(listenerConfig.getTlsConfig()));
//    }
    httpServer.addListener(listener);
    return httpServer;
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

  public HttpServer getHttpServer() {
    return _httpServer;
  }

  public PushBasedIngestionBufferManager getBufferManager() {
    return _bufferManager;
  }
}
