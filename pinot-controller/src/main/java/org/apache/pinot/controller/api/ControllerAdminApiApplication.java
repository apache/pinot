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

import io.swagger.jaxrs.listing.SwaggerSerializers;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.swagger.SwaggerApiListingResource;
import org.apache.pinot.common.swagger.SwaggerSetupUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AuthenticationFilter;
import org.apache.pinot.core.api.ServiceAutoDiscoveryFeature;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.monitoring.MonitoringAware;
import org.glassfish.grizzly.monitoring.MonitoringConfig;
import org.glassfish.grizzly.threadpool.AbstractThreadPool;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.glassfish.grizzly.threadpool.ThreadPoolProbe;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;


public class ControllerAdminApiApplication extends ResourceConfig {
  public static final String PINOT_CONFIGURATION = "pinotConfiguration";

  public static final String START_TIME = "controllerStartTime";

  private final String _controllerResourcePackages;
  private final boolean _useHttps;
  private final boolean _enableSwagger;
  private HttpServer _httpServer;

  public ControllerAdminApiApplication(ControllerConf conf) {
    super();
    property(PINOT_CONFIGURATION, conf);

    _controllerResourcePackages = conf.getControllerResourcePackages();
    packages(_controllerResourcePackages);
    // TODO See ControllerResponseFilter
    // register(new LoggingFeature());
    _useHttps = Boolean.parseBoolean(conf.getProperty(ControllerConf.CONSOLE_SWAGGER_USE_HTTPS));
    _enableSwagger = conf.isEnableSwagger();
    if (conf.getProperty(CommonConstants.Controller.CONTROLLER_SERVICE_AUTO_DISCOVERY, false)) {
      register(ServiceAutoDiscoveryFeature.class);
    }
    register(JacksonFeature.class);
    register(MultiPartFeature.class);
    register(SwaggerApiListingResource.class);
    register(SwaggerSerializers.class);
    register(new CorsFilter());
    register(AuthenticationFilter.class);
    // property("jersey.config.server.tracing.type", "ALL");
    // property("jersey.config.server.tracing.threshold", "VERBOSE");
  }

  public void registerBinder(AbstractBinder binder) {
    register(binder);
  }

  public void start(List<ListenerConfig> listenerConfigs) {
    _httpServer = ListenerConfigUtil.buildHttpServer(this, listenerConfigs);
    NetworkListener listener = _httpServer.getListeners().iterator().next();
    ThreadPoolConfig tpc = listener.getTransport().getWorkerThreadPoolConfig();
    try {
      _httpServer.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start http server", e);
    }
    ClassLoader classLoader = ControllerAdminApiApplication.class.getClassLoader();
    if (_enableSwagger) {
      PinotReflectionUtils.runWithLock(() ->
          SwaggerSetupUtils.setupSwagger("Controller", _controllerResourcePackages, _useHttps, "/", _httpServer));
    }

    // This is ugly from typical patterns to setup static resources but all our APIs are
    // at path "/". So, configuring static handler for path "/" does not work well.
    // Configuring this as a default servlet is an option but that is still ugly if we evolve
    // So, we setup specific handlers for static resource directory. index.html is served directly
    // by a jersey handler

    _httpServer.getServerConfiguration()
        .addHttpHandler(new CLStaticHttpHandler(classLoader, "/webapp/"), "/index.html");
    _httpServer.getServerConfiguration()
        .addHttpHandler(new CLStaticHttpHandler(classLoader, "/webapp/images/"), "/images/");
    _httpServer.getServerConfiguration().addHttpHandler(new CLStaticHttpHandler(classLoader, "/webapp/js/"), "/js/");
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
      containerResponseContext.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS, DELETE");
      containerResponseContext.getHeaders().add("Access-Control-Allow-Headers", "*");
      if (containerRequestContext.getMethod().equals("OPTIONS")) {
        containerResponseContext.setStatus(HttpServletResponse.SC_OK);
      }
    }
  }

  public HttpServer getHttpServer() {
    return _httpServer;
  }

  /**
   * Registers a gauge that tracks HTTP thread pool utilization without using reflection.
   * Instead, it uses a custom ThreadPoolProbe to count active threads.
   */
  public void registerHttpThreadUtilizationGauge(ControllerMetrics metrics) {
    NetworkListener listener = _httpServer.getListeners().iterator().next();
    ExecutorService executor = listener.getTransport().getWorkerThreadPool();
    ThreadPoolConfig poolCfg = listener.getTransport().getWorkerThreadPoolConfig();

    BusyThreadProbe probe = new BusyThreadProbe();
    // Try to attach probe to the executor if it supports monitoring
    if (executor instanceof MonitoringAware) {
      @SuppressWarnings("unchecked")
      MonitoringConfig<ThreadPoolProbe> mc = ((MonitoringAware<ThreadPoolProbe>) executor).getMonitoringConfig();
      mc.addProbes(probe);
    }

    metrics.setOrUpdateGauge(ControllerGauge.HTTP_THREAD_UTILIZATION_PERCENT.getGaugeName(), () -> {
        int max = poolCfg.getMaxPoolSize();
        if (max <= 0) {
          return 0L;
        }
        int busy = probe.busyCount();
        return Math.round(busy * 100.0 / max);
    });
  }

  /**
   * Custom probe to track busy threads in Grizzly thread pools without using reflection.
   */
  public final class BusyThreadProbe extends ThreadPoolProbe.Adapter {
    private final AtomicInteger _busy = new AtomicInteger();

    @Override
    public void onTaskDequeueEvent(AbstractThreadPool pool, Runnable task) {
      // one more thread just got real work
      _busy.incrementAndGet();
    }

    @Override
    public void onTaskCompleteEvent(AbstractThreadPool pool, Runnable task) {
      // work finished, thread is idle again
      _busy.decrementAndGet();
    }

    /** Current number of busy worker threads. */
    public int busyCount() {
      return _busy.get();
    }
  }
}
