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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.server.access.AccessControlFactory;
import org.apache.pinot.server.api.nativeapi.NativeTableSizeResource;
import org.apache.pinot.server.starter.NativeServerParticipant;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;


/// Explicit read-only native server administrative surface. It shares the normal listener TLS and administrative
/// authorization boundary, while registering no Helix, lifecycle mutation, or query resources.
public final class NativeServerAdminApplication extends ResourceConfig implements AutoCloseable {
  @Nullable
  private HttpServer _server;

  public NativeServerAdminApplication(Supplier<NativeServerParticipant> participant,
      AccessControlFactory accessControlFactory) {
    register(new NativeTableSizeResource(participant));
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(accessControlFactory).to(AccessControlFactory.class);
      }
    });
    register(ServerRequestMethodCaptureFilter.class);
    register(ServerAdminApiAccessControlFilter.class);
    register(JacksonFeature.class);
    register((ContainerResponseFilter) (request, response) ->
        response.getHeaders().putSingle(HttpHeaders.CACHE_CONTROL, "no-store"));
  }

  /// Binds listeners before registration and returns their actual addresses, including ephemeral test ports.
  public Map<String, String> start(List<ListenerConfig> listeners, String advertisedHost) throws IOException {
    checkState(_server == null, "Native server admin application is already started");
    checkArgument(advertisedHost != null && !advertisedHost.isBlank(), "Native admin host is required");
    _server = ListenerConfigUtil.buildHttpServer(this, listeners);
    try {
      _server.start();
      Map<String, String> endpoints = new LinkedHashMap<>();
      for (ListenerConfig listener : listeners) {
        NetworkListener bound = _server.getListener(listener.getName() + "-" + listener.getPort());
        String key = "https".equals(listener.getProtocol()) ? "adminHttps" : "adminHttp";
        String host = listener.getHost();
        if ("0.0.0.0".equals(host) || "::".equals(host)) {
          host = advertisedHost;
        }
        String endpoint =
            new URI(listener.getProtocol(), null, host, bound.getPort(), null, null, null).toASCIIString();
        checkArgument(endpoints.putIfAbsent(key, endpoint) == null,
            "Only one native server administrative listener per protocol is supported");
      }
      return Map.copyOf(endpoints);
    } catch (IOException | URISyntaxException | RuntimeException e) {
      close();
      throw new IOException("Failed to start native server administrative listeners", e);
    }
  }

  @Override
  public void close() {
    if (_server != null) {
      _server.shutdownNow();
      _server = null;
    }
  }
}
