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
package org.apache.pinot.controller.coordination;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.util.Timeout;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.util.TableSizeMetadataProvider.Replicas;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.UrlBuilderUtils;

import static com.google.common.base.Preconditions.checkArgument;


/// Native-only server observations with scoped verified TLS, no redirects/retries, bounded bodies and one deadline.
/// Concurrent callers share the runtime's bounded executor and four-connection pool. Each call owns at most four
/// requests; timeout, interruption and shutdown cancel both queued tasks and active HTTP requests. This class owns
/// its client/connection manager, but the runtime owns the executor. TLS store changes require runtime restart.
final class NativeServerTableSizeCollector implements AutoCloseable {
  static final int MAX_RESPONSE_BYTES = 2 * 1024 * 1024;
  private static final int MAX_IN_FLIGHT = 4;
  private final ThreadPoolExecutor _executor;
  private final PoolingHttpClientConnectionManager _connections;
  private final CloseableHttpClient _client;
  private final Map<Future<?>, HttpGet> _active = new ConcurrentHashMap<>();
  private volatile boolean _closed;

  NativeServerTableSizeCollector(ControllerConf config, ThreadPoolExecutor executor, int timeoutMillis) {
    checkArgument(timeoutMillis > 0, "Native table-size timeout must be positive");
    _executor = executor;
    TlsConfig tls = TlsUtils.extractTlsConfig(config, ControllerConf.CONTROLLER_TLS_PREFIX);
    checkArgument(!tls.isInsecure(), "Native server observations require verified TLS");
    SSLContext context = TlsUtils.createSslContextWithoutAutoRenewal(tls.getKeyStoreType(), tls.getKeyStorePath(),
        tls.getKeyStorePassword(), tls.getTrustStoreType(), tls.getTrustStorePath(), tls.getTrustStorePassword());
    ClientTlsStrategyBuilder strategy = ClientTlsStrategyBuilder.create().setSslContext(context);
    if (tls.getAllowedProtocols() != null) {
      strategy.setTlsVersions(tls.getAllowedProtocols());
    }
    Timeout timeout = Timeout.ofMilliseconds(timeoutMillis);
    ConnectionConfig connectionConfig =
        ConnectionConfig.custom().setConnectTimeout(timeout).setSocketTimeout(timeout).build();
    _connections = PoolingHttpClientConnectionManagerBuilder.create().setTlsSocketStrategy(strategy.buildClassic())
        .setMaxConnTotal(MAX_IN_FLIGHT).setMaxConnPerRoute(MAX_IN_FLIGHT).setDefaultConnectionConfig(connectionConfig)
        .build();
    _client = HttpClients.custom().setConnectionManager(_connections).disableRedirectHandling()
        .disableAutomaticRetries().disableContentCompression().disableCookieManagement().disableAuthCaching().build();
  }

  PoolingHttpClientConnectionManager connections() {
    return _connections;
  }

  Map<String, TableSizeInfo> read(Replicas replicas, String tableNameWithType, int timeoutMillis) {
    checkArgument(timeoutMillis > 0, "Native table-size timeout must be positive");
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    Map<String, TableSizeInfo> reports = new HashMap<>();
    Map<FutureTask<Report>, HttpGet> pending = new HashMap<>();
    BlockingQueue<FutureTask<Report>> completed = new LinkedBlockingQueue<>();
    Iterator<Map.Entry<String, String>> endpoints = replicas.serverEndpoints().entrySet().iterator();
    try {
      while ((endpoints.hasNext() || !pending.isEmpty()) && System.nanoTime() < deadline && !_closed) {
        while (endpoints.hasNext() && pending.size() < MAX_IN_FLIGHT && System.nanoTime() < deadline && !_closed) {
          Map.Entry<String, String> endpoint = endpoints.next();
          HttpGet request = new HttpGet(endpoint.getValue() + "/tables/"
              + UrlBuilderUtils.encode(tableNameWithType) + "/size");
          FutureTask<Report> task = new FutureTask<>(() -> new Report(endpoint.getKey(),
              request(request, replicas, deadline))) {
            @Override
            protected void done() {
              _active.remove(this);
              completed.add(this);
            }
          };
          pending.put(task, request);
          _active.put(task, request);
          if (_closed) {
            task.cancel(true);
            request.cancel();
          } else {
            _executor.execute(task);
          }
        }
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0 || pending.isEmpty()) {
          break;
        }
        FutureTask<Report> done = completed.poll(remaining, TimeUnit.NANOSECONDS);
        if (done == null || System.nanoTime() >= deadline) {
          break;
        }
        pending.remove(done);
        try {
          Report report = done.get();
          if (report.value() != null && System.nanoTime() < deadline) {
            reports.put(report.server(), report.value());
          }
        } catch (ExecutionException | CancellationException ignored) {
          // Unavailable or malformed responses stay missing; do not expose remote bodies or transport details.
        }
      }
      return reports;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Native table-size collection interrupted", e);
    } finally {
      pending.forEach((task, request) -> {
        request.cancel();
        task.cancel(true);
        _executor.remove(task);
      });
    }
  }

  @Nullable
  private TableSizeInfo request(HttpGet request, Replicas replicas, long deadline) throws IOException {
    long remaining = deadline - System.nanoTime();
    if (_closed || remaining <= 0 || request.isCancelled()) {
      return null;
    }
    Timeout timeout = Timeout.ofMilliseconds(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
    request.setConfig(RequestConfig.custom().setConnectionRequestTimeout(timeout).setResponseTimeout(timeout)
        .setRedirectsEnabled(false).build());
    AuthProviderUtils.makeAuthHeadersMap(replicas.authProvider()).forEach(request::setHeader);
    return _client.execute(request, response -> {
      HttpEntity entity = response.getEntity();
      if (response.getCode() != 200 || entity == null || entity.getContentLength() > MAX_RESPONSE_BYTES
          || System.nanoTime() >= deadline) {
        request.cancel();
        return null;
      }
      try (InputStream body = entity.getContent()) {
        byte[] bytes = body.readNBytes(MAX_RESPONSE_BYTES + 1);
        if (bytes.length > MAX_RESPONSE_BYTES || System.nanoTime() >= deadline) {
          request.cancel();
          return null;
        }
        return JsonUtils.bytesToObject(bytes, TableSizeInfo.class);
      } catch (IOException | RuntimeException e) {
        request.cancel();
        throw e;
      }
    });
  }

  @Override
  public void close() throws IOException {
    _closed = true;
    _active.forEach((task, request) -> {
      request.cancel();
      task.cancel(true);
    });
    _client.close(CloseMode.IMMEDIATE);
  }

  private record Report(String server, @Nullable TableSizeInfo value) {
  }
}
