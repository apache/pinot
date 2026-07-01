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
package org.apache.pinot.plugin.metrics.micrometer;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The opt-in Prometheus export leg of {@link MicrometerMetricsRegistry}.
///
/// <p>This is a thin holder around a native Micrometer {@link PrometheusMeterRegistry}. The registry is added as a
/// member of the registry's {@code CompositeMeterRegistry}, so every meter registered through the SPI is fanned to
/// it natively (no read-time mirroring). Optionally it also serves the Prometheus exposition format over an HTTP
/// scrape endpoint.
///
/// <p>Naming: meters keep their full dotted name (e.g. {@code ServerMetrics.pinot.server.numDocsScanned}); Micrometer's
/// Prometheus naming convention sanitises it (dots become underscores, type suffixes such as {@code _total} are
/// appended). These native names differ from the legacy JMX-derived names — an accepted divergence for this backend.
///
/// <p>Thread-safety: the wrapped {@link PrometheusMeterRegistry} is thread-safe; {@link #scrape()} may be called
/// concurrently.
public class MicrometerPrometheusLeg {
  private static final Logger LOGGER = LoggerFactory.getLogger(MicrometerPrometheusLeg.class);

  private final PrometheusMeterRegistry _registry;
  private final HttpServer _httpServer;

  /// @param port HTTP port to serve the Prometheus scrape endpoint on; {@code <= 0} means no HTTP server is started
  ///             (the registry can still be scraped programmatically via {@link #scrape()}).
  /// @param path HTTP path for the scrape endpoint (e.g. {@code /metrics}).
  public MicrometerPrometheusLeg(int port, String path) {
    _registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    _httpServer = port > 0 ? startHttpServer(port, path) : null;
    if (_httpServer != null) {
      LOGGER.info("Started Micrometer Prometheus scrape endpoint on port {} at path {}", port, path);
    }
  }

  /// Returns the underlying Micrometer Prometheus registry, to be added to the composite.
  public PrometheusMeterRegistry getRegistry() {
    return _registry;
  }

  /// Returns the Prometheus exposition-format text for all registered meters.
  public String scrape() {
    return _registry.scrape();
  }

  private HttpServer startHttpServer(int port, String path) {
    try {
      HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
      server.createContext(path, exchange -> {
        byte[] body = scrape().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(body);
        }
      });
      server.setExecutor(null);
      server.start();
      return server;
    } catch (IOException e) {
      throw new RuntimeException("Failed to start Micrometer Prometheus scrape endpoint on port " + port, e);
    }
  }

  /// Stops the HTTP server (if any) and closes the Prometheus registry.
  public void close() {
    if (_httpServer != null) {
      _httpServer.stop(0);
    }
    _registry.close();
  }
}
