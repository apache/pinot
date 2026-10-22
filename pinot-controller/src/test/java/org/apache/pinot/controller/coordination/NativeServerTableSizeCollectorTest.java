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

import com.google.common.collect.ImmutableBiMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.auth.StaticTokenAuthProvider;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.util.TableSizeMetadataProvider.Replicas;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Exercises native observation transport bounds, cancellation and scoped TLS without touching Helix HTTP behavior.
public class NativeServerTableSizeCollectorTest {
  @Test
  public void testRedirectAndOversizedChunkedResponseAreRejected() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    ExecutorService handlers = Executors.newCachedThreadPool();
    server.setExecutor(handlers);
    AtomicInteger redirectHits = new AtomicInteger();
    server.createContext("/tables/redirect_OFFLINE/size", exchange -> {
      assertEquals(exchange.getRequestHeaders().getFirst("Authorization"), "private-token");
      exchange.getResponseHeaders().set("Location", "/stolen");
      exchange.sendResponseHeaders(302, -1);
      exchange.close();
    });
    server.createContext("/stolen", exchange -> {
      redirectHits.incrementAndGet();
      exchange.sendResponseHeaders(200, -1);
      exchange.close();
    });
    server.createContext("/tables/large_OFFLINE/size", exchange -> {
      exchange.sendResponseHeaders(200, 0);
      try (var body = exchange.getResponseBody()) {
        body.write(new byte[NativeServerTableSizeCollector.MAX_RESPONSE_BYTES + 1]);
      } catch (IOException ignored) {
        // The bounded client closes this response before accepting an oversized body.
      }
    });
    server.createContext("/tables/orders_OFFLINE/size", NativeServerTableSizeCollectorTest::respond);
    ThreadPoolExecutor executor = executor();
    try (NativeServerTableSizeCollector collector = new NativeServerTableSizeCollector(new ControllerConf(), executor,
        2000)) {
      server.start();
      Replicas replicas = replicas("http://127.0.0.1:" + server.getAddress().getPort());
      assertTrue(collector.read(replicas, "redirect_OFFLINE", 2000).isEmpty());
      assertEquals(redirectHits.get(), 0);
      assertTrue(collector.read(replicas, "large_OFFLINE", 2000).isEmpty());
      assertEquals(collector.read(replicas, "orders_OFFLINE", 2000).get("Server_0").getDiskSizeInBytes(), 100);
    } finally {
      executor.shutdownNow();
      server.stop(0);
      handlers.shutdownNow();
    }
  }

  @Test
  public void testOneDeadlineIncludesQueueAndBodyRead() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    ExecutorService handlers = Executors.newCachedThreadPool();
    server.setExecutor(handlers);
    CountDownLatch bodyRelease = new CountDownLatch(1);
    CountDownLatch bodyStarted = new CountDownLatch(1);
    AtomicInteger slowRequests = new AtomicInteger();
    server.createContext("/tables/slow_OFFLINE/size", exchange -> {
      slowRequests.incrementAndGet();
      exchange.sendResponseHeaders(200, 0);
      try (var body = exchange.getResponseBody()) {
        body.write('{');
        body.flush();
        bodyStarted.countDown();
        bodyRelease.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    server.createContext("/tables/orders_OFFLINE/size", NativeServerTableSizeCollectorTest::respond);
    ThreadPoolExecutor executor = executor();
    CountDownLatch workerRelease = new CountDownLatch(1);
    CountDownLatch workersStarted = new CountDownLatch(4);
    try (NativeServerTableSizeCollector collector = new NativeServerTableSizeCollector(new ControllerConf(), executor,
        5000)) {
      server.start();
      Replicas replicas = replicas("http://127.0.0.1:" + server.getAddress().getPort());
      for (int i = 0; i < 4; i++) {
        executor.execute(() -> {
          workersStarted.countDown();
          try {
            workerRelease.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
      }
      assertTrue(workersStarted.await(5, TimeUnit.SECONDS));
      long started = System.nanoTime();
      assertTrue(collector.read(replicas, "slow_OFFLINE", 150).isEmpty());
      assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 2000);
      assertEquals(slowRequests.get(), 0);
      assertTrue(executor.getQueue().isEmpty(), "Timed-out queued requests must be removed");
      workerRelease.countDown();
      started = System.nanoTime();
      assertTrue(collector.read(replicas, "slow_OFFLINE", 500).isEmpty());
      assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 2000);
      assertEquals(bodyStarted.getCount(), 0L, "The deadline must cancel a response body already being read");
      bodyRelease.countDown();
      assertEquals(collector.read(replicas, "orders_OFFLINE", 2000).get("Server_0").getDiskSizeInBytes(), 100);
    } finally {
      workerRelease.countDown();
      bodyRelease.countDown();
      executor.shutdownNow();
      server.stop(0);
      handlers.shutdownNow();
    }
  }

  @Test
  public void testPrivateTrustStoreAndHostnameVerificationAreScoped() throws Exception {
    Path directory = Files.createTempDirectory("native-size-client-tls-");
    ThreadPoolExecutor executor = executor();
    HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    SSLContext previousContext = SSLContext.getDefault();
    var previousSocketFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
    try {
      Path keyStore = directory.resolve("server.p12");
      String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
      Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "test", "-keyalg", "EC", "-groupname",
          "secp256r1", "-dname", "CN=localhost", "-ext", "SAN=DNS:localhost", "-ext", "EKU=serverAuth",
          "-validity", "2", "-storetype", "PKCS12", "-keystore", keyStore.toString(), "-storepass",
          "test-password", "-keypass", "test-password", "-noprompt").redirectErrorStream(true).start();
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        throw new IOException("Timed out creating native size TLS fixture");
      }
      assertEquals(process.exitValue(), 0, new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
      SSLContext context = TlsUtils.createSslContextWithoutAutoRenewal("PKCS12", keyStore.toString(), "test-password",
          null, null, null);
      server.setHttpsConfigurator(new HttpsConfigurator(context));
      server.createContext("/tables/orders_OFFLINE/size", NativeServerTableSizeCollectorTest::respond);
      server.start();
      String endpoint = "https://localhost:" + server.getAddress().getPort();
      try (NativeServerTableSizeCollector untrusted = new NativeServerTableSizeCollector(new ControllerConf(), executor,
          2000)) {
        assertTrue(untrusted.read(replicas(endpoint), "orders_OFFLINE", 2000).isEmpty());
      }
      ControllerConf trusted = new ControllerConf();
      trusted.setProperty("controller.tls.truststore.type", "PKCS12");
      trusted.setProperty("controller.tls.truststore.path", keyStore.toString());
      trusted.setProperty("controller.tls.truststore.password", "test-password");
      try (NativeServerTableSizeCollector collector = new NativeServerTableSizeCollector(trusted, executor, 2000)) {
        assertEquals(collector.read(replicas(endpoint), "orders_OFFLINE", 2000)
            .get("Server_0").getDiskSizeInBytes(), 100);
        assertTrue(collector.read(replicas("https://127.0.0.1:" + server.getAddress().getPort()),
            "orders_OFFLINE", 2000).isEmpty());
      }
      assertSame(SSLContext.getDefault(), previousContext);
      assertSame(HttpsURLConnection.getDefaultSSLSocketFactory(), previousSocketFactory);
    } finally {
      executor.shutdownNow();
      server.stop(0);
      FileUtils.deleteDirectory(directory.toFile());
    }
  }

  private static ThreadPoolExecutor executor() {
    return new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(16));
  }

  private static Replicas replicas(String endpoint) {
    return new Replicas(Map.of("Server_0", List.of("segment")), ImmutableBiMap.of("Server_0", endpoint),
        new StaticTokenAuthProvider("private-token"));
  }

  private static void respond(HttpExchange exchange) throws IOException {
    byte[] body = JsonUtils.objectToString(new TableSizeInfo("orders_OFFLINE", 100,
        List.of(new SegmentSizeInfo("segment", 100)), 1)).getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, body.length);
    try (var output = exchange.getResponseBody()) {
      output.write(body);
    }
  }
}
