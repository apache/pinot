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
package org.apache.pinot.broker.transport;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.proto.PinotQueryServerGrpc;
import org.apache.pinot.common.proto.Server.ServerRequest;
import org.apache.pinot.common.proto.Server.ServerResponse;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.common.request.context.NativeQueryDeadline;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises real loopback TLS, live certificate revalidation, bounded channels and cancellable query streams.
public class NativeGrpcQueryClientTest {
  private static final NativeGrpcQueryClient.Limits LIMITS =
      new NativeGrpcQueryClient.Limits(4, 64 * 1024, 4096, 8, 32 * 1024, 5000);
  private static final ServerResponse FRAME =
      ServerResponse.newBuilder().setPayload(ByteString.copyFromUtf8("response")).build();

  @Test(timeOut = 30_000)
  public void testStrictTlsTrustHostnameAndNoPlaintextFallback() throws Exception {
    try (TlsFixture tls = new TlsFixture(); TlsFixture unrelated = new TlsFixture();
        var server = new QueryServer(tls, observer -> {
          observer.onNext(FRAME);
          observer.onCompleted();
        });
        var client = tls.client(LIMITS)) {
      try (var call = client.submit(server.endpoint(), request(), () -> 5000)) {
        assertTrue(call.responses().hasNext());
        assertEquals(call.responses().next(), FRAME);
        assertFalse(call.responses().hasNext());
      }
      assertEquals(server._requests.get(), 1);
      URI wrongHostname = URI.create("grpcs://127.0.0.1:" + server._server.getPort());
      try (var call = client.submit(wrongHostname, request(), () -> 5000)) {
        assertThrows(StatusRuntimeException.class, () -> call.responses().hasNext());
      }
      try (var untrusted = new NativeGrpcQueryClient(tls._certificate, tls._key, unrelated._certificate, LIMITS);
          var call = untrusted.submit(server.endpoint(), request(), () -> 5000)) {
        assertThrows(StatusRuntimeException.class, () -> call.responses().hasNext());
      }
      assertEquals(server._requests.get(), 1, "Invalid TLS peers must not reach the query service");
      for (String invalid : List.of("grpc://localhost:8090", "grpcs://LOCALHOST:8090",
          "grpcs://localhost:8090/", "grpcs://user@localhost:8090")) {
        assertThrows(IllegalArgumentException.class, () -> client.submit(URI.create(invalid), request(), () -> 5000));
      }
      assertThrows(IllegalArgumentException.class,
          () -> client.submit(server.endpoint(), request().toBuilder().clearNativeQueryContext().build(), () -> 5000));
      assertEquals(expectThrows(StatusRuntimeException.class, () -> client.submit(server.endpoint(),
          request().toBuilder().setSql("x".repeat(65 * 1024)).build(), () -> 5000)).getStatus().getCode(),
          Status.Code.RESOURCE_EXHAUSTED);
      try (var plaintext = new QueryServer(null, observer -> observer.onCompleted());
          var call = client.submit(plaintext.endpoint(), request(), () -> 5000)) {
        assertThrows(StatusRuntimeException.class, () -> call.responses().hasNext());
        assertEquals(plaintext._requests.get(), 0);
      }
      client.close();
      awaitClosed(client);
    }
  }

  @Test(timeOut = 30_000)
  public void testCertificateExpiryAfterHandshakeRejectsFramesAndNextRequest() throws Exception {
    try (TlsFixture tls = new TlsFixture()) {
      MutableClock clock = new MutableClock();
      AtomicReference<StreamObserver<ServerResponse>> producer = new AtomicReference<>();
      try (var server = new QueryServer(tls, observer -> {
        producer.set(observer);
        observer.onNext(FRAME);
      }); var client = new NativeGrpcQueryClient(tls._certificate, tls._key, tls._certificate, LIMITS, clock)) {
        try (var call = client.submit(server.endpoint(), request(), () -> 5000)) {
          assertEquals(call.responses().next(), FRAME, "A real TLS handshake and authenticated request must succeed");
          clock._now.set(tls._identity.getNotAfter().toInstant().plusSeconds(1));
          producer.get().onNext(FRAME);
          assertEquals(expectThrows(StatusRuntimeException.class, () -> call.responses().hasNext())
              .getStatus().getCode(), Status.Code.UNAUTHENTICATED);
        }
        try (var next = client.submit(server.endpoint(), request(), () -> 5000)) {
          assertThrows(StatusRuntimeException.class, () -> next.responses().hasNext());
        }
        assertEquals(server._requests.get(), 1,
            "Call credentials must reject expired identity before another request, independently of TLS resumption");
        client.close();
        awaitClosed(client);
      }
    }
  }

  @Test(timeOut = 30_000)
  public void testCancellationDeadlinesAndTransportBoundsReleaseOwnedChannels() throws Exception {
    try (TlsFixture tls = new TlsFixture(); var executor = Executors.newSingleThreadExecutor()) {
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch cancelled = new CountDownLatch(1);
      var limits = new NativeGrpcQueryClient.Limits(1, 64 * 1024, 1024, 1, 1024, 5000);
      try (var server = new QueryServer(tls, observer -> {
        ((ServerCallStreamObserver<ServerResponse>) observer).setOnCancelHandler(cancelled::countDown);
        entered.countDown();
      }); var client = tls.client(limits)) {
        var call = client.submit(server.endpoint(), request(), () -> 5000);
        var waiting = executor.submit(
            () -> expectThrows(StatusRuntimeException.class, () -> call.responses().hasNext()));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertEquals(client.channelCount(), 1);
        assertEquals(expectThrows(StatusRuntimeException.class,
            () -> client.submit(server.endpoint(), request(), () -> 5000)).getStatus().getCode(),
            Status.Code.RESOURCE_EXHAUSTED);
        call.cancel();
        assertEquals(waiting.get(5, TimeUnit.SECONDS).getStatus().getCode(), Status.Code.CANCELLED);
        assertTrue(cancelled.await(5, TimeUnit.SECONDS));
        awaitClosed(client);
        assertEquals(client.channelCount(), 0, "Slots are released only after the owned channel terminates");
        AtomicLong remaining = new AtomicLong(5000);
        try (var expired = client.submit(server.endpoint(), request(), remaining::get)) {
          remaining.set(0);
          assertEquals(expectThrows(StatusRuntimeException.class, () -> expired.responses().hasNext())
              .getStatus().getCode(), Status.Code.DEADLINE_EXCEEDED);
        }
        awaitClosed(client);
        try (var expired = client.submit(server.endpoint(), request(), () -> 50)) {
          assertEquals(expectThrows(StatusRuntimeException.class, () -> expired.responses().hasNext())
              .getStatus().getCode(), Status.Code.DEADLINE_EXCEEDED);
        }
        awaitClosed(client);
        AtomicInteger observations = new AtomicInteger();
        assertEquals(expectThrows(StatusRuntimeException.class,
            () -> client.submit(server.endpoint(), request(), () -> {
              if (observations.getAndIncrement() == 0) {
                try {
                  Thread.sleep(100);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new IllegalStateException(e);
                }
                return 50;
              }
              return 5000;
            })).getStatus().getCode(), Status.Code.DEADLINE_EXCEEDED,
            "A pause inside the remaining-budget supplier must consume the anchored RPC deadline");
        awaitClosed(client);
        var stopped = client.submit(server.endpoint(), request(), () -> 5000);
        client.close();
        assertThrows(StatusRuntimeException.class, () -> stopped.responses().hasNext());
        assertThrows(StatusRuntimeException.class, () -> client.submit(server.endpoint(), request(), () -> 5000));
        awaitClosed(client);
      }
      for (int limit = 0; limit < 3; limit++) {
        int bound = limit;
        var responseLimits = new NativeGrpcQueryClient.Limits(1, 64 * 1024, 1024, bound == 2 ? 8 : 1, 1024, 5000);
        try (var server = new QueryServer(tls, observer -> {
          ServerResponse frame = bound == 0 ? FRAME
              : FRAME.toBuilder().setPayload(ByteString.copyFrom(new byte[bound == 1 ? 2048 : 700])).build();
          observer.onNext(frame);
          if (bound != 1) {
            observer.onNext(frame);
          }
          observer.onCompleted();
        }); var client = tls.client(responseLimits);
            var call = client.submit(server.endpoint(), request(), () -> 5000)) {
          assertEquals(expectThrows(StatusRuntimeException.class, () -> {
            while (call.responses().hasNext()) {
              call.responses().next();
            }
          }).getStatus().getCode(), Status.Code.RESOURCE_EXHAUSTED);
        }
      }
    }
  }

  private static void awaitClosed(NativeGrpcQueryClient client) throws InterruptedException {
    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (client.channelCount() != 0 && until - System.nanoTime() > 0) {
      Thread.sleep(10);
    }
    assertEquals(client.channelCount(), 0, "Owned query channels did not terminate");
  }

  private static ServerRequest request() {
    NativeQueryContext context = new NativeQueryContext(1, UUID.randomUUID(),
        new NativeQueryContext.ProcessIdentity("broker", UUID.randomUUID(), 1),
        new Participant("server", Role.SERVER, UUID.randomUUID(), 1), UUID.randomUUID(), UUID.randomUUID(),
        "tbl_OFFLINE", object(), "tbl", object(),
        List.of(new NativeQueryContext.Segment("segment", object(), "a".repeat(64), object(), UUID.randomUUID())),
        List.of()).withDeadline(new NativeQueryDeadline("test-clock", 10, 100));
    return ServerRequest.newBuilder().setSql("SELECT count(*) FROM tbl_OFFLINE").addSegments("segment")
        .setNativeQueryContext(context.toProto()).build();
  }

  private static ObjectVersion object() {
    return new ObjectVersion(UUID.randomUUID(), 1);
  }

  private static final class QueryServer implements AutoCloseable {
    private final AtomicInteger _requests = new AtomicInteger();
    private final Server _server;

    private QueryServer(@Nullable TlsFixture tls, Consumer<StreamObserver<ServerResponse>> responder)
        throws IOException {
      var builder = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0));
      if (tls != null) {
        builder.sslContext(GrpcSslContexts.forServer(tls._certificate.toFile(), tls._key.toFile())
            .trustManager(tls._certificate.toFile()).clientAuth(ClientAuth.REQUIRE).build());
      }
      _server = builder.addService(new PinotQueryServerGrpc.PinotQueryServerImplBase() {
        @Override
        public void submit(ServerRequest request, StreamObserver<ServerResponse> observer) {
          _requests.incrementAndGet();
          responder.accept(observer);
        }
      }).build().start();
    }

    private URI endpoint() {
      return URI.create("grpcs://localhost:" + _server.getPort());
    }

    @Override
    public void close() throws InterruptedException {
      _server.shutdownNow();
      assertTrue(_server.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static final class MutableClock extends Clock {
    private final AtomicReference<Instant> _now = new AtomicReference<>(Instant.now());

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return _now.get();
    }
  }

  private static final class TlsFixture implements AutoCloseable {
    private final Path _directory = Files.createTempDirectory("native-grpc-client-");
    private final Path _certificate = _directory.resolve("identity.pem");
    private final Path _key = _directory.resolve("identity-key.pem");
    private final X509Certificate _identity;

    private TlsFixture() throws Exception {
      Path file = _directory.resolve("identity.p12");
      Process keytool = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
          "-genkeypair", "-alias", "test", "-keyalg", "EC", "-groupname", "secp256r1", "-dname", "CN=localhost",
          "-ext", "SAN=DNS:localhost", "-ext", "EKU=serverAuth,clientAuth", "-validity", "2", "-storetype", "PKCS12",
          "-keystore", file.toString(), "-storepass", "test-password", "-keypass", "test-password", "-noprompt")
          .redirectErrorStream(true).start();
      if (!keytool.waitFor(20, TimeUnit.SECONDS)) {
        keytool.destroyForcibly();
        throw new IOException("TLS test certificate creation timed out");
      }
      assertEquals(keytool.exitValue(), 0,
          new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
      KeyStore store = KeyStore.getInstance("PKCS12");
      try (var input = Files.newInputStream(file)) {
        store.load(input, "test-password".toCharArray());
      }
      _identity = (X509Certificate) store.getCertificate("test");
      pem(_certificate, "CERTIFICATE", _identity.getEncoded());
      pem(_key, "PRIVATE KEY", store.getKey("test", "test-password".toCharArray()).getEncoded());
    }

    private NativeGrpcQueryClient client(NativeGrpcQueryClient.Limits limits) throws IOException {
      return new NativeGrpcQueryClient(_certificate, _key, _certificate, limits);
    }

    private static void pem(Path file, String type, byte[] value) throws IOException {
      Files.writeString(file, "-----BEGIN " + type + "-----\n"
          + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(value) + "\n-----END " + type + "-----\n",
          StandardCharsets.US_ASCII);
    }

    @Override
    public void close() throws IOException {
      FileUtils.deleteDirectory(_directory.toFile());
    }
  }
}
