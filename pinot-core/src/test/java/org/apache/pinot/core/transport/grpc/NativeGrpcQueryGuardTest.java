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
package org.apache.pinot.core.transport.grpc;

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.PinotQueryServerGrpc;
import org.apache.pinot.common.proto.Server.ServerRequest;
import org.apache.pinot.common.proto.Server.ServerResponse;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.common.request.context.NativeQueryDeadline;
import org.apache.pinot.common.request.context.NativeQueryGate;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises the real query listener with TLS peer binding and rejection at every streamed/final delivery boundary.
public class NativeGrpcQueryGuardTest {
  @Test
  @SuppressWarnings("unchecked")
  public void testCertificateExpiryFencesAdmittedDeliveryAndSubsequentRpc() throws Exception {
    AtomicBoolean expired = new AtomicBoolean();
    X509Certificate certificate = mock(X509Certificate.class);
    when(certificate.getEncoded()).thenReturn(new byte[]{1, 2, 3});
    doAnswer(invocation -> {
      if (expired.get()) {
        throw new CertificateExpiredException("Expired after admission");
      }
      return null;
    }).when(certificate).checkValidity();
    SSLSession session = mock(SSLSession.class);
    when(session.getPeerCertificates()).thenReturn(new Certificate[]{certificate});
    ServerCall<ServerRequest, ServerResponse> call = mock(ServerCall.class);
    when(call.getAttributes()).thenReturn(
        Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_SSL_SESSION, session).build());
    StreamObserver<ServerResponse> observer = mock(StreamObserver.class);
    AtomicReference<StreamObserver<ServerResponse>> admitted = new AtomicReference<>();
    AtomicInteger handlers = new AtomicInteger();
    NativeQueryContext query = NativeQueryContext.fromProto(request().getNativeQueryContext());
    ServerCallHandler<ServerRequest, ServerResponse> handler = (ignored, headers) -> {
      handlers.incrementAndGet();
      admitted.set(NativeGrpcQueryGuard.admit((context, fingerprint) -> Runnable::run, query, observer));
      return new ServerCall.Listener<>() { };
    };
    NativeGrpcQueryGuard guard = new NativeGrpcQueryGuard();
    guard.interceptCall(call, new Metadata(), handler);
    assertEquals(handlers.get(), 1);
    StreamObserver<ServerResponse> response = admitted.get();
    response.onNext(ServerResponse.getDefaultInstance());
    expired.set(true);
    assertEquals(expectThrows(StatusRuntimeException.class,
        () -> response.onNext(ServerResponse.getDefaultInstance())).getStatus().getCode(), Status.Code.UNAUTHENTICATED);
    response.onError(new IllegalStateException("Producer cleanup after rejected delivery"));
    assertThrows(IllegalStateException.class, response::onCompleted);
    verify(observer, times(1)).onNext(any());
    verify(observer, times(1)).onError(any());
    verify(observer).onError(argThat(
        error -> Status.fromThrowable(error).getCode() == Status.Code.FAILED_PRECONDITION));
    verify(observer, never()).onCompleted();
    guard.interceptCall(call, new Metadata(), handler);
    assertEquals(handlers.get(), 1, "An expired certificate must be rejected before the next RPC handler");
    verify(call).close(argThat(status -> status.getCode() == Status.Code.UNAUTHENTICATED), any());
  }

  @Test
  public void testMutualTlsPeerAndEveryDeliveryAreGuarded() throws Exception {
    try (TlsFixture tls = new TlsFixture()) {
      AtomicReference<String> peer = new AtomicReference<>();
      AtomicInteger callbacks = new AtomicInteger();
      NativeQueryGate gate = (context, fingerprint) -> {
        peer.set(fingerprint);
        assertEquals(context.broker().participantId(), "broker");
        return callback -> {
          callbacks.incrementAndGet();
          callback.run();
        };
      };
      try (Fixture fixture = new Fixture(tls, gate)) {
        Iterator<ServerResponse> responses = fixture.submit(request());
        assertTrue(responses.hasNext());
        responses.next();
        assertTrue(responses.hasNext());
        responses.next();
        assertFalse(responses.hasNext());
        assertEquals(peer.get(), tls._fingerprint, "Request metadata must not supply the authenticated identity");
        assertEquals(callbacks.get(), 3, "Streaming data, final metadata and completion must all be guarded");
        ManagedChannel noCertificate = tls.channel(fixture._server.port(), false);
        try {
          assertThrows(StatusRuntimeException.class, () -> PinotQueryServerGrpc.newBlockingStub(noCertificate)
              .withDeadlineAfter(5, TimeUnit.SECONDS).submit(request()).hasNext());
        } finally {
          noCertificate.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        assertEquals(callbacks.get(), 3, "Unverified peers must not reach the query gate");
      }
    }
  }

  @Test
  public void testRevocationAtDataFinalResponseAndCompletionCannotReportSuccess() throws Exception {
    try (TlsFixture tls = new TlsFixture()) {
      for (int rejectedCallback = 1; rejectedCallback <= 3; rejectedCallback++) {
        int reject = rejectedCallback;
        AtomicInteger callbacks = new AtomicInteger();
        NativeQueryGate gate = (context, fingerprint) -> callback -> {
          if (callbacks.incrementAndGet() == reject) {
            throw new IllegalStateException("Authority revoked");
          }
          callback.run();
        };
        try (Fixture fixture = new Fixture(tls, gate)) {
          Iterator<ServerResponse> responses = fixture.submit(request());
          for (int received = 1; received < reject; received++) {
            assertTrue(responses.hasNext());
            responses.next();
          }
          StatusRuntimeException failure = expectThrows(StatusRuntimeException.class, responses::hasNext);
          assertEquals(failure.getStatus().getCode(), Status.Code.FAILED_PRECONDITION);
          assertEquals(callbacks.get(), reject);
        }
      }
    }
  }

  @Test
  public void testAdmissionRequiredWithoutChangingLegacyQueries() throws Exception {
    try (TlsFixture tls = new TlsFixture()) {
      NativeQueryGate reject = (context, peer) -> {
        throw new IllegalStateException("Broker is replaced");
      };
      try (Fixture fixture = new Fixture(tls, reject)) {
        assertEquals(expectThrows(StatusRuntimeException.class, () -> fixture.submit(request()).hasNext())
            .getStatus().getCode(), Status.Code.PERMISSION_DENIED);
        assertEquals(expectThrows(StatusRuntimeException.class,
            () -> fixture.submit(request().toBuilder().clearNativeQueryContext().build()).hasNext())
            .getStatus().getCode(), Status.Code.FAILED_PRECONDITION);
        verify(fixture._executor, never()).execute(any(), any(), any());
      }
      try (Fixture legacy = new Fixture(tls, null)) {
        assertEquals(expectThrows(StatusRuntimeException.class, () -> legacy.submit(request()).hasNext())
            .getStatus().getCode(), Status.Code.FAILED_PRECONDITION);
        Iterator<ServerResponse> responses = legacy.submit(request().toBuilder().clearNativeQueryContext().build());
        int count = 0;
        while (responses.hasNext()) {
          assertNotNull(responses.next());
          count++;
        }
        assertEquals(count, 2);
      }
      TlsConfig insecure = new TlsConfig(tls._serverConfig);
      insecure.setInsecure(true);
      assertThrows(IllegalArgumentException.class, () -> new GrpcQueryServer("server", 0,
          new GrpcConfig(Map.of()), insecure, mock(QueryExecutor.class), mock(AccessControl.class),
          mock(ThreadAccountant.class), reject));
      assertThrows(IllegalArgumentException.class, () -> new GrpcQueryServer("server", 0,
          new GrpcConfig(Map.of()), null, mock(QueryExecutor.class), mock(AccessControl.class),
          mock(ThreadAccountant.class), reject));
    }
  }

  private static ServerRequest request() {
    NativeQueryContext context = new NativeQueryContext(1, UUID.randomUUID(),
        new NativeQueryContext.ProcessIdentity("broker", UUID.randomUUID(), 1),
        new Participant("server", Role.SERVER, UUID.randomUUID(), 1), UUID.randomUUID(), UUID.randomUUID(),
        "tbl_OFFLINE", object(), "tbl", object(),
        List.of(new NativeQueryContext.Segment("segment", object(), "a".repeat(64), object(), UUID.randomUUID())),
        List.of()).withDeadline(new NativeQueryDeadline("test-clock", 10, 100));
    return ServerRequest.newBuilder().setSql("SELECT count(*) FROM tbl_OFFLINE").addSegments("segment")
        .putMetadata(Request.MetadataKeys.BROKER_ID, "broker")
        .putMetadata(Request.MetadataKeys.ENABLE_STREAMING, "true")
        .putMetadata("native-query-peer-certificate", "forged-request-identity")
        .setNativeQueryContext(context.toProto()).build();
  }

  private static ObjectVersion object() {
    return new ObjectVersion(UUID.randomUUID(), 1);
  }

  private static final class Fixture implements AutoCloseable {
    private final QueryExecutor _executor = mock(QueryExecutor.class);
    private final GrpcQueryServer _server;
    private final ManagedChannel _channel;

    private Fixture(TlsFixture tls, NativeQueryGate gate) throws Exception {
      ServerMetrics.register(mock(ServerMetrics.class));
      DataTable data = mock(DataTable.class);
      when(data.toBytes()).thenReturn(new byte[]{1, 2});
      BaseResultsBlock block = mock(BaseResultsBlock.class);
      when(block.getDataTable()).thenReturn(data);
      InstanceResponseBlock result = mock(InstanceResponseBlock.class);
      when(result.toDataTable()).thenReturn(data);
      when(_executor.execute(any(), any(), any())).thenAnswer(invocation -> {
        ResultsBlockStreamer streamer = invocation.getArgument(2);
        streamer.send(block);
        return result;
      });
      AccessControl access = mock(AccessControl.class);
      when(access.hasDataAccess(any(), anyString())).thenReturn(true);
      _server = new GrpcQueryServer("server", 0, new GrpcConfig(Map.of()), tls._serverConfig, _executor, access,
          mock(ThreadAccountant.class), gate);
      _server.start();
      _channel = tls.channel(_server.port(), true);
    }

    private Iterator<ServerResponse> submit(ServerRequest request) {
      return PinotQueryServerGrpc.newBlockingStub(_channel).withDeadlineAfter(5, TimeUnit.SECONDS).submit(request);
    }

    @Override
    public void close() throws Exception {
      _channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      _server.shutdown();
    }
  }

  private static final class TlsFixture implements AutoCloseable {
    private final Path _directory = Files.createTempDirectory("native-query-tls-");
    private final TlsConfig _serverConfig = new TlsConfig();
    private final KeyManagerFactory _keys;
    private final TrustManagerFactory _trust;
    private final String _fingerprint;

    private TlsFixture() throws Exception {
      Path file = _directory.resolve("identity.p12");
      Process keytool = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
          "-genkeypair", "-alias", "test", "-keyalg", "EC", "-groupname", "secp256r1", "-dname", "CN=localhost",
          "-ext", "SAN=DNS:localhost,IP:127.0.0.1", "-ext", "EKU=serverAuth,clientAuth", "-validity", "2",
          "-storetype", "PKCS12", "-keystore", file.toString(), "-storepass", "test-password", "-keypass",
          "test-password", "-noprompt").redirectErrorStream(true).start();
      if (!keytool.waitFor(30, TimeUnit.SECONDS)) {
        keytool.destroyForcibly();
        throw new IOException("TLS test certificate creation timed out");
      }
      assertEquals(keytool.exitValue(), 0,
          new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
      KeyStore store = KeyStore.getInstance("PKCS12");
      try (var input = Files.newInputStream(file)) {
        store.load(input, "test-password".toCharArray());
      }
      _keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      _keys.init(store, "test-password".toCharArray());
      _trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      _trust.init(store);
      _fingerprint = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
          .digest(store.getCertificate("test").getEncoded()));
      _serverConfig.setKeyStoreType("PKCS12");
      _serverConfig.setKeyStorePath(file.toString());
      _serverConfig.setKeyStorePassword("test-password");
      _serverConfig.setTrustStoreType("PKCS12");
      _serverConfig.setTrustStorePath(file.toString());
      _serverConfig.setTrustStorePassword("test-password");
      _serverConfig.setClientAuthEnabled(true);
    }

    private ManagedChannel channel(int port, boolean clientCertificate) throws Exception {
      var ssl = GrpcSslContexts.forClient().trustManager(_trust);
      if (clientCertificate) {
        ssl.keyManager(_keys);
      }
      return NettyChannelBuilder.forAddress("localhost", port).sslContext(ssl.build()).build();
    }

    @Override
    public void close() throws IOException {
      FileUtils.deleteDirectory(_directory.toFile());
    }
  }
}
