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
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Operation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Owner;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.restlet.resources.NativeTableSizeIdentity;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.core.data.manager.offline.NativeImmutableSegmentDataManager;
import org.apache.pinot.core.transport.HttpServerThreadPoolConfig;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.server.access.AccessControlFactory;
import org.apache.pinot.server.access.AllowAllAccessFactory;
import org.apache.pinot.server.access.BasicAuthAccessFactory;
import org.apache.pinot.server.access.ZkBasicAuthAccessFactory;
import org.apache.pinot.server.starter.NativeInstanceDataManager;
import org.apache.pinot.server.starter.NativeServerParticipant;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises the explicit native administrative HTTP surface, authentication and pinned size provenance together.
public class NativeServerAdminApplicationTest {
  @Test
  public void testAuthenticatedReadOnlyObservationUsesPinnedIdentity() throws Exception {
    try (Fixture fixture = new Fixture()) {
      assertEquals(fixture.get("/tables/native_OFFLINE/size", null).statusCode(), 401);
      assertEquals(fixture.get("/tables/native_OFFLINE/size", "reader:password").statusCode(), 403);
      HttpResponse<String> response = fixture.get("/tables/native_OFFLINE/size", "admin:password");
      assertEquals(response.statusCode(), 200, response.body());
      TableSizeInfo size = JsonUtils.stringToObject(response.body(), TableSizeInfo.class);
      assertEquals(size.getTableName(), "native_OFFLINE");
      assertEquals(size.getDiskSizeInBytes(), 1234);
      assertEquals(size.getSegments().get(0).getDiskSizeInBytes(), 1234);
      NativeTableSizeIdentity provenance = size.getNativeMetadata();
      assertEquals(provenance.getIncarnation().toString(), fixture._token.getIncarnation());
      assertEquals(provenance.getRegistrationEpoch(), fixture._token.getRegistrationEpoch());
      assertEquals(provenance.getSegments().get("segment").getMaterializationId(),
          fixture._identity.materializationId());
      verify(fixture._table).releaseSegment(fixture._installed);
      assertEquals(fixture.get("/tables", "admin:password").statusCode(), 404);
      assertEquals(fixture.get("/tables/native_OFFLINE/reload", "admin:password").statusCode(), 404);
      assertEquals(fixture.get("/query/sql", "admin:password").statusCode(), 404);
      assertEquals(fixture.get("/tables/absent_OFFLINE/size", "admin:password").statusCode(), 404);
    }
  }

  @Test
  public void testStaleProcessAndLostReadinessRejectAndReleasePinnedHandles() throws Exception {
    try (Fixture fixture = new Fixture()) {
      when(fixture._participant.participant()).thenReturn(fixture._token.toBuilder()
          .setIncarnation(UUID.randomUUID().toString()).build());
      assertEquals(fixture.get("/tables/native_OFFLINE/size", "admin:password").statusCode(), 503);
      verify(fixture._table).releaseSegment(fixture._installed);
      clearInvocations(fixture._table);
      when(fixture._participant.participant()).thenReturn(fixture._token);
      when(fixture._participant.observationRevision()).thenReturn(fixture._revision)
          .thenThrow(new IllegalStateException("fenced"));
      assertEquals(fixture.get("/tables/native_OFFLINE/size", "admin:password").statusCode(), 503);
      verify(fixture._table).releaseSegment(fixture._installed);
    }
  }

  @Test
  public void testHttpsListenerKeepsAdministrativeAuthentication() throws Exception {
    try (TlsFixture tls = new TlsFixture(); Fixture fixture = new Fixture(tls)) {
      assertEquals(URI.create(fixture._endpoint).getScheme(), "https");
      assertEquals(fixture.get("/tables/native_OFFLINE/size", null).statusCode(), 401);
      assertEquals(fixture.get("/tables/native_OFFLINE/size", "reader:password").statusCode(), 403);
      assertEquals(fixture.get("/tables/native_OFFLINE/size", "admin:password").statusCode(), 200);
      assertEquals(fixture.get("/query/sql", "admin:password").statusCode(), 404);
    }
  }

  @Test
  public void testOversizedObservationFailsAsWholeResponse() throws Exception {
    try (Fixture fixture = new Fixture()) {
      when(fixture._table.acquireAllSegments()).thenReturn(Collections.nCopies(4097, fixture._installed));
      assertEquals(fixture.get("/tables/native_OFFLINE/size", "admin:password").statusCode(), 413);
      assertEquals(fixture.get("/tables/native_OFFLINE/size?includeCompressionStats=true", "admin:password")
          .statusCode(), 400);
    }
  }

  @Test
  public void testUnknownAndMetadataBackedFactoriesMustExplicitlyOptIn() throws Exception {
    PinotConfiguration config = new PinotConfiguration();
    new AllowAllAccessFactory().initWithoutHelix(config);
    assertThrows(IOException.class, () -> new AllowAllAccessFactory() { }.initWithoutHelix(config));
    assertThrows(IOException.class, () -> new BasicAuthAccessFactory() { }.initWithoutHelix(config));
    assertThrows(IOException.class, () -> new ZkBasicAuthAccessFactory().initWithoutHelix(config));
    AccessControlFactory unsupported = () -> new AllowAllAccessFactory().create();
    assertThrows(IOException.class, () -> unsupported.initWithoutHelix(config));
  }

  private static final class Fixture implements AutoCloseable {
    private final NativeServerParticipant _participant = mock(NativeServerParticipant.class);
    private final TableDataManager _table = mock(TableDataManager.class);
    private final MetadataRpc.ParticipantToken _token;
    private final MetadataRpc.Revision _revision;
    private final Identity _identity;
    private final NativeImmutableSegmentDataManager _installed;
    private final NativeServerAdminApplication _application;
    private final HttpClient _client;
    private final String _endpoint;

    private Fixture() throws Exception {
      this(null);
    }

    private Fixture(@Nullable TlsFixture tls) throws Exception {
      HttpClient.Builder client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
          .connectTimeout(Duration.ofSeconds(5));
      if (tls != null) {
        client.sslContext(tls._clientContext);
      }
      _client = client.build();
      UUID history = UUID.randomUUID();
      Participant server = new Participant("Server_native", Role.SERVER, UUID.randomUUID(), 3);
      Owner owner = new Owner(1, 0, 1, new Participant("controller", Role.CONTROLLER, UUID.randomUUID(), 1));
      _identity = new Identity(history, "native_OFFLINE", new ObjectVersion(UUID.randomUUID(), 2), "native",
          new ObjectVersion(UUID.randomUUID(), 3), "segment", new ObjectVersion(UUID.randomUUID(), 1),
          "0".repeat(64), server, new ObjectVersion(UUID.randomUUID(), 4), UUID.randomUUID(),
          new Operation(UUID.randomUUID(), 1, owner));
      _token = MetadataRpc.ParticipantToken.newBuilder().setHistoryId(history.toString())
          .setParticipantId(server.participantId()).setRole(MetadataRpc.ParticipantRole.SERVER)
          .setIncarnation(server.incarnation().toString()).setRegistrationEpoch(server.registrationEpoch()).build();
      _revision = MetadataRpc.Revision.newBuilder().setHistoryId(history.toString()).setIndex(9).build();
      ImmutableSegment segment = mock(ImmutableSegment.class);
      when(segment.getSegmentName()).thenReturn("segment");
      when(segment.getSegmentSizeBytes()).thenReturn(1234L);
      _installed = new NativeImmutableSegmentDataManager(segment, _identity,
          new Schema.SchemaBuilder().setSchemaName("native").build());
      when(_table.getTableName()).thenReturn("native_OFFLINE");
      when(_table.acquireAllSegments()).thenReturn(List.of(_installed));
      NativeInstanceDataManager manager = mock(NativeInstanceDataManager.class);
      when(manager.getTableDataManager("native_OFFLINE")).thenReturn(_table);
      when(_participant.dataManager()).thenReturn(manager);
      when(_participant.participant()).thenReturn(_token);
      when(_participant.observationRevision()).thenReturn(_revision);
      BasicAuthAccessFactory auth = new BasicAuthAccessFactory();
      auth.initWithoutHelix(new PinotConfiguration(Map.of(
          "principals", "admin,reader", "principals.admin.password", "password",
          "principals.admin.permissions", "admin", "principals.reader.password", "password",
          "principals.reader.tables", "native")));
      _application = new NativeServerAdminApplication(() -> _participant, auth);
      String protocol = tls == null ? "http" : "https";
      _endpoint = _application.start(List.of(new ListenerConfig(protocol, "127.0.0.1", 0, protocol,
          tls == null ? new TlsConfig() : tls._serverConfig, new HttpServerThreadPoolConfig(1, 2))), "127.0.0.1")
          .get(tls == null ? "adminHttp" : "adminHttps");
      assertTrue(URI.create(_endpoint).getPort() > 0);
    }

    private HttpResponse<String> get(String path, String credentials) throws Exception {
      HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(_endpoint + path))
          .timeout(Duration.ofSeconds(5));
      if (credentials != null) {
        request.header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));
      }
      return _client.send(request.GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    @Override
    public void close() {
      _application.close();
      _client.close();
    }
  }

  private static final class TlsFixture implements AutoCloseable {
    private final Path _directory = Files.createTempDirectory("native-admin-tls-");
    private final TlsConfig _serverConfig = new TlsConfig();
    private final SSLContext _clientContext;

    private TlsFixture() throws Exception {
      Path keyStoreFile = _directory.resolve("server.p12");
      String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
      Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "test", "-keyalg", "EC", "-groupname",
          "secp256r1", "-dname", "CN=localhost", "-ext", "SAN=DNS:localhost,IP:127.0.0.1", "-ext",
          "EKU=serverAuth", "-validity", "2", "-storetype", "PKCS12", "-keystore", keyStoreFile.toString(),
          "-storepass", "test-password", "-keypass", "test-password", "-noprompt")
          .redirectErrorStream(true).start();
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        throw new IOException("Timed out generating TLS test certificate");
      }
      assertEquals(process.exitValue(), 0, new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
      KeyStore keyStore = KeyStore.getInstance("PKCS12");
      try (var input = Files.newInputStream(keyStoreFile)) {
        keyStore.load(input, "test-password".toCharArray());
      }
      TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trust.init(keyStore);
      _clientContext = SSLContext.getInstance("TLS");
      _clientContext.init(null, trust.getTrustManagers(), null);
      _serverConfig.setKeyStoreType("PKCS12");
      _serverConfig.setKeyStorePath(keyStoreFile.toString());
      _serverConfig.setKeyStorePassword("test-password");
    }

    @Override
    public void close() throws IOException {
      FileUtils.deleteDirectory(_directory.toFile());
    }
  }
}
