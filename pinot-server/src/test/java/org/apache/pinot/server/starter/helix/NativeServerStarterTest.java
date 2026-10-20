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
package org.apache.pinot.server.starter.helix;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManagerFactory;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.MembershipCommandRequest;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;
import org.apache.pinot.server.starter.NativeInstanceDataManager;
import org.apache.pinot.server.starter.NativeMetadataConnection;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises the normal starter's native dispatch and real participant lifecycle with controlled metadata replies.
/// Transport authentication and actual quorum composition are covered by the metadata transport integration tests.
public class NativeServerStarterTest {
  @Test
  public void testNativeStarterOwnsInstallationLifecycleWithoutHelixOrListeners() throws Exception {
    try (Fixture fixture = new Fixture(); ServerSocket occupied = new ServerSocket(0);
        MockedStatic<HelixManagerFactory> helix = mockStatic(HelixManagerFactory.class);
        MockedConstruction<NativeMetadataConnection> connections = fixture.connections()) {
      PinotConfiguration config = fixture.config();
      config.setProperty(Helix.KEY_OF_SERVER_NETTY_PORT, occupied.getLocalPort());
      HelixServerStarter starter = new HelixServerStarter();
      try {
        starter.init(config);
        assertFalse(Files.exists(fixture._directory.resolve("data")));
        assertTrue(connections.constructed().isEmpty(), "Initialization must not open metadata channels");
        starter.start();
        assertEquals(connections.constructed().size(), 2, "Membership and work need independent admission");
        assertEquals(starter.getInstanceId(), Fixture.INSTANCE_ID);
        assertEquals(ServiceStatus.getServiceStatus(Fixture.INSTANCE_ID), ServiceStatus.Status.GOOD);
        assertTrue(ServiceStatus.getStatusDescription(Fixture.INSTANCE_ID).contains("query listeners disabled"));
        HelixServerStarter unstarted = new HelixServerStarter();
        unstarted.init(fixture.config());
        unstarted.stop();
        assertEquals(ServiceStatus.getServiceStatus(Fixture.INSTANCE_ID), ServiceStatus.Status.GOOD,
            "Closing an unstarted duplicate must not remove the running participant's status");
        assertTrue(starter.getServerInstance().getInstanceDataManager() instanceof NativeInstanceDataManager);
        assertNull(starter._helixManager);
        assertNull(starter._helixAdmin);
        assertNull(starter._adminApiApplication);
        assertThrows(IllegalStateException.class, starter.getServerInstance()::getQueryScheduler);
        assertFalse(starter.isServerReadyToServeQueries());
        assertThrows(IllegalStateException.class, starter.getServerInstance()::startQueryServer);
        assertThrows(IllegalStateException.class, starter.getServerInstance()::getHelixManager);
        assertTrue(fixture._token != null && fixture._registeredEndpoints == 0);
      } finally {
        starter.stop();
      }
      starter.stop();
      assertNotNull(fixture._drained);
      assertNotNull(fixture._unregistered);
      assertEquals(ServiceStatus.getServiceStatus(Fixture.INSTANCE_ID), ServiceStatus.Status.NOT_STARTED);
      connections.constructed().forEach(connection -> verify(connection).close());
      helix.verifyNoInteractions();
      // A second process lifecycle can take the same durable directory after the first participant drains.
      HelixServerStarter restarted = new HelixServerStarter();
      try {
        restarted.init(fixture.config());
        restarted.start();
        assertEquals(ServiceStatus.getServiceStatus(Fixture.INSTANCE_ID), ServiceStatus.Status.GOOD);
      } finally {
        restarted.stop();
      }
    }
  }

  @Test
  public void testExplicitAdminListenerIsBoundBeforeRegistrationAndClosedWithStarter() throws Exception {
    try (Fixture fixture = new Fixture();
        MockedStatic<HelixManagerFactory> helix = mockStatic(HelixManagerFactory.class);
        MockedConstruction<NativeMetadataConnection> connections = fixture.connections()) {
      PinotConfiguration config = fixture.config();
      config.setProperty(Server.CONFIG_OF_ADMIN_API_PORT, 0);
      config.setProperty(Helix.KEY_OF_SERVER_NETTY_HOST, "127.0.0.1");
      config.setProperty("pinot.server.http.server.thread.pool.corePoolSize", 1);
      config.setProperty("pinot.server.http.server.thread.pool.maxPoolSize", 2);
      fixture._checkAdminBeforeRegistration = true;
      HelixServerStarter starter = new HelixServerStarter();
      String endpoint;
      try {
        starter.init(config);
        starter.start();
        endpoint = fixture._endpoints.get("adminHttp");
        assertTrue(URI.create(endpoint).getPort() > 0);
        assertEquals(fixture._registeredEndpoints, 1);
        assertFalse(starter.isServerReadyToServeQueries());
        assertThrows(IllegalStateException.class, starter.getServerInstance()::startQueryServer);
        assertEquals(get(endpoint + "/tables"), 404, "Legacy admin resources must not be registered");
      } finally {
        starter.stop();
      }
      try (ServerSocket released = new ServerSocket(URI.create(endpoint).getPort())) {
        assertTrue(released.isBound());
      }
      connections.constructed().forEach(connection -> verify(connection).close());
      helix.verifyNoInteractions();
    }
  }

  private static int get(String url) throws Exception {
    try (HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofSeconds(5)).build()) {
      return client.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build(),
          HttpResponse.BodyHandlers.discarding()).statusCode();
    }
  }

  @Test
  public void testFailedRegistrationClosesChannelsAndReleasesDirectory() throws Exception {
    try (Fixture fixture = new Fixture();
        MockedStatic<HelixManagerFactory> helix = mockStatic(HelixManagerFactory.class);
        MockedConstruction<NativeMetadataConnection> connections = fixture.connections()) {
      fixture._rejectRegistration = true;
      HelixServerStarter starter = new HelixServerStarter();
      PinotConfiguration config = fixture.config();
      config.setProperty(Server.CONFIG_OF_ADMIN_API_PORT, 0);
      config.setProperty("pinot.server.http.server.thread.pool.corePoolSize", 1);
      config.setProperty("pinot.server.http.server.thread.pool.maxPoolSize", 2);
      starter.init(config);
      IOException failure = expectThrows(IOException.class, starter::start);
      assertTrue(failure.getMessage().contains("Cannot acquire native registration"), failure.getMessage());
      starter.stop();
      try (ServerSocket released = new ServerSocket(URI.create(fixture._endpoints.get("adminHttp")).getPort())) {
        assertTrue(released.isBound(), "Failed registration must close the pre-bound admin listener");
      }
      connections.constructed().forEach(connection -> verify(connection).close());
      assertEquals(ServiceStatus.getServiceStatus(Fixture.INSTANCE_ID), ServiceStatus.Status.NOT_STARTED);
      fixture._rejectRegistration = false;
      HelixServerStarter recovered = new HelixServerStarter();
      try {
        recovered.init(fixture.config());
        recovered.start();
        assertEquals(ServiceStatus.getServiceStatus(Fixture.INSTANCE_ID), ServiceStatus.Status.GOOD);
      } finally {
        recovered.stop();
      }
      helix.verifyNoInteractions();
    }
  }

  @Test
  public void testNativeBootstrapRejectsMissingStorageOrMismatchedIdentityBeforeResources() throws Exception {
    try (Fixture fixture = new Fixture();
        MockedStatic<HelixManagerFactory> helix = mockStatic(HelixManagerFactory.class);
        MockedConstruction<NativeMetadataConnection> connections = fixture.connections()) {
      PinotConfiguration missingStorage = fixture.config();
      missingStorage.setProperty(Server.CONFIG_OF_INSTANCE_DATA_DIR, "");
      HelixServerStarter failed = new HelixServerStarter();
      assertThrows(IllegalArgumentException.class, () -> failed.init(missingStorage));
      failed.stop();
      assertThrows(IllegalStateException.class, failed::start);
      PinotConfiguration wrongIdentity = fixture.config();
      wrongIdentity.setProperty(Server.CONFIG_OF_INSTANCE_ID, "other-server");
      assertThrows(IllegalArgumentException.class, () -> new HelixServerStarter().init(wrongIdentity));
      PinotConfiguration unknownBackend = fixture.config();
      unknownBackend.setProperty("pinot.coordination.backend", "rtais");
      assertThrows(IllegalArgumentException.class, () -> new HelixServerStarter().init(unknownBackend));
      HelixServerStarter initialized = new HelixServerStarter();
      initialized.init(fixture.config());
      initialized.stop();
      assertFalse(Files.exists(fixture._directory.resolve("data")));
      assertTrue(connections.constructed().isEmpty());
      helix.verifyNoInteractions();
    }
  }

  private static final class Fixture implements AutoCloseable {
    private static final String INSTANCE_ID = "Server_native_starter";
    private final Path _directory = Files.createTempDirectory("native-starter-");
    private final UUID _history = UUID.randomUUID();
    private final UUID _activation = UUID.randomUUID();
    private final AtomicLong _index = new AtomicLong(1);
    private volatile MetadataRpc.ParticipantToken _token;
    private volatile MetadataRpc.ParticipantSession _drained;
    private volatile MetadataRpc.ParticipantSession _unregistered;
    private volatile boolean _ready;
    private volatile boolean _rejectRegistration;
    private volatile int _registeredEndpoints;
    private boolean _checkAdminBeforeRegistration;
    private Map<String, String> _endpoints;

    private Fixture() throws IOException {
      // These readable placeholders are never parsed: this fixture controls the metadata channel boundary.
      Files.writeString(_directory.resolve("tls.pem"), "fixture");
    }

    private PinotConfiguration config() {
      return new PinotConfiguration(Map.ofEntries(
          Map.entry("pinot.coordination.backend", "RATIS"),
          Map.entry("pinot.coordination.ratis.historyId", _history.toString()),
          Map.entry("pinot.coordination.ratis.participantId", INSTANCE_ID),
          Map.entry("pinot.coordination.ratis.metadata.endpoints", "localhost:1"),
          Map.entry("pinot.coordination.ratis.tls.certChain", _directory.resolve("tls.pem").toString()),
          Map.entry("pinot.coordination.ratis.tls.privateKey", _directory.resolve("tls.pem").toString()),
          Map.entry("pinot.coordination.ratis.tls.trustCerts", _directory.resolve("tls.pem").toString()),
          Map.entry("pinot.coordination.ratis.heartbeatMillis", 10),
          Map.entry("pinot.coordination.ratis.server.snapshotMillis", 10),
          Map.entry(Server.CONFIG_OF_INSTANCE_ID, INSTANCE_ID),
          Map.entry(Server.CONFIG_OF_INSTANCE_DATA_DIR, _directory.resolve("data").toString()),
          Map.entry(Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR, _directory.resolve("tar").toString())));
    }

    private MockedConstruction<NativeMetadataConnection> connections() {
      return mockConstruction(NativeMetadataConnection.class, (connection, context) -> {
        when(connection.membershipRead(any(), anyLong())).thenAnswer(ignored -> response(ResultStatus.NOT_FOUND));
        when(connection.membershipExecute(any(), anyLong())).thenAnswer(invocation ->
            membership(invocation.getArgument(0)));
        when(connection.snapshot(any(), anyLong())).thenAnswer(ignored -> snapshot());
        when(connection.execute(any(), anyLong())).thenAnswer(ignored -> MetadataRpc.CommandResult.newBuilder()
            .setStatus(ResultStatus.OK).setClientEpoch(1).setRevision(revision()).build());
      });
    }

    private MetadataRpc.MembershipResponse membership(MembershipCommandRequest request) throws Exception {
      switch (request.getCommandCase()) {
        case REGISTER:
          _endpoints = request.getRegister().getEndpointsMap();
          if (_checkAdminBeforeRegistration) {
            assertEquals(get(_endpoints.get("adminHttp") + "/tables/native_OFFLINE/size"), 503);
          }
          if (_rejectRegistration) {
            return response(ResultStatus.CONFLICT);
          }
          _token = MetadataRpc.ParticipantToken.newBuilder().setHistoryId(_history.toString())
              .setParticipantId(INSTANCE_ID).setRole(MetadataRpc.ParticipantRole.SERVER)
              .setIncarnation(request.getRegister().getIncarnation())
              .setRegistrationEpoch(request.getRegister().getExpectedEpoch() + 1).build();
          _registeredEndpoints = request.getRegister().getEndpointsCount();
          _ready = false;
          return response(ResultStatus.OK).toBuilder().setParticipant(participant()).build();
        case READY:
          _ready = request.getReady().getReady();
          return response(ResultStatus.OK).toBuilder().setParticipant(participant()).build();
        case RENEW:
          return response(ResultStatus.OK).toBuilder().addRenewals(MetadataRpc.RenewalResult.newBuilder()
              .setParticipant(_token).setSequence(request.getRenew().getSequence()).setStatus(ResultStatus.OK)).build();
        case DRAIN:
          _drained = request.getDrain();
          return response(ResultStatus.OK);
        case UNREGISTER:
          _unregistered = request.getUnregister();
          return response(ResultStatus.OK);
        default:
          throw new AssertionError("Unexpected membership operation: " + request.getCommandCase());
      }
    }

    private MetadataRpc.ParticipantState participant() {
      return MetadataRpc.ParticipantState.newBuilder().setParticipant(_token).setReady(_ready).setExpiryRound(100)
          .setLastModifiedIndex(1).setAppliedRevision(MetadataRpc.Revision.newBuilder()
              .setHistoryId(_history.toString()).setIndex(1)).build();
    }

    private MetadataRpc.InstallationSnapshot snapshot() {
      return MetadataRpc.InstallationSnapshot.newBuilder().setStatus(ResultStatus.OK).setRevision(revision())
          .setParticipant(participant()).setControl(MetadataRpc.MembershipControl.newBuilder()
              .setClockBarrierIndex(1).setMinimumTickDelayMillis(100).setMaxConfiguredPermitLifetimeMillis(100)
              .setControlEpoch(MetadataRpc.ControlToken.newBuilder().setHistoryId(_history.toString()).setRaftTerm(1)
                  .setBarrierIndex(1).setLeaderId("controller").setActivationId(_activation.toString()))
              .setPolicy(MetadataRpc.MembershipPolicy.newBuilder().setVersion(1).setMinTickIntervalMillis(100)
                  .setLeaseRounds(100).setPermitLifetimeMillis(100).setMaxQueryLifetimeMillis(100)
                  .setMaxRevisionLag(1000))).build();
    }

    private MetadataRpc.MembershipResponse response(ResultStatus status) {
      return MetadataRpc.MembershipResponse.newBuilder().setStatus(status).setRevision(revision()).build();
    }

    private MetadataRpc.Revision revision() {
      return MetadataRpc.Revision.newBuilder().setHistoryId(_history.toString()).setIndex(_index.incrementAndGet())
          .build();
    }

    @Override
    public void close() throws IOException {
      FileUtils.deleteDirectory(_directory.toFile());
    }
  }
}
