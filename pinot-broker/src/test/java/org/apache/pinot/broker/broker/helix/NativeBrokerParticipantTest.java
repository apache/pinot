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
package org.apache.pinot.broker.broker.helix;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.broker.routing.NativeBrokerQueryAdmission.Admission;
import org.apache.pinot.common.coordination.MetadataRpcConnection;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.common.coordination.QueryDeadlineGuard;
import org.apache.pinot.common.coordination.ServingPermitGuard.ClockReading;
import org.apache.pinot.common.coordination.ServingPermitGuard.QualifiedLifetimePolicy;
import org.apache.pinot.common.request.context.NativeQueryDeadline;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Covers isolated membership/routing lanes and exact renewal recovery without opening query or metadata listeners.
public class NativeBrokerParticipantTest {
  @Test(timeOut = 10_000)
  public void testNormalStarterBypassesZookeeperAndLeavesQueriesUnavailable() throws Exception {
    try (Fixture fixture = new Fixture()) {
      HelixBrokerStarter starter = new HelixBrokerStarter() {
        @Override
        NativeBrokerParticipant createNativeParticipant(NativeMetadataConfig config) {
          return fixture._participant;
        }
      };
      // No ZK configuration and no query ports are supplied. The mock avoids opening TLS connections in this unit test.
      starter.init(fixture._configuration);
      assertEquals(starter.getInstanceId(), fixture._id);
      assertNull(starter._listenerConfigs);
      starter.start();
      try {
        assertNull(starter._spectatorHelixManager);
        assertNull(starter._participantHelixManager);
        assertNull(starter._routingManager);
        assertNull(starter._brokerRequestHandler);
        assertNull(starter._brokerAdminApplication);
        assertNull(starter._brokerGrpcServer);
        assertEquals(ServiceStatus.getServiceStatus(fixture._id), ServiceStatus.Status.BAD);
        assertTrue(ServiceStatus.getStatusDescription(fixture._id).contains("query listeners disabled"));
        assertTrue(fixture._requests.stream().noneMatch(MetadataRpc.MembershipCommandRequest::hasReady));
        assertEquals(fixture._requests.getFirst().getRegister().getEndpointsCount(), 0);
        assertThrows(IllegalArgumentException.class,
            () -> new MultiClusterHelixBrokerStarter().init(fixture._configuration));
        HelixBrokerStarter initializedOnly = new HelixBrokerStarter();
        initializedOnly.init(fixture._configuration);
        initializedOnly.stop();
        assertEquals(ServiceStatus.getServiceStatus(fixture._id), ServiceStatus.Status.BAD);
        NativeBrokerParticipant rejected = mock(NativeBrokerParticipant.class);
        doThrow(new IOException("Registration rejected")).when(rejected).start();
        HelixBrokerStarter duplicate = new HelixBrokerStarter() {
          @Override
          NativeBrokerParticipant createNativeParticipant(NativeMetadataConfig config) {
            return rejected;
          }
        };
        duplicate.init(fixture._configuration);
        assertThrows(IOException.class, duplicate::start);
        duplicate.stop();
        assertTrue(ServiceStatus.getStatusDescription(fixture._id).contains("query listeners disabled"));
      } finally {
        starter.stop();
      }
      assertEquals(ServiceStatus.getServiceStatus(fixture._id), ServiceStatus.Status.NOT_STARTED);
      fixture.assertOwnUnregister();
    }
    try (Fixture fixture = new Fixture()) {
      HelixBrokerStarter starter = new HelixBrokerStarter() {
        @Override
        NativeBrokerParticipant createNativeParticipant(NativeMetadataConfig config) {
          return fixture._participant;
        }
      };
      starter.init(fixture._configuration);
      starter.start();
      ServiceStatus.setServiceStatusCallback(fixture._id,
          new ServiceStatus.LifecycleServiceStatusCallback(() -> false, () -> false));
      try {
        starter.stop();
        assertEquals(ServiceStatus.getServiceStatus(fixture._id), ServiceStatus.Status.GOOD,
            "An old process must not remove a replacement's status callback");
      } finally {
        starter.stop();
        ServiceStatus.removeServiceStatusCallback(fixture._id);
      }
    }
    HelixBrokerStarter invalid = new HelixBrokerStarter();
    assertThrows(IllegalArgumentException.class,
        () -> invalid.init(new PinotConfiguration(Map.of("pinot.coordination.backend", "RATIS"))));
    assertThrows(IllegalStateException.class, invalid::start);
    invalid.stop();
    assertNull(invalid._participantHelixManager);
  }

  @Test(timeOut = 10_000)
  public void testUnknownRenewalRetriesAndConsumedNotReadySequence() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture._participant.start();
      fixture._renewals.add(Status.DEADLINE_EXCEEDED.asRuntimeException());
      fixture._renewals.add("outer-not-ready");
      fixture._renewals.add(ResultStatus.NOT_READY);
      fixture._renewals.add(ResultStatus.OK);
      fixture._renewals.add(ResultStatus.FENCED);
      for (int i = 0; i < 5; i++) {
        fixture._participant.heartbeat();
      }
      List<MetadataRpc.MembershipCommandRequest> renewals = fixture._requests.stream()
          .filter(MetadataRpc.MembershipCommandRequest::hasRenew).toList();
      assertEquals(renewals.size(), 5);
      assertEquals(renewals.get(0), renewals.get(1), "An outer rejection must preserve the exact pending request");
      assertEquals(renewals.get(1), renewals.get(2), "A lost reply must not change the applied revision or sequence");
      assertEquals(renewals.get(2).getRenew().getSequence(), 1L);
      assertEquals(renewals.get(3).getRenew().getSequence(), 2L, "A durable inner NOT_READY consumes its sequence");
      assertEquals(renewals.get(4).getRenew().getSequence(), 3L);
      assertTrue(renewals.get(3).getRenew().getAppliedRevision().getIndex()
          > renewals.getFirst().getRenew().getAppliedRevision().getIndex());
      int requests = fixture._requests.size();
      fixture._participant.heartbeat();
      assertEquals(fixture._requests.size(), requests, "Fenced sessions must stop renewing");
      assertEquals(fixture._participant.getServiceStatus(), ServiceStatus.Status.BAD);
    }
  }

  @Test(timeOut = 10_000)
  public void testUnexpectedAuthorityAndReplacementFenceTheConfirmedSession() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture._grantCapabilities = true;
      assertThrows(IllegalArgumentException.class, fixture._participant::start);
      fixture.assertOwnUnregister();
      verify(fixture._connection).close();
    }
    try (Fixture fixture = new Fixture()) {
      fixture._participant.start();
      MetadataRpc.ParticipantToken original = fixture._token;
      fixture._token = original.toBuilder().setIncarnation(UUID.randomUUID().toString())
          .setRegistrationEpoch(original.getRegistrationEpoch() + 1).build();
      fixture._participantModifiedIndex = ++fixture._index;
      fixture._participant.heartbeat();
      assertFalse(fixture._requests.stream().anyMatch(MetadataRpc.MembershipCommandRequest::hasRenew));
      fixture._participant.close();
      MetadataRpc.MembershipCommandRequest last = fixture._requests.getLast();
      assertTrue(last.hasUnregister());
      assertEquals(last.getUnregister().getIncarnation(), original.getIncarnation());
      assertEquals(last.getUnregister().getRegistrationEpoch(), original.getRegistrationEpoch());
    }
    try (Fixture fixture = new Fixture()) {
      fixture._participant.start();
      fixture._renewals.add("unexpected-permit");
      fixture._participant.heartbeat();
      int requests = fixture._requests.size();
      fixture._participant.heartbeat();
      assertEquals(fixture._requests.size(), requests);
      assertTrue(fixture._participant.getStatusDescription().contains("unexpected authority"));
    }
  }

  @Test(timeOut = 10_000)
  public void testRoutingFailureInvalidatesImagesWithoutClaimingGlobalApplication() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture._participant.start();
      fixture._participant.refreshRouting();
      long routingIndex = fixture._participant.routingImage("orders_OFFLINE").revision().getIndex();
      // A newer routing read completes while the heartbeat's earlier own-membership read is still in flight.
      fixture._afterReadAction = fixture._participant::refreshRouting;
      fixture._participant.heartbeat();
      fixture._afterReadAction = () -> { };
      var renewal = fixture._requests.getLast().getRenew();
      assertEquals(renewal.getAppliedRevision().getIndex(), fixture._lastOwnReadIndex);
      assertTrue(renewal.getAppliedRevision().getIndex()
          < fixture._participant.routingImage("orders_OFFLINE").revision().getIndex(),
          "A later routing read must not rewrite the already captured own-membership applied revision");
      fixture._routingFailure = true;
      fixture._participant.refreshRouting();
      assertThrows(IllegalStateException.class, () -> fixture._participant.routingImage("orders_OFFLINE"));
      fixture._routingFailure = false;
      fixture._participant.refreshRouting();
      assertTrue(fixture._participant.routingImage("orders_OFFLINE").revision().getIndex() > routingIndex);
      fixture._routingTables = List.of();
      fixture._participant.refreshRouting();
      assertThrows(IllegalStateException.class, () -> fixture._participant.routingImage("orders_OFFLINE"));
      assertEquals(fixture._participant.getServiceStatus(), ServiceStatus.Status.BAD);
      assertTrue(fixture._requests.stream().noneMatch(MetadataRpc.MembershipCommandRequest::hasReady));
    }
  }

  @Test(timeOut = 10_000)
  public void testSlowRoutingCannotBlockHeartbeatOrRepublishAnInvalidatedImage() throws Exception {
    try (Fixture fixture = new Fixture(); var executor = Executors.newFixedThreadPool(2)) {
      fixture._participant.start();
      fixture._participant.refreshRouting();
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      doAnswer(invocation -> {
        MetadataRpc.RoutingSnapshotRequest request = invocation.getArgument(0);
        if (!request.getTableName().isEmpty()) {
          entered.countDown();
          assertTrue(release.await(3, TimeUnit.SECONDS));
        }
        return fixture.routingResponse(request);
      }).when(fixture._routingConnection).routingSnapshot(any(), anyLong());
      var refresh = executor.submit(fixture._participant::refreshRouting);
      try {
        assertTrue(entered.await(1, TimeUnit.SECONDS));
        fixture._renewals.add("outer-not-ready");
        executor.submit(fixture._participant::heartbeat).get(1, TimeUnit.SECONDS);
        assertThrows(IllegalStateException.class, () -> fixture._participant.routingImage("orders_OFFLINE"));
      } finally {
        release.countDown();
      }
      refresh.get(1, TimeUnit.SECONDS);
      assertThrows(IllegalStateException.class, () -> fixture._participant.routingImage("orders_OFFLINE"));
      fixture._participant.heartbeat();
      fixture._participant.refreshRouting();
      assertEquals(fixture._participant.routingImage("orders_OFFLINE").segmentCount(), 0);
    }
  }

  @Test(timeOut = 10_000)
  public void testExplicitServingCapturesPlanningAndRenewalCannotExtendOldAdmission() throws Exception {
    try (Fixture fixture = new Fixture(true)) {
      fixture._participant.start();
      fixture._participant.refreshRouting();
      assertThrows(IllegalStateException.class, fixture::admit);
      fixture._participant.heartbeat();
      fixture._participant.refreshRouting();
      var original = fixture.admit();
      long originalBudget = fixture._participant.remainingMillis(original);
      assertTrue(originalBudget > 0 && originalBudget <= 300);
      assertThrows(IllegalArgumentException.class, () -> fixture._participant.remainingMillis(mock(Admission.class)));
      assertEquals(original.requests().size(), 1);
      assertEquals(original.requests().getFirst().context().requiredNames(), List.of("segment"));
      assertEquals(original.planningMetadata(false).tableCache().getSchema("orders").getSchemaName(), "orders");
      assertTrue(fixture._requests.stream().anyMatch(MetadataRpc.MembershipCommandRequest::hasReady));
      assertEquals(fixture._requests.getFirst().getRegister().getEndpointsCount(), 0);
      AtomicInteger delivered = new AtomicInteger();
      fixture._participant.deliverQuery(original, delivered::incrementAndGet);
      assertEquals(delivered.get(), 1);
      fixture._millis.set(50);
      fixture._participant.heartbeat();
      fixture._participant.refreshRouting();
      fixture._participant.checkQueryDelivery(original);
      assertTrue(fixture._participant.remainingMillis(original) < originalBudget);
      var renewed = fixture.admit();
      fixture._millis.set(201);
      assertThrows(IllegalStateException.class,
          () -> fixture._participant.deliverQuery(original, delivered::incrementAndGet));
      assertThrows(IllegalStateException.class, () -> fixture._participant.remainingMillis(original));
      assertEquals(delivered.get(), 1, "Rejected delivery must never invoke the callback");
      fixture._participant.deliverQuery(renewed, delivered::incrementAndGet);
      assertEquals(delivered.get(), 2);
      assertEquals(fixture._participant.getServiceStatus(), ServiceStatus.Status.BAD,
          "Explicit authority does not start or report a query listener");
    }
    try (Fixture fixture = new Fixture(true)) {
      fixture.startServing();
      NativeQueryDeadline beforePlanning = fixture._participant.originateDeadline(100);
      fixture._millis.set(101);
      fixture._participant.heartbeat();
      fixture._participant.refreshRouting();
      assertThrows(IllegalStateException.class, () -> fixture._participant.admitQuery("orders_OFFLINE",
          UUID.randomUUID(), UUID.randomUUID(), beforePlanning));
      fixture._participant.checkQueryDelivery(fixture.admit());
    }
  }

  @Test(timeOut = 10_000)
  public void testPostRenewalReadClosesAuthorityWithoutBlockingLocalChecks() throws Exception {
    try (Fixture fixture = new Fixture(true); var executor = Executors.newFixedThreadPool(2)) {
      fixture.startServing();
      var original = fixture.admit();
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      fixture._postRenewalReadAction = () -> awaitRelease(entered, release);
      var heartbeat = executor.submit(fixture._participant::heartbeat);
      try {
        assertTrue(entered.await(1, TimeUnit.SECONDS));
        executor.submit(() -> assertThrows(IllegalStateException.class,
            () -> fixture._participant.checkQueryDelivery(original))).get(1, TimeUnit.SECONDS);
        executor.submit(() -> assertThrows(IllegalStateException.class, fixture::admit)).get(1, TimeUnit.SECONDS);
      } finally {
        release.countDown();
      }
      heartbeat.get(1, TimeUnit.SECONDS);
      fixture._participant.refreshRouting();
      fixture._participant.checkQueryDelivery(fixture.admit());
      List<Long> sequences = fixture._requests.stream().filter(MetadataRpc.MembershipCommandRequest::hasRenew)
          .map(request -> request.getRenew().getSequence()).toList();
      assertEquals(sequences, List.of(1L, 2L));
    }
    try (Fixture fixture = new Fixture(true)) {
      fixture.startServing();
      var original = fixture.admit();
      fixture._postRenewalReadAction = () -> {
        throw Status.UNAVAILABLE.asRuntimeException();
      };
      fixture._participant.heartbeat();
      assertThrows(IllegalStateException.class, () -> fixture._participant.checkQueryDelivery(original));
      fixture._postRenewalReadAction = () -> { };
      fixture._participant.heartbeat();
      fixture._participant.refreshRouting();
      fixture._participant.checkQueryDelivery(fixture.admit());
      assertThrows(IllegalStateException.class, () -> fixture._participant.checkQueryDelivery(original));
      assertEquals(fixture._requests.stream().filter(MetadataRpc.MembershipCommandRequest::hasRenew)
          .map(request -> request.getRenew().getSequence()).toList(), List.of(1L, 2L, 3L),
          "A failed follow-up read must not replay an already consumed renewal");
    }
  }

  @Test(timeOut = 10_000)
  public void testBlockedMembershipRpcDoesNotBlockDeliveryAndCloseFencesBeforeUnregister() throws Exception {
    try (Fixture fixture = new Fixture(true); var executor = Executors.newFixedThreadPool(3)) {
      fixture.startServing();
      var admission = fixture.admit();
      fixture._unregisterAction = () -> assertThrows(IllegalStateException.class,
          () -> fixture._participant.checkQueryDelivery(admission));
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      fixture._readAction = () -> awaitRelease(entered, release);
      var heartbeat = executor.submit(fixture._participant::heartbeat);
      try {
        assertTrue(entered.await(1, TimeUnit.SECONDS));
        AtomicInteger delivered = new AtomicInteger();
        executor.submit(() -> fixture._participant.deliverQuery(admission, delivered::incrementAndGet))
            .get(1, TimeUnit.SECONDS);
        assertEquals(delivered.get(), 1, "Delivery must not acquire the RPC-holding lifecycle monitor");
        var close = executor.submit(fixture._participant::close);
        // The status projection is local and must become closed even while heartbeat owns the lifecycle monitor.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (fixture._participant.getServiceStatus() != ServiceStatus.Status.SHUTTING_DOWN
            && System.nanoTime() < deadline) {
          Thread.onSpinWait();
        }
        assertEquals(fixture._participant.getServiceStatus(), ServiceStatus.Status.SHUTTING_DOWN);
        executor.submit(() -> assertThrows(IllegalStateException.class,
            () -> fixture._participant.deliverQuery(admission, delivered::incrementAndGet)))
            .get(1, TimeUnit.SECONDS);
        release.countDown();
        close.get(1, TimeUnit.SECONDS);
        assertEquals(delivered.get(), 1);
      } finally {
        release.countDown();
      }
      heartbeat.get(1, TimeUnit.SECONDS);
      fixture.assertOwnUnregister();
    }
  }

  @Test(timeOut = 10_000)
  public void testPeerLeaseStartsBeforeRoutingRpcAndCannotBeExtendedByRefresh() throws Exception {
    try (Fixture fixture = new Fixture(true)) {
      fixture._participant.start();
      fixture._participant.heartbeat();
      fixture._serverExpiryRound = 2;
      fixture._routingAdvanceMillis = 101;
      fixture._participant.refreshRouting();
      // A delayed response cannot restart the server's one-full-round lease budget.
      assertThrows(IllegalStateException.class, fixture::admit);
      fixture._routingAdvanceMillis = 0;
      fixture._serverExpiryRound = 3;
      fixture._participant.refreshRouting();
      fixture._participant.checkQueryDelivery(fixture.admit());
    }
    try (Fixture fixture = new Fixture(true)) {
      fixture._serverExpiryRound = 2;
      fixture.startServing();
      var original = fixture.admit();
      fixture._millis.set(50);
      fixture._participant.heartbeat();
      fixture._serverExpiryRound = 3;
      fixture._participant.refreshRouting();
      var refreshed = fixture.admit();
      fixture._millis.set(101);
      assertThrows(IllegalStateException.class, () -> fixture._participant.checkQueryDelivery(original));
      fixture._participant.checkQueryDelivery(refreshed);
    }
  }

  @Test(timeOut = 10_000)
  public void testRoutingRemovalAndFailureCannotResurrectOldAdmissions() throws Exception {
    try (Fixture fixture = new Fixture(true)) {
      fixture.startServing();
      var original = fixture.admit();
      fixture._routingTables = List.of();
      fixture._participant.refreshRouting();
      assertThrows(IllegalStateException.class, () -> fixture._participant.checkQueryDelivery(original));
      fixture._routingTables = List.of("orders_OFFLINE");
      fixture._participant.refreshRouting();
      var replacement = fixture.admit();
      assertThrows(IllegalStateException.class, () -> fixture._participant.checkQueryDelivery(original));
      fixture._routingFailure = true;
      fixture._participant.refreshRouting();
      fixture._routingFailure = false;
      fixture._participant.refreshRouting();
      assertThrows(IllegalStateException.class, () -> fixture._participant.checkQueryDelivery(replacement));
      var beforeEndpointChange = fixture.admit();
      String endpoint = "grpcs://server-replaced:8090";
      var changed = fixture._table.toBuilder();
      changed.getSegmentsBuilder(0).getReplicasBuilder(0).setEndpoint(endpoint);
      changed.getServerAuthoritiesBuilder(0).putEndpoints("queryGrpc", endpoint);
      fixture._table = changed.build();
      fixture._participant.refreshRouting();
      assertThrows(IllegalStateException.class, () -> fixture._participant.checkQueryDelivery(beforeEndpointChange));
      fixture._participant.checkQueryDelivery(fixture.admit());
    }
  }

  @Test(timeOut = 10_000)
  public void testLostGrantKeepsFirstSendAndOldControlReplayCannotRestoreAuthority() throws Exception {
    try (Fixture fixture = new Fixture(true)) {
      fixture.startServing();
      fixture._millis.set(50);
      fixture._renewals.add("lost-grant");
      fixture._participant.heartbeat();
      fixture._millis.set(240);
      fixture._participant.heartbeat();
      fixture._participant.refreshRouting();
      var admitted = fixture.admit();
      var requests = fixture._requests.stream().filter(MetadataRpc.MembershipCommandRequest::hasRenew).toList();
      assertEquals(requests.get(1), requests.get(2), "Retry must keep the original sequence and applied revision");
      fixture._millis.set(251);
      assertThrows(IllegalStateException.class, () -> fixture._participant.checkQueryDelivery(admitted));
      assertThrows(IllegalStateException.class, fixture::admit);
    }
    try (Fixture fixture = new Fixture(true)) {
      fixture.startServing();
      var original = fixture.admit();
      fixture._renewals.add("lost-grant");
      fixture._participant.heartbeat();
      long barrier = fixture._index + 1;
      fixture._control = fixture._control.toBuilder().setClockBarrierIndex(barrier)
          .setControlEpoch(fixture._control.getControlEpoch().toBuilder().setRaftTerm(2).setBarrierIndex(barrier)
              .setActivationId(UUID.randomUUID().toString())).build();
      fixture._participant.heartbeat();
      fixture._participant.refreshRouting();
      assertThrows(IllegalStateException.class, fixture::admit);
      assertThrows(IllegalStateException.class, () -> fixture._participant.checkQueryDelivery(original));
      fixture._participant.heartbeat();
      fixture._participant.refreshRouting();
      fixture._participant.checkQueryDelivery(fixture.admit());
      assertThrows(IllegalStateException.class, () -> fixture._participant.checkQueryDelivery(original));
    }
  }

  @Test(timeOut = 10_000)
  public void testDrainPreservesFiniteDeliveryButConsumedNotReadyRevokesOldAdmissions() throws Exception {
    try (Fixture fixture = new Fixture(true)) {
      fixture.startServing();
      var original = fixture.admit();
      fixture._ready = false;
      fixture._draining = true;
      fixture._participantModifiedIndex = ++fixture._index;
      fixture._participant.heartbeat();
      fixture._participant.refreshRouting();
      assertThrows(IllegalStateException.class, fixture::admit);
      fixture._participant.checkQueryDelivery(original);
      fixture._millis.set(201);
      assertThrows(IllegalStateException.class, () -> fixture._participant.checkQueryDelivery(original));
    }
    try (Fixture fixture = new Fixture(true)) {
      fixture.startServing();
      var original = fixture.admit();
      fixture._renewals.add(ResultStatus.NOT_READY);
      fixture._participant.heartbeat();
      assertThrows(IllegalStateException.class, fixture::admit);
      fixture._participant.heartbeat();
      fixture._participant.refreshRouting();
      fixture._participant.checkQueryDelivery(fixture.admit());
      assertThrows(IllegalStateException.class, () -> fixture._participant.checkQueryDelivery(original));
    }
  }

  private static void awaitRelease(CountDownLatch entered, CountDownLatch release) {
    entered.countDown();
    try {
      assertTrue(release.await(3, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final String _history = UUID.randomUUID().toString();
    private final String _id = "Broker_native_" + UUID.randomUUID();
    private final Path _tls = Files.createTempFile("native-broker-test-", ".pem");
    private final MetadataRpcConnection _connection = mock(MetadataRpcConnection.class);
    private final MetadataRpcConnection _routingConnection = mock(MetadataRpcConnection.class);
    private final PinotConfiguration _configuration;
    private final NativeBrokerParticipant _participant;
    private final List<MetadataRpc.MembershipCommandRequest> _requests = new ArrayList<>();
    private final Deque<Object> _renewals = new ArrayDeque<>();
    private final Map<Long, MetadataRpc.ServingPermit> _grants = new HashMap<>();
    private final AtomicLong _millis = new AtomicLong();
    private final boolean _serving;
    private MetadataRpc.TableRouting _table;
    private MetadataRpc.MembershipControl _control;
    private MetadataRpc.ParticipantToken _token;
    private long _index;
    private long _appliedIndex;
    private long _participantModifiedIndex;
    private long _lastOwnReadIndex;
    private long _renewalSequence;
    private List<String> _routingTables = List.of("orders_OFFLINE");
    private boolean _routingFailure;
    private boolean _grantCapabilities;
    private boolean _ready;
    private boolean _draining;
    private long _serverExpiryRound = 3;
    private long _routingAdvanceMillis;
    private boolean _followupRead;
    private Runnable _readAction = () -> { };
    private Runnable _afterReadAction = () -> { };
    private Runnable _postRenewalReadAction = () -> { };
    private Runnable _unregisterAction = () -> { };

    private Fixture() throws Exception {
      this(false);
    }

    private Fixture(boolean serving) throws Exception {
      _serving = serving;
      _grantCapabilities = serving;
      _configuration = new PinotConfiguration(Map.of("pinot.coordination.backend", "RATIS",
          NativeMetadataConfig.PREFIX + "historyId", _history,
          NativeMetadataConfig.PREFIX + "participantId", _id,
          NativeMetadataConfig.PREFIX + "metadata.endpoints", "localhost:10000",
          NativeMetadataConfig.PREFIX + "tls.certChain", _tls.toString(),
          NativeMetadataConfig.PREFIX + "tls.privateKey", _tls.toString(),
          NativeMetadataConfig.PREFIX + "tls.trustCerts", _tls.toString(),
          NativeMetadataConfig.PREFIX + "heartbeatMillis", 30_000));
      _control = MetadataRpc.MembershipControl.newBuilder().setControlEpoch(MetadataRpc.ControlToken.newBuilder()
              .setHistoryId(_history).setRaftTerm(1).setBarrierIndex(1).setLeaderId("controller")
              .setActivationId(UUID.randomUUID().toString())).setClockBarrierIndex(1)
          .setMinimumTickDelayMillis(100).setMaxConfiguredPermitLifetimeMillis(200)
          .setPolicy(MetadataRpc.MembershipPolicy.newBuilder().setVersion(1).setMinTickIntervalMillis(100)
              .setLeaseRounds(3).setPermitLifetimeMillis(200).setMaxQueryLifetimeMillis(300).setMaxRevisionLag(1000))
          .build();
      _table = planningTable();
      when(_connection.membershipRead(any(), anyLong())).thenAnswer(invocation -> {
        _readAction.run();
        if (_followupRead) {
          _followupRead = false;
          _postRenewalReadAction.run();
        }
        MetadataRpc.MembershipResponse.Builder response =
            response(_token == null ? ResultStatus.NOT_FOUND : ResultStatus.OK);
        if (_token != null) {
          response.setParticipant(state());
        }
        _lastOwnReadIndex = response.getRevision().getIndex();
        _afterReadAction.run();
        return response.build();
      });
      when(_connection.membershipExecute(any(), anyLong())).thenAnswer(invocation -> {
        MetadataRpc.MembershipCommandRequest request = invocation.getArgument(0);
        _requests.add(request);
        if (request.hasRegister()) {
          _token = MetadataRpc.ParticipantToken.newBuilder().setHistoryId(_history).setParticipantId(_id)
              .setRole(MetadataRpc.ParticipantRole.BROKER).setIncarnation(request.getRegister().getIncarnation())
              .setRegistrationEpoch(request.getRegister().getExpectedEpoch() + 1).build();
          var response = response(ResultStatus.OK);
          _participantModifiedIndex = _index;
          return response.setParticipant(state()).build();
        }
        if (request.hasRenew()) {
          Object next = _renewals.isEmpty() ? ResultStatus.OK : _renewals.removeFirst();
          if (next instanceof RuntimeException failure) {
            throw failure;
          }
          if (next.equals("outer-not-ready")) {
            return response(ResultStatus.NOT_READY).build();
          }
          MetadataRpc.RenewalResult.Builder renewal = MetadataRpc.RenewalResult.newBuilder()
              .setParticipant(_token).setSequence(request.getRenew().getSequence());
          var response = response(ResultStatus.OK);
          if (next.equals("unexpected-permit")) {
            renewal.setStatus(ResultStatus.OK).setPermit(MetadataRpc.ServingPermit.getDefaultInstance());
          } else {
            renewal.setStatus(next.equals("lost-grant") ? ResultStatus.OK : (ResultStatus) next);
            if (_serving && renewal.getStatus() == ResultStatus.OK) {
              long sequence = request.getRenew().getSequence();
              var grant = _grants.get(sequence);
              if (grant == null && _ready && !_draining) {
                grant = MetadataRpc.ServingPermit.newBuilder().setParticipant(_token).setRenewalSequence(sequence)
                      .setControlEpoch(_control.getControlEpoch()).setPolicyVersion(_control.getPolicy().getVersion())
                      .setMinAppliedRevision(response.getRevision()).setPermitLifetimeMillis(200)
                      .setMaxQueryLifetimeMillis(300).addCapabilities(MetadataRpc.ServingCapability.BROKER_QUERY)
                      .build();
                _grants.put(sequence, grant);
              }
              if (grant != null) {
                renewal.setPermit(grant);
              }
            }
          }
          if (request.getRenew().getSequence() > _renewalSequence) {
            if (renewal.getStatus() == ResultStatus.OK) {
              _appliedIndex = request.getRenew().getAppliedRevision().getIndex();
            }
            _renewalSequence = request.getRenew().getSequence();
            _participantModifiedIndex = _index;
          }
          _followupRead = _serving;
          if (next.equals("lost-grant")) {
            throw Status.DEADLINE_EXCEEDED.asRuntimeException();
          }
          return response.addRenewals(renewal).build();
        }
        if (request.hasReady()) {
          assertTrue(_serving, "Default brokers must not set ready");
          assertEquals(request.getReady().getSession().getIncarnation(), _token.getIncarnation());
          _ready = request.getReady().getReady();
          _appliedIndex = request.getReady().getAppliedRevision().getIndex();
          var response = response(ResultStatus.OK);
          _participantModifiedIndex = _index;
          return response.setParticipant(state()).build();
        }
        assertTrue(request.hasUnregister(), "Membership-only brokers must never set ready or advertise endpoints");
        _unregisterAction.run();
        return response(ResultStatus.OK).build();
      });
      when(_routingConnection.routingSnapshot(any(), anyLong())).thenAnswer(invocation -> {
        if (_routingFailure) {
          throw Status.UNAVAILABLE.asRuntimeException();
        }
        return routingResponse(invocation.getArgument(0));
      });
      var config = new NativeMetadataConfig(_configuration);
      _participant = serving ? new NativeBrokerParticipant(config, _connection, _routingConnection,
          new NativeBrokerParticipant.ServingSettings(
              () -> new ClockReading(TimeUnit.MILLISECONDS.toNanos(_millis.get()), 0, true),
              new QualifiedLifetimePolicy(200, 300, 1_000_000, 0, 0),
              new QueryDeadlineGuard(() -> new QueryDeadlineGuard.TimeInterval(
                  1000 + _millis.get(), 1000 + _millis.get(), true), "test-clock", 300, 0, 1_000_000)))
          : new NativeBrokerParticipant(config, _connection, _routingConnection);
    }

    private MetadataRpc.RoutingSnapshot routingResponse(MetadataRpc.RoutingSnapshotRequest request) {
      assertEquals(request.getSession().getIncarnation(), _token.getIncarnation());
      assertEquals(request.getSession().getRegistrationEpoch(), _token.getRegistrationEpoch());
      var response = MetadataRpc.RoutingSnapshot.newBuilder().setStatus(ResultStatus.OK).setControl(_control)
          .setParticipant(state()).setRevision(MetadataRpc.Revision.newBuilder().setHistoryId(_history)
              .setIndex(++_index));
      if (request.getTableName().isEmpty()) {
        response.addAllTables(_routingTables);
      } else if (_serving) {
        _millis.addAndGet(_routingAdvanceMillis);
        response.setTable(_table.toBuilder().setServerAuthorities(0,
            _table.getServerAuthorities(0).toBuilder().setExpiryRound(_serverExpiryRound)
                .setLastRenewalSequence(_renewalSequence).setLastModifiedIndex(_index)));
      } else {
        response.setTable(MetadataRpc.TableRouting.newBuilder().setTableName(request.getTableName())
            .setSchemaName("orders")
            .setTable(MetadataRpc.ObjectVersion.newBuilder().setHistoryId(_history)
                .setObjectId(UUID.randomUUID().toString()).setVersion(1))
            .setSchema(MetadataRpc.ObjectVersion.newBuilder().setHistoryId(_history)
                .setObjectId(UUID.randomUUID().toString()).setVersion(1)));
      }
      return response.build();
    }

    private MetadataRpc.MembershipResponse.Builder response(ResultStatus status) {
      return MetadataRpc.MembershipResponse.newBuilder().setStatus(status).setControl(_control)
          .setRevision(MetadataRpc.Revision.newBuilder().setHistoryId(_history).setIndex(++_index));
    }

    private MetadataRpc.ParticipantState state() {
      MetadataRpc.ParticipantState.Builder state = MetadataRpc.ParticipantState.newBuilder().setParticipant(_token)
          .setExpiryRound(3).setLastModifiedIndex(_participantModifiedIndex).setReady(_ready).setDraining(_draining)
          .setLastRenewalSequence(_renewalSequence)
          .setAppliedRevision(MetadataRpc.Revision.newBuilder().setHistoryId(_history).setIndex(_appliedIndex));
      if (_grantCapabilities) {
        state.addCapabilities(MetadataRpc.ServingCapability.BROKER_QUERY);
      }
      return state.build();
    }

    private MetadataRpc.TableRouting planningTable() throws IOException {
      var server = MetadataRpc.ParticipantToken.newBuilder().setHistoryId(_history).setParticipantId("Server_native")
          .setRole(MetadataRpc.ParticipantRole.SERVER).setIncarnation(UUID.randomUUID().toString())
          .setRegistrationEpoch(1).build();
      var table = object();
      var schema = object();
      var replica = MetadataRpc.ReplicaRouting.newBuilder().setServer(server).setAssignment(object())
          .setAssignmentEpoch(1).setMaterializationId(UUID.randomUUID().toString())
          .setInstallationId(UUID.randomUUID().toString()).setEndpoint("grpcs://server-native:8090");
      return MetadataRpc.TableRouting.newBuilder().setTableName("orders_OFFLINE").setSchemaName("orders")
          .setTable(table).setSchema(schema)
          .setTableConfig(record(table, "orders_OFFLINE", MetadataRpc.RecordKind.TABLE_CONFIG,
              MetadataRpc.CatalogValue.newBuilder().setTableConfigJson(ByteString.copyFrom(JsonUtils.objectToBytes(
                  new TableConfigBuilder(TableType.OFFLINE).setTableName("orders").build()))).build()))
          .setSchemaRecord(record(schema, "orders", MetadataRpc.RecordKind.SCHEMA,
              MetadataRpc.CatalogValue.newBuilder().setSchemaJson(ByteString.copyFrom(JsonUtils.objectToBytes(
                  new Schema.SchemaBuilder().setSchemaName("orders")
                      .addSingleValueDimension("id", DataType.INT).build()))).build()))
          .addSegments(MetadataRpc.SegmentRouting.newBuilder().setName("segment").setContent(object())
              .setContentSha256("a".repeat(64)).addReplicas(replica))
          .addServerAuthorities(MetadataRpc.ParticipantState.newBuilder().setParticipant(server).setReady(true)
              .setExpiryRound(3).setLastModifiedIndex(1)
              .setAppliedRevision(MetadataRpc.Revision.newBuilder().setHistoryId(_history).setIndex(1))
              .addCapabilities(MetadataRpc.ServingCapability.SERVER_QUERY)
              .putEndpoints("queryGrpc", "grpcs://server-native:8090").putEndpoints("nativeQueryVersion", "2"))
          .build();
    }

    private MetadataRpc.ObjectVersion object() {
      return MetadataRpc.ObjectVersion.newBuilder().setHistoryId(_history)
          .setObjectId(UUID.randomUUID().toString()).setVersion(1).build();
    }

    private MetadataRpc.CatalogRecord record(MetadataRpc.ObjectVersion identity, String name,
        MetadataRpc.RecordKind kind, MetadataRpc.CatalogValue value) {
      return MetadataRpc.CatalogRecord.newBuilder().setKind(kind).setName(name).setObjectId(identity.getObjectId())
          .setVersion(1).setSchemaVersion(1).setLastModifiedIndex(1).setValue(value).build();
    }

    private void startServing() throws IOException {
      _participant.start();
      _participant.heartbeat();
      _participant.refreshRouting();
    }

    private Admission admit() {
      return _participant.admitQuery("orders_OFFLINE", UUID.randomUUID(), UUID.randomUUID(),
          _participant.originateDeadline(300));
    }

    private void assertOwnUnregister() {
      MetadataRpc.MembershipCommandRequest last = _requests.getLast();
      assertTrue(last.hasUnregister());
      assertEquals(last.getUnregister().getIncarnation(), _token.getIncarnation());
      assertEquals(last.getUnregister().getRegistrationEpoch(), _token.getRegistrationEpoch());
    }

    @Override
    public void close() throws Exception {
      _participant.close();
      Files.deleteIfExists(_tls);
    }
  }
}
