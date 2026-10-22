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
package org.apache.pinot.server.starter;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.coordination.QueryDeadlineGuard;
import org.apache.pinot.common.coordination.QueryDeadlineGuard.TimeInterval;
import org.apache.pinot.common.coordination.ServingPermitGuard.ClockReading;
import org.apache.pinot.common.coordination.ServingPermitGuard.QualifiedLifetimePolicy;
import org.apache.pinot.common.coordination.ServingPermitGuard.ServingDeniedException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.common.request.context.NativeQueryContext.ProcessIdentity;
import org.apache.pinot.common.request.context.NativeQueryGate;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.MembershipCommandRequest;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.MembershipResponse;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.server.starter.NativeServerParticipant.QueryAdmission;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises asynchronous participant retry, session fencing and independent renewal admission with real local
/// directory ownership. Controlled wire responses distinguish unknown outcomes from consumed renewal sequences.
public class NativeServerParticipantTest {
  private static final String PEER_FINGERPRINT = "a".repeat(64);

  @Test(timeOut = 15_000)
  public void testRenewalRetriesExactRequestUntilOutcomeIsConsumed() throws Exception {
    try (Fixture fixture = new Fixture(false)) {
      fixture.start();
      RenewalCall lost = fixture.nextRenewal();
      assertEquals(lost._request.getRenew().getSequence(), 1);
      long snapshots = fixture._snapshotCount.get();
      await(() -> fixture._snapshotCount.get() >= snapshots + 2);
      lost._response.completeExceptionally(new IOException("Lost renewal reply after remote commit"));
      RenewalCall retry = fixture.nextRenewal();
      assertEquals(retry._request, lost._request,
          "A newer local image must not rewrite a renewal whose outcome is unknown");
      retry._response.complete(fixture.response(ResultStatus.NOT_READY));
      RenewalCall unconsumed = fixture.nextRenewal();
      assertEquals(unconsumed._request, lost._request, "Outer NOT_READY did not consume a renewal sequence");
      unconsumed._response.complete(fixture.renewed(unconsumed._request, ResultStatus.NOT_READY));
      RenewalCall next = fixture.nextRenewal();
      assertEquals(next._request.getRenew().getSequence(), 2,
          "Durable inner NOT_READY consumes the sequence and requires a fresh renewal");
      assertTrue(next._request.getRenew().getAppliedRevision().getIndex()
          > lost._request.getRenew().getAppliedRevision().getIndex());
      next._response.complete(fixture.renewed(next._request, ResultStatus.OK));
      await(fixture._server::isReadyForInstallations);
    }
  }

  @Test(timeOut = 15_000)
  public void testBlockedWorkKeepsRenewingAndOuterFenceClosesAdmission() throws Exception {
    try (Fixture fixture = new Fixture(false)) {
      fixture.start();
      RenewalCall first = fixture.nextRenewal();
      first._response.complete(fixture.renewed(first._request, ResultStatus.OK));
      await(fixture._server::isReadyForInstallations);
      assertTrue(fixture._server.wake(UUID.randomUUID()));
      assertTrue(fixture._lookupEntered.await(5, TimeUnit.SECONDS));
      RenewalCall independent = fixture.nextRenewal();
      assertEquals(independent._request.getRenew().getSequence(), 2);
      independent._response.complete(fixture.renewed(independent._request, ResultStatus.OK));
      RenewalCall fenced = fixture.nextRenewal();
      assertEquals(fenced._request.getRenew().getSequence(), 3);
      assertEquals(fixture._releaseLookup.getCount(), 1L, "Membership must progress while work is blocked");
      fenced._response.complete(fixture.response(ResultStatus.FENCED));
      await(() -> fixture._server.failure() != null
          && fixture._server.failure().contains("expired or history changed"));
      assertFalse(fixture._server.isReadyForInstallations());
      assertFalse(fixture._server.wake(UUID.randomUUID()));
      assertTrue(fixture._server.failure().contains("expired or history changed"));
      assertNull(fixture._renewals.poll(100, TimeUnit.MILLISECONDS), "A fenced registration must stop renewing");
      fixture._releaseLookup.countDown();
    }
  }

  @Test(timeOut = 15_000)
  public void testRejectedQueryCapabilitiesRetireConfirmedRegistrationAndReleaseDirectory() throws Exception {
    try (Fixture fixture = new Fixture(true)) {
      IOException rejected = expectThrows(IOException.class, fixture::start);
      assertTrue(rejected.getMessage().contains("query-serving capabilities"));
      assertFalse(fixture._server.isReadyForInstallations());
      assertEquals(fixture._drained, fixture.session());
      assertEquals(fixture._unregistered, fixture.session());
      verify(fixture._membership).close();
      verify(fixture._work).close();
      NativeInstanceDataManager replacement = new NativeInstanceDataManager();
      try {
        replacement.initWithMetadata(fixture.config(), table -> {
          throw new AssertionError("No table is assigned in this test");
        }, ServerMetrics.get(), null, mock(ServerReloadJobStatusCache.class));
      } finally {
        replacement.shutDown();
      }
    }
  }

  @Test(timeOut = 15_000)
  public void testClockQualificationLossFencesRegistrationInsteadOfRetryingRenewals() throws Exception {
    try (Fixture fixture = new Fixture(true, true)) {
      fixture.start();
      RenewalCall first = fixture.nextRenewal();
      fixture.freezeSnapshots();
      fixture._clockTrusted.set(false);
      first._response.complete(fixture.renewed(first._request, ResultStatus.OK));
      await(() -> fixture._server.failure() != null
          && fixture._server.failure().contains("Native serving authority failed"));
      assertFalse(fixture._server.isReadyForInstallations());
      assertFalse(fixture._server.wake(UUID.randomUUID()));
      // Restoring a provider's apparent health cannot revive this registration or restart logical sequences.
      fixture._clockTrusted.set(true);
      fixture.resumeSnapshots();
      assertNull(fixture._renewals.poll(100, TimeUnit.MILLISECONDS));
      assertTrue(fixture._server.failure().contains("Native serving authority failed"), fixture._server.failure());
    }
  }

  @Test(timeOut = 15_000)
  public void testKnownRenewalOutcomeRequiresCoherentImageAndRetryKeepsFirstSendDeadline() throws Exception {
    try (Fixture fixture = new Fixture(true, true)) {
      fixture.start();
      RenewalCall first = fixture.nextRenewal();
      first._response.complete(fixture.renewed(first._request, ResultStatus.OK));
      QueryAdmission prior = fixture.admission();

      RenewalCall noGrant = fixture.nextRenewal();
      fixture.freezeSnapshots();
      fixture._server.checkQueryDelivery(prior);
      MembershipResponse consumed = fixture.renewed(noGrant._request, ResultStatus.NOT_READY);
      noGrant._response.complete(consumed);
      RenewalCall lost = fixture.nextRenewal();
      assertEquals(lost._request.getRenew().getSequence(), 3, "Known outcome must have been consumed");
      assertTrue(consumed.getRevision().getIndex() > lost._request.getRenew().getAppliedRevision().getIndex());
      expectThrows(ServingDeniedException.class, () -> fixture._server.checkQueryDelivery(prior));
      expectThrows(ServingDeniedException.class, () -> fixture._server.admitQuery(fixture.query(), PEER_FINGERPRINT));

      fixture.resumeSnapshots();
      fixture.admission();
      fixture.freezeSnapshots();
      fixture._server.checkQueryDelivery(prior);

      // Sequence 3 was first sent at zero. An unknown outcome must not move that deadline to its retry time.
      fixture._clockNanos.set(TimeUnit.MILLISECONDS.toNanos(50));
      lost._response.completeExceptionally(new IOException("Renewal transport timeout after possible commit"));
      RenewalCall retry = fixture.nextRenewal();
      assertEquals(retry._request, lost._request);
      fixture._clockNanos.set(TimeUnit.MILLISECONDS.toNanos(90));
      retry._response.complete(fixture.renewed(retry._request, ResultStatus.OK));
      RenewalCall pending = fixture.nextRenewal();
      assertEquals(pending._request.getRenew().getSequence(), 4);
      fixture.resumeSnapshots();
      QueryAdmission renewed = fixture.admission();
      fixture.freezeSnapshots();
      fixture._server.checkQueryDelivery(renewed);
      fixture._clockNanos.set(TimeUnit.MILLISECONDS.toNanos(101));
      expectThrows(ServingDeniedException.class, () -> fixture._server.checkQueryDelivery(renewed));
      expectThrows(ServingDeniedException.class, () -> fixture._server.admitQuery(fixture.query(), PEER_FINGERPRINT));
    }
  }

  @Test(timeOut = 15_000)
  public void testOldControlRenewalReplayAllowsFreshRenewalWithoutRestoringAuthority() throws Exception {
    try (Fixture fixture = new Fixture(true, true)) {
      fixture.start();
      RenewalCall first = fixture.nextRenewal();
      first._response.complete(fixture.renewed(first._request, ResultStatus.OK));
      QueryAdmission prior = fixture.admission();
      RenewalCall lost = fixture.nextRenewal();
      MembershipResponse committed = fixture.renewed(lost._request, ResultStatus.OK);
      lost._response.completeExceptionally(new IOException("Lost successful renewal before leadership changed"));
      RenewalCall retry = fixture.nextRenewal();
      assertEquals(retry._request, lost._request);

      fixture.freezeSnapshots();
      fixture.advanceControl();
      long snapshots = fixture._snapshotCount.get();
      fixture.resumeSnapshots();
      await(() -> fixture._snapshotCount.get() >= snapshots + 2);
      fixture.freezeSnapshots();
      ServingDeniedException changed = expectThrows(ServingDeniedException.class,
          () -> fixture._server.checkQueryDelivery(prior));
      assertEquals(changed.getMessage(), "Serving control epoch changed");

      // The new leader returns the original durable grant inside a response at its current revision.
      retry._response.complete(fixture.response(ResultStatus.OK).toBuilder()
          .addAllRenewals(committed.getRenewalsList()).build());
      RenewalCall current = fixture.nextRenewal();
      assertEquals(current._request.getRenew().getSequence(), lost._request.getRenew().getSequence() + 1);
      expectThrows(ServingDeniedException.class, () -> fixture._server.admitQuery(fixture.query(), PEER_FINGERPRINT));
      current._response.complete(fixture.renewed(current._request, ResultStatus.OK));
      fixture.resumeSnapshots();
      QueryAdmission admitted = fixture.admission();
      fixture.freezeSnapshots();
      fixture._server.checkQueryDelivery(admitted);
      expectThrows(ServingDeniedException.class, () -> fixture._server.checkQueryDelivery(prior));
      assertTrue(fixture._server.isReadyForInstallations());
    }
  }

  @Test(timeOut = 15_000)
  public void testDefinitiveRenewalConflictDoesNotRetryOrSkipSequence() throws Exception {
    try (Fixture fixture = new Fixture(false)) {
      fixture.start();
      RenewalCall lost = fixture.nextRenewal();
      lost._response.completeExceptionally(new IOException("Lost reply before a capability policy change"));
      RenewalCall retry = fixture.nextRenewal();
      assertEquals(retry._request, lost._request);
      retry._response.complete(fixture.renewed(retry._request, ResultStatus.CONFLICT));
      await(() -> fixture._server.failure() != null
          && fixture._server.failure().contains("unusable durable outcome: CONFLICT"));
      assertFalse(fixture._server.isReadyForInstallations());
      assertNull(fixture._renewals.poll(100, TimeUnit.MILLISECONDS),
          "A consumed conflict cannot be repaired by retrying or skipping to a new sequence");
    }
  }

  @Test(timeOut = 15_000)
  public void testQueryDeadlineSurvivesTransitAndFencesDeliveryBeforePermitExpiry() throws Exception {
    try (Fixture fixture = new Fixture(true, true)) {
      fixture.start();
      RenewalCall first = fixture.nextRenewal();
      first._response.complete(fixture.renewed(first._request, ResultStatus.OK));
      fixture.admission();
      fixture.freezeSnapshots();
      expectThrows(IllegalStateException.class, () -> fixture._server.admitQuery(fixture.identity(), PEER_FINGERPRINT));
      expectThrows(IllegalStateException.class, () -> fixture._server.admitQuery(fixture.query()));
      expectThrows(IllegalStateException.class, () -> fixture._server.admitQuery(fixture.query(), "b".repeat(64)));

      NativeQueryContext context = fixture.identity().withDeadline(fixture._queryDeadlines.originate(10));
      QueryAdmission admitted = fixture._server.admitQuery(context, PEER_FINGERPRINT);
      fixture._wallMillis.addAndGet(11);
      fixture._clockNanos.set(TimeUnit.MILLISECONDS.toNanos(11));
      expectThrows(ServingDeniedException.class, () -> fixture._server.admitQuery(context, PEER_FINGERPRINT));
      expectThrows(ServingDeniedException.class, () -> fixture._server.checkQueryDelivery(admitted));

      // A fresh query can use the still-valid permit, but retries of the expired query cannot reset its deadline.
      fixture._server.checkQueryDelivery(fixture._server.admitQuery(fixture.query(), PEER_FINGERPRINT));
      expectThrows(ServingDeniedException.class, () -> fixture._server.admitQuery(context, PEER_FINGERPRINT));
    }
  }

  @Test(timeOut = 15_000)
  public void testQueryDeliverySerializesRevocationAndCannotReviveAfterRegrant() throws Exception {
    try (Fixture fixture = new Fixture(true, true)) {
      fixture.start();
      RenewalCall first = fixture.nextRenewal();
      first._response.complete(fixture.renewed(first._request, ResultStatus.OK));
      QueryAdmission prior = fixture.admission();
      fixture.freezeSnapshots();
      NativeQueryGate.Delivery gate = fixture._server.queryGate().admit(fixture.query(), PEER_FINGERPRINT);
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      CountDownLatch publication = new CountDownLatch(1);
      AtomicBoolean armed = new AtomicBoolean();
      fixture._server.clusterConfig().registerClusterConfigChangeListener((changed, values) -> {
        if (armed.get()) {
          publication.countDown();
        }
      });
      CompletableFuture<Void> delivery = CompletableFuture.runAsync(() -> gate.deliver(() -> {
        entered.countDown();
        try {
          assertTrue(release.await(5, TimeUnit.SECONDS), "Query delivery was not released");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }));
      MetadataRpc.QueryBrokerAuthority authority = fixture._brokerAuthority;
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        armed.set(true);
        fixture._brokerAuthority = null;
        long snapshots = fixture._snapshotCount.get();
        fixture.resumeSnapshots();
        await(() -> fixture._snapshotCount.get() > snapshots);
        assertFalse(publication.await(100, TimeUnit.MILLISECONDS),
            "Revocation publication cannot pass a callback holding the admission lock");
      } finally {
        release.countDown();
      }
      delivery.get(5, TimeUnit.SECONDS);
      assertTrue(publication.await(5, TimeUnit.SECONDS));
      fixture.freezeSnapshots();
      expectThrows(IllegalStateException.class, () -> fixture._server.checkQueryDelivery(prior));
      AtomicBoolean leaked = new AtomicBoolean();
      expectThrows(IllegalStateException.class, () -> gate.deliver(() -> leaked.set(true)));
      assertFalse(leaked.get(), "No result callback may run after certificate authority is revoked");

      fixture._brokerAuthority = authority;
      fixture.resumeSnapshots();
      QueryAdmission fresh = fixture.admission();
      fixture.freezeSnapshots();
      fixture._server.checkQueryDelivery(fresh);
      expectThrows(IllegalStateException.class, () -> fixture._server.checkQueryDelivery(prior));
      expectThrows(IllegalStateException.class, () -> gate.deliver(() -> leaked.set(true)));
      assertFalse(leaked.get(), "Restoring the grant cannot revive a prior delivery attempt");
      expectThrows(IllegalArgumentException.class, () -> fixture._server.deliverQuery(fresh, () -> {
        throw new IllegalArgumentException("Callback failure");
      }));
      fixture._server.checkQueryDelivery(fresh);
    }
  }

  @Test(timeOut = 15_000)
  public void testPeerLeaseConsumesSnapshotTransitAndRemainsBoundAcrossRenewal() throws Exception {
    try (Fixture fixture = new Fixture(true, true)) {
      fixture._round = 9;
      fixture._minTickMillis = 10;
      fixture._brokerAuthority = fixture._brokerAuthority.toBuilder().setParticipant(
          fixture._brokerAuthority.getParticipant().toBuilder().setExpiryRound(11)).build();
      fixture.start();
      RenewalCall first = fixture.nextRenewal();
      first._response.complete(fixture.renewed(first._request, ResultStatus.OK));
      QueryAdmission original = fixture.admission();
      fixture.freezeSnapshots();
      // The request is already in flight. Its response leaves one full 10ms tick interval at the captured round.
      fixture._clockNanos.set(TimeUnit.MILLISECONDS.toNanos(11));
      fixture._wallMillis.addAndGet(11);
      assertTrue(fixture.resumeOneSnapshot().await(5, TimeUnit.SECONDS));
      assertTrue(fixture._server.isReadyForInstallations(), "Peer expiry must not break installation hydration");
      ServingDeniedException expired = expectThrows(ServingDeniedException.class,
          () -> fixture._server.admitQuery(fixture.query(), PEER_FINGERPRINT));
      assertTrue(expired.getMessage().contains("Peer membership lease"));
      expectThrows(ServingDeniedException.class, () -> fixture._server.checkQueryDelivery(original));

      fixture._brokerAuthority = fixture._brokerAuthority.toBuilder().setParticipant(
          fixture._brokerAuthority.getParticipant().toBuilder().setExpiryRound(100).setLastRenewalSequence(1)).build();
      assertTrue(fixture.resumeOneSnapshot().await(5, TimeUnit.SECONDS));
      QueryAdmission renewed = fixture._server.admitQuery(fixture.query(), PEER_FINGERPRINT);
      fixture._server.checkQueryDelivery(renewed);
      expectThrows(ServingDeniedException.class, () -> fixture._server.checkQueryDelivery(original));
      // No further snapshot or heartbeat response arrives; only the peer's original local bound expires.
      fixture._clockNanos.set(TimeUnit.MILLISECONDS.toNanos(21));
      fixture._wallMillis.addAndGet(10);
      fixture._server.checkQueryDelivery(renewed);
      expectThrows(ServingDeniedException.class, () -> fixture._server.checkQueryDelivery(original));
    }
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertTrue(condition.getAsBoolean(), "Participant did not reach the expected state");
  }

  private static final class RenewalCall {
    private final MembershipCommandRequest _request;
    private final CompletableFuture<MembershipResponse> _response = new CompletableFuture<>();

    private RenewalCall(MembershipCommandRequest request) {
      _request = request;
    }
  }

  private static final class Fixture implements AutoCloseable {
    private static final String SERVER = "Server_test";
    private final UUID _history = UUID.randomUUID();
    private final UUID _activation = UUID.randomUUID();
    private final UUID _tableId = UUID.randomUUID();
    private final UUID _schemaId = UUID.randomUUID();
    private final UUID _brokerIncarnation = UUID.randomUUID();
    private final Path _directory = Files.createTempDirectory("native-participant-");
    private final NativeServerParticipant.Rpc _membership = mock(NativeServerParticipant.Rpc.class);
    private final NativeServerParticipant.Rpc _work = mock(NativeServerParticipant.Rpc.class);
    private final LinkedBlockingQueue<RenewalCall> _renewals = new LinkedBlockingQueue<>(4);
    private final CountDownLatch _lookupEntered = new CountDownLatch(1);
    private final CountDownLatch _releaseLookup = new CountDownLatch(1);
    private final AtomicLong _index = new AtomicLong(1);
    private final AtomicLong _snapshotCount = new AtomicLong();
    private final AtomicBoolean _clockTrusted = new AtomicBoolean(true);
    private final AtomicLong _clockNanos = new AtomicLong();
    private final AtomicLong _wallMillis = new AtomicLong(10_000);
    private final QueryDeadlineGuard _queryDeadlines = new QueryDeadlineGuard(
        () -> new TimeInterval(_wallMillis.get(), _wallMillis.get(), true), "test-clock", 100, 0, 1_000_000);
    private final List<MetadataRpc.CatalogRecord> _records;
    private final boolean _queryCapability;
    private final NativeServerParticipant _server;
    private volatile MetadataRpc.ControlToken _control = MetadataRpc.ControlToken.newBuilder()
        .setHistoryId(_history.toString()).setRaftTerm(1).setBarrierIndex(1).setLeaderId("controller")
        .setActivationId(_activation.toString()).build();
    private volatile MetadataRpc.ParticipantToken _token;
    private volatile MetadataRpc.ParticipantSession _drained;
    private volatile MetadataRpc.ParticipantSession _unregistered;
    private volatile boolean _ready;
    private volatile MetadataRpc.Revision _applied;
    private boolean _freezeSnapshots;
    private boolean _freezeAfterSnapshot;
    private CountDownLatch _snapshotBlocked;
    private long _round;
    private long _minTickMillis = 100;
    @Nullable
    private volatile MetadataRpc.QueryBrokerAuthority _brokerAuthority;

    private Fixture(boolean queryCapability) throws Exception {
      this(queryCapability, false);
    }

    private Fixture(boolean queryCapability, boolean serving) throws Exception {
      _queryCapability = queryCapability;
      _records = serving ? queryRecords() : List.of();
      _applied = revision(0);
      _brokerAuthority = MetadataRpc.QueryBrokerAuthority.newBuilder()
          .setParticipant(MetadataRpc.ParticipantState.newBuilder()
              .setParticipant(MetadataRpc.ParticipantToken.newBuilder().setHistoryId(_history.toString())
                  .setParticipantId("broker").setRole(MetadataRpc.ParticipantRole.BROKER)
                  .setIncarnation(_brokerIncarnation.toString()).setRegistrationEpoch(1))
              .setExpiryRound(100).setReady(true).setAppliedRevision(revision(1)).setLastModifiedIndex(1)
              .addCapabilities(MetadataRpc.ServingCapability.BROKER_QUERY))
          .addCertificateSha256(PEER_FINGERPRINT).addBrokerTenants("DefaultTenant").build();
      when(_membership.membershipRead(any(), anyLong())).thenAnswer(ignored -> response(ResultStatus.NOT_FOUND));
      when(_membership.membershipExecute(any(), anyLong())).thenAnswer(invocation ->
          membership(invocation.getArgument(0)));
      when(_work.snapshot(any(), anyLong())).thenAnswer(ignored -> snapshot());
      when(_work.execute(any(), anyLong())).thenAnswer(invocation -> {
        MetadataRpc.InstallationCommandRequest request = invocation.getArgument(0);
        assertTrue(request.hasRegister(), "No installation report is expected in this membership test");
        return MetadataRpc.CommandResult.newBuilder().setStatus(ResultStatus.OK).setClientEpoch(1)
            .setRevision(revision(_index.incrementAndGet())).build();
      });
      when(_work.lookup(any(), anyLong())).thenAnswer(ignored -> {
        _lookupEntered.countDown();
        if (!_releaseLookup.await(5, TimeUnit.SECONDS)) {
          throw new IOException("Work lane was not released");
        }
        return MetadataRpc.InstallationWorkResult.newBuilder().setStatus(ResultStatus.NOT_FOUND)
            .setRevision(revision(_index.incrementAndGet())).build();
      });
      _server = new NativeServerParticipant(new NativeServerParticipant.Settings(_history, SERVER, Map.of(),
          5000, 10, 10, 1, 2), _membership, _work, serving
          ? new NativeServerParticipant.ServingSettings(
              () -> new ClockReading(_clockNanos.get(), 0, _clockTrusted.get()),
              new QualifiedLifetimePolicy(100, 100, 1_000_000, 0, 0), _queryDeadlines) : null);
    }

    private PinotConfiguration config() {
      return new PinotConfiguration(Map.of("id", SERVER, "dataDir", _directory.resolve("data").toString(),
          "segmentTarDir", _directory.resolve("tar").toString(), "max.parallel.refresh.threads", 1));
    }

    private void start() throws Exception {
      _server.start(config(), ServerMetrics.get(), null, mock(ServerReloadJobStatusCache.class));
    }

    private MembershipResponse membership(MembershipCommandRequest request) throws Exception {
      return switch (request.getCommandCase()) {
        case REGISTER -> {
          _token = MetadataRpc.ParticipantToken.newBuilder().setHistoryId(_history.toString()).setParticipantId(SERVER)
              .setRole(MetadataRpc.ParticipantRole.SERVER).setIncarnation(request.getRegister().getIncarnation())
              .setRegistrationEpoch(request.getRegister().getExpectedEpoch() + 1).build();
          yield response(ResultStatus.OK).toBuilder().setParticipant(participant()).build();
        }
        case READY -> {
          _applied = request.getReady().getAppliedRevision();
          _ready = request.getReady().getReady();
          yield response(ResultStatus.OK).toBuilder().setParticipant(participant()).build();
        }
        case RENEW -> {
          RenewalCall call = new RenewalCall(request);
          _renewals.add(call);
          yield call._response.get(5, TimeUnit.SECONDS);
        }
        case DRAIN -> {
          _drained = request.getDrain();
          yield response(ResultStatus.OK);
        }
        case UNREGISTER -> {
          _unregistered = request.getUnregister();
          yield response(ResultStatus.OK);
        }
        default -> throw new AssertionError("Unexpected membership request");
      };
    }

    private RenewalCall nextRenewal() throws InterruptedException {
      RenewalCall call = _renewals.poll(5, TimeUnit.SECONDS);
      assertNotNull(call, "No renewal arrived: " + _server.failure());
      return call;
    }

    private MembershipResponse response(ResultStatus status) {
      return MembershipResponse.newBuilder().setStatus(status).setRevision(revision(_index.incrementAndGet())).build();
    }

    private MembershipResponse renewed(MembershipCommandRequest request, ResultStatus status) {
      MembershipResponse response = response(ResultStatus.OK);
      MetadataRpc.RenewalResult.Builder renewal = MetadataRpc.RenewalResult.newBuilder()
          .setParticipant(_token).setSequence(request.getRenew().getSequence()).setStatus(status);
      if (_queryCapability && status == ResultStatus.OK) {
        renewal.setPermit(MetadataRpc.ServingPermit.newBuilder().setParticipant(_token)
            .setRenewalSequence(request.getRenew().getSequence()).setControlEpoch(control())
            .setMinAppliedRevision(response.getRevision()).setPolicyVersion(1)
            .setPermitLifetimeMillis(100).setMaxQueryLifetimeMillis(100)
            .addCapabilities(MetadataRpc.ServingCapability.SERVER_QUERY));
      }
      return response.toBuilder().addRenewals(renewal).build();
    }

    private synchronized MetadataRpc.InstallationSnapshot snapshot() throws InterruptedException {
      while (_freezeSnapshots) {
        _snapshotBlocked.countDown();
        wait();
      }
      long index = _index.incrementAndGet();
      MetadataRpc.InstallationSnapshot.Builder snapshot = MetadataRpc.InstallationSnapshot.newBuilder()
          .setStatus(ResultStatus.OK).setRevision(revision(index)).setParticipant(participant()).addAllRecords(_records)
          .setControl(MetadataRpc.MembershipControl.newBuilder().setClockBarrierIndex(control().getBarrierIndex())
              .setRound(_round).setMinimumTickDelayMillis(_minTickMillis).setMaxConfiguredPermitLifetimeMillis(100)
              .setControlEpoch(control())
              .setPolicy(MetadataRpc.MembershipPolicy.newBuilder().setVersion(1)
                  .setMinTickIntervalMillis(_minTickMillis)
                  .setLeaseRounds(100).setPermitLifetimeMillis(100).setMaxQueryLifetimeMillis(100)
                  .setMaxRevisionLag(1000)));
      if (!_records.isEmpty() && _brokerAuthority != null) {
        snapshot.addQueryBrokers(_brokerAuthority);
      }
      _snapshotCount.incrementAndGet();
      if (_freezeAfterSnapshot) {
        _freezeAfterSnapshot = false;
        _freezeSnapshots = true;
      }
      return snapshot.build();
    }

    private void freezeSnapshots() throws InterruptedException {
      CountDownLatch blocked;
      synchronized (this) {
        _freezeSnapshots = true;
        blocked = new CountDownLatch(1);
        _snapshotBlocked = blocked;
      }
      assertTrue(blocked.await(5, TimeUnit.SECONDS), "Snapshot worker did not reach the controlled RPC barrier");
    }

    private synchronized void resumeSnapshots() {
      _freezeSnapshots = false;
      _freezeAfterSnapshot = false;
      notifyAll();
    }

    private synchronized CountDownLatch resumeOneSnapshot() {
      _freezeSnapshots = false;
      _freezeAfterSnapshot = true;
      _snapshotBlocked = new CountDownLatch(1);
      notifyAll();
      return _snapshotBlocked;
    }

    private QueryAdmission admission() throws InterruptedException {
      AtomicReference<QueryAdmission> admitted = new AtomicReference<>();
      await(() -> {
        if (admitted.get() != null) {
          return true;
        }
        try {
          admitted.set(_server.admitQuery(query(), PEER_FINGERPRINT));
          return true;
        } catch (IllegalStateException e) {
          return false;
        }
      });
      return admitted.get();
    }

    private NativeQueryContext query() {
      return identity().withDeadline(_queryDeadlines.originate(100));
    }

    private NativeQueryContext identity() {
      // Only exercises local admission; no loaded segment or normal query execution is claimed by this fixture.
      return new NativeQueryContext(1, _history, new ProcessIdentity("broker", _brokerIncarnation, 1),
          new Participant(SERVER, Role.SERVER, UUID.fromString(_token.getIncarnation()), _token.getRegistrationEpoch()),
          UUID.randomUUID(), UUID.randomUUID(), "nativeQuery_OFFLINE", new ObjectVersion(_tableId, 1), "nativeQuery",
          new ObjectVersion(_schemaId, 1), List.of(), List.of());
    }

    private List<MetadataRpc.CatalogRecord> queryRecords() throws IOException {
      TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName("nativeQuery").build();
      Schema schema = new Schema.SchemaBuilder().setSchemaName("nativeQuery")
          .addSingleValueDimension("value", DataType.INT).build();
      return List.of(MetadataRpc.CatalogRecord.newBuilder().setKind(MetadataRpc.RecordKind.TABLE_CONFIG)
              .setName(table.getTableName()).setObjectId(_tableId.toString()).setVersion(1).setSchemaVersion(1)
              .setLastModifiedIndex(1).setValue(MetadataRpc.CatalogValue.newBuilder()
                  .setTableConfigJson(ByteString.copyFrom(JsonUtils.objectToBytes(table)))).build(),
          MetadataRpc.CatalogRecord.newBuilder().setKind(MetadataRpc.RecordKind.SCHEMA)
              .setName(schema.getSchemaName()).setObjectId(_schemaId.toString()).setVersion(1).setSchemaVersion(1)
              .setLastModifiedIndex(1).setValue(MetadataRpc.CatalogValue.newBuilder()
                  .setSchemaJson(ByteString.copyFrom(JsonUtils.objectToBytes(schema)))).build());
    }

    private MetadataRpc.ControlToken control() {
      return _control;
    }

    private synchronized void advanceControl() {
      _control = control().toBuilder().setRaftTerm(control().getRaftTerm() + 1)
          .setBarrierIndex(_index.incrementAndGet()).setLeaderId("next-controller")
          .setActivationId(UUID.randomUUID().toString()).build();
    }

    private MetadataRpc.ParticipantState participant() {
      MetadataRpc.ParticipantState.Builder state = MetadataRpc.ParticipantState.newBuilder().setParticipant(_token)
          .setReady(_ready).setExpiryRound(100).setAppliedRevision(_applied).setLastModifiedIndex(1);
      if (_queryCapability) {
        state.addCapabilities(MetadataRpc.ServingCapability.SERVER_QUERY);
      }
      return state.build();
    }

    private MetadataRpc.ParticipantSession session() {
      return MetadataRpc.ParticipantSession.newBuilder().setIncarnation(_token.getIncarnation())
          .setRegistrationEpoch(_token.getRegistrationEpoch()).build();
    }

    private MetadataRpc.Revision revision(long index) {
      return MetadataRpc.Revision.newBuilder().setHistoryId(_history.toString()).setIndex(index).build();
    }

    @Override
    public void close() throws Exception {
      _releaseLookup.countDown();
      resumeSnapshots();
      _server.close();
      FileUtils.deleteDirectory(_directory.toFile());
    }
  }
}
