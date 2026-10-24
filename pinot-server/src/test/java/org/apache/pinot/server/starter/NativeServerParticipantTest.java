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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.MembershipCommandRequest;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.MembershipResponse;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.spi.env.PinotConfiguration;
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
    private final Path _directory = Files.createTempDirectory("native-participant-");
    private final NativeServerParticipant.Rpc _membership = mock(NativeServerParticipant.Rpc.class);
    private final NativeServerParticipant.Rpc _work = mock(NativeServerParticipant.Rpc.class);
    private final LinkedBlockingQueue<RenewalCall> _renewals = new LinkedBlockingQueue<>(4);
    private final CountDownLatch _lookupEntered = new CountDownLatch(1);
    private final CountDownLatch _releaseLookup = new CountDownLatch(1);
    private final AtomicLong _index = new AtomicLong(1);
    private final AtomicLong _snapshotCount = new AtomicLong();
    private final boolean _queryCapability;
    private final NativeServerParticipant _server;
    private volatile MetadataRpc.ParticipantToken _token;
    private volatile MetadataRpc.ParticipantSession _drained;
    private volatile MetadataRpc.ParticipantSession _unregistered;
    private volatile boolean _ready;
    private volatile MetadataRpc.Revision _applied;

    private Fixture(boolean queryCapability) throws Exception {
      _queryCapability = queryCapability;
      _applied = revision(0);
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
          5000, 10, 10, 1, 2), _membership, _work);
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
      return response(ResultStatus.OK).toBuilder().addRenewals(MetadataRpc.RenewalResult.newBuilder()
          .setParticipant(_token).setSequence(request.getRenew().getSequence()).setStatus(status)).build();
    }

    private MetadataRpc.InstallationSnapshot snapshot() {
      long index = _index.incrementAndGet();
      MetadataRpc.InstallationSnapshot snapshot = MetadataRpc.InstallationSnapshot.newBuilder()
          .setStatus(ResultStatus.OK).setRevision(revision(index)).setParticipant(participant())
          .setControl(MetadataRpc.MembershipControl.newBuilder().setClockBarrierIndex(1)
              .setMinimumTickDelayMillis(100).setMaxConfiguredPermitLifetimeMillis(100)
              .setControlEpoch(MetadataRpc.ControlToken.newBuilder().setHistoryId(_history.toString())
                  .setRaftTerm(1).setBarrierIndex(1).setLeaderId("controller").setActivationId(_activation.toString()))
              .setPolicy(MetadataRpc.MembershipPolicy.newBuilder().setVersion(1).setMinTickIntervalMillis(100)
                  .setLeaseRounds(100).setPermitLifetimeMillis(100).setMaxQueryLifetimeMillis(100)
                  .setMaxRevisionLag(1000))).build();
      _snapshotCount.incrementAndGet();
      return snapshot;
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
      _server.close();
      FileUtils.deleteDirectory(_directory.toFile());
    }
  }
}
