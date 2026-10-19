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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.CommandResult;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.InstallationCommandRequest;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.InstallationOutcomeRequest;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.InstallationReport;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ParticipantToken;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises durable report identity and recovery across lost responses without substituting an in-memory outbox.
public class InstallationOutboxTest {
  @Test
  public void testLostRegistrationReportAndAcknowledgementAcrossReopen() throws Exception {
    try (Fixture fixture = new Fixture()) {
      ModelRpc rpc = new ModelRpc();
      InstallationReport report = report(fixture._server);
      rpc._loseRegister = true;
      InstallationOutbox first = fixture.open(rpc);
      assertThrows(IOException.class, () -> first.publish(report));
      MetadataRpc.RegisterClient registration = rpc._requests.get(0).getRegister();

      fixture.reopen();
      InstallationOutbox registered = fixture.open(rpc);
      assertNull(registered.recover());
      assertEquals(rpc._requests.get(1).getRegister(), registration);
      rpc._loseReport = true;
      assertThrows(IOException.class, () -> registered.publish(report));
      InstallationCommandRequest submitted = rpc._requests.get(2);
      assertEquals(submitted.getReport().getSequence(), 1L);

      fixture.reopen();
      rpc._loseAck = true;
      InstallationOutbox resolved = fixture.open(rpc);
      assertThrows(IOException.class, resolved::recover);
      assertEquals(rpc._outcomes.size(), 1);
      InstallationCommandRequest acknowledgement = rpc._requests.get(3);

      fixture.reopen();
      InstallationOutbox complete = fixture.open(rpc);
      CommandResult result = complete.recover();
      assertEquals(result.getStatus(), ResultStatus.OK);
      assertEquals(rpc._requests.get(4), acknowledgement);
      assertEquals(rpc._outcomes.size(), 1, "A durable result survives an ack that already expired the remote outcome");
      assertEquals(complete.publish(report), result);
      assertEquals(rpc._requests.size(), 5, "An exact completed replay does not send a new report");
      InstallationReport conflicting = report.toBuilder().setInstallationId(UUID.randomUUID().toString()).build();
      assertThrows(IOException.class, () -> complete.publish(conflicting));
      assertEquals(rpc._requests.size(), 5);

      complete.publish(report(fixture._server));
      assertEquals(rpc._requests.get(5).getReport().getSequence(), 2L);
      assertEquals(rpc._requests.get(5).getReport().getClientEpoch(), 1L);
      assertEquals(rpc._requests.get(5).getReport().getStreamId(), submitted.getReport().getStreamId());
      assertEquals(rpc._committedReports, 2);
    }
  }

  @Test
  public void testExactRetryAndStaleOperationRejectionPreserveStreamWhileSessionRejectionFences() throws Exception {
    try (Fixture fixture = new Fixture()) {
      ModelRpc rpc = new ModelRpc();
      InstallationOutbox outbox = fixture.open(rpc);
      InstallationReport report = report(fixture._server);
      InstallationReport oversized = report.toBuilder().setInstalled(report.getInstalled().toBuilder()
          .setSegmentName("x".repeat(64 * 1024))).build();
      assertThrows(IOException.class, () -> outbox.publish(oversized));
      assertTrue(rpc._requests.isEmpty());

      rpc._dropReportBeforeCommit = true;
      assertThrows(IOException.class, () -> outbox.publish(report));
      InstallationCommandRequest first = rpc._requests.get(1);
      fixture.reopen();
      InstallationOutbox retry = fixture.open(rpc);
      assertEquals(retry.recover().getStatus(), ResultStatus.OK);
      assertEquals(rpc._requests.get(2), first);
      assertEquals(rpc._committedReports, 1);

      rpc._reportRejection = ResultStatus.FENCED;
      rpc._loseReport = true;
      assertThrows(IOException.class, () -> retry.publish(report(fixture._server)));
      fixture.reopen();
      rpc._loseAck = true;
      assertThrows(IOException.class, () -> fixture.open(rpc).recover());
      fixture.reopen();
      InstallationOutbox recovered = fixture.open(rpc);
      assertEquals(recovered.recover().getStatus(), ResultStatus.FENCED,
          "A proven per-operation rejection remains durable across a lost ack");
      rpc._reportRejection = ResultStatus.OK;
      assertEquals(recovered.publish(report(fixture._server)).getStatus(), ResultStatus.OK);
      assertEquals(rpc._requests.get(rpc._requests.size() - 2).getReport().getSequence(), 3L,
          "A stale operation must not poison the next operation's stream");

      rpc._sessionFenced = true;
      assertThrows(IOException.class, () -> recovered.publish(report(fixture._server)));
      int calls = rpc._requests.size();
      fixture.reopen();
      InstallationOutbox rejected = fixture.open(rpc);
      assertThrows(IOException.class, rejected::recover);
      assertThrows(IOException.class, () -> rejected.publish(report(fixture._server)));
      assertEquals(rpc._requests.size(), calls, "A rejected durable stream must not invent another sequence");
      assertEquals(rpc._requests.get(calls - 1).getReport().getSequence(), 4L);
    }
    try (Fixture fixture = new Fixture()) {
      ModelRpc rpc = new ModelRpc();
      InstallationOutbox outbox = fixture.open(rpc);
      rpc._loseReport = true;
      assertThrows(IOException.class, () -> outbox.publish(report(fixture._server)));
      rpc._outcomeStatus = ResultStatus.RESULT_EXPIRED;
      int calls = rpc._requests.size();
      assertThrows(IOException.class, outbox::recover);
      fixture.reopen();
      assertThrows(IOException.class, () -> fixture.open(rpc).recover());
      assertEquals(rpc._requests.size(), calls, "An expired unknown outcome must never be resubmitted or renumbered");
    }
  }

  @Test
  public void testSessionReplacementPreservesEvidenceAndCorruptOrLinkedStateFailsClosed() throws Exception {
    try (Fixture fixture = new Fixture()) {
      ModelRpc rpc = new ModelRpc();
      InstallationOutbox old = fixture.open(rpc);
      rpc._loseReport = true;
      assertThrows(IOException.class, () -> old.publish(report(fixture._server)));
      Path oldFile = fixture.stateFile();
      byte[] oldEvidence = Files.readAllBytes(oldFile);
      fixture._server = fixture._server.toBuilder().setIncarnation(UUID.randomUUID().toString())
          .setRegistrationEpoch(2).build();
      InstallationOutbox current = fixture.open(rpc);
      assertNull(current.recover());
      assertThrows(IOException.class, old::recover);
      assertEquals(Files.readAllBytes(oldFile), oldEvidence);
      assertEquals(rpc._committedReports, 1, "Opening a new SERVER session must not replay an old report");
      assertNotEquals(rpc._requests.get(0).getRegister(), rpc._requests.get(2).getRegister());
      current.publish(report(fixture._server));
      assertEquals(rpc._requests.get(3).getReport().getSequence(), 1L);
      assertEquals(rpc._requests.get(3).getSession().getRegistrationEpoch(), 2L);
      assertEquals(rpc._committedReports, 2);

      fixture._manager.shutDown();
      Path currentFile = fixture.stateFile();
      byte[] bytes = Files.readAllBytes(currentFile);
      bytes[0] ^= 1;
      Files.write(currentFile, bytes);
      fixture.reopen();
      int calls = rpc._requests.size();
      assertThrows(IOException.class, () -> fixture.open(rpc));
      assertEquals(rpc._requests.size(), calls);
      assertEquals(Files.readAllBytes(oldFile), oldEvidence);
    }
    try (Fixture fixture = new Fixture()) {
      Path outside = Files.createDirectory(fixture._root.resolve("outside"));
      Path directory = fixture._root.resolve("data/.installation-outbox");
      Files.createSymbolicLink(directory, outside);
      ModelRpc rpc = new ModelRpc();
      assertThrows(IOException.class, () -> fixture.open(rpc));
      assertTrue(rpc._requests.isEmpty());
      try (var entries = Files.list(outside)) {
        assertEquals(entries.count(), 0L);
      } finally {
        Files.delete(directory);
      }
    }
  }

  @Test(timeOut = 15_000)
  public void testCompetingCallDoesNotQueueAndShutdownRetainsDirectoryUntilRpcDrains() throws Exception {
    try (Fixture fixture = new Fixture()) {
      ModelRpc rpc = new ModelRpc();
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      InstallationOutbox.Rpc blocking = new InstallationOutbox.Rpc() {
        @Override
        public CommandResult execute(InstallationCommandRequest request, long timeoutMillis) throws Exception {
          if (request.hasReport()) {
            entered.countDown();
            assertTrue(release.await(timeoutMillis, TimeUnit.MILLISECONDS));
          }
          return rpc.execute(request, timeoutMillis);
        }

        @Override
        public CommandResult outcome(InstallationOutcomeRequest request, long timeoutMillis) {
          return rpc.outcome(request, timeoutMillis);
        }
      };
      InstallationOutbox outbox = fixture._manager.openInstallationOutbox(fixture._server, blocking, 5_000);
      FutureTask<CommandResult> publication = new FutureTask<>(() -> outbox.publish(report(fixture._server)));
      new Thread(publication, "outbox-publication").start();
      FutureTask<Void> shutdown = new FutureTask<>(() -> {
        fixture._manager.shutDown();
        return null;
      });
      NativeInstanceDataManager replacement = new NativeInstanceDataManager();
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertThrows(IOException.class, () -> outbox.publish(report(fixture._server)));
        new Thread(shutdown, "outbox-shutdown").start();
        // Directory ownership is still held while the bounded RPC is outstanding.
        assertThrows(IOException.class, () -> fixture.initialize(replacement));
        assertFalse(shutdown.isDone());
        release.countDown();
        assertEquals(publication.get(5, TimeUnit.SECONDS).getStatus(), ResultStatus.OK);
        shutdown.get(5, TimeUnit.SECONDS);
        fixture.initialize(replacement);
        assertThrows(IOException.class, outbox::recover);
      } finally {
        release.countDown();
        replacement.shutDown();
      }
    }
  }

  private static InstallationReport report(ParticipantToken server) {
    MetadataRpc.ObjectVersion table = version(server);
    MetadataRpc.ObjectVersion schema = version(server);
    MetadataRpc.OwnerToken owner = MetadataRpc.OwnerToken.newBuilder().setHistoryId(server.getHistoryId())
        .setLayoutGeneration(1).setScope(MetadataRpc.OwnershipScope.newBuilder().setTableShard(0)).setOwnerEpoch(1)
        .setParticipant(server.toBuilder().setParticipantId("controller")
            .setRole(MetadataRpc.ParticipantRole.CONTROLLER))
        .build();
    MetadataRpc.InstallationTarget target = MetadataRpc.InstallationTarget.newBuilder().setTableName("native_OFFLINE")
        .setTableObjectId(table.getObjectId()).setSegmentName("segment")
        .setSegmentObjectId(UUID.randomUUID().toString()).setContentSha256("a".repeat(64))
        .setAssignment(version(server))
        .setMaterializationId(UUID.randomUUID().toString())
        .setCompatibility(MetadataRpc.MaterializationCompatibility.newBuilder().setTable(table).setSchema(schema))
        .build();
    return InstallationReport.newBuilder().setOperationId(UUID.randomUUID().toString()).setAttempt(1).setOwner(owner)
        .setInstalled(target).setInstallationId(UUID.randomUUID().toString()).setManifestSha256("b".repeat(64)).build();
  }

  private static MetadataRpc.ObjectVersion version(ParticipantToken server) {
    return MetadataRpc.ObjectVersion.newBuilder().setHistoryId(server.getHistoryId())
        .setObjectId(UUID.randomUUID().toString()).setVersion(1).build();
  }

  private static final class Fixture implements AutoCloseable {
    private final Path _root = Files.createTempDirectory("installation-outbox-");
    private NativeInstanceDataManager _manager = new NativeInstanceDataManager();
    private ParticipantToken _server = ParticipantToken.newBuilder().setHistoryId(UUID.randomUUID().toString())
        .setParticipantId("server").setRole(MetadataRpc.ParticipantRole.SERVER)
        .setIncarnation(UUID.randomUUID().toString()).setRegistrationEpoch(1).build();

    private Fixture() throws Exception {
      initialize(_manager);
    }

    private void initialize(NativeInstanceDataManager manager) throws Exception {
      manager.initWithMetadata(new PinotConfiguration(Map.of("id", "server", "dataDir",
          _root.resolve("data").toString(),
          "segmentTarDir", _root.resolve("tar").toString(), "max.parallel.refresh.threads", 1)), name -> {
            throw new UnsupportedOperationException("No table loading in outbox tests");
          }, ServerMetrics.get(), null, mock(ServerReloadJobStatusCache.class));
    }

    private InstallationOutbox open(ModelRpc rpc) throws IOException {
      return _manager.openInstallationOutbox(_server, rpc, 5_000);
    }

    private void reopen() throws Exception {
      _manager.shutDown();
      _manager = new NativeInstanceDataManager();
      initialize(_manager);
    }

    private Path stateFile() {
      return _root.resolve("data/.installation-outbox/" + DigestUtils.sha256Hex(_server.toByteArray()) + ".state");
    }

    @Override
    public void close() throws Exception {
      _manager.shutDown();
      FileUtils.deleteDirectory(_root.toFile());
    }
  }

  /// Models the remote durable stream only; all local state and recovery use real fsynced outbox files.
  private static final class ModelRpc implements InstallationOutbox.Rpc {
    private final Map<String, Session> _sessions = new HashMap<>();
    private final List<InstallationCommandRequest> _requests = new ArrayList<>();
    private final List<InstallationOutcomeRequest> _outcomes = new ArrayList<>();
    private long _index;
    private int _committedReports;
    private boolean _loseRegister;
    private boolean _loseReport;
    private boolean _dropReportBeforeCommit;
    private boolean _loseAck;
    private boolean _sessionFenced;
    private ResultStatus _reportRejection = ResultStatus.OK;
    private ResultStatus _outcomeStatus = ResultStatus.RESULT_STATUS_UNSPECIFIED;

    @Override
    public CommandResult execute(InstallationCommandRequest request, long timeoutMillis) throws IOException {
      assertTrue(timeoutMillis > 0 && timeoutMillis <= 5_000);
      _requests.add(request);
      if (_sessionFenced) {
        return result(request.getHistoryId(), ResultStatus.FENCED);
      }
      Session session = _sessions.computeIfAbsent(request.getSession().getIncarnation(), ignored -> new Session());
      if (request.hasRegister()) {
        assertEquals(request.getRegister().getExpectedEpoch(), 0L);
        if (session._registration == null) {
          session._registration = request.getRegister();
        } else {
          assertEquals(request.getRegister(), session._registration);
        }
        if (_loseRegister) {
          _loseRegister = false;
          throw new IOException("Lost committed registration response");
        }
        return result(request.getHistoryId(), ResultStatus.OK).toBuilder().setClientEpoch(1).build();
      }
      assertTrue(session._registration != null);
      if (request.hasReport()) {
        assertEquals(request.getReport().getClientEpoch(), 1L);
        assertEquals(request.getReport().getSequence(), session._acknowledged + 1);
        if (_dropReportBeforeCommit) {
          _dropReportBeforeCommit = false;
          throw new IOException("Disconnected before report commit");
        }
        if (session._pending != null) {
          assertEquals(request, session._pending);
        } else {
          session._pending = request;
          session._result = result(request.getHistoryId(), _reportRejection);
          _committedReports++;
        }
        if (_loseReport) {
          _loseReport = false;
          throw new IOException("Lost committed report response");
        }
        return session._result;
      }
      assertTrue(request.hasAcknowledge());
      long through = request.getAcknowledge().getThroughSequence();
      if (through > session._acknowledged) {
        assertTrue(session._pending != null);
        assertEquals(through, session._pending.getReport().getSequence());
        session._acknowledged = through;
        session._pending = null;
        session._result = null;
      }
      assertEquals(through, session._acknowledged);
      if (_loseAck) {
        _loseAck = false;
        throw new IOException("Lost committed acknowledgement response");
      }
      return result(request.getHistoryId(), ResultStatus.OK);
    }

    @Override
    public CommandResult outcome(InstallationOutcomeRequest request, long timeoutMillis) {
      assertTrue(timeoutMillis > 0 && timeoutMillis <= 5_000);
      _outcomes.add(request);
      if (_sessionFenced) {
        return result(request.getHistoryId(), ResultStatus.FENCED);
      }
      Session session = _sessions.get(request.getSession().getIncarnation());
      assertTrue(session != null);
      if (_outcomeStatus != ResultStatus.RESULT_STATUS_UNSPECIFIED) {
        return result(request.getHistoryId(), _outcomeStatus);
      }
      if (request.getOutcome().getSequence() <= session._acknowledged) {
        return result(request.getHistoryId(), ResultStatus.RESULT_EXPIRED);
      }
      return session._result == null ? result(request.getHistoryId(), ResultStatus.NOT_FOUND) : session._result;
    }

    private CommandResult result(String history, ResultStatus status) {
      return CommandResult.newBuilder().setStatus(status)
          .setRevision(MetadataRpc.Revision.newBuilder().setHistoryId(history).setIndex(++_index)).build();
    }
  }

  private static final class Session {
    private MetadataRpc.RegisterClient _registration;
    private long _acknowledged;
    private InstallationCommandRequest _pending;
    private CommandResult _result;
  }
}
