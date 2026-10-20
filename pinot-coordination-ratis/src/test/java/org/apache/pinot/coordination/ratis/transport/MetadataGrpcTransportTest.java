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
package org.apache.pinot.coordination.ratis.transport;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction;
import org.apache.pinot.common.coordination.QueryDeadlineGuard;
import org.apache.pinot.common.coordination.QueryDeadlineGuard.TimeInterval;
import org.apache.pinot.common.coordination.ServingPermitGuard.ClockReading;
import org.apache.pinot.common.coordination.ServingPermitGuard.QualifiedLifetimePolicy;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.coordination.ratis.RatisMetadataAccess;
import org.apache.pinot.coordination.ratis.RatisMetadataStore;
import org.apache.pinot.coordination.ratis.RatisMetadataStore.MembershipClockSettings;
import org.apache.pinot.coordination.ratis.client.RatisOfflineSegmentExecutor;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataCatalog;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangePage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Command;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Query;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.ReportInstallation;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.Work;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.HashVersion;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.Layout;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.TableShard;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataServiceGrpc;
import org.apache.pinot.coordination.ratis.transport.ControllerAccessPolicy.ControllerIdentity;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;
import org.apache.pinot.core.query.executor.SingleTableExecutionInfo;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.server.starter.NativeMetadataConnection;
import org.apache.pinot.server.starter.NativeServerParticipant;
import org.apache.pinot.server.starter.NativeServerParticipant.QueryAdmission;
import org.apache.pinot.server.starter.NativeServerParticipant.ServingSettings;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Real TLS sockets verify authentication, controller-only authorization and metadata feed safety. One complete feed
/// uses a three-voter quorum through the authenticated snapshot and change-feed service.
public class MetadataGrpcTransportTest {
  private Path _directory;
  private final Map<String, CertificateFiles> _certificates = new HashMap<>();
  private Path _trustClients;

  @BeforeClass
  public void generateCertificates()
      throws Exception {
    _directory = Files.createTempDirectory("pinot-metadata-mtls-");
    for (String name : List.of("server", "controller-a", "controller-b", "worker", "untrusted")) {
      _certificates.put(name, certificate(name));
    }
    _trustClients = _directory.resolve("trusted-clients.pem");
    Files.writeString(_trustClients, Files.readString(_certificates.get("controller-a").certificate())
        + Files.readString(_certificates.get("controller-b").certificate())
        + Files.readString(_certificates.get("worker").certificate()));
  }

  @AfterClass(alwaysRun = true)
  public void cleanupCertificates()
      throws IOException {
    if (_directory != null) {
      FileUtils.deleteDirectory(_directory.toFile());
    }
  }

  @Test
  public void testRestoredTombstoneHasNoWireValue()
      throws Exception {
    UUID history = UUID.randomUUID();
    MetadataProtocol.CatalogRecord tombstone = new MetadataProtocol.CatalogRecord(
        MetadataProtocol.RecordKind.TABLE_CONFIG, "deleted_OFFLINE", UUID.randomUUID(), 2, 1, 10, true, null);
    Result result = new Result(MetadataProtocol.Status.OK, new MetadataProtocol.Revision(history, 10), "", null,
        List.of(), tombstone);
    MetadataProtocol.CatalogRecord restored = MetadataProtocol.decodeResult(JsonUtils.objectToBytes(result)).record();
    MetadataRpc.CatalogRecord wire = MetadataRpcCodec.record(restored);
    assertTrue(wire.getDeleted());
    assertFalse(wire.hasValue(), "JSON-null restoration must not publish a value on a deleted record");
    assertEquals(restored, tombstone);
    assertEquals(MetadataRpcCodec.record(wire), tombstone);
  }

  @Test(timeOut = 60_000)
  public void testMutualTlsAndAuthenticatedClientBinding()
      throws Exception {
    try (Fixture fixture = new Fixture(MetadataGrpcService.Limits.defaults());
        MetadataGrpcClient first = fixture.client("controller-a");
        MetadataGrpcClient second = fixture.client("controller-b");
        MetadataGrpcClient worker = fixture.client("worker");
        MetadataGrpcClient untrusted = fixture.client("untrusted")) {
      assertStatus(Status.Code.PERMISSION_DENIED, () -> worker.read(fixture.revision()));
      assertStatus(Status.Code.UNAVAILABLE, () -> untrusted.read(fixture.revision()));
      ManagedChannel noCertificate = NettyChannelBuilder.forAddress("127.0.0.1", fixture._server.port())
          .sslContext(GrpcSslContexts.forClient()
              .trustManager(_certificates.get("server").certificate().toFile()).build())
          .build();
      try {
        assertStatus(Status.Code.UNAVAILABLE, () -> MetadataServiceGrpc.newBlockingStub(noCertificate)
            .withDeadlineAfter(3, TimeUnit.SECONDS).read(fixture.revision()));
      } finally {
        noCertificate.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      }

      assertEquals(first.execute(fixture.register()).getClientEpoch(), 1L);
      assertEquals(second.execute(fixture.register()).getClientEpoch(), 1L);
      MetadataRpc.CommandRequest create = fixture.create();
      assertEquals(first.execute(create).getStatus(), MetadataRpc.ResultStatus.OK);
      MetadataRpc.ReadRequest outcome = MetadataRpc.ReadRequest.newBuilder().setHistoryId(fixture._history.toString())
          .setOutcome(MetadataRpc.OutcomeQuery.newBuilder().setClientEpoch(1).setStreamId("main").setSequence(1))
          .build();
      assertEquals(first.read(outcome).getStatus(), MetadataRpc.ResultStatus.OK);
      assertEquals(second.read(outcome).getStatus(), MetadataRpc.ResultStatus.NOT_FOUND);
      BigDecimal decimal = new BigDecimal("1.123456789012345678901234567890123456789");
      TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName("precise")
          .setFieldConfigList(List.of(new FieldConfig.Builder("id")
              .withIndexes(JsonUtils.newObjectNode().set("custom", JsonUtils.newObjectNode().put("decimal", decimal)))
              .build())).build();
      MetadataRpc.Mutation precise = MetadataRpc.Mutation.newBuilder().setAction(MetadataRpc.Mutation.Action.CREATE)
          .setKind(MetadataRpc.RecordKind.TABLE_CONFIG).setName("precise_OFFLINE")
          .setExpected(MetadataRpc.ObjectVersion.newBuilder().setHistoryId(fixture._history.toString())
              .setObjectId(UUID.randomUUID().toString()))
          .setValue(MetadataRpc.CatalogValue.newBuilder()
              .setTableConfigJson(ByteString.copyFrom(JsonUtils.objectToBytes(table))))
          .build();
      MetadataRpc.CommandResult precisionResult = first.execute(MetadataRpc.CommandRequest.newBuilder()
          .setHistoryId(fixture._history.toString()).setTransaction(MetadataRpc.Transaction.newBuilder()
              .setClientEpoch(1).setStreamId("precision").setSequence(1).addMutations(precise)).build());
      assertEquals(precisionResult.getStatus(), MetadataRpc.ResultStatus.OK);
      MetadataRpc.CommandResult preciseRead = first.read(MetadataRpc.ReadRequest.newBuilder()
          .setHistoryId(fixture._history.toString()).setRecord(MetadataRpc.RecordQuery.newBuilder()
              .setKind(MetadataRpc.RecordKind.TABLE_CONFIG).setName("precise_OFFLINE")).build());
      assertTrue(preciseRead.getRecord().getValue().getTableConfigJson().toStringUtf8()
          .contains(decimal.toPlainString()));

      // A caller cannot add a client identity through a newer/unknown protobuf field: mandatory vocabulary is closed.
      MetadataRpc.CommandRequest spoofed = create.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
          .addField(100, UnknownFieldSet.Field.newBuilder().addLengthDelimited(ByteString.copyFromUtf8("controller-b"))
              .build()).build()).build();
      assertStatus(Status.Code.INVALID_ARGUMENT, () -> first.execute(spoofed));
      assertEquals(second.read(outcome).getStatus(), MetadataRpc.ResultStatus.NOT_FOUND);
      assertTrue(fixture._audit.contains(List.of("controller-a", "Execute", "OK")));
      assertTrue(fixture._audit.stream().flatMap(List::stream).noneMatch(value -> value.contains("private-value")));
    }
  }

  @Test(timeOut = 60_000)
  public void testPagedImageChangesScopeTheftAndRevocation()
      throws Exception {
    try (Quorum quorum = new Quorum();
        Fixture fixture = new Fixture(MetadataGrpcService.Limits.defaults(), quorum._history, quorum._access);
        MetadataGrpcClient first = fixture.client("controller-a");
        MetadataGrpcClient second = fixture.client("controller-b")) {
      first.execute(fixture.register());
      first.execute(fixture.create());
      MetadataRpc.OpenSnapshotResult opened = first.openSnapshot(fixture.open());
      assertEquals(opened.getStatus(), MetadataRpc.ResultStatus.OK);
      assertEquals(opened.getRecordCount(), 2);
      MetadataRpc.SnapshotPageRequest page = fixture.page(opened.getSnapshotHandle(), 0, 1);
      assertStatus(Status.Code.PERMISSION_DENIED, () -> second.page(page));
      assertStatus(Status.Code.PERMISSION_DENIED, () -> second.changes(fixture.changes(opened.getCursor())));
      MetadataRpc.SnapshotPage initial = first.page(page);
      assertEquals(initial.getRecordsCount(), 1);
      assertFalse(initial.getComplete());
      assertEquals(initial.getRecords(0).getValue().getClusterConfig().getPropertiesOrThrow("key"),
          "private-value-1");

      MetadataRpc.CommandResult acknowledged = first.execute(fixture.acknowledge(1));
      assertEquals(acknowledged.getStatus(), MetadataRpc.ResultStatus.OK);
      MetadataRpc.CommandResult updated = first.execute(fixture.update(2));
      assertEquals(updated.getStatus(), MetadataRpc.ResultStatus.OK);
      MetadataRpc.SnapshotPage last = first.page(fixture.page(opened.getSnapshotHandle(), initial.getNextOffset(), 1));
      assertTrue(last.getComplete());
      assertEquals(last.getCursor(), opened.getCursor());
      assertEquals(Schema.fromString(last.getRecords(0).getValue().getSchemaJson().toStringUtf8())
          .getFieldSpecFor("id").getDataType(), DataType.INT);
      MetadataRpc.ChangePage changes = first.changes(fixture.changes(last.getCursor()));
      assertEquals(changes.getStatus(), MetadataRpc.ResultStatus.OK);
      assertEquals(changes.getFrom(), opened.getCursor());
      long previousIndex = opened.getCursor().getIndex();
      int dataTransactions = 0;
      boolean acknowledgementPresent = false;
      for (MetadataRpc.ChangeTransaction transaction : changes.getTransactionsList()) {
        assertEquals(transaction.getPreviousIndex(), previousIndex);
        assertTrue(transaction.getIndex() > previousIndex);
        previousIndex = transaction.getIndex();
        if (transaction.getIndex() == updated.getRevision().getIndex()) {
          dataTransactions++;
          assertEquals(transaction.getRecordsCount(), 1);
          assertEquals(transaction.getRecords(0).getValue().getClusterConfig().getPropertiesOrThrow("key"),
              "private-value-2");
        } else {
          assertEquals(transaction.getRecordsCount(), 0, "Read barriers and acknowledgements only advance progress");
        }
        if (transaction.getIndex() == acknowledged.getRevision().getIndex()) {
          acknowledgementPresent = true;
          assertEquals(transaction.getRecordsCount(), 0, "Acknowledgement is an explicit progress event");
        }
      }
      assertEquals(dataTransactions, 1);
      assertTrue(acknowledgementPresent);
      assertEquals(changes.getNext(), opened.getCursor().toBuilder().setIndex(previousIndex).build());
      fixture.authorize("controller-a", "generation-2");
      assertStatus(Status.Code.PERMISSION_DENIED, () -> first.page(page));
      assertStatus(Status.Code.PERMISSION_DENIED, () -> first.changes(fixture.changes(opened.getCursor())));
      fixture.revoke("controller-a");
      assertStatus(Status.Code.PERMISSION_DENIED, () -> first.read(fixture.revision()));
      assertStatus(Status.Code.PERMISSION_DENIED, () -> first.changes(fixture.changes(changes.getNext())));
      fixture.authorize("controller-a", "generation-2");
      MetadataRpc.OpenSnapshotResult replacement = first.openSnapshot(fixture.open());
      assertEquals(first.page(fixture.page(replacement.getSnapshotHandle(), 0, 1))
          .getRecords(0).getValue().getClusterConfig().getPropertiesOrThrow("key"), "private-value-2");
      first.closeSnapshot(replacement.getSnapshotHandle());
      assertEquals(first.page(fixture.page(replacement.getSnapshotHandle(), 0, 1)).getStatus(),
          MetadataRpc.ResultStatus.CURSOR_EXPIRED);
    }
  }

  @Test(timeOut = 60_000)
  public void testCompactionDuringFinalPageExpiresHandoff()
      throws Exception {
    try (Fixture fixture = new Fixture(MetadataGrpcService.Limits.defaults());
        MetadataGrpcClient client = fixture.client("controller-a")) {
      client.execute(fixture.register());
      client.execute(fixture.create());
      MetadataRpc.OpenSnapshotResult opened = client.openSnapshot(fixture.open());
      AtomicInteger checks = new AtomicInteger();
      fixture._backend._beforeChanges.set(() -> {
        if (checks.incrementAndGet() == 2) {
          fixture._backend.advancePastJournal();
        }
      });
      MetadataRpc.SnapshotPage page = client.page(fixture.page(opened.getSnapshotHandle(), 0, 128));
      assertEquals(page.getStatus(), MetadataRpc.ResultStatus.CURSOR_EXPIRED);
      assertEquals(page.getRecordsCount(), 0, "A compacted handoff cannot publish an apparently complete image");
      assertEquals(client.changes(fixture.changes(opened.getCursor())).getStatus(),
          MetadataRpc.ResultStatus.CURSOR_EXPIRED);
    }
  }

  @Test(timeOut = 60_000)
  public void testSnapshotQuotaAndLifetime()
      throws Exception {
    MetadataGrpcService.Limits limits = new MetadataGrpcService.Limits(1, 1024 * 1024, 1024 * 1024, 100,
        2 * 1024 * 1024, 128, 200, 5000, 4);
    try (Fixture fixture = new Fixture(limits); MetadataGrpcClient client = fixture.client("controller-a")) {
      client.execute(fixture.register());
      MetadataRpc.OpenSnapshotResult first = client.openSnapshot(fixture.open());
      assertEquals(first.getStatus(), MetadataRpc.ResultStatus.OK);
      assertStatus(Status.Code.RESOURCE_EXHAUSTED, () -> client.openSnapshot(fixture.open()));
      Thread.sleep(250);
      assertEquals(client.page(fixture.page(first.getSnapshotHandle(), 0, 1)).getStatus(),
          MetadataRpc.ResultStatus.CURSOR_EXPIRED);
      MetadataRpc.OpenSnapshotResult next = client.openSnapshot(fixture.open());
      assertEquals(next.getStatus(), MetadataRpc.ResultStatus.OK);
      AtomicInteger checks = new AtomicInteger();
      fixture._backend._beforeChanges.set(() -> {
        if (checks.incrementAndGet() == 2) {
          try {
            Thread.sleep(250);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      });
      assertEquals(client.page(fixture.page(next.getSnapshotHandle(), 0, 1)).getStatus(),
          MetadataRpc.ResultStatus.CURSOR_EXPIRED, "Expiry during the final retention check must reject the page");
      client.closeSnapshot(next.getSnapshotHandle());
    }
  }

  @Test(timeOut = 60_000)
  public void testServerCapsClientDeadline()
      throws Exception {
    MetadataGrpcService.Limits limits = new MetadataGrpcService.Limits(1, 1024 * 1024, 1024 * 1024, 100,
        2 * 1024 * 1024, 128, 5000, 100, 4);
    CountDownLatch release = new CountDownLatch(1);
    try (Fixture fixture = new Fixture(limits); MetadataGrpcClient client = fixture.client("controller-a")) {
      fixture._backend._beforeQuery.set(() -> {
        try {
          release.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      long start = System.nanoTime();
      try {
        assertStatus(Status.Code.DEADLINE_EXCEEDED, () -> client.read(fixture.revision()));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 2000,
            "The server deadline must end the call before the client's five-second timeout");
        assertTrue(fixture._backend._lastTimeout.get() > 0 && fixture._backend._lastTimeout.get() <= 100);
      } finally {
        release.countDown();
      }
    }
  }

  private static void assertStatus(Status.Code expected, Runnable call) {
    StatusRuntimeException error = expectThrows(StatusRuntimeException.class, call::run);
    assertEquals(error.getStatus().getCode(), expected, error.getMessage());
  }

  @Test(timeOut = 90_000)
  public void testParticipantIdentityCapabilitiesAndProtectedRenewal()
      throws Exception {
    ParticipantIdentity identity = new ParticipantIdentity("Server_worker", Role.SERVER,
        Set.of(Capability.SERVER_QUERY));
    AtomicReference<ParticipantIdentity> policy = new AtomicReference<>(identity);
    ParticipantIdentity otherIdentity = new ParticipantIdentity("Server_other", Role.SERVER,
        Set.of(Capability.SERVER_QUERY));
    try (Quorum quorum = new Quorum()) {
      quorum.startMembership();
      BlockingAccess blocked = new BlockingAccess(quorum._access);
      ParticipantAccessPolicy participantPolicy = fingerprint -> {
        if (fingerprint.equals(_certificates.get("worker").fingerprint())) {
          return policy.get();
        }
        return fingerprint.equals(_certificates.get("controller-b").fingerprint()) ? otherIdentity : null;
      };
      try (Fixture fixture = new Fixture(MetadataGrpcService.Limits.defaults(), quorum._history, blocked,
          quorum._access, participantPolicy);
          MetadataGrpcClient worker = fixture.client("worker");
          MetadataGrpcClient other = fixture.client("controller-b");
          MetadataGrpcClient controller = fixture.client("controller-a")) {
        MetadataRpc.MembershipReadRequest read = MetadataRpc.MembershipReadRequest.newBuilder()
            .setHistoryId(quorum._history.toString()).build();
        assertStatus(Status.Code.PERMISSION_DENIED, () -> worker.read(fixture.revision()));
        assertStatus(Status.Code.PERMISSION_DENIED, () -> worker.openSnapshot(fixture.open()));
        assertStatus(Status.Code.PERMISSION_DENIED, () -> controller.membershipRead(read));
        MetadataRpc.MembershipCommandRequest register = memberRegistration(quorum._history);
        MetadataRpc.MembershipResponse registered = worker.membershipExecute(register);
        assertEquals(registered.getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(registered.getParticipant().getParticipant().getParticipantId(), identity.participantId());
        assertEquals(registered.getParticipant().getParticipant().getRole(), MetadataRpc.ParticipantRole.SERVER);
        MetadataRpc.ParticipantSession session = session(registered.getParticipant().getParticipant());
        MetadataRpc.MembershipResponse otherRegistered = other.membershipExecute(memberRegistration(quorum._history));
        MetadataRpc.MembershipResponse spoof = worker.membershipExecute(
            MetadataRpc.MembershipCommandRequest.newBuilder().setHistoryId(quorum._history.toString())
                .setDrain(session(otherRegistered.getParticipant().getParticipant())).build());
        assertEquals(spoof.getStatus(), MetadataRpc.ResultStatus.FENCED);
        assertFalse(other.membershipRead(read).getParticipant().getDraining());

        MetadataRpc.MembershipCommandRequest hiddenControl = register.toBuilder()
            .setUnknownFields(UnknownFieldSet.newBuilder().addField(100,
                UnknownFieldSet.Field.newBuilder().addLengthDelimited(ByteString.copyFromUtf8("TICK")).build())
                .build()).build();
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> worker.membershipExecute(hiddenControl));
        MetadataRpc.CommandRequest genericMembershipWrite = MetadataRpc.CommandRequest.newBuilder()
            .setHistoryId(quorum._history.toString()).setTransaction(MetadataRpc.Transaction.newBuilder()
                .addMutations(MetadataRpc.Mutation.newBuilder().setKind(MetadataRpc.RecordKind.PARTICIPANT))).build();
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> controller.execute(genericMembershipWrite));
        MetadataRpc.MembershipCommandRequest oversized = register.toBuilder().setRegister(register.getRegister()
            .toBuilder().putEndpoints("query", "x".repeat(MembershipGrpcService.MAX_MESSAGE_BYTES))).build();
        assertStatus(Status.Code.RESOURCE_EXHAUSTED, () -> worker.membershipExecute(oversized));
        MetadataRpc.ParticipantRegistration.Builder tooMany = register.getRegister().toBuilder().clearEndpoints();
        for (int i = 0; i <= MembershipProtocol.MAX_ENDPOINTS; i++) {
          tooMany.putEndpoints("endpoint-" + i, "https://localhost:8098");
        }
        assertEquals(worker.membershipExecute(register.toBuilder().setRegister(tooMany).build()).getStatus(),
            MetadataRpc.ResultStatus.INVALID_REQUEST);

        MetadataRpc.MembershipResponse ready = worker.membershipExecute(
            MetadataRpc.MembershipCommandRequest.newBuilder().setHistoryId(quorum._history.toString())
                .setReady(MetadataRpc.ParticipantReadiness.newBuilder().setSession(session)
                    .setAppliedRevision(worker.membershipRead(read).getRevision()).setReady(true)).build());
        assertEquals(ready.getStatus(), MetadataRpc.ResultStatus.OK);
        MetadataRpc.MembershipCommandRequest firstRenew = memberRenewal(quorum._history, session, 1,
            ready.getRevision());
        MetadataRpc.MembershipResponse renewed = worker.membershipExecute(firstRenew);
        assertEquals(renewed.getRenewals(0).getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(renewed.getRenewals(0).getPermit().getCapabilitiesList(),
            List.of(MetadataRpc.ServingCapability.SERVER_QUERY));
        assertEquals(MetadataGrpcClient.decodeMembershipResponse(renewed).renewals().getFirst().permit().participant()
            .participantId(), identity.participantId());
        MetadataRpc.CommandResult catalogParticipant = controller.read(MetadataRpc.ReadRequest.newBuilder()
            .setHistoryId(quorum._history.toString()).setRecord(MetadataRpc.RecordQuery.newBuilder()
                .setKind(MetadataRpc.RecordKind.PARTICIPANT).setName(identity.participantId())).build());
        assertTrue(catalogParticipant.getRecord().getValue().hasParticipant());
        assertEquals(MembershipProtocol.decodeParticipant(
            MetadataGrpcClient.decodeRecord(catalogParticipant.getRecord()).value()).participant().participantId(),
            identity.participantId());

        policy.set(new ParticipantIdentity(identity.participantId(), Role.SERVER, Set.of()));
        assertEquals(worker.membershipExecute(firstRenew).getRenewals(0).getStatus(), MetadataRpc.ResultStatus.CONFLICT,
            "Narrowed authority cannot replay a prior broader grant");
        MetadataRpc.MembershipResponse narrowed = worker.membershipExecute(
            memberRenewal(quorum._history, session, 2, renewed.getRevision()));
        assertEquals(narrowed.getRenewals(0).getStatus(), MetadataRpc.ResultStatus.OK);
        assertTrue(narrowed.getRenewals(0).hasPermit());
        assertEquals(narrowed.getRenewals(0).getPermit().getCapabilitiesCount(), 0);
        policy.set(new ParticipantIdentity(identity.participantId(), Role.BROKER, Set.of(Capability.BROKER_QUERY)));
        assertEquals(worker.membershipExecute(memberRenewal(quorum._history, session, 3, narrowed.getRevision()))
            .getRenewals(0).getStatus(), MetadataRpc.ResultStatus.FENCED);
        policy.set(identity);

        ExecutorService catalogCalls = Executors.newFixedThreadPool(4);
        try {
          List<Future<MetadataRpc.CommandResult>> pending = new ArrayList<>();
          blocked._block = true;
          for (int i = 0; i < 4; i++) {
            pending.add(catalogCalls.submit(() -> controller.read(fixture.revision())));
          }
          assertTrue(blocked._entered.await(5, TimeUnit.SECONDS),
              "All catalog workers must enter the blocking backend");
          MetadataRpc.MembershipResponse protectedRenewal = worker.membershipExecute(
              memberRenewal(quorum._history, session, 3, narrowed.getRevision()));
          assertEquals(protectedRenewal.getRenewals(0).getStatus(), MetadataRpc.ResultStatus.OK);
          assertTrue(protectedRenewal.getRenewals(0).hasPermit());
          assertEquals(blocked._release.getCount(), 1L, "Renewal must complete before catalog handlers are released");
          blocked._release.countDown();
          for (Future<MetadataRpc.CommandResult> call : pending) {
            assertEquals(call.get(5, TimeUnit.SECONDS).getStatus(), MetadataRpc.ResultStatus.OK);
          }
        } finally {
          blocked._release.countDown();
          catalogCalls.shutdownNow();
          catalogCalls.awaitTermination(5, TimeUnit.SECONDS);
        }
        policy.set(null);
        assertStatus(Status.Code.PERMISSION_DENIED, () -> worker.membershipRead(read));
        policy.set(identity);
        assertEquals(worker.membershipExecute(MetadataRpc.MembershipCommandRequest.newBuilder()
            .setHistoryId(quorum._history.toString()).setDrain(session).build()).getStatus(),
            MetadataRpc.ResultStatus.OK);
        assertTrue(worker.membershipRead(read).getParticipant().getDraining());
        assertTrue(fixture._audit.contains(List.of(identity.participantId(), "MembershipExecute", "OK")));
      }
    }
  }

  @Test(timeOut = 90_000)
  public void testControllerOperationAuthorityRetryAndTypedFeed()
      throws Exception {
    ParticipantIdentity controllerIdentity = new ParticipantIdentity("Controller_a", Role.CONTROLLER, Set.of());
    AtomicReference<ParticipantIdentity> binding = new AtomicReference<>(controllerIdentity);
    AtomicReference<ParticipantIdentity> otherBinding = new AtomicReference<>(
        new ParticipantIdentity("Controller_b", Role.CONTROLLER, Set.of()));
    ParticipantIdentity workerIdentity = new ParticipantIdentity("Server_worker", Role.SERVER,
        Set.of(Capability.SERVER_QUERY));
    AtomicReference<ParticipantIdentity> workerBinding = new AtomicReference<>(workerIdentity);
    try (Quorum quorum = new Quorum()) {
      quorum.startMembership();
      BlockingAccess access = new BlockingAccess(quorum._access);
      ParticipantAccessPolicy participants = fingerprint -> {
        if (fingerprint.equals(_certificates.get("controller-a").fingerprint())) {
          return binding.get();
        }
        if (fingerprint.equals(_certificates.get("controller-b").fingerprint())) {
          return otherBinding.get();
        }
        return fingerprint.equals(_certificates.get("worker").fingerprint()) ? workerBinding.get() : null;
      };
      try (Fixture fixture = new Fixture(MetadataGrpcService.Limits.defaults(), quorum._history, access,
          quorum._access, participants);
          MetadataGrpcClient first = fixture.client("controller-a");
          MetadataGrpcClient second = fixture.client("controller-b");
          MetadataGrpcClient worker = fixture.client("worker")) {
        UUID history = quorum._history;
        MetadataRpc.MembershipResponse registered = first.membershipExecute(memberRegistration(history));
        MetadataRpc.ParticipantToken controller = registered.getParticipant().getParticipant();
        assertEquals(first.membershipExecute(MetadataRpc.MembershipCommandRequest.newBuilder()
            .setHistoryId(history.toString()).setReady(MetadataRpc.ParticipantReadiness.newBuilder()
                .setSession(session(controller)).setAppliedRevision(registered.getRevision()).setReady(true)).build())
            .getStatus(), MetadataRpc.ResultStatus.OK);
        second.membershipExecute(memberRegistration(history));
        MetadataRpc.MembershipResponse serverRegistered = worker.membershipExecute(memberRegistration(history));
        MetadataRpc.ParticipantToken server = serverRegistered.getParticipant().getParticipant();
        MetadataRpc.InstallationSnapshotRequest serverSnapshot = MetadataRpc.InstallationSnapshotRequest.newBuilder()
            .setHistoryId(history.toString()).setSession(session(server)).setMaxRecords(128)
            .setMaxBytes(OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES).build();
        MetadataRpc.InstallationSnapshot bootstrap = worker.installationSnapshot(serverSnapshot, 5000);
        assertEquals(bootstrap.getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(bootstrap.getParticipant().getParticipant(), server);
        assertFalse(bootstrap.getParticipant().getReady());
        assertTrue(bootstrap.hasControl());
        assertEquals(bootstrap.getRecordsCount(), 0);
        assertEquals(bootstrap.getOperationIdsCount(), 0);
        assertStatus(Status.Code.PERMISSION_DENIED, () -> first.installationSnapshot(serverSnapshot, 5000));
        assertEquals(worker.membershipExecute(MetadataRpc.MembershipCommandRequest.newBuilder()
            .setHistoryId(history.toString()).setReady(MetadataRpc.ParticipantReadiness.newBuilder()
                .setSession(session(server)).setAppliedRevision(bootstrap.getRevision()).setReady(true)).build())
            .getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(first.execute(fixture.register()).getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(second.execute(fixture.register()).getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(quorum._access.execute(new MetadataProtocol.RegisterClient(history, "coordinator",
            UUID.randomUUID(), 0), 5000).status(), MetadataProtocol.Status.OK);
        MembershipProtocol.ControlToken control = quorum._access.query(new MetadataProtocol.MembershipQuery(history,
            new MembershipProtocol.GetControl()), 5000).control().controlEpoch();
        Layout layout = new Layout(1, 24, HashVersion.MURMUR2_UTF8_MASK_POSITIVE_V1);
        assertEquals(quorum._access.execute(new MetadataProtocol.OwnershipCommand(history, "coordinator", 1,
            "ownership", 1, new OwnershipProtocol.ConfigureLayout(control, layout)), 5000).status(),
            MetadataProtocol.Status.OK);
        assertEquals(quorum._access.execute(new MetadataProtocol.Acknowledge(history, "coordinator", 1,
            "ownership", 1), 5000).status(), MetadataProtocol.Status.OK);
        String tableName = "analytics.orders_OFFLINE";
        UUID tableId = UUID.randomUUID();
        TableShard shard = OwnershipProtocol.tableScope(layout, tableName);
        assertEquals(quorum._access.execute(new MetadataProtocol.OwnershipCommand(history, "coordinator", 1,
            "ownership", 2, new OwnershipProtocol.AssignOwner(control, 1, shard, 0,
                MembershipRpcCodec.participant(controller))), 5000).status(), MetadataProtocol.Status.OK);
        TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
        UUID schemaId = UUID.randomUUID();
        Schema schema = new Schema.SchemaBuilder().setSchemaName("analytics.orders")
            .addSingleValueDimension("id", DataType.INT)
            .build();
        assertEquals(quorum._access.execute(new MetadataProtocol.Transaction(history, "coordinator", 1, "table", 1,
            List.of(MetadataProtocol.Mutation.createTableConfig(history, tableId, table),
                MetadataProtocol.Mutation.createSchema(history, schemaId, schema))), 5000).status(),
            MetadataProtocol.Status.OK);
        OwnershipProtocol.OwnerToken ownerToken = new OwnershipProtocol.OwnerToken(history, 1, shard, 1,
            MembershipRpcCodec.participant(controller));
        UUID segmentId = UUID.randomUUID();
        MetadataProtocol.ObjectVersion tableVersion = new MetadataProtocol.ObjectVersion(history, tableId, 1);
        OfflineSegmentProtocol.Content content = new OfflineSegmentProtocol.Content(tableName, tableId, "segment",
            segmentId, "a".repeat(64), "https://localhost/immutable/segment.tar.gz",
            new OfflineSegmentProtocol.LoaderDescriptor(123, -1, false, null, "test-crypter", -1, 1000,
                Map.of("custom-key", "value")));
        MetadataProtocol.OfflineSegmentCommand publish = new MetadataProtocol.OfflineSegmentCommand(history,
            "coordinator", 1, "segments", 1,
            new OfflineSegmentProtocol.PublishContent(ownerToken, tableVersion, content));
        assertEquals(quorum._access.execute(publish, 5000).status(), MetadataProtocol.Status.OK);
        assertEquals(quorum._access.execute(new MetadataProtocol.Acknowledge(history, "coordinator", 1,
            "segments", 1), 5000).status(), MetadataProtocol.Status.OK);
        assertEquals(quorum._access.execute(new MetadataProtocol.OfflineSegmentCommand(history, "coordinator", 1,
            "segments", 2, new OfflineSegmentProtocol.AssignReplica(ownerToken, segmentId, 0,
                MembershipRpcCodec.participant(server), new OfflineSegmentProtocol.Compatibility(tableVersion,
                    new MetadataProtocol.ObjectVersion(history, schemaId, 1)), true)), 5000).status(),
            MetadataProtocol.Status.OK);
        MetadataRpc.OfflineReplicaAssignment assignedReplica = first.read(MetadataRpc.ReadRequest.newBuilder()
            .setHistoryId(history.toString()).setRecord(MetadataRpc.RecordQuery.newBuilder()
                .setKind(MetadataRpc.RecordKind.OFFLINE_REPLICA_ASSIGNMENT)
                .setName(OfflineSegmentProtocol.assignmentName(segmentId, server.getParticipantId()))).build())
            .getRecord().getValue().getOfflineReplicaAssignment();
        String operationId = UUID.randomUUID().toString();
        MetadataRpc.OperationSpec input = MetadataRpc.OperationSpec.newBuilder().setOfflineSegmentReload(
            MetadataRpc.OfflineSegmentReload.newBuilder().setTableName(tableName).setTableObjectId(tableId.toString())
                .setSegmentName("segment").setSegmentObjectId(segmentId.toString())
                .setContentSha256("a".repeat(64)).setForceDownload(true)
                .addTargets(MetadataRpc.TargetMaterialization.newBuilder().setServer(server)
                    .setAssignmentEpoch(assignedReplica.getAssignmentEpoch())
                    .setMaterializationId(assignedReplica.getMaterializationId()))).build();
        MetadataRpc.ControllerOperationRequest submit = operationEnvelope(1).setSubmit(
            MetadataRpc.SubmitOperation.newBuilder().setOperationId(operationId).setInput(input)).build();
        MetadataRpc.CommandRequest request = operationRequest(history, submit);
        binding.set(null);
        assertStatus(Status.Code.PERMISSION_DENIED, () -> first.execute(request));
        binding.set(new ParticipantIdentity("Server_worker", Role.SERVER, Set.of(Capability.SERVER_QUERY)));
        assertStatus(Status.Code.PERMISSION_DENIED, () -> first.execute(request));
        binding.set(controllerIdentity);
        assertStatus(Status.Code.PERMISSION_DENIED, () -> worker.execute(request));
        MetadataRpc.OfflineSegmentReload.Builder tooManyTargets = input.getOfflineSegmentReload().toBuilder();
        for (int i = 0; i < OperationProtocol.MAX_TARGETS; i++) {
          tooManyTargets.addTargets(input.getOfflineSegmentReload().getTargets(0));
        }
        MetadataRpc.ControllerOperationRequest oversized = submit.toBuilder().setSubmit(submit.getSubmit().toBuilder()
            .setInput(input.toBuilder().setOfflineSegmentReload(tooManyTargets))).build();
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> first.execute(operationRequest(history, oversized)));
        MetadataRpc.OfflineSegmentReload forgedTarget = input.getOfflineSegmentReload().toBuilder().setTargets(0,
            input.getOfflineSegmentReload().getTargets(0).toBuilder()
                .setMaterializationId(UUID.randomUUID().toString())).build();
        MetadataRpc.ControllerOperationRequest forged = submit.toBuilder().setStreamId("invalid-target")
            .setSubmit(submit.getSubmit().toBuilder().setInput(input.toBuilder().setOfflineSegmentReload(forgedTarget)))
            .build();
        assertEquals(first.execute(operationRequest(history, forged)).getStatus(), MetadataRpc.ResultStatus.FENCED,
            "Caller-supplied materialization identities must match the authoritative assignment");
        assertEquals(first.execute(request).getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(first.execute(operationAck(history, 1)).getStatus(), MetadataRpc.ResultStatus.OK);
        MetadataRpc.OpenSnapshotRequest open = MetadataRpc.OpenSnapshotRequest.newBuilder()
            .setHistoryId(history.toString()).addKinds(MetadataRpc.RecordKind.OWNER_LAYOUT)
            .addKinds(MetadataRpc.RecordKind.OWNER_ASSIGNMENT).addKinds(MetadataRpc.RecordKind.CONTROLLER_OPERATION)
            .addKinds(MetadataRpc.RecordKind.OFFLINE_SEGMENT_CONTENT)
            .addKinds(MetadataRpc.RecordKind.OFFLINE_REPLICA_ASSIGNMENT)
            .addKinds(MetadataRpc.RecordKind.OFFLINE_INSTALLATION).build();
        MetadataRpc.OpenSnapshotResult snapshot = first.openSnapshot(open);
        assertEquals(snapshot.getStatus(), MetadataRpc.ResultStatus.OK);
        MetadataRpc.SnapshotPage page = first.page(fixture.page(snapshot.getSnapshotHandle(), 0, 128));
        assertEquals(page.getRecordsCount(), 5);
        for (MetadataRpc.CatalogRecord record : page.getRecordsList()) {
          assertEquals(MetadataGrpcClient.decodeRecord(record), quorum._access.query(new MetadataProtocol.GetRecord(
              history, MetadataProtocol.RecordKind.valueOf(record.getKind().name()), record.getName()), 5000).record());
        }
        MetadataRpc.ControllerOperationState stored = first.read(MetadataRpc.ReadRequest.newBuilder()
            .setHistoryId(history.toString()).setRecord(MetadataRpc.RecordQuery.newBuilder()
                .setKind(MetadataRpc.RecordKind.CONTROLLER_OPERATION).setName(operationId)).build())
            .getRecord().getValue().getControllerOperation();
        assertTrue(stored.getInput().hasAssignedOfflineSegmentReload());
        assertEquals(stored.getInput().getAssignedOfflineSegmentReload().getDependenciesCount(), 4);
        MetadataRpc.ControllerOperationRequest forgedAssigned = operationEnvelope(2).setSubmit(
            MetadataRpc.SubmitOperation.newBuilder().setOperationId(UUID.randomUUID().toString())
                .setInput(stored.getInput())).build();
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> first.execute(operationRequest(history, forgedAssigned)));
        MetadataRpc.ClaimedOwner owner = MetadataRpc.ClaimedOwner.newBuilder().setLayoutGeneration(1)
            .setScope(MetadataRpc.OwnershipScope.newBuilder().setTableShard(shard.shard())).setOwnerEpoch(1)
            .setSession(session(controller)).build();
        MetadataRpc.ClaimOperation claim = MetadataRpc.ClaimOperation.newBuilder().setOperationId(operationId)
            .setExpectedVersion(1).setOwner(owner)
            .addAllDependencies(stored.getInput().getAssignedOfflineSegmentReload().getDependenciesList()).build();
        MetadataRpc.CommandRequest spoof = operationRequest(history, operationEnvelope(1).setClaim(claim).build());
        assertEquals(second.execute(spoof).getStatus(), MetadataRpc.ResultStatus.FENCED,
            "A different certificate cannot claim the supplied owner's session");
        MetadataRpc.CommandRequest unknown = operationRequest(history, operationEnvelope(2).setClaim(claim).build())
            .toBuilder().setUnknownFields(UnknownFieldSet.newBuilder().addField(100,
                UnknownFieldSet.Field.newBuilder().addLengthDelimited(ByteString.copyFromUtf8("ASSIGN_OWNER")).build())
                .build()).build();
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> first.execute(unknown));
        assertEquals(first.execute(operationRequest(history, operationEnvelope(2).setClaim(claim).build())).getStatus(),
            MetadataRpc.ResultStatus.OK);
        assertEquals(first.execute(operationAck(history, 2)).getStatus(), MetadataRpc.ResultStatus.OK);
        MetadataRpc.CommandRequest checkpoint = operationRequest(history, operationEnvelope(3).setCheckpoint(
            MetadataRpc.CheckpointOperation.newBuilder().setOperationId(operationId).setExpectedVersion(2)
                .setOwner(owner).setAttempt(1).setProgress(MetadataRpc.OperationProgress.newBuilder()
                    .setDetail("attempt started"))).build());
        access._afterExecute.set(() -> binding.set(null));
        assertStatus(Status.Code.PERMISSION_DENIED, () -> first.execute(checkpoint));
        access._afterExecute.set(() -> { });
        binding.set(controllerIdentity);
        MetadataRpc.CommandResult recovered = first.execute(checkpoint);
        assertEquals(recovered.getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(first.execute(checkpoint), recovered, "Exact retry preserves the committed outcome");
        assertEquals(first.execute(operationAck(history, 3)).getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(first.execute(operationRequest(history, operationEnvelope(4).setComplete(
            MetadataRpc.CompleteOperation.newBuilder().setOperationId(operationId).setExpectedVersion(3)
                .setOwner(owner).setAttempt(1).setTerminal(MetadataRpc.OperationStatus.SUCCEEDED)
                .setResult(MetadataRpc.OperationProgress.newBuilder().setCompletedTargets(1).setDetail("loaded")))
            .build())).getStatus(), MetadataRpc.ResultStatus.INVALID_REQUEST,
            "Native success cannot be declared without target observations");
        assertEquals(first.execute(operationAck(history, 4)).getStatus(), MetadataRpc.ResultStatus.OK);
        MetadataRpc.InstallationLookupRequest lookup = MetadataRpc.InstallationLookupRequest.newBuilder()
            .setHistoryId(history.toString()).setSession(session(server)).setOperationId(operationId).build();
        assertStatus(Status.Code.PERMISSION_DENIED, () -> first.installationLookup(lookup, 5000));
        otherBinding.set(new ParticipantIdentity("Server_other", Role.SERVER, Set.of(Capability.SERVER_QUERY)));
        MetadataRpc.MembershipResponse otherServer = second.membershipExecute(memberRegistration(history));
        assertEquals(second.membershipExecute(MetadataRpc.MembershipCommandRequest.newBuilder()
            .setHistoryId(history.toString()).setReady(MetadataRpc.ParticipantReadiness.newBuilder()
                .setSession(session(otherServer.getParticipant().getParticipant()))
                .setAppliedRevision(otherServer.getRevision()).setReady(true)).build()).getStatus(),
            MetadataRpc.ResultStatus.OK);
        assertEquals(second.installationLookup(lookup, 5000).getStatus(), MetadataRpc.ResultStatus.FENCED,
            "A certificate cannot borrow another server's session");
        assertEquals(second.installationLookup(lookup.toBuilder()
            .setSession(session(otherServer.getParticipant().getParticipant())).build(), 5000).getStatus(),
            MetadataRpc.ResultStatus.FENCED, "Only the assigned server can obtain the target's work");
        assertEquals(second.installationSnapshot(serverSnapshot, 5000).getStatus(), MetadataRpc.ResultStatus.FENCED,
            "A certificate cannot hydrate another server's registration");
        MetadataRpc.InstallationSnapshot otherSnapshot = second.installationSnapshot(serverSnapshot.toBuilder()
            .setSession(session(otherServer.getParticipant().getParticipant())).build(), 5000);
        assertEquals(otherSnapshot.getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(otherSnapshot.getRecordsCount(), 0);
        assertEquals(otherSnapshot.getOperationIdsCount(), 0);
        MetadataRpc.InstallationSnapshot ownSnapshot = worker.installationSnapshot(serverSnapshot, 5000);
        assertEquals(ownSnapshot.getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(ownSnapshot.getRecordsCount(), 4);
        assertEquals(ownSnapshot.getOperationIdsList(), List.of(operationId));
        for (MetadataRpc.CatalogRecord record : ownSnapshot.getRecordsList()) {
          assertTrue(record.getLastModifiedIndex() <= ownSnapshot.getRevision().getIndex());
          assertTrue(Set.of(MetadataRpc.RecordKind.TABLE_CONFIG, MetadataRpc.RecordKind.SCHEMA,
              MetadataRpc.RecordKind.OFFLINE_SEGMENT_CONTENT, MetadataRpc.RecordKind.OFFLINE_REPLICA_ASSIGNMENT)
              .contains(record.getKind()));
        }
        MetadataRpc.InstallationSnapshot oversizedSnapshot = worker.installationSnapshot(serverSnapshot.toBuilder()
            .setMaxRecords(4).build(), 5000);
        assertEquals(oversizedSnapshot.getStatus(), MetadataRpc.ResultStatus.RESPONSE_TOO_LARGE);
        assertEquals(oversizedSnapshot.getRecordsCount(), 0);
        assertEquals(oversizedSnapshot.getOperationIdsCount(), 0);
        assertFalse(oversizedSnapshot.hasParticipant());
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> worker.installationSnapshot(serverSnapshot.toBuilder()
            .setMaxBytes(0).build(), 5000));
        Work work = MetadataGrpcClient.decodeInstallationWork(worker.installationLookup(lookup, 5000)).work();
        assertEquals(work.target().server(), MembershipRpcCodec.participant(server));
        assertEquals(work.content(), content);
        assertTrue(work.forceDownload());
        MetadataRpc.InstallationCommandRequest registration = MetadataRpc.InstallationCommandRequest.newBuilder()
            .setHistoryId(history.toString()).setSession(session(server)).setRegister(MetadataRpc.RegisterClient
                .newBuilder().setIncarnation(server.getIncarnation()).setExpectedEpoch(0)).build();
        assertEquals(worker.installationExecute(registration, 5000).getStatus(), MetadataRpc.ResultStatus.OK);
        assertStatus(Status.Code.PERMISSION_DENIED, () -> worker.execute(fixture.register()));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> worker.installationLookup(lookup.toBuilder()
            .setUnknownFields(UnknownFieldSet.newBuilder().addField(100,
                UnknownFieldSet.Field.newBuilder().addLengthDelimited(ByteString.copyFromUtf8("GET_RECORD")).build())
                .build()).build(), 5000));
        ReportInstallation installed = new ReportInstallation(work.operationId(), work.attempt(), work.owner(),
            work.target(), UUID.randomUUID(), "b".repeat(64));
        MetadataRpc.InstallationCommandRequest report = MetadataRpc.InstallationCommandRequest.newBuilder()
            .setHistoryId(history.toString()).setSession(session(server))
            .setReport(MetadataRpc.SequencedInstallationReport.newBuilder().setClientEpoch(1).setStreamId("reports")
                .setSequence(1).setReport(MetadataGrpcClient.encodeInstallationReport(installed))).build();
        access._afterExecute.set(() -> workerBinding.set(null));
        assertStatus(Status.Code.PERMISSION_DENIED, () -> worker.installationExecute(report, 5000));
        access._afterExecute.set(() -> { });
        workerBinding.set(workerIdentity);
        MetadataRpc.CommandResult installationResult = worker.installationExecute(report, 5000);
        assertEquals(installationResult.getStatus(), MetadataRpc.ResultStatus.OK);
        MetadataRpc.InstallationOutcomeRequest outcome = MetadataRpc.InstallationOutcomeRequest.newBuilder()
            .setHistoryId(history.toString()).setSession(session(server))
            .setOutcome(MetadataRpc.OutcomeQuery.newBuilder()
                .setClientEpoch(1).setStreamId("reports").setSequence(1)).build();
        assertEquals(worker.installationOutcome(outcome, 5000), installationResult);
        assertEquals(worker.installationExecute(report, 5000), installationResult);
        CountDownLatch workEntered = new CountDownLatch(2);
        CountDownLatch releaseWork = new CountDownLatch(1);
        access._beforeWork.set(() -> {
          workEntered.countDown();
          try {
            if (!releaseWork.await(5, TimeUnit.SECONDS)) {
              throw new IllegalStateException("Installation test gate was not released");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
        });
        ExecutorService lookups = Executors.newFixedThreadPool(2);
        try {
          Future<MetadataRpc.InstallationWorkResult> one =
              lookups.submit(() -> worker.installationLookup(lookup, 5000));
          Future<MetadataRpc.InstallationWorkResult> two =
              lookups.submit(() -> worker.installationLookup(lookup, 5000));
          assertTrue(workEntered.await(5, TimeUnit.SECONDS));
          MetadataRpc.MembershipResponse current = worker.membershipRead(MetadataRpc.MembershipReadRequest.newBuilder()
              .setHistoryId(history.toString()).build());
          assertEquals(worker.membershipExecute(memberRenewal(history, session(server), 1, current.getRevision()))
              .getStatus(), MetadataRpc.ResultStatus.OK);
          assertEquals(releaseWork.getCount(), 1L, "Renewal must not wait for installation handlers");
          releaseWork.countDown();
          assertEquals(one.get(5, TimeUnit.SECONDS).getStatus(), MetadataRpc.ResultStatus.OK);
          assertEquals(two.get(5, TimeUnit.SECONDS).getStatus(), MetadataRpc.ResultStatus.OK);
        } finally {
          releaseWork.countDown();
          access._beforeWork.set(() -> { });
          lookups.shutdownNow();
          lookups.awaitTermination(5, TimeUnit.SECONDS);
        }
        assertEquals(first.execute(operationRequest(history, operationEnvelope(5).setCompleteInstalled(
            MetadataRpc.CompleteInstalledOperation.newBuilder().setOperationId(operationId).setExpectedVersion(3)
                .setOwner(owner).setAttempt(1)).build())).getStatus(), MetadataRpc.ResultStatus.OK);
        MetadataRpc.InstallationCommandRequest acknowledge = registration.toBuilder().setAcknowledge(
            MetadataRpc.Acknowledge.newBuilder().setClientEpoch(1).setStreamId("reports").setThroughSequence(1))
            .build();
        assertEquals(worker.installationExecute(acknowledge, 5000).getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(worker.installationOutcome(outcome, 5000).getStatus(), MetadataRpc.ResultStatus.RESULT_EXPIRED);
        MetadataRpc.ChangePage changes = first.changes(MetadataRpc.ChangesRequest.newBuilder()
            .setCursor(snapshot.getCursor()).addAllKinds(open.getKindsList()).setMaxTransactions(128)
            .setMaxRecords(128).setMaxBytes(MetadataProtocol.MAX_CHANGE_BYTES).build());
        List<MetadataProtocol.CatalogRecord> records = MetadataGrpcClient.decodeChanges(changes).transactions().stream()
            .flatMap(transaction -> transaction.records().stream()).toList();
        assertEquals(records.size(), 4);
        OperationProtocol.OperationState completed = OperationProtocol.decodeOperation(records.getLast().value());
        assertEquals(completed.status(), OperationProtocol.State.SUCCEEDED);
        assertEquals(completed.completionProof().installations().size(), 1);
        MetadataProtocol.CatalogRecord receipt = records.stream()
            .filter(record -> record.kind() == MetadataProtocol.RecordKind.OFFLINE_INSTALLATION)
            .findFirst().orElseThrow();
        assertEquals(OfflineInstallationProtocol.decodeReceipt(receipt.value()).installation(), installed);
        assertEquals(completed.owner().participant().participantId(), controllerIdentity.participantId());
        assertTrue(((OperationProtocol.AssignedOfflineSegmentReload) completed.input()).reload().forceDownload());
        MetadataRpc.CommandRequest genericWrite = MetadataRpc.CommandRequest.newBuilder()
            .setHistoryId(history.toString())
            .setTransaction(MetadataRpc.Transaction.newBuilder().addMutations(MetadataRpc.Mutation.newBuilder()
                .setKind(MetadataRpc.RecordKind.OWNER_ASSIGNMENT))).build();
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> first.execute(genericWrite));
        first.closeSnapshot(snapshot.getSnapshotHandle());
        assertEquals(worker.membershipExecute(MetadataRpc.MembershipCommandRequest.newBuilder()
            .setHistoryId(history.toString()).setUnregister(session(server)).build()).getStatus(),
            MetadataRpc.ResultStatus.OK);
        assertEquals(worker.installationExecute(acknowledge, 5000).getStatus(), MetadataRpc.ResultStatus.FENCED);
        assertEquals(worker.installationOutcome(outcome, 5000).getStatus(), MetadataRpc.ResultStatus.FENCED);
      }
    }
  }

  private static MetadataRpc.ControllerOperationRequest.Builder operationEnvelope(long sequence) {
    return MetadataRpc.ControllerOperationRequest.newBuilder().setClientEpoch(1).setStreamId("operations")
        .setSequence(sequence);
  }

  private static MetadataRpc.CommandRequest operationRequest(UUID history,
      MetadataRpc.ControllerOperationRequest request) {
    return MetadataRpc.CommandRequest.newBuilder().setHistoryId(history.toString()).setControllerOperation(request)
        .build();
  }

  private static MetadataRpc.CommandRequest operationAck(UUID history, long sequence) {
    return MetadataRpc.CommandRequest.newBuilder().setHistoryId(history.toString())
        .setAcknowledge(MetadataRpc.Acknowledge.newBuilder().setClientEpoch(1).setStreamId("operations")
            .setThroughSequence(sequence)).build();
  }

  private static MetadataRpc.MembershipCommandRequest memberRegistration(UUID history) {
    return MetadataRpc.MembershipCommandRequest.newBuilder().setHistoryId(history.toString())
        .setRegister(MetadataRpc.ParticipantRegistration.newBuilder().setIncarnation(UUID.randomUUID().toString())
            .putEndpoints("query", "https://localhost:8098")).build();
  }

  private static MetadataRpc.ParticipantSession session(MetadataRpc.ParticipantToken token) {
    return MetadataRpc.ParticipantSession.newBuilder().setIncarnation(token.getIncarnation())
        .setRegistrationEpoch(token.getRegistrationEpoch()).build();
  }

  private static MetadataRpc.MembershipCommandRequest memberRenewal(UUID history,
      MetadataRpc.ParticipantSession session, long sequence, MetadataRpc.Revision applied) {
    return MetadataRpc.MembershipCommandRequest.newBuilder().setHistoryId(history.toString())
        .setRenew(MetadataRpc.ParticipantRenewal.newBuilder().setSession(session).setSequence(sequence)
            .setAppliedRevision(applied)).build();
  }

  private CertificateFiles certificate(String name)
      throws Exception {
    Path keyStoreFile = _directory.resolve(name + ".p12");
    Path certificateFile = _directory.resolve(name + ".pem");
    Path keyFile = _directory.resolve(name + "-key.pem");
    String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
    Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "test", "-keyalg", "EC", "-groupname",
        "secp256r1", "-dname", "CN=" + name, "-ext", "SAN=DNS:localhost,IP:127.0.0.1", "-ext",
        "EKU=serverAuth,clientAuth", "-validity", "2", "-storetype", "PKCS12", "-keystore", keyStoreFile.toString(),
        "-storepass", "test-password", "-keypass", "test-password", "-noprompt").redirectErrorStream(true).start();
    if (!process.waitFor(30, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      throw new IOException("Timed out generating TLS test certificate");
    }
    assertEquals(process.exitValue(), 0, new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    try (var input = Files.newInputStream(keyStoreFile)) {
      keyStore.load(input, "test-password".toCharArray());
    }
    X509Certificate certificate = (X509Certificate) keyStore.getCertificate("test");
    Files.writeString(certificateFile, pem("CERTIFICATE", certificate.getEncoded()));
    Files.writeString(keyFile, pem("PRIVATE KEY", keyStore.getKey("test", "test-password".toCharArray()).getEncoded()));
    return new CertificateFiles(certificateFile, keyFile, ControllerAccessPolicy.fingerprint(certificate));
  }

  private static String pem(String kind, byte[] bytes) {
    return "-----BEGIN " + kind + "-----\n" + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(bytes)
        + "\n-----END " + kind + "-----\n";
  }

  private record CertificateFiles(Path certificate, Path key, String fingerprint) {
  }

  /// Per-test trusted policy, engine and actual TLS server. Policy replacement affects already-connected clients.
  @Test(timeOut = 90_000)
  public void testRealNativeInstallationAndLostReceiptResponse()
      throws Exception {
    ParticipantAccessPolicy participants = fingerprint -> {
      if (fingerprint.equals(_certificates.get("controller-a").fingerprint())) {
        return new ParticipantIdentity("Controller_native", Role.CONTROLLER, Set.of());
      }
      return fingerprint.equals(_certificates.get("worker").fingerprint())
          ? new ParticipantIdentity("Server_native", Role.SERVER, Set.of(Capability.SERVER_QUERY)) : null;
    };
    try (Quorum quorum = new Quorum()) {
      quorum.startMembership();
      try (Fixture fixture = new Fixture(MetadataGrpcService.Limits.defaults(), quorum._history, quorum._access,
          quorum._access, participants);
          MetadataGrpcClient controller = fixture.client("controller-a");
          MetadataGrpcClient worker = fixture.client("worker")) {
        UUID history = quorum._history;
        MetadataRpc.ParticipantToken controllerSession = registerReady(controller, history);
        MetadataRpc.ParticipantToken serverSession = registerReady(worker, history);
        MembershipProtocol.ParticipantToken server = MembershipRpcCodec.participant(serverSession);
        assertEquals(quorum._access.execute(new MetadataProtocol.RegisterClient(history, "native-test",
            UUID.randomUUID(), 0), 5000).status(), MetadataProtocol.Status.OK);
        MembershipProtocol.ControlToken control = quorum._access.query(new MetadataProtocol.MembershipQuery(history,
            new MembershipProtocol.GetControl()), 5000).control().controlEpoch();
        Layout layout = new Layout(1, 24, HashVersion.MURMUR2_UTF8_MASK_POSITIVE_V1);
        commitNativeTest(quorum, new MetadataProtocol.OwnershipCommand(history, "native-test", 1, "ownership", 1,
            new OwnershipProtocol.ConfigureLayout(control, layout)));
        String tableName = "analytics.orders_OFFLINE";
        UUID tableId = UUID.randomUUID();
        UUID schemaId = UUID.randomUUID();
        UUID segmentId = UUID.randomUUID();
        TableShard shard = OwnershipProtocol.tableScope(layout, tableName);
        commitNativeTest(quorum, new MetadataProtocol.OwnershipCommand(history, "native-test", 1, "ownership", 2,
            new OwnershipProtocol.AssignOwner(control, 1, shard, 0,
                MembershipRpcCodec.participant(controllerSession))));
        OwnershipProtocol.OwnerToken owner = new OwnershipProtocol.OwnerToken(history, 1, shard, 1,
            MembershipRpcCodec.participant(controllerSession));
        TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
        Schema schema = new Schema.SchemaBuilder().setSchemaName("analytics.orders")
            .addSingleValueDimension("id", DataType.INT).build();
        commitNativeTest(quorum, new MetadataProtocol.Transaction(history, "native-test", 1, "config", 1,
            List.of(MetadataProtocol.Mutation.createTableConfig(history, tableId, table),
                MetadataProtocol.Mutation.createSchema(history, schemaId, schema))));
        try (NativeInstallationScenario local = new NativeInstallationScenario(
            Files.createTempDirectory(_directory, "native-installation-"), tableId, segmentId, table, schema)) {
          OfflineSegmentProtocol.Content content = local.content();
          MetadataProtocol.ObjectVersion tableVersion = new MetadataProtocol.ObjectVersion(history, tableId, 1);
          MetadataProtocol.ObjectVersion schemaVersion = new MetadataProtocol.ObjectVersion(history, schemaId, 1);
          commitNativeTest(quorum, new MetadataProtocol.OfflineSegmentCommand(history, "native-test", 1, "segments", 1,
              new OfflineSegmentProtocol.PublishContent(owner, tableVersion, content)));
          commitNativeTest(quorum, new MetadataProtocol.OfflineSegmentCommand(history, "native-test", 1, "segments", 2,
              new OfflineSegmentProtocol.AssignReplica(owner, segmentId, 0, server,
                  new OfflineSegmentProtocol.Compatibility(tableVersion, schemaVersion), true)));
          UUID load = prepareNativeOperation(quorum, owner, content, server, true);
          RatisOfflineSegmentExecutor.WorkSource source = operationId -> MetadataGrpcClient.decodeInstallationWork(
              worker.installationLookup(MetadataRpc.InstallationLookupRequest.newBuilder()
                  .setHistoryId(history.toString()).setSession(session(serverSession))
                  .setOperationId(operationId.toString()).build(), 5000));
          Work loadWork = source.lookup(load).work();
          local.start(loadWork);
          assertEquals(worker.installationExecute(MetadataRpc.InstallationCommandRequest.newBuilder()
              .setHistoryId(history.toString()).setSession(session(serverSession))
              .setRegister(MetadataRpc.RegisterClient.newBuilder().setIncarnation(serverSession.getIncarnation()))
              .build(), 5000).getStatus(), MetadataRpc.ResultStatus.OK);
          AtomicLong reportSequence = new AtomicLong(1);
          AtomicBoolean loseReply = new AtomicBoolean();
          AtomicReference<Result> committedReceipt = new AtomicReference<>();
          RatisOfflineSegmentExecutor executor = new RatisOfflineSegmentExecutor(server, source, receipt -> {
            Result result = MetadataGrpcClient.decodeResult(worker.installationExecute(
                MetadataRpc.InstallationCommandRequest.newBuilder().setHistoryId(history.toString())
                    .setSession(session(serverSession)).setReport(MetadataRpc.SequencedInstallationReport.newBuilder()
                        .setClientEpoch(1).setStreamId("reports").setSequence(reportSequence.get())
                        .setReport(MetadataGrpcClient.encodeInstallationReport(receipt))).build(), 5000));
            committedReceipt.set(result);
            if (loseReply.getAndSet(false)) {
              throw new IOException("Injected lost installation response after consensus commit");
            }
            return result;
          });
          RatisOfflineSegmentExecutor.Execution loaded = executor.execute(load, local.installer());
          assertEquals(loaded.local().status(), NativeOfflineSegmentAction.Status.INSTALLED);
          assertEquals(loaded.receipt().status(), MetadataProtocol.Status.OK);
          local.verifyRows(false);
          completeNativeOperation(quorum, owner, load);
          assertEquals(worker.installationExecute(MetadataRpc.InstallationCommandRequest.newBuilder()
              .setHistoryId(history.toString()).setSession(session(serverSession))
              .setAcknowledge(MetadataRpc.Acknowledge.newBuilder().setClientEpoch(1).setStreamId("reports")
                  .setThroughSequence(1)).build(), 5000).getStatus(), MetadataRpc.ResultStatus.OK);
          reportSequence.incrementAndGet();

          TableConfig reloadConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName)
              .setInvertedIndexColumns(List.of("id")).build();
          commitNativeTest(quorum, new MetadataProtocol.Transaction(history, "native-test", 1, "config", 2,
              List.of(MetadataProtocol.Mutation.updateTableConfig(tableVersion, reloadConfig))));
          commitNativeTest(quorum, new MetadataProtocol.OfflineSegmentCommand(history, "native-test", 1, "segments", 3,
              new OfflineSegmentProtocol.AssignReplica(owner, segmentId, 1, server,
                  new OfflineSegmentProtocol.Compatibility(new MetadataProtocol.ObjectVersion(history, tableId, 2),
                      schemaVersion), true)));
          UUID reload = prepareNativeOperation(quorum, owner, content, server, false);
          Work reloadWork = source.lookup(reload).work();
          loseReply.set(true);
          expectThrows(IOException.class, () -> executor.execute(reload, local.installer()));
          local.verifyRows(true);
          Result originalOutcome = committedReceipt.get();
          UUID installedId = local.installer().installed(content.segmentName()).installationId();
          // Restart the table worker inside the same SERVER session. A new process registration instead needs a new
          // assignment and operation; this exercises durable selection recovery without borrowing old authority.
          local.start(reloadWork);
          RatisOfflineSegmentExecutor.Execution recovered = executor.execute(reload, local.installer());
          assertEquals(recovered.local().status(), NativeOfflineSegmentAction.Status.INSTALLED);
          assertEquals(recovered.local().installation().installationId(), installedId);
          assertEquals(recovered.receipt(), originalOutcome, "Retry must recover the original committed result");
          local.verifyRows(true);
          completeNativeOperation(quorum, owner, reload);
          OperationProtocol.OperationState completed = OperationProtocol.decodeOperation(quorum._access.query(
              new MetadataProtocol.GetRecord(history, MetadataProtocol.RecordKind.CONTROLLER_OPERATION,
                  reload.toString()), 5000).record().value());
          assertEquals(completed.status(), OperationProtocol.State.SUCCEEDED);
          assertEquals(completed.result().completedTargets(), 1);
          assertEquals(completed.completionProof().installations().size(), 1);

          // A process restart registers a new incarnation: old receipts and the selected old manifest confer no
          // authority. Reconcile the new assignment using verified local bytes after removing the deep-store source.
          assertEquals(worker.membershipExecute(MetadataRpc.MembershipCommandRequest.newBuilder()
              .setHistoryId(history.toString()).setUnregister(session(serverSession)).build()).getStatus(),
              MetadataRpc.ResultStatus.OK);
          MetadataRpc.ParticipantToken replacementSession = registerReady(worker, history,
              serverSession.getRegistrationEpoch());
          MembershipProtocol.ParticipantToken replacement = MembershipRpcCodec.participant(replacementSession);
          commitNativeTest(quorum, new MetadataProtocol.OfflineSegmentCommand(history, "native-test", 1, "segments", 4,
              new OfflineSegmentProtocol.AssignReplica(owner, segmentId, 2, replacement,
                  new OfflineSegmentProtocol.Compatibility(new MetadataProtocol.ObjectVersion(history, tableId, 2),
                      schemaVersion), true)));
          UUID reconcile = prepareNativeOperation(quorum, owner, content, replacement, false, true);
          RatisOfflineSegmentExecutor.WorkSource replacementSource = operationId ->
              MetadataGrpcClient.decodeInstallationWork(worker.installationLookup(
                  MetadataRpc.InstallationLookupRequest.newBuilder().setHistoryId(history.toString())
                      .setSession(session(replacementSession)).setOperationId(operationId.toString()).build(), 5000));
          Work reconcileWork = replacementSource.lookup(reconcile).work();
          assertTrue(reconcileWork.reconcile());
          assertFalse(reconcileWork.initialLoad());
          local.start(reconcileWork);
          Files.delete(Path.of(URI.create(content.downloadUri())));
          assertEquals(executor.execute(reconcile, local.installer()).local().status(),
              NativeOfflineSegmentAction.Status.FENCED);
          assertEquals(worker.installationExecute(MetadataRpc.InstallationCommandRequest.newBuilder()
              .setHistoryId(history.toString()).setSession(session(replacementSession))
              .setRegister(MetadataRpc.RegisterClient.newBuilder().setIncarnation(replacementSession.getIncarnation()))
              .build(), 5000).getStatus(), MetadataRpc.ResultStatus.OK);
          RatisOfflineSegmentExecutor replacementExecutor = new RatisOfflineSegmentExecutor(replacement,
              replacementSource, receipt -> MetadataGrpcClient.decodeResult(worker.installationExecute(
                  MetadataRpc.InstallationCommandRequest.newBuilder().setHistoryId(history.toString())
                      .setSession(session(replacementSession)).setReport(MetadataRpc.SequencedInstallationReport
                          .newBuilder().setClientEpoch(1).setStreamId("reports").setSequence(1)
                          .setReport(MetadataGrpcClient.encodeInstallationReport(receipt))).build(), 5000)));
          RatisOfflineSegmentExecutor.Execution reconciled = replacementExecutor.execute(reconcile, local.installer());
          assertEquals(reconciled.local().status(), NativeOfflineSegmentAction.Status.INSTALLED);
          assertEquals(reconciled.receipt().status(), MetadataProtocol.Status.OK);
          assertNotEquals(reconciled.local().installation().installationId(), installedId);
          assertNotEquals(reconciled.local().installation().manifestSha256(),
              recovered.local().installation().manifestSha256());
          local.verifyRows(true);
          local.start(reconcileWork);
          RatisOfflineSegmentExecutor.Execution replay = replacementExecutor.execute(reconcile, local.installer());
          assertEquals(replay.local().installation().installationId(),
              reconciled.local().installation().installationId());
          assertEquals(replay.receipt(), reconciled.receipt());
          completeNativeOperation(quorum, owner, reconcile);
          OperationProtocol.OperationState restored = OperationProtocol.decodeOperation(quorum._access.query(
              new MetadataProtocol.GetRecord(history, MetadataProtocol.RecordKind.CONTROLLER_OPERATION,
                  reconcile.toString()), 5000).record().value());
          assertEquals(restored.status(), OperationProtocol.State.SUCCEEDED);
          assertEquals(restored.completionProof().installations().size(), 1);
          OfflineInstallationProtocol.Receipt receipt = OfflineInstallationProtocol.decodeReceipt(quorum._access.query(
              new MetadataProtocol.GetRecord(history, MetadataProtocol.RecordKind.OFFLINE_INSTALLATION,
                  OfflineInstallationProtocol.recordName(reconcile, 1, replacement.participantId())), 5000)
              .record().value());
          assertEquals(receipt.installation().installed().server(), replacement);
          assertEquals(receipt.installation().installationId(), reconciled.local().installation().installationId());
          local.verifyRows(true);
        }
      }
    }
  }

  @Test(timeOut = 90_000)
  public void testNativeServerParticipantDiscoversAndReconcilesInstallations()
      throws Exception {
    String participantId = "Server_runtime";
    ParticipantAccessPolicy participants = fingerprint -> {
      if (fingerprint.equals(_certificates.get("controller-a").fingerprint())) {
        return new ParticipantIdentity("Controller_runtime", Role.CONTROLLER, Set.of());
      }
      return fingerprint.equals(_certificates.get("worker").fingerprint())
          ? new ParticipantIdentity(participantId, Role.SERVER, Set.of(Capability.SERVER_QUERY)) : null;
    };
    try (Quorum quorum = new Quorum()) {
      quorum.startMembership();
      try (Fixture fixture = new Fixture(MetadataGrpcService.Limits.defaults(), quorum._history, quorum._access,
          quorum._access, participants); MetadataGrpcClient controller = fixture.client("controller-a");
          MetadataGrpcClient observer = fixture.client("worker")) {
        UUID history = quorum._history;
        MetadataRpc.ParticipantToken controllerSession = registerReady(controller, history);
        assertEquals(quorum._access.execute(new MetadataProtocol.RegisterClient(history, "native-test",
            UUID.randomUUID(), 0), 5000).status(), MetadataProtocol.Status.OK);
        MembershipProtocol.ControlToken control = quorum._access.query(new MetadataProtocol.MembershipQuery(history,
            new MembershipProtocol.GetControl()), 5000).control().controlEpoch();
        Layout layout = new Layout(1, 24, HashVersion.MURMUR2_UTF8_MASK_POSITIVE_V1);
        commitNativeTest(quorum, new MetadataProtocol.OwnershipCommand(history, "native-test", 1, "ownership", 1,
            new OwnershipProtocol.ConfigureLayout(control, layout)));
        String tableName = "analytics.orders_OFFLINE";
        UUID tableId = UUID.randomUUID();
        UUID schemaId = UUID.randomUUID();
        UUID segmentId = UUID.randomUUID();
        TableShard shard = OwnershipProtocol.tableScope(layout, tableName);
        commitNativeTest(quorum, new MetadataProtocol.OwnershipCommand(history, "native-test", 1, "ownership", 2,
            new OwnershipProtocol.AssignOwner(control, 1, shard, 0,
                MembershipRpcCodec.participant(controllerSession))));
        OwnershipProtocol.OwnerToken owner = new OwnershipProtocol.OwnerToken(history, 1, shard, 1,
            MembershipRpcCodec.participant(controllerSession));
        TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
        Schema schema = new Schema.SchemaBuilder().setSchemaName("analytics.orders")
            .addSingleValueDimension("id", DataType.INT).build();
        commitNativeTest(quorum, new MetadataProtocol.Transaction(history, "native-test", 1, "config", 1,
            List.of(MetadataProtocol.Mutation.createTableConfig(history, tableId, table),
                MetadataProtocol.Mutation.createSchema(history, schemaId, schema))));
        // Only this test supplies a synthetic qualified clock. No network query listener is started.
        AtomicLong clockNanos = new AtomicLong();
        ServingSettings serving = new ServingSettings(() -> new ClockReading(clockNanos.get(), 0, true),
            new QualifiedLifetimePolicy(1000, 1000, 1_000_000, 0, 0),
            new QueryDeadlineGuard(() -> {
              long now = TimeUnit.NANOSECONDS.toMillis(clockNanos.get());
              return new TimeInterval(now, now, true);
            }, "test-clock", 1000, 0, 1_000_000));
        Path directory = Files.createTempDirectory(_directory, "native-runtime-");
        PinotConfiguration config = new PinotConfiguration(Map.of("id", participantId,
            "dataDir", directory.resolve("server").toString(), "segmentTarDir", directory.resolve("tar").toString(),
            "max.parallel.refresh.threads", 1));
        try (NativeInstallationScenario bytes = new NativeInstallationScenario(directory, tableId, segmentId,
            table, schema); NativeMetadataConnection work = spy(nativeConnection(fixture));
            NativeServerParticipant first = nativeParticipant(fixture, participantId, work, serving)) {
          AtomicBoolean loseReport = new AtomicBoolean(true);
          AtomicInteger recoveredOutcomes = new AtomicInteger();
          CountDownLatch reportCommitted = new CountDownLatch(1);
          CountDownLatch releaseReport = new CountDownLatch(1);
          CountDownLatch recoveredAck = new CountDownLatch(1);
          doAnswer(invocation -> {
            MetadataRpc.InstallationCommandRequest request = invocation.getArgument(0);
            MetadataRpc.CommandResult result = (MetadataRpc.CommandResult) invocation.callRealMethod();
            if (request.hasReport() && loseReport.getAndSet(false)) {
              reportCommitted.countDown();
              if (!releaseReport.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Committed report response was not released");
              }
              throw Status.UNAVAILABLE.withDescription("Injected lost committed installation response")
                  .asRuntimeException();
            }
            if (request.hasAcknowledge()) {
              recoveredAck.countDown();
            }
            return result;
          }).when(work).execute(any(), anyLong());
          doAnswer(invocation -> {
            MetadataRpc.CommandResult result = (MetadataRpc.CommandResult) invocation.callRealMethod();
            recoveredOutcomes.incrementAndGet();
            return result;
          }).when(work).outcome(any(), anyLong());
          first.start(config, ServerMetrics.get(), null, mock(ServerReloadJobStatusCache.class));
          assertTrue(first.isReadyForInstallations());
          MetadataRpc.ParticipantToken firstToken = first.participant();
          MembershipProtocol.ParticipantState before = quorum._access.query(new MetadataProtocol.MembershipQuery(
              history, new MembershipProtocol.GetParticipant(participantId)), 5000).participant();
          assertTrue(before.ready());
          assertEquals(before.capabilities(), Set.of(Capability.SERVER_QUERY));
          assertTrue(first.dataManager().getAllTables().isEmpty(), "No assignment exists at bootstrap");
          OfflineSegmentProtocol.Content content = bytes.content();
          OfflineSegmentProtocol.Compatibility compatibility = new OfflineSegmentProtocol.Compatibility(
              new MetadataProtocol.ObjectVersion(history, tableId, 1),
              new MetadataProtocol.ObjectVersion(history, schemaId, 1));
          commitNativeTest(quorum, new MetadataProtocol.OfflineSegmentCommand(history, "native-test", 1, "segments", 1,
              new OfflineSegmentProtocol.PublishContent(owner, compatibility.table(), content)));
          commitNativeTest(quorum, new MetadataProtocol.OfflineSegmentCommand(history, "native-test", 1, "segments", 2,
              new OfflineSegmentProtocol.AssignReplica(owner, segmentId, 0,
                  MembershipRpcCodec.participant(firstToken), compatibility, true)));
          UUID load = prepareNativeOperation(quorum, owner, content, MembershipRpcCodec.participant(firstToken), true);
          assertTrue(reportCommitted.await(15, TimeUnit.SECONDS), first.failure());
          OfflineInstallationProtocol.Receipt loaded = awaitNativeReceipt(quorum, first, load);
          assertEquals(loaded.installation().installed().server(), MembershipRpcCodec.participant(firstToken));
          bytes.verifyRows(first.dataManager().getTableDataManager(tableName), false);
          completeNativeOperation(quorum, owner, load);
          // The controller has removed the completed operation from discovery before the worker learns whether its
          // report committed. Independent outbox recovery must still query and acknowledge the original outcome.
          releaseReport.countDown();
          assertTrue(recoveredAck.await(10, TimeUnit.SECONDS), first.failure());
          assertTrue(recoveredOutcomes.get() > 0, "A completed operation's unknown receipt still needs recovery");
          awaitNativeHeartbeat(quorum, first, before);
          NativeQueryContext firstContext = installedQuery(first, tableName, content.segmentName(),
              serving.queryDeadlines());
          QueryAdmission firstAdmission = awaitNativeAdmission(first, firstContext);
          assertEquals(firstAdmission.context(), firstContext);
          SingleTableExecutionInfo pinned = SingleTableExecutionInfo.create(first.dataManager(), tableName,
              firstContext.requiredNames(), firstContext.optionalNames(), mock(QueryContext.class),
              firstAdmission.context());
          try {
            assertEquals(pinned.getNumSegmentsAcquired(), 1);
            assertEquals(pinned.getIndexSegments().get(0).getSegmentMetadata().getTotalDocs(), 5);
            awaitNativeDelivery(first, firstAdmission, false);
          } finally {
            pinned.releaseSegmentDataManagers();
          }
          assertEquals(observer.membershipExecute(MetadataRpc.MembershipCommandRequest.newBuilder()
              .setHistoryId(history.toString()).setDrain(session(firstToken)).build()).getStatus(),
              MetadataRpc.ResultStatus.OK);
          awaitNativeDelivery(first, firstAdmission, true);
          expectThrows(IllegalStateException.class, () -> first.admitQuery(firstContext));
          assertFalse(first.wake(load), "Draining rejects new installation work");
          clockNanos.set(TimeUnit.MILLISECONDS.toNanos(1001));
          expectThrows(IllegalStateException.class, () -> first.checkQueryDelivery(firstAdmission));
          UUID originalInstallation = loaded.installation().installationId();
          first.close();
          expectThrows(IllegalStateException.class, () -> first.checkQueryDelivery(firstAdmission));
          assertFalse(first.isReadyForInstallations());
          assertTrue(quorum._access.query(new MetadataProtocol.MembershipQuery(history,
              new MembershipProtocol.GetParticipant(participantId)), 5000).participant().retired());
          // A real process lifecycle owns the same directory again, registers a new token and discovers fresh
          // RECONCILE intent. The old selected bytes remain usable even after the source archive disappears.
          Files.delete(Path.of(URI.create(content.downloadUri())));
          try (NativeServerParticipant restarted =
              nativeParticipant(fixture, participantId, nativeConnection(fixture), serving)) {
            restarted.start(config, ServerMetrics.get(), null, mock(ServerReloadJobStatusCache.class));
            MetadataRpc.ParticipantToken replacement = restarted.participant();
            assertNotEquals(replacement.getIncarnation(), firstToken.getIncarnation());
            assertEquals(replacement.getRegistrationEpoch(), firstToken.getRegistrationEpoch() + 1);
            assertTrue(restarted.dataManager().getAllTables().isEmpty(),
                "A new session cannot silently expose selected files from the previous registration");
            commitNativeTest(quorum, new MetadataProtocol.OfflineSegmentCommand(history, "native-test", 1,
                "segments", 3, new OfflineSegmentProtocol.AssignReplica(owner, segmentId, 1,
                    MembershipRpcCodec.participant(replacement), compatibility, true)));
            UUID reconcile = prepareNativeOperation(quorum, owner, content,
                MembershipRpcCodec.participant(replacement), false, true);
            OfflineInstallationProtocol.Receipt reconciled = awaitNativeReceipt(quorum, restarted, reconcile);
            assertEquals(reconciled.installation().installed().server(), MembershipRpcCodec.participant(replacement));
            assertNotEquals(reconciled.installation().installationId(), originalInstallation);
            assertEquals(restarted.dataManager().executor(tableName).installed(content.segmentName()).installationId(),
                reconciled.installation().installationId());
            bytes.verifyRows(restarted.dataManager().getTableDataManager(tableName), false);
            completeNativeOperation(quorum, owner, reconcile);
            OperationProtocol.OperationState completed = OperationProtocol.decodeOperation(quorum._access.query(
                new MetadataProtocol.GetRecord(history, MetadataProtocol.RecordKind.CONTROLLER_OPERATION,
                    reconcile.toString()), 5000).record().value());
            assertEquals(completed.status(), OperationProtocol.State.SUCCEEDED);
            assertEquals(completed.completionProof().installations().size(), 1);
            NativeQueryContext replacementContext = installedQuery(restarted, tableName, content.segmentName(),
                serving.queryDeadlines());
            QueryAdmission replacementAdmission = awaitNativeAdmission(restarted, replacementContext);
            expectThrows(IllegalStateException.class, () -> restarted.admitQuery(firstContext));
            expectThrows(IllegalStateException.class, () -> restarted.checkQueryDelivery(firstAdmission));
            awaitNativeDelivery(restarted, replacementAdmission, false);
          }
        }
      }
    }
  }

  private NativeServerParticipant nativeParticipant(Fixture fixture, String participantId,
      NativeMetadataConnection work, ServingSettings serving) throws IOException {
    return new NativeServerParticipant(new NativeServerParticipant.Settings(fixture._history, participantId, Map.of(),
        5000, 100, 100, 1, 16), nativeConnection(fixture), work, serving);
  }

  private static NativeQueryContext installedQuery(NativeServerParticipant server, String tableName,
      String segmentName, QueryDeadlineGuard queryDeadlines) throws IOException {
    NativeOfflineSegmentAction.Identity installed = server.dataManager().executor(tableName).installed(segmentName)
        .identity();
    // The normal broker's authentication/routing is outside this local admission test and remains gated.
    return new NativeQueryContext(NativeQueryContext.VERSION, installed.historyId(),
        new NativeQueryContext.ProcessIdentity("Broker_test", UUID.randomUUID(), 1), installed.server(),
        UUID.randomUUID(), UUID.randomUUID(), installed.tableName(), installed.table(), installed.schemaName(),
        installed.schema(),
        List.of(new NativeQueryContext.Segment(segmentName, installed.content(), installed.contentSha256(),
            installed.assignment(), installed.materializationId())), List.of())
        .withDeadline(queryDeadlines.originate(1000));
  }

  private static QueryAdmission awaitNativeAdmission(NativeServerParticipant server, NativeQueryContext context)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      try {
        return server.admitQuery(context);
      } catch (IllegalStateException e) {
        // A real committed heartbeat grant still needs a coherent own-server image at its minimum revision.
        Thread.sleep(20);
      }
    }
    throw new AssertionError("Native runtime did not admit current content: " + server.failure());
  }

  private static void awaitNativeDelivery(NativeServerParticipant server, QueryAdmission admitted, boolean draining)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      if (!draining || !server.isReadyForInstallations()) {
        try {
          server.checkQueryDelivery(admitted);
          return;
        } catch (IllegalStateException e) {
          // An in-progress hydration is unavailable; wait for the complete committed image.
        }
      }
      Thread.sleep(20);
    }
    throw new AssertionError("Native runtime did not permit delivery, draining=" + draining + ": " + server.failure());
  }

  private NativeMetadataConnection nativeConnection(Fixture fixture)
      throws IOException {
    CertificateFiles certificate = _certificates.get("worker");
    int unavailablePort;
    try (ServerSocket reserved = new ServerSocket(0)) {
      unavailablePort = reserved.getLocalPort();
    }
    List<NativeMetadataConnection.Endpoint> endpoints = List.of(
        new NativeMetadataConnection.Endpoint("127.0.0.1", unavailablePort),
        new NativeMetadataConnection.Endpoint("127.0.0.1", fixture._server.port()));
    Path trust = _certificates.get("server").certificate();
    return new NativeMetadataConnection(endpoints, certificate.certificate(), certificate.key(), trust, 5000);
  }

  private static OfflineInstallationProtocol.Receipt awaitNativeReceipt(Quorum quorum, NativeServerParticipant server,
      UUID operation) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (System.nanoTime() < deadline) {
      Result result = quorum._access.query(new MetadataProtocol.GetRecord(quorum._history,
          MetadataProtocol.RecordKind.OFFLINE_INSTALLATION,
          OfflineInstallationProtocol.recordName(operation, 1, server.participant().getParticipantId())), 5000);
      if (result.status() == MetadataProtocol.Status.OK) {
        return OfflineInstallationProtocol.decodeReceipt(result.record().value());
      }
      assertEquals(result.status(), MetadataProtocol.Status.NOT_FOUND);
      Thread.sleep(20);
    }
    throw new AssertionError("Native runtime did not commit an installation receipt: " + server.failure());
  }

  private static void awaitNativeHeartbeat(Quorum quorum, NativeServerParticipant server,
      MembershipProtocol.ParticipantState previous) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      MembershipProtocol.ParticipantState current = quorum._access.query(new MetadataProtocol.MembershipQuery(
          quorum._history, new MembershipProtocol.GetParticipant(server.participant().getParticipantId())), 5000)
          .participant();
      if (current.lastRenewalSequence() > previous.lastRenewalSequence()
          && current.appliedRevision().index() > previous.appliedRevision().index()) {
        assertEquals(current.capabilities(), previous.capabilities());
        return;
      }
      Thread.sleep(20);
    }
    throw new AssertionError("Native runtime did not advance its applied metadata heartbeat: " + server.failure());
  }

  @Test(timeOut = 90_000)
  public void testMinionTaskIdentityOutcomesAndProtectedRenewal() throws Exception {
    ParticipantIdentity minion = new ParticipantIdentity("Minion_worker", Role.MINION, Set.of(Capability.MINION_TASK));
    AtomicReference<ParticipantIdentity> binding = new AtomicReference<>(minion);
    ParticipantAccessPolicy policy = fingerprint -> {
      if (fingerprint.equals(_certificates.get("worker").fingerprint())) {
        return binding.get();
      }
      if (fingerprint.equals(_certificates.get("controller-a").fingerprint())) {
        return new ParticipantIdentity("Controller_tasks", Role.CONTROLLER, Set.of());
      }
      return fingerprint.equals(_certificates.get("controller-b").fingerprint())
          ? new ParticipantIdentity("Minion_other", Role.MINION, Set.of(Capability.MINION_TASK)) : null;
    };
    try (Quorum quorum = new Quorum()) {
      quorum.startMembership();
      BlockingAccess access = new BlockingAccess(quorum._access);
      try (Fixture fixture = new Fixture(MetadataGrpcService.Limits.defaults(), quorum._history, access,
          quorum._access, policy);
          MetadataGrpcClient controller = fixture.client("controller-a");
          MetadataGrpcClient worker = fixture.client("worker");
          MetadataGrpcClient other = fixture.client("controller-b")) {
        UUID history = quorum._history;
        MetadataRpc.ParticipantToken controllerSession = registerReady(controller, history);
        MetadataRpc.ParticipantToken workerSession = registerReady(worker, history);
        MetadataRpc.ParticipantToken otherSession = registerReady(other, history);
        assertEquals(workerSession.getRole(), MetadataRpc.ParticipantRole.MINION);
        assertStatus(Status.Code.PERMISSION_DENIED, () -> worker.read(fixture.revision()));
        assertStatus(Status.Code.PERMISSION_DENIED, () -> worker.openSnapshot(fixture.open()));
        assertEquals(quorum._access.execute(new MetadataProtocol.RegisterClient(history, "native-test",
            UUID.randomUUID(), 0), 5000).status(), MetadataProtocol.Status.OK);
        MembershipProtocol.ControlToken control = quorum._access.query(new MetadataProtocol.MembershipQuery(history,
            new MembershipProtocol.GetControl()), 5000).control().controlEpoch();
        Layout layout = new Layout(1, 24, HashVersion.MURMUR2_UTF8_MASK_POSITIVE_V1);
        commitNativeTest(quorum, new MetadataProtocol.OwnershipCommand(history, "native-test", 1, "ownership", 1,
            new OwnershipProtocol.ConfigureLayout(control, layout)));
        String tableName = "tasks_OFFLINE";
        UUID tableId = UUID.randomUUID();
        TableShard scope = OwnershipProtocol.tableScope(layout, tableName);
        commitNativeTest(quorum, new MetadataProtocol.OwnershipCommand(history, "native-test", 1, "ownership", 2,
            new OwnershipProtocol.AssignOwner(control, 1, scope, 0,
                MembershipRpcCodec.participant(controllerSession))));
        OwnershipProtocol.OwnerToken owner = new OwnershipProtocol.OwnerToken(history, 1, scope, 1,
            MembershipRpcCodec.participant(controllerSession));
        commitNativeTest(quorum, new MetadataProtocol.Transaction(history, "native-test", 1, "config", 1,
            List.of(MetadataProtocol.Mutation.createTableConfig(history, tableId,
                    new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build()),
                MetadataProtocol.Mutation.createSchema(history, UUID.randomUUID(),
                    new Schema.SchemaBuilder().setSchemaName("tasks").addSingleValueDimension("id", DataType.INT)
                        .build()))));
        TaskProtocol.QueueKey queue = new TaskProtocol.QueueKey(tableName, tableId,
            TaskProtocol.NATIVE_SEGMENT_GENERATION_TASK);
        UUID ownTask = UUID.randomUUID();
        UUID otherTask = UUID.randomUUID();
        commitNativeTest(quorum, new MetadataProtocol.TaskCommand(history, "native-test", 1, "tasks", 1,
            new TaskProtocol.Submit(owner, ownTask, new TaskProtocol.Input(queue, minion.participantId(), Map.of(), 2),
                List.of())));
        commitNativeTest(quorum, new MetadataProtocol.TaskCommand(history, "native-test", 1, "tasks", 2,
            new TaskProtocol.Submit(owner, otherTask,
                new TaskProtocol.Input(queue, otherSession.getParticipantId(), Map.of(), 2), List.of())));
        MetadataRpc.TaskWorkRequest work = MetadataRpc.TaskWorkRequest.newBuilder().setHistoryId(history.toString())
            .setSession(session(workerSession)).setMaxRecords(128).setMaxBytes(TaskProtocol.MAX_SNAPSHOT_BYTES).build();
        MetadataRpc.TaskWorkResult image = worker.taskWork(work, 5000);
        assertEquals(image.getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(image.getParticipant().getParticipant(), workerSession);
        assertEquals(image.getRecordsList().stream().filter(r -> r.getKind() == MetadataRpc.RecordKind.TASK)
            .map(r -> r.getValue().getTask().getTaskId()).toList(), List.of(ownTask.toString()));
        assertEquals(other.taskWork(work, 5000).getStatus(), MetadataRpc.ResultStatus.FENCED);
        assertStatus(Status.Code.PERMISSION_DENIED, () -> controller.taskWork(work, 5000));
        MetadataRpc.TaskWorkResult oversized = worker.taskWork(work.toBuilder().setMaxRecords(1).build(), 5000);
        assertEquals(oversized.getStatus(), MetadataRpc.ResultStatus.RESPONSE_TOO_LARGE);
        assertEquals(oversized.getRecordsCount(), 0);
        MetadataRpc.TaskCommandRequest registration = MetadataRpc.TaskCommandRequest.newBuilder()
            .setHistoryId(history.toString()).setSession(session(workerSession)).setRegister(
                MetadataRpc.RegisterClient.newBuilder().setIncarnation(workerSession.getIncarnation())).build();
        assertEquals(worker.taskExecute(registration, 5000).getStatus(), MetadataRpc.ResultStatus.OK);
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> worker.taskExecute(registration.toBuilder().setRegister(
            registration.getRegister().toBuilder().setIncarnation(UUID.randomUUID().toString())).build(), 5000));
        MetadataRpc.TaskCommandRequest claim = registration.toBuilder().setOperation(
            MetadataRpc.SequencedTaskOperation.newBuilder().setClientEpoch(1).setStreamId("tasks").setSequence(1)
                .setClaim(MetadataRpc.TaskClaim.newBuilder().setTaskId(ownTask.toString()).setExpectedVersion(1)))
            .build();
        MetadataRpc.CommandResult accepted = worker.taskExecute(claim, 5000);
        assertEquals(accepted.getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(worker.taskExecute(claim, 5000), accepted, "Lost reply retries retain the committed outcome");
        MetadataRpc.TaskOutcomeRequest outcome = MetadataRpc.TaskOutcomeRequest.newBuilder()
            .setHistoryId(history.toString()).setSession(session(workerSession)).setOutcome(
                MetadataRpc.OutcomeQuery.newBuilder().setClientEpoch(1).setStreamId("tasks").setSequence(1)).build();
        assertEquals(worker.taskOutcome(outcome, 5000), accepted);
        assertEquals(other.taskOutcome(outcome, 5000).getStatus(), MetadataRpc.ResultStatus.FENCED);
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> worker.taskExecute(claim.toBuilder().setUnknownFields(
            UnknownFieldSet.newBuilder().addField(100, UnknownFieldSet.Field.newBuilder()
                .addLengthDelimited(ByteString.copyFromUtf8("generic mutation")).build()).build()).build(), 5000));
        binding.set(new ParticipantIdentity(minion.participantId(), Role.MINION, Set.of()));
        assertStatus(Status.Code.PERMISSION_DENIED, () -> worker.taskWork(work, 5000));
        assertStatus(Status.Code.PERMISSION_DENIED, () -> worker.taskExecute(claim, 5000));
        binding.set(minion);
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        access._beforeWork.set(() -> {
          entered.countDown();
          try {
            if (!release.await(10, TimeUnit.SECONDS)) {
              throw new IllegalStateException("Task test gate was not released");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
        });
        ExecutorService requests = Executors.newFixedThreadPool(2);
        try {
          Future<MetadataRpc.TaskWorkResult> first = requests.submit(() -> worker.taskWork(work, 5000));
          Future<MetadataRpc.TaskWorkResult> second = requests.submit(() -> worker.taskWork(work, 5000));
          assertTrue(entered.await(5, TimeUnit.SECONDS));
          MetadataRpc.MembershipResponse renewed = worker.membershipExecute(memberRenewal(history,
              session(workerSession), 1, image.getRevision()));
          assertEquals(renewed.getRenewals(0).getStatus(), MetadataRpc.ResultStatus.OK);
          assertEquals(renewed.getRenewals(0).getPermit().getCapabilitiesList(),
              List.of(MetadataRpc.ServingCapability.MINION_TASK));
          assertEquals(release.getCount(), 1L, "Membership renewal must not wait for task handlers");
          binding.set(new ParticipantIdentity(minion.participantId(), Role.MINION, Set.of()));
          release.countDown();
          for (Future<MetadataRpc.TaskWorkResult> pending : List.of(first, second)) {
            ExecutionException denied = expectThrows(ExecutionException.class, () -> pending.get(5, TimeUnit.SECONDS));
            assertEquals(Status.fromThrowable(denied.getCause()).getCode(), Status.Code.PERMISSION_DENIED);
          }
        } finally {
          release.countDown();
          requests.shutdownNow();
          access._beforeWork.set(() -> { });
          binding.set(minion);
        }
        MetadataRpc.TaskCommandRequest acknowledge = registration.toBuilder().setAcknowledge(
            MetadataRpc.Acknowledge.newBuilder().setClientEpoch(1).setStreamId("tasks").setThroughSequence(1)).build();
        assertEquals(worker.taskExecute(acknowledge, 5000).getStatus(), MetadataRpc.ResultStatus.OK);
        assertEquals(worker.taskOutcome(outcome, 5000).getStatus(), MetadataRpc.ResultStatus.RESULT_EXPIRED);
        assertEquals(worker.membershipExecute(MetadataRpc.MembershipCommandRequest.newBuilder()
            .setHistoryId(history.toString()).setUnregister(session(workerSession)).build()).getStatus(),
            MetadataRpc.ResultStatus.OK);
        assertEquals(worker.taskExecute(claim, 5000).getStatus(), MetadataRpc.ResultStatus.FENCED);
        assertEquals(worker.taskOutcome(outcome, 5000).getStatus(), MetadataRpc.ResultStatus.FENCED);
      }
    }
  }

  private static MetadataRpc.ParticipantToken registerReady(MetadataGrpcClient client, UUID history)
      throws IOException {
    return registerReady(client, history, 0);
  }

  private static MetadataRpc.ParticipantToken registerReady(MetadataGrpcClient client, UUID history, long expectedEpoch)
      throws IOException {
    MetadataRpc.MembershipCommandRequest registration = memberRegistration(history);
    MetadataRpc.MembershipResponse registered = client.membershipExecute(registration.toBuilder()
        .setRegister(registration.getRegister().toBuilder().setExpectedEpoch(expectedEpoch)).build());
    assertEquals(registered.getStatus(), MetadataRpc.ResultStatus.OK);
    MetadataRpc.ParticipantToken token = registered.getParticipant().getParticipant();
    MetadataRpc.MembershipResponse ready = client.membershipExecute(MetadataRpc.MembershipCommandRequest.newBuilder()
        .setHistoryId(history.toString()).setReady(MetadataRpc.ParticipantReadiness.newBuilder()
            .setSession(session(token)).setAppliedRevision(registered.getRevision()).setReady(true)).build());
    assertEquals(ready.getStatus(), MetadataRpc.ResultStatus.OK);
    return token;
  }

  private static void commitNativeTest(Quorum quorum, MetadataProtocol.SequencedCommand command)
      throws IOException {
    Result result = quorum._access.execute(command, 5000);
    assertEquals(result.status(), MetadataProtocol.Status.OK, result.detail());
    assertEquals(quorum._access.execute(new MetadataProtocol.Acknowledge(command.historyId(), command.clientId(),
        command.clientEpoch(), command.streamId(), command.sequence()), 5000).status(), MetadataProtocol.Status.OK);
  }

  private static UUID prepareNativeOperation(Quorum quorum, OwnershipProtocol.OwnerToken owner,
      OfflineSegmentProtocol.Content content, MembershipProtocol.ParticipantToken server, boolean initialLoad)
      throws IOException {
    return prepareNativeOperation(quorum, owner, content, server, initialLoad, false);
  }

  private static UUID prepareNativeOperation(Quorum quorum, OwnershipProtocol.OwnerToken owner,
      OfflineSegmentProtocol.Content content, MembershipProtocol.ParticipantToken server, boolean initialLoad,
      boolean reconcile) throws IOException {
    UUID history = quorum._history;
    OfflineSegmentProtocol.Assignment assignment = OfflineSegmentProtocol.decodeAssignment(quorum._access.query(
        new MetadataProtocol.GetRecord(history, MetadataProtocol.RecordKind.OFFLINE_REPLICA_ASSIGNMENT,
            OfflineSegmentProtocol.assignmentName(content.segmentObjectId(), server.participantId())), 5000)
        .record().value());
    UUID id = UUID.randomUUID();
    OperationProtocol.OfflineSegmentReload intent = new OperationProtocol.OfflineSegmentReload(content.tableName(),
        content.tableObjectId(), content.segmentName(), content.segmentObjectId(), content.contentSha256(),
        List.of(new OperationProtocol.TargetMaterialization(server, assignment.assignmentEpoch(),
            assignment.materializationId())), !reconcile, initialLoad, reconcile);
    commitNativeTest(quorum, new MetadataProtocol.NativeOperationCommand(history, "native-test", 1, id.toString(), 1,
        new OperationProtocol.Submit(id, intent)));
    OperationProtocol.OperationState accepted = OperationProtocol.decodeOperation(quorum._access.query(
        new MetadataProtocol.GetRecord(history, MetadataProtocol.RecordKind.CONTROLLER_OPERATION, id.toString()), 5000)
        .record().value());
    List<MetadataProtocol.ObjectVersion> dependencies =
        ((OperationProtocol.AssignedOfflineSegmentReload) accepted.input()).dependencies();
    commitNativeTest(quorum, new MetadataProtocol.NativeOperationCommand(history, "native-test", 1, id.toString(), 2,
        new OperationProtocol.Claim(id, 1, owner, dependencies)));
    return id;
  }

  private static void completeNativeOperation(Quorum quorum, OwnershipProtocol.OwnerToken owner, UUID id)
      throws IOException {
    commitNativeTest(quorum, new MetadataProtocol.NativeOperationCommand(quorum._history, "native-test", 1,
        id.toString(), 3, new OperationProtocol.CompleteInstalled(id, 2, owner, 1)));
  }

  private final class Fixture implements AutoCloseable {
    private final UUID _history;
    private final UUID _objectId = UUID.randomUUID();
    private final Backend _backend;
    private final AtomicReference<Map<String, ControllerIdentity>> _policy = new AtomicReference<>(Map.of());
    private final List<List<String>> _audit = new CopyOnWriteArrayList<>();
    private final MetadataGrpcServer _server;

    private Fixture(MetadataGrpcService.Limits limits)
        throws IOException {
      this(limits, new Backend(UUID.randomUUID()));
    }

    private Fixture(MetadataGrpcService.Limits limits, Backend backend)
        throws IOException {
      this(limits, backend._history, backend);
    }

    private Fixture(MetadataGrpcService.Limits limits, UUID history, MetadataAccess access)
        throws IOException {
      this(limits, history, access, null, null);
    }

    private Fixture(MetadataGrpcService.Limits limits, UUID history, MetadataAccess access,
        MembershipAccess membershipAccess, ParticipantAccessPolicy participantPolicy)
        throws IOException {
      _history = history;
      _backend = access instanceof Backend backend ? backend : null;
      authorize("controller-a", "generation-1");
      authorize("controller-b", "generation-1");
      CertificateFiles server = _certificates.get("server");
      _server = new MetadataGrpcServer(new InetSocketAddress("127.0.0.1", 0), server.certificate(), server.key(),
          _trustClients, access, fingerprint -> _policy.get().get(fingerprint), membershipAccess, participantPolicy,
          (identity, operation, outcome) -> _audit.add(List.of(identity, operation, outcome)), limits);
    }

    private void authorize(String name, String generation) {
      _policy.updateAndGet(previous -> {
        Map<String, ControllerIdentity> copy = new HashMap<>(previous);
        copy.put(_certificates.get(name).fingerprint(), new ControllerIdentity(name, generation));
        return Map.copyOf(copy);
      });
    }

    private void revoke(String name) {
      _policy.updateAndGet(previous -> {
        Map<String, ControllerIdentity> copy = new HashMap<>(previous);
        copy.remove(_certificates.get(name).fingerprint());
        return Map.copyOf(copy);
      });
    }

    private MetadataGrpcClient client(String name)
        throws IOException {
      CertificateFiles certificate = _certificates.get(name);
      return new MetadataGrpcClient("127.0.0.1", _server.port(), certificate.certificate(), certificate.key(),
          _certificates.get("server").certificate(), 5000);
    }

    private MetadataRpc.ReadRequest revision() {
      return MetadataRpc.ReadRequest.newBuilder().setHistoryId(_history.toString())
          .setRevision(MetadataRpc.Empty.getDefaultInstance()).build();
    }

    private MetadataRpc.CommandRequest register() {
      return MetadataRpc.CommandRequest.newBuilder().setHistoryId(_history.toString())
          .setRegister(MetadataRpc.RegisterClient.newBuilder().setIncarnation(UUID.randomUUID().toString())).build();
    }

    private MetadataRpc.CommandRequest create()
        throws IOException {
      Schema schema = new Schema.SchemaBuilder().setSchemaName("schema").addSingleValueDimension("id", DataType.INT)
          .build();
      MetadataRpc.Mutation schemaMutation = MetadataRpc.Mutation.newBuilder()
          .setAction(MetadataRpc.Mutation.Action.CREATE)
          .setKind(MetadataRpc.RecordKind.SCHEMA).setName("schema")
          .setExpected(MetadataRpc.ObjectVersion.newBuilder().setHistoryId(_history.toString())
              .setObjectId(UUID.randomUUID().toString()))
          .setValue(MetadataRpc.CatalogValue.newBuilder()
              .setSchemaJson(ByteString.copyFrom(JsonUtils.objectToBytes(schema))))
          .build();
      return transaction(1, MetadataRpc.Mutation.Action.CREATE).toBuilder()
          .setTransaction(transaction(1, MetadataRpc.Mutation.Action.CREATE).getTransaction().toBuilder()
              .addMutations(schemaMutation)).build();
    }

    private MetadataRpc.CommandRequest update(int sequence) {
      return transaction(sequence, MetadataRpc.Mutation.Action.UPDATE);
    }

    private MetadataRpc.CommandRequest transaction(int sequence, MetadataRpc.Mutation.Action action) {
      MetadataRpc.Mutation mutation = MetadataRpc.Mutation.newBuilder().setAction(action)
          .setKind(MetadataRpc.RecordKind.CLUSTER_CONFIG).setName(MetadataProtocol.CLUSTER_CONFIG_NAME)
          .setExpected(MetadataRpc.ObjectVersion.newBuilder().setHistoryId(_history.toString())
              .setObjectId(_objectId.toString()).setVersion(sequence - 1))
          .setValue(MetadataRpc.CatalogValue.newBuilder().setClusterConfig(MetadataRpc.ClusterConfig.newBuilder()
              .putProperties("key", "private-value-" + sequence))).build();
      return MetadataRpc.CommandRequest.newBuilder().setHistoryId(_history.toString())
          .setTransaction(MetadataRpc.Transaction.newBuilder().setClientEpoch(1).setStreamId("main")
              .setSequence(sequence).addMutations(mutation)).build();
    }

    private MetadataRpc.CommandRequest acknowledge(int sequence) {
      return MetadataRpc.CommandRequest.newBuilder().setHistoryId(_history.toString())
          .setAcknowledge(MetadataRpc.Acknowledge.newBuilder().setClientEpoch(1).setStreamId("main")
              .setThroughSequence(sequence)).build();
    }

    private MetadataRpc.OpenSnapshotRequest open() {
      return MetadataRpc.OpenSnapshotRequest.newBuilder().setHistoryId(_history.toString())
          .addKinds(MetadataRpc.RecordKind.CLUSTER_CONFIG).addKinds(MetadataRpc.RecordKind.SCHEMA).build();
    }

    private MetadataRpc.SnapshotPageRequest page(String handle, int offset, int maxRecords) {
      return MetadataRpc.SnapshotPageRequest.newBuilder().setSnapshotHandle(handle).setOffset(offset)
          .setMaxRecords(maxRecords).setMaxBytes(MetadataProtocol.MAX_CHANGE_BYTES).build();
    }

    private MetadataRpc.ChangesRequest changes(MetadataRpc.Cursor cursor) {
      return MetadataRpc.ChangesRequest.newBuilder().setCursor(cursor).addAllKinds(open().getKindsList())
          .setMaxTransactions(128).setMaxRecords(128).setMaxBytes(MetadataProtocol.MAX_CHANGE_BYTES).build();
    }

    @Override
    public void close() {
      _server.close();
    }
  }

  /// Actual quorum behind the public TLS service; native consensus remains confined to the loopback test host.
  private final class Quorum implements AutoCloseable {
    private final UUID _history = UUID.randomUUID();
    private final List<RatisMetadataStore> _stores = new ArrayList<>();
    private final RatisMetadataAccess _access;

    private Quorum()
        throws IOException {
      String voters;
      try (ServerSocket first = new ServerSocket(0); ServerSocket second = new ServerSocket(0);
          ServerSocket third = new ServerSocket(0)) {
        voters = "voter-0@127.0.0.1:" + first.getLocalPort() + ",voter-1@127.0.0.1:" + second.getLocalPort()
            + ",voter-2@127.0.0.1:" + third.getLocalPort();
      }
      String prefix = "pinot.coordination.ratis.";
      String clusterId = UUID.randomUUID().toString();
      try {
        for (int i = 0; i < 3; i++) {
          _stores.add(new RatisMetadataStore(new PinotConfiguration(Map.of(
              prefix + "clusterId", clusterId, prefix + "historyId", _history.toString(), prefix + "voters", voters,
              prefix + "id", "voter-" + i, prefix + "dataDir", _directory.resolve(_history + "/voter-" + i).toString(),
              prefix + "bootstrap", true, prefix + "request.timeoutMs", 10_000L), Map.of())));
        }
        _access = _stores.getFirst().openMetadataAccess();
      } catch (IOException | RuntimeException | Error e) {
        for (RatisMetadataStore store : _stores) {
          try {
            store.close();
          } catch (IOException cleanup) {
            e.addSuppressed(cleanup);
          }
        }
        throw e;
      }
    }

    private void startMembership()
        throws Exception {
      for (RatisMetadataStore store : _stores) {
        // Test-only clock assumptions do not qualify a production provider.
        store.startMembershipClock(new MembershipClockSettings(
            () -> new ClockReading(System.nanoTime(), 0, true), 1_000_000, 0));
      }
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
      while (System.nanoTime() < deadline) {
        MembershipProtocol.Response control = _access.query(new MetadataProtocol.MembershipQuery(_history,
            new MembershipProtocol.GetControl()), 5000);
        if (control.control() != null && control.control().controlEpoch() != null) {
          MembershipProtocol.Policy policy = new MembershipProtocol.Policy(1, 1000, 100, 1000, 1000, 1000);
          MembershipProtocol.Response configured = _access.execute(new MetadataProtocol.MembershipCommand(_history,
              "test-controller", new MembershipProtocol.ConfigurePolicy(control.control().controlEpoch(), 0, policy)),
              5000);
          if (configured.status() == MetadataProtocol.Status.OK) {
            return;
          }
        }
        Thread.sleep(20);
      }
      throw new IOException("Membership control epoch and policy were not established");
    }

    @Override
    public void close()
        throws IOException {
      try {
        _access.close();
      } finally {
        IOException failure = null;
        for (RatisMetadataStore store : _stores) {
          try {
            store.close();
          } catch (IOException e) {
            if (failure == null) {
              failure = e;
            } else {
              failure.addSuppressed(e);
            }
          }
        }
        if (failure != null) {
          throw failure;
        }
      }
    }
  }

  /// Blocks the four catalog handler threads without blocking membership access to the same real quorum.
  private static final class BlockingAccess implements MetadataAccess, InstallationAccess, TaskAccess {
    private final MetadataAccess _delegate;
    private final AtomicReference<Runnable> _afterExecute = new AtomicReference<>(() -> { });
    private final AtomicReference<Runnable> _beforeWork = new AtomicReference<>(() -> { });
    private final CountDownLatch _entered = new CountDownLatch(4);
    private final CountDownLatch _release = new CountDownLatch(1);
    private volatile boolean _block;

    private BlockingAccess(MetadataAccess delegate) {
      _delegate = delegate;
    }

    @Override
    public Result execute(Command command, long timeoutMillis)
        throws IOException {
      Result result = _delegate.execute(command, timeoutMillis);
      _afterExecute.get().run();
      return result;
    }

    @Override
    public OfflineInstallationProtocol.WorkResult query(MetadataProtocol.GetInstallationWork query, long timeoutMillis)
        throws IOException {
      _beforeWork.get().run();
      return ((InstallationAccess) _delegate).query(query, timeoutMillis);
    }

    @Override
    public TaskProtocol.WorkSnapshot taskSnapshot(MembershipProtocol.ParticipantToken minion, int maxRecords,
        int maxBytes, long timeoutMillis) throws IOException {
      _beforeWork.get().run();
      return ((TaskAccess) _delegate).taskSnapshot(minion, maxRecords, maxBytes, timeoutMillis);
    }

    @Override
    public OfflineInstallationProtocol.InstallationSnapshot installationSnapshot(
        MembershipProtocol.ParticipantToken server, int maxRecords, int maxBytes, long timeoutMillis)
        throws IOException {
      return ((InstallationAccess) _delegate).installationSnapshot(server, maxRecords, maxBytes, timeoutMillis);
    }

    @Override
    public Result query(Query query, long timeoutMillis)
        throws IOException {
      if (_block) {
        _entered.countDown();
        try {
          if (!_release.await(10, TimeUnit.SECONDS)) {
            throw new IOException("Catalog test gate was not released");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted at catalog test gate", e);
        }
      }
      return _delegate.query(query, timeoutMillis);
    }

    @Override
    public CatalogImage image(FeedScope scope, int maxRecords, int maxBytes, long timeoutMillis)
        throws IOException {
      return _delegate.image(scope, maxRecords, maxBytes, timeoutMillis);
    }

    @Override
    public ChangePage changes(FeedScope scope, FeedCursor cursor, int maxTransactions, int maxRecords, int maxBytes,
        long timeoutMillis)
        throws IOException {
      return _delegate.changes(scope, cursor, maxTransactions, maxRecords, maxBytes, timeoutMillis);
    }
  }

  /// Direct committed catalog fixture for transport checks. The production implementation uses a Raft read barrier.
  private static final class Backend implements MetadataAccess {
    private final MetadataCatalog _catalog;
    private final UUID _history;
    private long _index;
    private final AtomicReference<Runnable> _beforeChanges = new AtomicReference<>(() -> { });
    private final AtomicReference<Runnable> _beforeQuery = new AtomicReference<>(() -> { });
    private final AtomicLong _lastTimeout = new AtomicLong();

    private Backend(UUID history) {
      _history = history;
      _catalog = new MetadataCatalog(UUID.randomUUID(), history);
    }

    @Override
    public synchronized Result execute(Command command, long timeoutMillis)
        throws IOException {
      return MetadataProtocol.decodeResult(_catalog.execute(MetadataProtocol.encode(command), ++_index));
    }

    @Override
    public synchronized Result query(Query query, long timeoutMillis)
        throws IOException {
      _lastTimeout.set(timeoutMillis);
      _beforeQuery.get().run();
      return MetadataProtocol.decodeResult(_catalog.query(MetadataProtocol.encode(query)));
    }

    @Override
    public CatalogImage image(FeedScope scope, int maxRecords, int maxBytes, long timeoutMillis)
        throws IOException {
      return _catalog.captureImage(scope, maxRecords, maxBytes);
    }

    @Override
    public ChangePage changes(FeedScope scope, FeedCursor cursor, int maxTransactions, int maxRecords, int maxBytes,
        long timeoutMillis)
        throws IOException {
      _beforeChanges.get().run();
      return _catalog.changes(scope, cursor, maxTransactions, maxRecords, maxBytes);
    }

    private void advancePastJournal() {
      try {
        Command progress = new MetadataProtocol.RegisterClient(_history, "progress", UUID.randomUUID(), 0);
        for (int i = 0; i < 1025; i++) {
          execute(progress, 5000);
        }
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
