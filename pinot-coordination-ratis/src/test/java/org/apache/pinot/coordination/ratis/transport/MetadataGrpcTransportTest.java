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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.coordination.ratis.RatisMetadataAccess;
import org.apache.pinot.coordination.ratis.RatisMetadataStore;
import org.apache.pinot.coordination.ratis.RatisMetadataStore.MembershipClockSettings;
import org.apache.pinot.coordination.ratis.client.ServingPermitGuard.ClockReading;
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
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataServiceGrpc;
import org.apache.pinot.coordination.ratis.transport.ControllerAccessPolicy.ControllerIdentity;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
  private static final class BlockingAccess implements MetadataAccess {
    private final MetadataAccess _delegate;
    private final CountDownLatch _entered = new CountDownLatch(4);
    private final CountDownLatch _release = new CountDownLatch(1);
    private volatile boolean _block;

    private BlockingAccess(MetadataAccess delegate) {
      _delegate = delegate;
    }

    @Override
    public Result execute(Command command, long timeoutMillis)
        throws IOException {
      return _delegate.execute(command, timeoutMillis);
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
