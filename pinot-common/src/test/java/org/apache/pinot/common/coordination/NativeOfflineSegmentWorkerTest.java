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
package org.apache.pinot.common.coordination;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Installation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Mode;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ReadGuard;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Result;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Status;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Checks the neutral worker's wire boundary and authority refresh without a consensus or segment-server dependency.
public class NativeOfflineSegmentWorkerTest {
  @Test
  public void testReconcileAndLostReceiptReplyPreserveExactInstallation()
      throws Exception {
    Fixture fixture = new Fixture();
    MetadataRpc.InstallationWork work = fixture._work.toBuilder().setReconcile(true).setForceDownload(true).build();
    AtomicReference<Installation> installed = new AtomicReference<>();
    List<MetadataRpc.InstallationReport> reports = new ArrayList<>();
    NativeOfflineSegmentWorker worker = new NativeOfflineSegmentWorker(fixture._server,
        operation -> fixture.response(work), report -> {
          reports.add(report);
          if (reports.size() == 1) {
            throw new IOException("Lost committed reply");
          }
          return fixture.result();
        });
    NativeOfflineSegmentAction.Executor installer = mock(NativeOfflineSegmentAction.Executor.class);
    when(installer.installed(anyString())).thenAnswer(invocation -> installed.get());
    when(installer.execute(any(), any())).thenAnswer(invocation -> {
      NativeOfflineSegmentAction.Request request = invocation.getArgument(0);
      NativeOfflineSegmentAction.Authority authority = invocation.getArgument(1);
      assertEquals(request.mode(), Mode.RECONCILE);
      assertTrue(request.forceDownload());
      assertEquals(request.snapshot().tableConfig().getTableName(), "db.orders_OFFLINE");
      assertEquals(request.snapshot().schema().getSchemaName(), "db.orders");
      assertEquals(request.snapshot().segment().downloadUri(), "file:/segments/orders.tar.gz");
      assertEquals(request.expectedInstallationId(), installed.get() == null ? null : installed.get().installationId());
      authority.refresh();
      try (ReadGuard guard = authority.acquire(request.identity())) {
        assertTrue(guard.isCurrent());
        if (installed.get() == null) {
          installed.set(new Installation(request.identity(), UUID.randomUUID(), "b".repeat(64)));
        }
        return new Result(Status.INSTALLED, installed.get());
      }
    });
    assertThrows(IOException.class, () -> worker.execute(fixture._operationId, table -> {
      assertEquals(table, "db.orders_OFFLINE");
      return installer;
    }));
    NativeOfflineSegmentWorker.Execution execution = worker.execute(fixture._operationId, installer);
    assertEquals(execution.local().status(), Status.INSTALLED);
    assertEquals(execution.receipt(), fixture.result());
    assertEquals(reports.size(), 2);
    assertEquals(reports.get(0), reports.get(1));
    assertEquals(reports.get(0).getInstalled(), work.getTarget().getTarget());
    assertEquals(reports.get(0).getOwner(), work.getOwner());
    assertEquals(reports.get(0).getInstallationId(), installed.get().installationId().toString());
  }

  @Test
  public void testChangedWorkAndFailedRefreshFencePublication()
      throws Exception {
    Fixture fixture = new Fixture();
    List<MetadataRpc.InstallationWork> changes = List.of(
        fixture._work.toBuilder().setAttempt(2).build(),
        fixture._work.toBuilder().setOwner(fixture._work.getOwner().toBuilder().setOwnerEpoch(2)).build(),
        fixture._work.toBuilder().setInitialLoad(true).build(),
        fixture._work.toBuilder().setContent(fixture._work.getContent().toBuilder()
            .setDownloadUri("file:/segments/replaced.tar.gz")).build());
    for (MetadataRpc.InstallationWork changed : changes) {
      AtomicInteger lookups = new AtomicInteger();
      NativeOfflineSegmentWorker.ReceiptPublisher publisher = mock(NativeOfflineSegmentWorker.ReceiptPublisher.class);
      NativeOfflineSegmentWorker worker = new NativeOfflineSegmentWorker(fixture._server,
          operation -> fixture.response(lookups.getAndIncrement() == 0 ? fixture._work : changed), publisher);
      NativeOfflineSegmentAction.Executor installer = mock(NativeOfflineSegmentAction.Executor.class);
      when(installer.execute(any(), any())).thenAnswer(invocation -> {
        NativeOfflineSegmentAction.Request request = invocation.getArgument(0);
        NativeOfflineSegmentAction.Authority authority = invocation.getArgument(1);
        authority.refresh();
        try (ReadGuard guard = authority.acquire(request.identity())) {
          assertFalse(guard.isCurrent());
        }
        return new Result(Status.FENCED, null);
      });
      assertEquals(worker.execute(fixture._operationId, installer).local().status(), Status.FENCED);
      verifyNoInteractions(publisher);
    }

    AtomicInteger lookups = new AtomicInteger();
    NativeOfflineSegmentWorker worker = new NativeOfflineSegmentWorker(fixture._server, operation -> {
      if (lookups.getAndIncrement() == 0) {
        return fixture.response(fixture._work);
      }
      throw new IOException("No quorum");
    }, report -> {
      throw new AssertionError("Failed refresh cannot publish");
    });
    NativeOfflineSegmentAction.Executor installer = mock(NativeOfflineSegmentAction.Executor.class);
    when(installer.execute(any(), any())).thenAnswer(invocation -> {
      NativeOfflineSegmentAction.Request request = invocation.getArgument(0);
      NativeOfflineSegmentAction.Authority authority = invocation.getArgument(1);
      assertThrows(IOException.class, authority::refresh);
      try (ReadGuard guard = authority.acquire(request.identity())) {
        assertFalse(guard.isCurrent());
      }
      return new Result(Status.FENCED, null);
    });
    assertNull(worker.execute(fixture._operationId, installer).receipt());
  }

  @Test
  public void testRemoteRefreshReleasesLocalLockWithOldAuthorityInvalidated()
      throws Exception {
    Fixture fixture = new Fixture();
    CountDownLatch refreshing = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    AtomicInteger lookups = new AtomicInteger();
    NativeOfflineSegmentWorker worker = new NativeOfflineSegmentWorker(fixture._server, operation -> {
      if (lookups.getAndIncrement() != 0) {
        refreshing.countDown();
        assertTrue(release.await(5, TimeUnit.SECONDS));
      }
      return fixture.response(fixture._work);
    }, report -> {
      throw new AssertionError("Absent local segment cannot publish");
    });
    NativeOfflineSegmentAction.Executor installer = mock(NativeOfflineSegmentAction.Executor.class);
    when(installer.execute(any(), any())).thenAnswer(invocation -> {
      NativeOfflineSegmentAction.Request request = invocation.getArgument(0);
      NativeOfflineSegmentAction.Authority authority = invocation.getArgument(1);
      assertEquals(request.mode(), Mode.RELOAD);
      Future<?> refresh = executor.submit(() -> {
        authority.refresh();
        return null;
      });
      assertTrue(refreshing.await(5, TimeUnit.SECONDS));
      Future<Boolean> admitted = executor.submit(() -> {
        try (ReadGuard guard = authority.acquire(request.identity())) {
          return guard.isCurrent();
        }
      });
      assertFalse(admitted.get(5, TimeUnit.SECONDS));
      release.countDown();
      refresh.get(5, TimeUnit.SECONDS);
      try (ReadGuard guard = authority.acquire(request.identity())) {
        assertTrue(guard.isCurrent());
      }
      return new Result(Status.ABSENT, null);
    });
    try {
      assertEquals(worker.execute(fixture._operationId, installer).local().status(), Status.ABSENT);
    } finally {
      release.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testMalformedOrForeignWorkNeverCreatesInstaller()
      throws Exception {
    Fixture fixture = new Fixture();
    MetadataRpc.InstallationWork work = fixture._work;
    UnknownFieldSet unknown = UnknownFieldSet.newBuilder().addField(100,
        UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    List<MetadataRpc.InstallationWorkResult> invalid = List.of(
        fixture.response(work).toBuilder().setStatusValue(100).build(),
        fixture.response(work).toBuilder().clearRevision().build(),
        fixture.response(work.toBuilder().setOwner(work.getOwner().toBuilder().setUnknownFields(unknown)).build()),
        fixture.response(work.toBuilder().setTarget(work.getTarget().toBuilder().setServer(fixture._server.toBuilder()
            .setRegistrationEpoch(2))).build()),
        fixture.response(work.toBuilder().setOwner(work.getOwner().toBuilder()
            .setHistoryId(UUID.randomUUID().toString())).build()),
        fixture.response(work.toBuilder().setSchema(work.getSchema().toBuilder().setVersion(2)).build()),
        fixture.response(work.toBuilder().setTable(work.getTable().toBuilder().setLastModifiedIndex(11)).build()),
        fixture.response(work.toBuilder().setInitialLoad(true).setReconcile(true).build()),
        fixture.response(work.toBuilder().setTable(work.getTable().toBuilder().setValue(
            MetadataRpc.CatalogValue.newBuilder().setTableConfigJson(ByteString.copyFromUtf8(
                "{\"tableName\":\"db.orders_OFFLINE\",\"tableName\":\"other_OFFLINE\"}")))).build()),
        fixture.response(work.toBuilder().setSchema(work.getSchema().toBuilder().setValue(
            MetadataRpc.CatalogValue.newBuilder().setSchemaJson(ByteString.copyFromUtf8("{} {}")))).build()));
    for (MetadataRpc.InstallationWorkResult response : invalid) {
      NativeOfflineSegmentAction.Executor installer = mock(NativeOfflineSegmentAction.Executor.class);
      NativeOfflineSegmentWorker.ReceiptPublisher publisher = mock(NativeOfflineSegmentWorker.ReceiptPublisher.class);
      NativeOfflineSegmentWorker worker = new NativeOfflineSegmentWorker(fixture._server, operation -> response,
          publisher);
      AtomicInteger factoryCalls = new AtomicInteger();
      assertThrows(IOException.class, () -> worker.execute(fixture._operationId, table -> {
        factoryCalls.incrementAndGet();
        return installer;
      }));
      assertEquals(factoryCalls.get(), 0);
      verifyNoInteractions(installer, publisher);
    }
  }

  @Test
  public void testRevokedWorkAndDifferentInstallerIdentityDoNotPublish()
      throws Exception {
    Fixture fixture = new Fixture();
    NativeOfflineSegmentWorker.ReceiptPublisher publisher = mock(NativeOfflineSegmentWorker.ReceiptPublisher.class);
    NativeOfflineSegmentAction.Executor installer = mock(NativeOfflineSegmentAction.Executor.class);
    NativeOfflineSegmentWorker revoked = new NativeOfflineSegmentWorker(fixture._server,
        operation -> fixture.response(fixture._work).toBuilder().setStatus(MetadataRpc.ResultStatus.FENCED)
            .clearWork().build(), publisher);
    assertEquals(revoked.execute(fixture._operationId, installer).local().status(), Status.FENCED);
    verifyNoInteractions(installer, publisher);
    NativeOfflineSegmentWorker worker = new NativeOfflineSegmentWorker(fixture._server,
        operation -> fixture.response(fixture._work), publisher);
    AtomicReference<Installation> other = new AtomicReference<>();
    when(installer.execute(any(), any())).thenAnswer(invocation -> {
      NativeOfflineSegmentAction.Request request = invocation.getArgument(0);
      if (other.get() == null) {
        other.set(new Installation(request.identity(), UUID.randomUUID(), "b".repeat(64)));
        return new Result(Status.ABSENT, null);
      }
      return new Result(Status.INSTALLED, other.get());
    });
    assertEquals(worker.execute(fixture._operationId, installer).local().status(), Status.ABSENT);
    NativeOfflineSegmentWorker nextAttempt = new NativeOfflineSegmentWorker(fixture._server,
        operation -> fixture.response(fixture._work.toBuilder().setAttempt(2).build()), publisher);
    assertThrows(IOException.class, () -> nextAttempt.execute(fixture._operationId, installer));
    verifyNoInteractions(publisher);
  }

  private static final class Fixture {
    private final UUID _history = UUID.randomUUID();
    private final UUID _operationId = UUID.randomUUID();
    private final MetadataRpc.ParticipantToken _server;
    private final MetadataRpc.InstallationWork _work;

    private Fixture()
        throws IOException {
      UUID tableId = UUID.randomUUID();
      UUID schemaId = UUID.randomUUID();
      UUID segmentId = UUID.randomUUID();
      _server = participant("server", MetadataRpc.ParticipantRole.SERVER);
      MetadataRpc.OwnerToken owner = MetadataRpc.OwnerToken.newBuilder().setHistoryId(_history.toString())
          .setLayoutGeneration(1).setOwnerEpoch(1).setScope(MetadataRpc.OwnershipScope.newBuilder().setTableShard(0))
          .setParticipant(participant("controller", MetadataRpc.ParticipantRole.CONTROLLER)).build();
      MetadataRpc.InstallationTarget target = MetadataRpc.InstallationTarget.newBuilder()
          .setTableName("db.orders_OFFLINE").setTableObjectId(tableId.toString()).setSegmentName("orders_0")
          .setSegmentObjectId(segmentId.toString()).setContentSha256("a".repeat(64))
          .setAssignment(version(UUID.randomUUID())).setMaterializationId(UUID.randomUUID().toString())
          .setCompatibility(MetadataRpc.MaterializationCompatibility.newBuilder()
              .setTable(version(tableId)).setSchema(version(schemaId))).build();
      TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName("db.orders").build();
      Schema schema = new Schema.SchemaBuilder().setSchemaName("db.orders")
          .addSingleValueDimension("value", DataType.INT).build();
      MetadataRpc.CatalogRecord tableRecord = record(MetadataRpc.RecordKind.TABLE_CONFIG, tableId, table.getTableName())
          .setValue(MetadataRpc.CatalogValue.newBuilder()
              .setTableConfigJson(ByteString.copyFrom(JsonUtils.objectToBytes(table)))).build();
      MetadataRpc.CatalogRecord schemaRecord = record(MetadataRpc.RecordKind.SCHEMA, schemaId, schema.getSchemaName())
          .setValue(MetadataRpc.CatalogValue.newBuilder()
              .setSchemaJson(ByteString.copyFrom(JsonUtils.objectToBytes(schema)))).build();
      _work = MetadataRpc.InstallationWork.newBuilder().setOperationId(_operationId.toString()).setAttempt(1)
          .setOwner(owner).setTarget(MetadataRpc.InstalledMaterialization.newBuilder().setTarget(target)
              .setServer(_server)).setContent(MetadataRpc.OfflineSegmentContent.newBuilder()
              .setTableName(target.getTableName()).setTableObjectId(target.getTableObjectId())
              .setSegmentName(target.getSegmentName()).setSegmentObjectId(target.getSegmentObjectId())
              .setContentSha256(target.getContentSha256()).setDownloadUri("file:/segments/orders.tar.gz")
              .setLoader(MetadataRpc.SegmentLoaderDescriptor.newBuilder().setCrc(123).setDataCrc(-1)
                  .setCreationTimeMs(-1).setPushTimeMs(-1)))
          .setTable(tableRecord).setSchema(schemaRecord).build();
    }

    private MetadataRpc.ParticipantToken participant(String id, MetadataRpc.ParticipantRole role) {
      return MetadataRpc.ParticipantToken.newBuilder().setHistoryId(_history.toString()).setParticipantId(id)
          .setRole(role).setIncarnation(UUID.randomUUID().toString()).setRegistrationEpoch(1).build();
    }

    private MetadataRpc.ObjectVersion version(UUID id) {
      return MetadataRpc.ObjectVersion.newBuilder().setHistoryId(_history.toString()).setObjectId(id.toString())
          .setVersion(1).build();
    }

    private MetadataRpc.CatalogRecord.Builder record(MetadataRpc.RecordKind kind, UUID id, String name) {
      return MetadataRpc.CatalogRecord.newBuilder().setKind(kind).setName(name).setObjectId(id.toString())
          .setVersion(1).setSchemaVersion(1).setLastModifiedIndex(1);
    }

    private MetadataRpc.InstallationWorkResult response(MetadataRpc.InstallationWork work) {
      return MetadataRpc.InstallationWorkResult.newBuilder().setStatus(MetadataRpc.ResultStatus.OK)
          .setRevision(MetadataRpc.Revision.newBuilder().setHistoryId(_history.toString()).setIndex(10))
          .setWork(work).build();
    }

    private MetadataRpc.CommandResult result() {
      return MetadataRpc.CommandResult.newBuilder().setStatus(MetadataRpc.ResultStatus.OK)
          .setRevision(MetadataRpc.Revision.newBuilder().setHistoryId(_history.toString()).setIndex(11)).build();
    }
  }
}
