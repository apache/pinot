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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Authority;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Mode;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Operation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Owner;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ReadGuard;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Request;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Result;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Status;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata.SegmentSnapshot;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata.TableSnapshot;
import org.apache.pinot.common.metadata.segment.SegmentLoadMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.query.runtime.KeepPipelineBreakerStatsPredicate;
import org.apache.pinot.query.runtime.SendStatsPredicate;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.server.conf.ServerConf;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises real ordinary OFFLINE table ownership and shutdown without creating any Helix or ZooKeeper client.
public class NativeInstanceDataManagerTest {
  private static final String TABLE = "native_OFFLINE";
  private static final String SERVER = "server";
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName("native").build();
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("native")
      .addSingleValueDimension("id", DataType.INT).build();

  @Test
  public void testConditionalActionLifecycleAndLegacyRejection() throws Exception {
    Path root = Files.createTempDirectory("native-instance-");
    NativeInstanceDataManager manager = new NativeInstanceDataManager();
    try {
      manager.initWithMetadata(config(root), name -> metadata(TABLE_CONFIG), ServerMetrics.get(), null,
          mock(ServerReloadJobStatusCache.class));
      assertThrows(IllegalStateException.class, () -> manager.executor(TABLE));
      manager.start();
      NativeInstanceDataManager conflicting = new NativeInstanceDataManager();
      try {
        assertThrows(IOException.class, () -> conflicting.initWithMetadata(config(root),
            name -> metadata(TABLE_CONFIG), ServerMetrics.get(), null, mock(ServerReloadJobStatusCache.class)));
      } finally {
        conflicting.shutDown();
      }
      NativeOfflineSegmentAction.Executor executor = manager.executor(TABLE);
      Request request = request();
      assertEquals(executor.execute(request, authority(true)).status(), Status.ABSENT);
      assertEquals(manager.execute(request, authority(false)).status(), Status.FENCED);
      TableDataManager table = manager.getTableDataManager(TABLE);
      assertNotNull(table);
      assertSame(table, manager.getTableDataManager(TABLE));
      assertEquals(manager.getAllTables(), Set.of(TABLE));
      assertTrue(manager.getAllSegmentsMetadata(TABLE).isEmpty());
      assertNull(manager.getSegmentMetadata(TABLE, "segment"));
      assertThrows(UnsupportedOperationException.class, () -> manager.getAllTables().clear());
      assertThrows(UnsupportedOperationException.class, () -> manager.addOnlineSegment(TABLE, "segment"));
      assertThrows(UnsupportedOperationException.class, () -> manager.reloadSegment(TABLE, "segment", true, "job"));
      assertThrows(UnsupportedOperationException.class, manager::getPropertyStore);
      assertThrows(UnsupportedOperationException.class,
          () -> manager.setSupplierOfIsServerReadyToServeQueries(() -> true));
      assertThrows(IllegalStateException.class, () -> table.addOnlineSegment("segment"));
      assertThrows(IllegalStateException.class, () -> table.reloadSegment("segment", true, "job"));
      assertThrows(IllegalArgumentException.class, () -> manager.executor("native_REALTIME"));
      Request load = realLoad(root);
      Result installed = manager.execute(load, authority(true));
      assertEquals(installed.status(), Status.INSTALLED);
      assertEquals(manager.getSegmentMetadata(TABLE, "segment").getTotalDocs(), 2);
      Path installedPath = manager.getSegmentDataDirectory(TABLE, "segment").toPath();
      assertTrue(installedPath.startsWith(root.resolve("data/" + TABLE + "/.native/materializations")));
      assertEquals(manager.execute(load, authority(true)).installation().installationId(),
          installed.installation().installationId());
      manager.shutDown();
      assertTrue(table.isShutDown());
      assertTrue(manager._segmentReloadRefreshExecutor.isShutdown());
      assertThrows(IllegalStateException.class, () -> manager.executor(TABLE));
      assertEquals(executor.execute(load, authority(true)).status(), Status.FENCED);
      assertTrue(Files.isRegularFile(root.resolve("data/.native-instance.lock")));
      NativeInstanceDataManager reopened = new NativeInstanceDataManager();
      try {
        reopened.initWithMetadata(config(root), name -> metadata(TABLE_CONFIG), ServerMetrics.get(), null,
            mock(ServerReloadJobStatusCache.class));
        reopened.start();
        Result restored = reopened.execute(load, authority(true));
        assertEquals(restored.status(), Status.INSTALLED);
        assertEquals(restored.installation().installationId(), installed.installation().installationId());
        assertEquals(reopened.getSegmentMetadata(TABLE, "segment").getTotalDocs(), 2);
      } finally {
        reopened.shutDown();
      }
    } finally {
      manager.shutDown();
      FileUtils.deleteDirectory(root.toFile());
    }
  }

  @Test
  public void testShutdownFencesPreparedActionAndUnsupportedTable() throws Exception {
    Path root = Files.createTempDirectory("native-shutdown-");
    NativeInstanceDataManager manager = new NativeInstanceDataManager();
    CountDownLatch refreshed = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    Thread actionThread = null;
    Thread shutdownThread = null;
    try {
      TableConfig upsert = new TableConfigBuilder(TableType.OFFLINE).setTableName("upsert")
          .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).build();
      manager.initWithMetadata(config(root), name -> metadata(name.equals(TABLE) ? TABLE_CONFIG : upsert),
          ServerMetrics.get(), null, mock(ServerReloadJobStatusCache.class));
      manager.start();
      assertThrows(IllegalArgumentException.class, () -> manager.executor("upsert_OFFLINE"));
      assertNull(manager.getTableDataManager("upsert_OFFLINE"));
      assertFalse(Files.exists(root.resolve("data/upsert_OFFLINE")));
      Authority authority = new Authority() {
        @Override
        public void refresh() throws Exception {
          refreshed.countDown();
          assertTrue(resume.await(10, TimeUnit.SECONDS));
        }

        @Override
        public ReadGuard acquire(Identity expected) throws Exception {
          return NativeInstanceDataManagerTest.authority(true).acquire(expected);
        }
      };
      FutureTask<Result> action = new FutureTask<>(() -> manager.execute(request(), authority));
      actionThread = new Thread(action, "native-action-shutdown");
      actionThread.start();
      assertTrue(refreshed.await(10, TimeUnit.SECONDS));
      TableDataManager table = manager.getTableDataManager(TABLE);
      FutureTask<Void> shutdown = new FutureTask<>(() -> {
        manager.shutDown();
        return null;
      });
      shutdownThread = new Thread(shutdown, "native-instance-shutdown");
      shutdownThread.start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (!table.isShutDown() && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertTrue(table.isShutDown());
      assertFalse(shutdown.isDone(), "Directory ownership must outlive the entire prepared action");
      NativeInstanceDataManager conflicting = new NativeInstanceDataManager();
      try {
        assertThrows(IOException.class, () -> conflicting.initWithMetadata(config(root),
            name -> metadata(TABLE_CONFIG), ServerMetrics.get(), null, mock(ServerReloadJobStatusCache.class)));
      } finally {
        conflicting.shutDown();
      }
      resume.countDown();
      assertEquals(action.get(10, TimeUnit.SECONDS).status(), Status.FENCED);
      shutdown.get(10, TimeUnit.SECONDS);
      assertFalse(table.hasSegment("segment"));
    } finally {
      resume.countDown();
      if (actionThread != null) {
        actionThread.join(TimeUnit.SECONDS.toMillis(10));
      }
      if (shutdownThread != null) {
        shutdownThread.join(TimeUnit.SECONDS.toMillis(10));
      }
      manager.shutDown();
      FileUtils.deleteDirectory(root.toFile());
    }
  }

  @Test
  public void testServerInstanceUsesInjectedManagerAndBlocksQueryStartup() throws Exception {
    Path root = Files.createTempDirectory("native-server-");
    NativeInstanceDataManager manager = new NativeInstanceDataManager();
    ServerInstance server = null;
    try {
      ServerReloadJobStatusCache jobs = mock(ServerReloadJobStatusCache.class);
      manager.initWithMetadata(config(root), name -> metadata(TABLE_CONFIG), ServerMetrics.get(), null, jobs);
      // Normal query defaults are enabled; native construction must not allocate any query service resources.
      PinotConfiguration serverConfig = new PinotConfiguration();
      server = new ServerInstance(new ServerConf(serverConfig), SERVER, manager, mock(AccessControl.class), null,
          mock(ThreadAccountant.class), mock(SendStatsPredicate.class), new KeepPipelineBreakerStatsPredicate(false),
          jobs);
      assertSame(server.getInstanceDataManager(), manager);
      server.startDataManager();
      server.startDataManager();
      manager.executor(TABLE);
      assertThrows(IllegalStateException.class, server::startQueryServer);
      assertThrows(IllegalStateException.class, server::getHelixManager);
      assertThrows(IllegalStateException.class, server::getQueryScheduler);
      assertNull(server.getWorkerQueryServer());
      assertThrows(IllegalStateException.class, server::getPageCacheWarmupServerQueryExecutor);
      server.shutDown();
      assertTrue(manager.getTableDataManager(TABLE).isShutDown());
      NativeInstanceDataManager unstarted = new NativeInstanceDataManager();
      NativeInstanceDataManager replacement = new NativeInstanceDataManager();
      try {
        unstarted.initWithMetadata(config(root), name -> metadata(TABLE_CONFIG), ServerMetrics.get(), null, jobs);
        ServerInstance unstartedServer = new ServerInstance(new ServerConf(serverConfig), SERVER, unstarted,
            mock(AccessControl.class), null, mock(ThreadAccountant.class), mock(SendStatsPredicate.class),
            new KeepPipelineBreakerStatsPredicate(false), jobs);
        unstartedServer.shutDown();
        assertTrue(unstarted._segmentReloadRefreshExecutor.isShutdown());
        replacement.initWithMetadata(config(root), name -> metadata(TABLE_CONFIG), ServerMetrics.get(), null, jobs);
        replacement.start();
        assertNotNull(replacement.executor(TABLE));
      } finally {
        unstarted.shutDown();
        replacement.shutDown();
      }
    } finally {
      if (server != null) {
        server.shutDown();
      }
      manager.shutDown();
      FileUtils.deleteDirectory(root.toFile());
    }
  }

  private static PinotConfiguration config(Path root) {
    return new PinotConfiguration(Map.of("id", SERVER, "dataDir", root.resolve("data").toString(),
        "segmentTarDir", root.resolve("tar").toString(), "max.parallel.refresh.threads", 1));
  }

  private static TableDataManagerMetadata metadata(TableConfig table) {
    TableDataManagerMetadata metadata = mock(TableDataManagerMetadata.class);
    when(metadata.fetchTable(table.getTableName())).thenReturn(new TableSnapshot(table, SCHEMA));
    return metadata;
  }

  private static Request realLoad(Path root) throws Exception {
    Path output = root.resolve("generated");
    SegmentGeneratorConfig generator = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    generator.setOutDir(output.toString());
    generator.setSegmentName("segment");
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    GenericRow first = new GenericRow();
    first.putValue("id", 1);
    GenericRow second = new GenericRow();
    second.putValue("id", 2);
    driver.init(generator, new GenericRowRecordReader(List.of(first, second)));
    driver.build();
    Path directory = output.resolve("segment");
    long crc = new SegmentMetadataImpl(directory.toFile()).getCrc();
    Path archive = root.resolve("segment.tar.gz");
    TarCompressionUtils.createCompressedTarFile(directory.toFile(), archive.toFile());
    String digest;
    try (InputStream input = Files.newInputStream(archive)) {
      digest = DigestUtils.sha256Hex(input);
    }
    SegmentFetcherFactory.init(new PinotConfiguration());
    Identity old = request().identity();
    Identity identity = new Identity(old.historyId(), old.tableName(), old.table(), old.schemaName(), old.schema(),
        old.segmentName(), old.content(), digest, old.server(), old.assignment(), old.materializationId(),
        old.operation());
    SegmentLoadMetadata segment = new SegmentLoadMetadata("segment", crc, 0, false, archive.toUri().toString(),
        null, null, 0, 0, Map.of(), true);
    return new Request(Mode.LOAD, identity, new SegmentSnapshot(TABLE_CONFIG, SCHEMA, segment), true, null);
  }

  private static Request request() {
    Participant server = new Participant(SERVER, Role.SERVER, UUID.randomUUID(), 1);
    Owner owner = new Owner(1, 0, 1, new Participant("controller", Role.CONTROLLER, UUID.randomUUID(), 1));
    Identity identity = new Identity(UUID.randomUUID(), TABLE, new ObjectVersion(UUID.randomUUID(), 1), "native",
        new ObjectVersion(UUID.randomUUID(), 1), "segment", new ObjectVersion(UUID.randomUUID(), 1), "0".repeat(64),
        server, new ObjectVersion(UUID.randomUUID(), 1), UUID.randomUUID(), new Operation(UUID.randomUUID(), 1, owner));
    SegmentLoadMetadata segment = new SegmentLoadMetadata("segment", 0, 0, false, "file:///absent", null, null,
        0, 0, Map.of(), true);
    return new Request(Mode.RELOAD, identity, new SegmentSnapshot(TABLE_CONFIG, SCHEMA, segment), false, null);
  }

  private static Authority authority(boolean current) {
    return new Authority() {
      @Override
      public void refresh() {
      }

      @Override
      public ReadGuard acquire(Identity expected) {
        return new ReadGuard() {
          @Override
          public boolean isCurrent() {
            return current;
          }

          @Override
          public void close() {
          }
        };
      }
    };
  }
}
