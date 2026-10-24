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
package org.apache.pinot.core.data.manager;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Installation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Mode;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Operation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Owner;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Request;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Result;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Status;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata.SegmentSnapshot;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata.TableSnapshot;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentLoadMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.common.request.context.NativeQueryContext.ProcessIdentity;
import org.apache.pinot.common.request.context.NativeQueryContext.Segment;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.fetcher.BaseSegmentFetcher;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.common.utils.helix.FakePropertyStore;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.offline.NativeImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottler;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottlerSet;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.crypt.PinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class BaseTableDataManagerTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "BaseTableDataManagerTest-" + UUID.randomUUID());
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final File TABLE_DATA_DIR = new File(TEMP_DIR, OFFLINE_TABLE_NAME);
  private static final String SEGMENT_NAME = "testSegment";
  private static final String TIER_SEGMENT_DIRECTORY_LOADER = "tierBased";
  private static final String TIER_NAME = "coolTier";
  private static final String STRING_COLUMN = "col1";
  private static final String[] STRING_VALUES = {"A", "D", "E", "B", "C"};
  private static final String LONG_COLUMN = "col2";
  private static final long[] LONG_VALUES = {10000L, 20000L, 50000L, 40000L, 30000L};

  private static final TableConfig DEFAULT_TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final TableConfig TIER_TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTierConfigList(List.of(
          new TierConfig(TIER_NAME, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "3d", null,
              TierFactory.PINOT_SERVER_STORAGE_TYPE, "tag_OFFLINE", null,
              Map.of("dataDir", new File(TEMP_DIR, TIER_NAME).getAbsolutePath())))).build();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addSingleValueDimension(STRING_COLUMN, DataType.STRING)
          .addMetric(LONG_COLUMN, DataType.LONG).build();
  static final SegmentOperationsThrottlerSet SEGMENT_OPERATIONS_THROTTLER = new SegmentOperationsThrottlerSet(
      new SegmentOperationsThrottler(2, 4, true),
      new SegmentOperationsThrottler(2, 4, true),
      new SegmentOperationsThrottler(2, 4, true),
      new SegmentOperationsThrottler(2, 4, true));

  private ServerMetrics _previousServerMetrics;

  /// Registers the mock the tests verify against. [ServerMetrics#register] only swaps in against the NOOP default,
  /// so the instance an earlier test class in the same JVM registered is cleared first, and restored in [#tearDown]
  /// for the test classes that run afterwards.
  @BeforeClass
  public void setUp()
      throws Exception {
    _previousServerMetrics = ServerMetrics.get();
    ServerMetrics.deregister();
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    ServerMetrics.deregister();
    ServerMetrics.register(_previousServerMetrics);
  }

  @BeforeMethod
  public void setUpMethod()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    initSegmentFetcher();
  }

  @AfterMethod
  public void tearDownMethod()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  public static void initSegmentFetcher()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(BaseSegmentFetcher.RETRY_COUNT_CONFIG_KEY, 3);
    properties.put(BaseSegmentFetcher.RETRY_WAIT_MS_CONFIG_KEY, 100);
    properties.put(BaseSegmentFetcher.RETRY_DELAY_SCALE_FACTOR_CONFIG_KEY, 5);
    SegmentFetcherFactory.init(new PinotConfiguration(properties));

    // Setup crypter
    properties.put("class.fakePinotCrypter", BaseTableDataManagerTest.FakePinotCrypter.class.getName());
    PinotCrypterFactory.init(new PinotConfiguration(properties));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testOnlineSegmentLoadedDuringShutdownIsDestroyed()
      throws Exception {
    BaseTableDataManager table = spy(createTableManager());
    ServerMetrics metrics = table._serverMetrics;
    clearInvocations(metrics);
    ImmutableSegment segment = mock(ImmutableSegment.class);
    when(segment.getSegmentName()).thenReturn(SEGMENT_NAME);
    CountDownLatch loading = new CountDownLatch(1);
    CountDownLatch finishLoading = new CountDownLatch(1);
    doAnswer(invocation -> {
      loading.countDown();
      assertTrue(finishLoading.await(10, TimeUnit.SECONDS));
      table.addSegment(segment, null);
      return null;
    }).when(table).doAddOnlineSegment(SEGMENT_NAME);
    FutureTask<Void> addition = new FutureTask<>(() -> {
      table.addOnlineSegment(SEGMENT_NAME);
      return null;
    });
    Thread additionThread = new Thread(addition, "load-online-segment");
    try {
      additionThread.start();
      assertTrue(loading.await(10, TimeUnit.SECONDS));
      table.shutDown();
      finishLoading.countDown();
      ExecutionException failure = expectThrows(ExecutionException.class, () -> addition.get(10, TimeUnit.SECONDS));
      assertTrue(failure.getCause() instanceof IllegalStateException);
      assertEquals(table.getNumSegments(), 0);
      verify(segment).destroy();
      verify(metrics, never()).addValueToTableGauge(eq(OFFLINE_TABLE_NAME), eq(ServerGauge.SEGMENT_COUNT), anyLong());
      verify(metrics, never()).addValueToTableGauge(eq(OFFLINE_TABLE_NAME), eq(ServerGauge.DOCUMENT_COUNT), anyLong());
    } finally {
      finishLoading.countDown();
      additionThread.join(10000);
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testShutdownWaitsForImmutableSegmentRegistration()
      throws Exception {
    BaseTableDataManager table = spy(createTableManager());
    ServerMetrics metrics = table._serverMetrics;
    clearInvocations(metrics);
    ImmutableSegment segment = mock(ImmutableSegment.class);
    when(segment.getSegmentName()).thenReturn(SEGMENT_NAME);
    SegmentMetadata metadata = mock(SegmentMetadata.class);
    when(metadata.getTotalDocs()).thenReturn(5);
    when(segment.getSegmentMetadata()).thenReturn(metadata);
    CountDownLatch registering = new CountDownLatch(1);
    CountDownLatch finishRegistration = new CountDownLatch(1);
    doAnswer(invocation -> {
      registering.countDown();
      assertTrue(finishRegistration.await(10, TimeUnit.SECONDS));
      return invocation.callRealMethod();
    }).when(table).registerSegment(eq(SEGMENT_NAME), any());
    FutureTask<Void> addition = new FutureTask<>(() -> {
      table.addSegment(segment, null);
      return null;
    });
    FutureTask<Void> shutdown = new FutureTask<>(() -> {
      table.shutDown();
      return null;
    });
    Thread additionThread = new Thread(addition, "register-immutable-segment");
    Thread shutdownThread = new Thread(shutdown, "shutdown-immutable-table");
    try {
      additionThread.start();
      assertTrue(registering.await(10, TimeUnit.SECONDS));
      shutdownThread.start();
      TestUtils.waitForCondition(ignored -> shutdown.isDone() || (table.isShutDown()
          && (shutdownThread.getState() == Thread.State.WAITING
          || shutdownThread.getState() == Thread.State.TIMED_WAITING)), 10, 10000,
          "Shutdown did not wait for the admitted segment");
      assertFalse(shutdown.isDone(), "Shutdown must wait for the admitted segment to finish registration");
      verify(segment, never()).destroy();
      finishRegistration.countDown();
      addition.get(10, TimeUnit.SECONDS);
      shutdown.get(10, TimeUnit.SECONDS);
      assertEquals(table.getNumSegments(), 0);
      verify(segment).destroy();
      verify(metrics).addValueToTableGauge(OFFLINE_TABLE_NAME, ServerGauge.SEGMENT_COUNT, 1L);
      verify(metrics).addValueToTableGauge(OFFLINE_TABLE_NAME, ServerGauge.SEGMENT_COUNT, -1L);
      verify(metrics).addValueToTableGauge(OFFLINE_TABLE_NAME, ServerGauge.DOCUMENT_COUNT, 5L);
      verify(metrics).addValueToTableGauge(OFFLINE_TABLE_NAME, ServerGauge.DOCUMENT_COUNT, -5L);
    } finally {
      finishRegistration.countDown();
      additionThread.join(10000);
      shutdownThread.join(10000);
    }
  }

  @Test
  public void testReloadSegmentNewData()
      throws Exception {
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn(0L);

    BaseTableDataManager tableDataManager = spy(createTableManager());
    tableDataManager.registerSegment(SEGMENT_NAME, createImmutableSegmentDataManager(SEGMENT_NAME, localMetadata));
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> new IndexLoadingConfig()).when(tableDataManager).fetchIndexLoadingConfig();

    File dataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    assertFalse(dataDir.exists());
    tableDataManager.reloadSegment(SEGMENT_NAME, false, null);
    assertTrue(dataDir.exists());
    assertEquals(new SegmentMetadataImpl(dataDir).getTotalDocs(), 5);
  }

  @Test
  public void testReloadSegmentNewDataNewTier()
      throws Exception {
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);
    zkMetadata.setTier(TIER_NAME);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn(0L);

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tableDataManager = spy(createTableManager());
    tableDataManager.registerSegment(SEGMENT_NAME, createImmutableSegmentDataManager(SEGMENT_NAME, localMetadata));
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> createTierIndexLoadingConfig(DEFAULT_TABLE_CONFIG))
        .when(tableDataManager).fetchIndexLoadingConfig();
    File defaultDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    assertFalse(defaultDataDir.exists());
    tableDataManager.reloadSegment(SEGMENT_NAME, false, null);
    assertTrue(defaultDataDir.exists());
    assertEquals(new SegmentMetadataImpl(defaultDataDir).getTotalDocs(), 5);

    // Configured dataDir for coolTier, thus move to new dir.
    tableDataManager = spy(createTableManager());
    tableDataManager.registerSegment(SEGMENT_NAME, createImmutableSegmentDataManager(SEGMENT_NAME, localMetadata));
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> createTierIndexLoadingConfig(TIER_TABLE_CONFIG))
        .when(tableDataManager).fetchIndexLoadingConfig();
    tableDataManager.reloadSegment(SEGMENT_NAME, false, null);
    File tierDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME, TIER_NAME, TIER_TABLE_CONFIG);
    assertTrue(tierDataDir.exists());
    assertFalse(defaultDataDir.exists());
    assertEquals(new SegmentMetadataImpl(tierDataDir).getTotalDocs(), 5);
  }

  @Test
  public void testReloadSegmentUseLocalCopy()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v1, 5);
    long crc = getCRC(indexDir);

    // Same CRCs so load the local segment directory directly.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getCrc()).thenReturn(crc);
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn(crc);

    BaseTableDataManager tableDataManager = spy(createTableManager());
    tableDataManager.registerSegment(SEGMENT_NAME, createImmutableSegmentDataManager(SEGMENT_NAME, localMetadata));
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> new IndexLoadingConfig()).when(tableDataManager).fetchIndexLoadingConfig();
    tableDataManager.reloadSegment(SEGMENT_NAME, false, null);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);

    FileUtils.deleteQuietly(indexDir);
    try {
      tableDataManager.reloadSegment(SEGMENT_NAME, false, null);
      fail();
    } catch (Exception e) {
      // As expected, segment reloading fails due to missing the local segment dir.
    }
  }

  @Test
  public void testReloadSegmentUseLocalCopyNewTier()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v1, 5);
    long crc = getCRC(indexDir);

    // Same CRCs so load the local segment directory directly.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getCrc()).thenReturn(crc);
    when(zkMetadata.getTier()).thenReturn(TIER_NAME);
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn(crc);

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tableDataManager = spy(createTableManager());
    tableDataManager.registerSegment(SEGMENT_NAME, createImmutableSegmentDataManager(SEGMENT_NAME, localMetadata));
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> createTierIndexLoadingConfig(DEFAULT_TABLE_CONFIG))
        .when(tableDataManager).fetchIndexLoadingConfig();
    tableDataManager.reloadSegment(SEGMENT_NAME, false, null);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);

    // Configured dataDir for coolTier, thus move to new dir.
    tableDataManager = spy(createTableManager());
    tableDataManager.registerSegment(SEGMENT_NAME, createImmutableSegmentDataManager(SEGMENT_NAME, localMetadata));
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> createTierIndexLoadingConfig(TIER_TABLE_CONFIG))
        .when(tableDataManager).fetchIndexLoadingConfig();
    tableDataManager.reloadSegment(SEGMENT_NAME, false, null);
    File tierDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME, TIER_NAME, TIER_TABLE_CONFIG);
    assertTrue(tierDataDir.exists());
    assertFalse(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(tierDataDir).getTotalDocs(), 5);
  }

  /// Regression test for https://github.com/apache/pinot/issues/18164: the table-level config passed into reload is
  /// shared by all segments, so reloading one segment must not set its tier on the shared config.
  @Test
  public void testReloadSegmentFromDefaultTierDoesNotMutateSharedIndexLoadingConfig()
      throws Exception {
    // The current tier is null while the target tier is coolTier, exercising both tier mutations in the reload path.
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);
    zkMetadata.setTier(TIER_NAME);
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn(0L);

    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(SEGMENT_NAME, localMetadata);
    BaseTableDataManager tableDataManager = spy(createTableManager());
    tableDataManager.registerSegment(SEGMENT_NAME, segmentDataManager);
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);

    IndexLoadingConfig sharedConfig = new IndexLoadingConfig(DEFAULT_TABLE_CONFIG, SCHEMA);
    tableDataManager.reloadSegment(segmentDataManager, sharedConfig, true);
    assertNull(sharedConfig.getSegmentTier());
    assertNull(sharedConfig.getTableDataDir());
  }

  @Test
  public void testReloadSegmentConvertVersion()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v1, 5);
    long crc = getCRC(indexDir);

    // Same CRCs so load the local segment directory directly.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getCrc()).thenReturn(crc);
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn(crc);

    // Require to use v3 format.
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);

    BaseTableDataManager tableDataManager = spy(createTableManager());
    tableDataManager.registerSegment(SEGMENT_NAME, createImmutableSegmentDataManager(SEGMENT_NAME, localMetadata));
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> indexLoadingConfig).when(tableDataManager).fetchIndexLoadingConfig();
    tableDataManager.reloadSegment(SEGMENT_NAME, false, null);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);
  }

  @Test
  public void testReloadSegmentAddIndex()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);
    assertFalse(hasInvertedIndex(indexDir, STRING_COLUMN));
    assertFalse(hasInvertedIndex(indexDir, LONG_COLUMN));

    // Same CRCs so load the local segment directory directly.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getCrc()).thenReturn(crc);
    SegmentMetadata localMetadata = mock(SegmentMetadata.class);
    when(localMetadata.getCrc()).thenReturn(crc);

    // Require to add indices.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of(STRING_COLUMN, LONG_COLUMN)).build();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);

    BaseTableDataManager tableDataManager = spy(createTableManager());
    tableDataManager.registerSegment(SEGMENT_NAME, createImmutableSegmentDataManager(SEGMENT_NAME, localMetadata));
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> indexLoadingConfig).when(tableDataManager).fetchIndexLoadingConfig();
    tableDataManager.reloadSegment(SEGMENT_NAME, false, null);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);
    assertTrue(hasInvertedIndex(indexDir, STRING_COLUMN));
    assertTrue(hasInvertedIndex(indexDir, LONG_COLUMN));
  }

  @Test
  public void testReloadSegmentForceDownload()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    SegmentZKMetadata zkMetadata =
        makeRawSegment(indexDir, new File(TEMP_DIR, SEGMENT_NAME + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION),
            false);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(segmentMetadata.getCrc(), zkMetadata.getCrc());

    // Same CRC but force to download.
    BaseTableDataManager tableDataManager = spy(createTableManager());
    tableDataManager.registerSegment(SEGMENT_NAME, createImmutableSegmentDataManager(SEGMENT_NAME, segmentMetadata));
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> new IndexLoadingConfig()).when(tableDataManager).fetchIndexLoadingConfig();

    // Remove the local segment dir. Segment reloading fails unless force to download.
    FileUtils.deleteQuietly(indexDir);
    try {
      tableDataManager.reloadSegment(SEGMENT_NAME, false, null);
      fail();
    } catch (Exception e) {
      // As expected, segment reloading fails due to missing the local segment dir.
    }

    tableDataManager.reloadSegment(SEGMENT_NAME, true, null);
    assertTrue(indexDir.exists());
    segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(segmentMetadata.getCrc(), zkMetadata.getCrc());
    assertEquals(segmentMetadata.getTotalDocs(), 5);
  }

  @Test
  public void testReloadSegmentAcquiresSegmentLockBeforeSemaphore()
      throws Exception {
    TestSegmentReloadSemaphore reloadSemaphore = new TestSegmentReloadSemaphore(1);
    BaseTableDataManager tableDataManager = createTableManager(reloadSemaphore);

    Lock segmentLock = tableDataManager.getSegmentLock(SEGMENT_NAME);
    segmentLock.lock();
    ExecutorService executor = Executors.newCachedThreadPool();
    Thread reloadThread = new Thread(() -> {
      try {
        tableDataManager.reloadSegment(SEGMENT_NAME, new IndexLoadingConfig(), mock(SegmentZKMetadata.class),
            mock(SegmentMetadata.class), false);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, "test-reload-thread");

    try {
      reloadThread.start();
      TestUtils.waitForCondition(aVoid -> ((ReentrantLock) segmentLock).hasQueuedThread(reloadThread), 10, 5000,
          "Reload thread did not wait on segment lock");

      // Verify lock acquisition ordering:
      // In refresh (doReplaceSegment), segmentLock is acquired first, then the reload semaphore.
      // To avoid AB-BA deadlock between refresh and reload on the same segment, reloadSegment must also acquire
      // segmentLock before acquiring the reload semaphore.
      // Under unpatched code (the bug), reloadSegment acquires the semaphore before waiting on segmentLock,
      // exhausting permits (availablePermits == 0) and deadlocking any concurrent refresh.
      // Under patched code, reloadSegment blocks on segmentLock first, leaving the semaphore available (1 permit).
      assertEquals(reloadSemaphore.availablePermits(), 1,
          "Reload thread must not acquire the reload semaphore before acquiring segment lock");

      // Verify that another operation (simulating refresh holding segmentLock) can acquire the semaphore
      // without deadlocking.
      Future<Boolean> refreshSemaphoreAcquire = executor.submit(() -> {
        reloadSemaphore.acquire(SEGMENT_NAME, tableDataManager.getLogger());
        reloadSemaphore.release();
        return true;
      });
      assertTrue(refreshSemaphoreAcquire.get(2, TimeUnit.SECONDS),
          "Thread holding segment lock must be able to acquire reload semaphore without deadlocking");
    } finally {
      segmentLock.unlock();
      reloadThread.join(5000);
      executor.shutdownNow();
    }
  }

  @Test
  public void testReloadSegmentInterruptedDuringSemaphoreAcquire()
      throws Exception {
    TestSegmentReloadSemaphore reloadSemaphore = new TestSegmentReloadSemaphore(1);
    BaseTableDataManager tableDataManager = createTableManager(reloadSemaphore);
    // Pre-capture segmentLock to keep strong reference and prevent weak-value GC eviction (Issue #19611).
    Lock segmentLock = tableDataManager.getSegmentLock(SEGMENT_NAME);

    // Acquire the only permit so that reloadSegment blocks when attempting to acquire the semaphore.
    reloadSemaphore.acquire("otherSegment", tableDataManager.getLogger());
    assertEquals(reloadSemaphore.availablePermits(), 0);

    AtomicBoolean caughtInterruptedException = new AtomicBoolean(false);
    Thread reloadThread = new Thread(() -> {
      try {
        tableDataManager.reloadSegment(SEGMENT_NAME, new IndexLoadingConfig(), mock(SegmentZKMetadata.class),
            mock(SegmentMetadata.class), false);
      } catch (InterruptedException e) {
        caughtInterruptedException.set(true);
      } catch (Exception e) {
        if (e.getCause() instanceof InterruptedException) {
          caughtInterruptedException.set(true);
        }
      }
    }, "test-reload-interrupted-thread");

    try {
      reloadThread.start();
      // Wait until reloadThread is blocked waiting to acquire the semaphore.
      TestUtils.waitForCondition(
          aVoid -> reloadThread.getState() == Thread.State.WAITING && reloadSemaphore.isAcquireAttempted(), 10, 5000,
          "Reload thread did not wait on reload semaphore");

      // Interrupt reloadThread while it is waiting in acquire()
      reloadThread.interrupt();
      reloadThread.join(5000);
      assertTrue(caughtInterruptedException.get(), "Reload thread should have caught InterruptedException");

      // Verify exception safety:
      // 1. segmentLock must NOT be leaked (must be unlocked)
      assertFalse(((ReentrantLock) segmentLock).isLocked(), "Segment lock must be released when interrupted");
      assertTrue(segmentLock.tryLock(), "Segment lock should be acquirable after thread was interrupted");
      segmentLock.unlock();

      // 2. No semaphore permit must be leaked: release the permit held by the test, and verify 1 permit remains
      reloadSemaphore.release();
      assertEquals(reloadSemaphore.availablePermits(), 1, "Semaphore permit count must remain balanced");
    } finally {
      if (reloadThread.isAlive()) {
        reloadThread.interrupt();
        reloadThread.join(5000);
      }
      if (reloadSemaphore.availablePermits() == 0) {
        reloadSemaphore.release();
      }
    }
  }

  @Test
  public void testReloadOnlyIfNeededSkipsUpToDateSegment()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    SegmentZKMetadata zkMetadata = new SegmentZKMetadata(SEGMENT_NAME);
    zkMetadata.setCrc(getCRC(indexDir));

    BaseTableDataManager tableDataManager = spy(createTableManager());
    ImmutableSegmentDataManager segmentDataManager = loadSegmentDataManager(indexDir);
    tableDataManager.registerSegment(SEGMENT_NAME, segmentDataManager);
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> new IndexLoadingConfig(DEFAULT_TABLE_CONFIG, SCHEMA)).when(tableDataManager)
        .fetchIndexLoadingConfig();

    tableDataManager.reloadSegments(List.of(SEGMENT_NAME), false, true, "job");
    assertSame(tableDataManager.getSegmentDataManager(SEGMENT_NAME), segmentDataManager);
    verify(tableDataManager, never()).reloadSegment(any(SegmentDataManager.class), any(IndexLoadingConfig.class),
        anyBoolean());
    verify(tableDataManager._reloadJobStatusCache).recordSkipped("job", List.of(SEGMENT_NAME));

    tableDataManager.reloadSegment(SEGMENT_NAME, false, true, "job2");
    assertSame(tableDataManager.getSegmentDataManager(SEGMENT_NAME), segmentDataManager);
    verify(tableDataManager._reloadJobStatusCache).recordSkipped("job2", List.of(SEGMENT_NAME));

    // Without onlyIfNeeded the same segment goes through the regular reload path. Use the single-segment API, which
    // reloads synchronously in every implementation (batch reloads may complete asynchronously in subclasses).
    tableDataManager.reloadSegment(SEGMENT_NAME, false, false, "job3");
    assertNotSame(tableDataManager.getSegmentDataManager(SEGMENT_NAME), segmentDataManager);
  }

  @Test
  public void testReloadOnlyIfNeededReloadsSegmentNeedingPreprocess()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    assertFalse(hasInvertedIndex(indexDir, STRING_COLUMN));
    SegmentZKMetadata zkMetadata = new SegmentZKMetadata(SEGMENT_NAME);
    zkMetadata.setCrc(getCRC(indexDir));

    BaseTableDataManager tableDataManager = spy(createTableManager());
    ImmutableSegmentDataManager segmentDataManager = loadSegmentDataManager(indexDir);
    tableDataManager.registerSegment(SEGMENT_NAME, segmentDataManager);
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of(STRING_COLUMN)).build();
    doAnswer(invocation -> new IndexLoadingConfig(tableConfig, SCHEMA)).when(tableDataManager)
        .fetchIndexLoadingConfig();

    // Single-segment API so the reload has completed when it returns (see testReloadOnlyIfNeededSkipsUpToDateSegment)
    tableDataManager.reloadSegment(SEGMENT_NAME, false, true, "job");
    assertNotSame(tableDataManager.getSegmentDataManager(SEGMENT_NAME), segmentDataManager);
    assertTrue(hasInvertedIndex(indexDir, STRING_COLUMN));
    verify(tableDataManager._reloadJobStatusCache, never()).recordSkipped(any(), any());
  }

  @Test
  public void testReloadForceDownloadTakesPrecedenceOverOnlyIfNeeded()
      throws Exception {
    // The segment is up to date, so onlyIfNeeded alone would skip it; forceDownload must still re-download it
    File indexDir = createSegment(SegmentVersion.v3, 5);
    SegmentZKMetadata zkMetadata =
        makeRawSegment(indexDir, new File(TEMP_DIR, SEGMENT_NAME + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION),
            false);

    BaseTableDataManager tableDataManager = spy(createTableManager());
    ImmutableSegmentDataManager segmentDataManager = loadSegmentDataManager(indexDir);
    tableDataManager.registerSegment(SEGMENT_NAME, segmentDataManager);
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> new IndexLoadingConfig(DEFAULT_TABLE_CONFIG, SCHEMA)).when(tableDataManager)
        .fetchIndexLoadingConfig();

    tableDataManager.reloadSegment(SEGMENT_NAME, true, true, "job");
    assertNotSame(tableDataManager.getSegmentDataManager(SEGMENT_NAME), segmentDataManager);
    verify(tableDataManager._reloadJobStatusCache, never()).recordSkipped(any(), any());
  }

  @Test
  public void testIsReloadNeeded()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);
    BaseTableDataManager tableDataManager = spy(createTableManager());
    ImmutableSegmentDataManager segmentDataManager = loadSegmentDataManager(indexDir);
    tableDataManager.registerSegment(SEGMENT_NAME, segmentDataManager);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(DEFAULT_TABLE_CONFIG, SCHEMA);

    // Up to date
    SegmentZKMetadata zkMetadata = new SegmentZKMetadata(SEGMENT_NAME);
    zkMetadata.setCrc(crc);
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    assertFalse(tableDataManager.isReloadNeeded(segmentDataManager, indexLoadingConfig));

    // CRC changed on a completed segment, so the reload path would download it
    zkMetadata.setCrc(crc + 1);
    assertTrue(tableDataManager.isReloadNeeded(segmentDataManager, indexLoadingConfig));

    // CRC changed but the segment is not completed yet, so the reload path would keep the local copy
    zkMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    assertFalse(tableDataManager.isReloadNeeded(segmentDataManager, indexLoadingConfig));

    // Target tier differs from the current one
    SegmentZKMetadata tierZkMetadata = new SegmentZKMetadata(SEGMENT_NAME);
    tierZkMetadata.setCrc(crc);
    tierZkMetadata.setTier(TIER_NAME);
    seedZKMetadata(tableDataManager, SEGMENT_NAME, tierZkMetadata);
    assertTrue(tableDataManager.isReloadNeeded(segmentDataManager,
        createTierIndexLoadingConfig(TIER_TABLE_CONFIG)));

    // Consuming segments are never reloaded in this mode, and unknown segment types always are
    assertFalse(tableDataManager.isReloadNeeded(mock(RealtimeSegmentDataManager.class), indexLoadingConfig));
    assertTrue(tableDataManager.isReloadNeeded(createImmutableSegmentDataManager(SEGMENT_NAME, crc),
        indexLoadingConfig));
  }

  @Test
  public void testIsReloadNeededForEmptySegment()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 0);
    long crc = getCRC(indexDir);
    BaseTableDataManager tableDataManager = spy(createTableManager());
    ImmutableSegmentDataManager segmentDataManager = loadSegmentDataManager(indexDir);
    assertTrue(segmentDataManager.getSegment() instanceof EmptyIndexSegment);
    tableDataManager.registerSegment(SEGMENT_NAME, segmentDataManager);
    SegmentZKMetadata zkMetadata = new SegmentZKMetadata(SEGMENT_NAME);
    zkMetadata.setCrc(crc);
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);

    // A new index does not apply to an empty segment, so a reload would not change it
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of(STRING_COLUMN)).build();
    assertFalse(tableDataManager.isReloadNeeded(segmentDataManager, new IndexLoadingConfig(tableConfig, SCHEMA)));

    // A changed CRC still needs a download
    zkMetadata.setCrc(crc + 1);
    assertTrue(tableDataManager.isReloadNeeded(segmentDataManager, new IndexLoadingConfig(tableConfig, SCHEMA)));
  }

  @Test
  public void testNeedReloadSegmentsUsesReloadCheck()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);
    BaseTableDataManager tableDataManager = spy(createTableManager());
    tableDataManager.registerSegment(SEGMENT_NAME, loadSegmentDataManager(indexDir));
    SegmentZKMetadata zkMetadata = new SegmentZKMetadata(SEGMENT_NAME);
    zkMetadata.setCrc(crc);
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> new IndexLoadingConfig(DEFAULT_TABLE_CONFIG, SCHEMA)).when(tableDataManager)
        .fetchIndexLoadingConfig();
    assertFalse(tableDataManager.needReloadSegments());

    // A CRC change is reported, which the preprocessing-only check used to miss
    zkMetadata.setCrc(crc + 1);
    assertTrue(tableDataManager.needReloadSegments());

    // A segment whose check fails is reported as needing reload
    zkMetadata.setCrc(crc);
    assertFalse(tableDataManager.needReloadSegments());
    doThrow(new RuntimeException("injected")).when(tableDataManager).isReloadNeeded(any(), any());
    assertTrue(tableDataManager.needReloadSegments());
  }

  @Test
  public void testSelectSegmentsNeedingReloadKeepsSegmentsThatCannotBeChecked()
      throws Exception {
    BaseTableDataManager tableDataManager = spy(createTableManager());
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(DEFAULT_TABLE_CONFIG, SCHEMA);

    SegmentDataManager failingCheck = mock(SegmentDataManager.class);
    when(failingCheck.getSegmentName()).thenReturn("failingCheck");
    when(failingCheck.increaseReferenceCount()).thenReturn(true);
    doThrow(new RuntimeException("injected")).when(tableDataManager).isReloadNeeded(eq(failingCheck), any());

    SegmentDataManager destroyed = mock(SegmentDataManager.class);
    when(destroyed.getSegmentName()).thenReturn("destroyed");
    when(destroyed.increaseReferenceCount()).thenReturn(false);

    SegmentDataManager upToDate = mock(SegmentDataManager.class);
    when(upToDate.getSegmentName()).thenReturn("upToDate");
    when(upToDate.increaseReferenceCount()).thenReturn(true);
    doReturn(false).when(tableDataManager).isReloadNeeded(eq(upToDate), any());

    List<SegmentDataManager> selected = tableDataManager.selectSegmentsNeedingReload(
        List.of(failingCheck, destroyed, upToDate), indexLoadingConfig, "job");
    assertEquals(selected, List.of(failingCheck, destroyed));
    verify(tableDataManager, never()).isReloadNeeded(eq(destroyed), any());
    verify(tableDataManager._reloadJobStatusCache).recordSkipped("job", List.of("upToDate"));
  }

  @Test
  public void testReplaceSegmentNewData()
      throws Exception {
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(SEGMENT_NAME, 0);

    BaseTableDataManager tableDataManager = createTableManager();
    File dataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    assertFalse(dataDir.exists());
    tableDataManager.replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, new IndexLoadingConfig());
    assertTrue(dataDir.exists());
    assertEquals(new SegmentMetadataImpl(dataDir).getTotalDocs(), 5);
  }

  @Test
  public void testReplaceSegmentNewDataNewTier()
      throws Exception {
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);
    zkMetadata.setTier(TIER_NAME);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(SEGMENT_NAME, 0);

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tableDataManager = createTableManager();
    File defaultDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    assertFalse(defaultDataDir.exists());
    tableDataManager.replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata,
        createTierIndexLoadingConfig(DEFAULT_TABLE_CONFIG));
    assertTrue(defaultDataDir.exists());
    assertEquals(new SegmentMetadataImpl(defaultDataDir).getTotalDocs(), 5);

    // Configured dataDir for coolTier, thus move to new dir.
    tableDataManager = createTableManager();
    tableDataManager.replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata,
        createTierIndexLoadingConfig(TIER_TABLE_CONFIG));
    File tierDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME, TIER_NAME, TIER_TABLE_CONFIG);
    assertTrue(tierDataDir.exists());
    assertFalse(defaultDataDir.exists());
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(tierDataDir);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    assertEquals(segmentMetadata.getIndexDir(), tierDataDir);
  }

  @Test
  public void testReplaceSegmentNoop()
      throws Exception {
    String segmentName = "seg01";
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(segmentName);
    when(zkMetadata.getCrc()).thenReturn(1024L);

    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(segmentName, 1024L);

    BaseTableDataManager tableDataManager = createTableManager();
    assertFalse(tableDataManager.getSegmentDataDir(segmentName).exists());
    tableDataManager.replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, new IndexLoadingConfig());
    // As CRC is same, the index dir is left as is, so not get created by the test.
    assertFalse(tableDataManager.getSegmentDataDir(segmentName).exists());
  }

  @Test
  public void testReplaceSegmentNewDataWithAsyncSegmentRefresh()
      throws Exception {
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(SEGMENT_NAME, 0);

    BaseTableDataManager tableDataManager = spy(createTableManagerWithAsyncSegmentRefreshEnabled());
    File dataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    assertFalse(dataDir.exists());

    // Add the segment to the manager's internal map so replaceSegment can find it
    tableDataManager._segmentDataManagerMap.put(SEGMENT_NAME, segmentDataManager);

    // Mock the methods that will be called during segment replacement
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> new IndexLoadingConfig()).when(tableDataManager).fetchIndexLoadingConfig();

    // Use CountDownLatch to wait for async execution
    CountDownLatch latch = new CountDownLatch(1);

    // Mock replaceSegmentIfCrcMismatch which is called by doReplaceSegment
    doAnswer(invocation -> {
      // Call the original replaceSegmentIfCrcMismatch method
      invocation.callRealMethod();
      latch.countDown();
      return null;
    }).when(tableDataManager).replaceSegmentIfCrcMismatch(
        any(SegmentDataManager.class), any(SegmentZKMetadata.class), any(IndexLoadingConfig.class));

    // Call replaceSegment which will execute asynchronously
    tableDataManager.replaceSegment(SEGMENT_NAME);

    // Wait for async execution to complete
    assertTrue(latch.await(10, TimeUnit.SECONDS), "Segment replacement should complete");
    assertTrue(dataDir.exists());
    assertEquals(new SegmentMetadataImpl(dataDir).getTotalDocs(), 5);
  }

  @Test
  public void testReplaceSegmentNewDataNewTierWithAsyncSegmentRefresh()
      throws Exception {
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);
    zkMetadata.setTier(TIER_NAME);

    // Mock the case where segment is loaded but its CRC is different from
    // the one in zk, thus raw segment is downloaded and loaded.
    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(SEGMENT_NAME, 0);

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tableDataManager = spy(createTableManagerWithAsyncSegmentRefreshEnabled());
    File defaultDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    assertFalse(defaultDataDir.exists());

    // Add the segment to the manager's internal map so replaceSegment can find it
    tableDataManager._segmentDataManagerMap.put(SEGMENT_NAME, segmentDataManager);

    // Mock the methods that will be called during segment replacement
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> createTierIndexLoadingConfig(DEFAULT_TABLE_CONFIG))
        .when(tableDataManager).fetchIndexLoadingConfig();

    // Use CountDownLatch to wait for async execution
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(invocation -> {
      // Call the original method
      invocation.callRealMethod();
      latch.countDown();
      return null;
    }).when(tableDataManager).replaceSegmentIfCrcMismatch(
        any(SegmentDataManager.class), any(SegmentZKMetadata.class), any(IndexLoadingConfig.class));

    // Call replaceSegment which will execute asynchronously
    tableDataManager.replaceSegment(SEGMENT_NAME);

    // Wait for async execution to complete
    assertTrue(latch.await(10, TimeUnit.SECONDS), "Segment replacement should complete");
    assertTrue(defaultDataDir.exists());
    assertEquals(new SegmentMetadataImpl(defaultDataDir).getTotalDocs(), 5);

    // Configured dataDir for coolTier, thus move to new dir.
    tableDataManager = spy(createTableManagerWithAsyncSegmentRefreshEnabled());

    // Add the segment to the manager's internal map
    tableDataManager._segmentDataManagerMap.put(SEGMENT_NAME, segmentDataManager);

    // Mock the methods for the second part of the test
    seedZKMetadata(tableDataManager, SEGMENT_NAME, zkMetadata);
    doAnswer(invocation -> createTierIndexLoadingConfig(TIER_TABLE_CONFIG))
        .when(tableDataManager).fetchIndexLoadingConfig();

    CountDownLatch latch2 = new CountDownLatch(1);
    doAnswer(invocation -> {
      // Call the original method
      invocation.callRealMethod();
      latch2.countDown();
      return null;
    }).when(tableDataManager).replaceSegmentIfCrcMismatch(
        any(SegmentDataManager.class), any(SegmentZKMetadata.class), any(IndexLoadingConfig.class));

    // Call replaceSegment which will execute asynchronously
    tableDataManager.replaceSegment(SEGMENT_NAME);

    // Wait for async execution to complete
    assertTrue(latch2.await(10, TimeUnit.SECONDS), "Second segment replacement should complete");

    File tierDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME, TIER_NAME, TIER_TABLE_CONFIG);
    assertTrue(tierDataDir.exists());
    assertFalse(defaultDataDir.exists());
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(tierDataDir);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    assertEquals(segmentMetadata.getIndexDir(), tierDataDir);
  }

  @Test
  public void testReplaceSegmentNoopWithAsyncSegmentRefresh()
      throws Exception {
    String segmentName = "seg01";
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(segmentName);
    when(zkMetadata.getCrc()).thenReturn(1024L);

    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(segmentName, 1024L);

    BaseTableDataManager tableDataManager = spy(createTableManagerWithAsyncSegmentRefreshEnabled());
    assertFalse(tableDataManager.getSegmentDataDir(segmentName).exists());

    // Add the segment to the manager's internal map so replaceSegment can find it
    tableDataManager._segmentDataManagerMap.put(segmentName, segmentDataManager);

    // Mock the methods that will be called during segment replacement
    seedZKMetadata(tableDataManager, segmentName, zkMetadata);
    doAnswer(invocation -> new IndexLoadingConfig()).when(tableDataManager).fetchIndexLoadingConfig();

    // Use CountDownLatch to wait for async execution
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(invocation -> {
      // Call the original method - this should be a no-op since CRCs match
      invocation.callRealMethod();
      latch.countDown();
      return null;
    }).when(tableDataManager).replaceSegmentIfCrcMismatch(
        any(SegmentDataManager.class), any(SegmentZKMetadata.class), any(IndexLoadingConfig.class));

    // Call replaceSegment which will execute asynchronously
    tableDataManager.replaceSegment(segmentName);

    // Wait for async execution to complete
    assertTrue(latch.await(10, TimeUnit.SECONDS), "Segment replacement should complete");

    // As CRC is same, the index dir is left as is, so not get created by the test.
    assertFalse(tableDataManager.getSegmentDataDir(segmentName).exists());
  }

  @Test
  public void testNativeConditionalLoadReloadAndHeldReader() throws Exception {
    try (NativeActionFixture fixture = new NativeActionFixture()) {
      Request load = fixture.request(Mode.LOAD, DEFAULT_TABLE_CONFIG, 1, null, fixture._digest);
      TableConfig refreshedTable = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
          .setInvertedIndexColumns(List.of(LONG_COLUMN)).build();
      Schema refreshedSchema = Schema.cloneSchemaWithName(SCHEMA, RAW_TABLE_NAME);
      refreshedSchema.addField(new DimensionFieldSpec("futureColumn", DataType.STRING, true));
      fixture._onRefresh = () -> {
        when(fixture._metadata.fetchTable(OFFLINE_TABLE_NAME))
            .thenReturn(new TableSnapshot(refreshedTable, refreshedSchema));
        fixture._manager.onTableConfigOrSchemaRefresh();
      };
      Result first = fixture.execute(load);
      assertEquals(first.status(), Status.INSTALLED);
      fixture.assertCount(first.installation().identity());
      // A newer table-level refresh can finish while older physical work is prepared in another segment lane.
      TableConfig cachedTable = fixture._manager.getCachedTableConfigAndSchema().getLeft();
      Schema cachedSchema = fixture._manager.getCachedTableConfigAndSchema().getRight();
      assertEquals(cachedTable.toJsonString(), refreshedTable.toJsonString());
      assertEquals(cachedSchema, refreshedSchema);
      fixture._onRefresh = () -> { };
      SegmentDataManager old = fixture._manager.acquireSegment(SEGMENT_NAME);
      assertNotNull(old);
      assertTrue(fixture._manager.requiresQueryIdentity());
      assertTrue(old instanceof NativeImmutableSegmentDataManager);
      NativeImmutableSegmentDataManager oldNative = (NativeImmutableSegmentDataManager) old;
      assertEquals(oldNative.getInstallationIdentity(), load.identity());
      assertEquals(old.getReferenceCount(), 2);
      assertEquals(oldNative.getQuerySchema(), SCHEMA);
      assertFalse(oldNative.getQuerySchema().hasColumn("futureColumn"));
      Schema readerSchema = oldNative.getQuerySchema();
      readerSchema.setSchemaName("readerLocalName");
      readerSchema.getFieldSpecFor(STRING_COLUMN).setDefaultNullValue("readerLocalDefault");
      assertEquals(oldNative.getQuerySchema(), SCHEMA);
      File original = old.getSegment().getSegmentMetadata().getIndexDir();
      try {
        TableConfig indexed = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
            .setInvertedIndexColumns(List.of(STRING_COLUMN)).build();
        Request reload = fixture.request(Mode.RELOAD, indexed, 2, first.installation().installationId(),
            fixture._digest);
        Result second = fixture.execute(reload);
        assertEquals(second.status(), Status.INSTALLED);
        fixture.assertCount(second.installation().identity());
        assertSame(fixture._manager.getCachedTableConfigAndSchema().getLeft(), cachedTable);
        assertSame(fixture._manager.getCachedTableConfigAndSchema().getRight(), cachedSchema);
        assertNotEquals(second.installation().installationId(), first.installation().installationId());
        SegmentDataManager current = fixture._manager.acquireSegment(SEGMENT_NAME);
        assertNotNull(current);
        try {
          assertTrue(current instanceof NativeImmutableSegmentDataManager);
          NativeImmutableSegmentDataManager currentNative = (NativeImmutableSegmentDataManager) current;
          assertEquals(currentNative.getInstallationIdentity(), reload.identity());
          assertEquals(currentNative.getQuerySchema(), SCHEMA);
          assertEquals(oldNative.getInstallationIdentity(), load.identity());
          assertEquals(old.getReferenceCount(), 1);
          assertEquals(current.getReferenceCount(), 2);
          File replacement = current.getSegment().getSegmentMetadata().getIndexDir();
          assertNotEquals(replacement, original);
          assertTrue(hasInvertedIndex(replacement, STRING_COLUMN));
          assertFalse(hasInvertedIndex(original, STRING_COLUMN));
          assertEquals(old.getSegment().getDataSource(STRING_COLUMN).getDictionary().getStringValue(0), "A");
          assertEquals(current.getSegment().getSegmentMetadata().getTotalDocs(), 5);
        } finally {
          fixture._manager.releaseSegment(current);
        }
        fixture.restart();
        assertFalse(fixture._manager.hasSegment(SEGMENT_NAME));
        Files.delete(fixture._archive.toPath());
        Result replay = fixture.execute(reload);
        assertEquals(replay.status(), Status.INSTALLED);
        assertEquals(replay.installation().installationId(), second.installation().installationId());
        assertEquals(replay.installation().manifestSha256(), second.installation().manifestSha256());
        assertTrue(fixture._manager.hasSegment(SEGMENT_NAME));
        SegmentDataManager restored = fixture._manager.acquireSegment(SEGMENT_NAME);
        assertNotNull(restored);
        try {
          assertTrue(restored instanceof NativeImmutableSegmentDataManager);
          assertEquals(((NativeImmutableSegmentDataManager) restored).getInstallationIdentity(), reload.identity());
          assertEquals(((NativeImmutableSegmentDataManager) restored).getQuerySchema(), SCHEMA);
          assertEquals(oldNative.getInstallationIdentity(), load.identity());
          assertEquals(old.getReferenceCount(), 1);
        } finally {
          fixture._manager.releaseSegment(restored);
        }
      } finally {
        fixture._manager.releaseSegment(old);
      }
      assertEquals(old.getReferenceCount(), 0);
    }
  }

  @Test
  public void testNativeRegistrationFailureKeepsConcurrentReaderAlive() throws Exception {
    try (NativeActionFixture fixture = new NativeActionFixture()) {
      Request load = fixture.request(Mode.LOAD, DEFAULT_TABLE_CONFIG, 1, null, fixture._digest);
      Result first = fixture.execute(load);
      SegmentDataManager original = fixture._manager._segmentDataManagerMap.get(SEGMENT_NAME);
      SegmentDataManager old = spy(original);
      assertTrue(fixture._manager._segmentDataManagerMap.replace(SEGMENT_NAME, original, old));
      CountDownLatch registered = new CountDownLatch(1);
      CountDownLatch removed = new CountDownLatch(1);
      doAnswer(invocation -> {
        registered.countDown();
        assertTrue(removed.await(10, TimeUnit.SECONDS));
        throw new IllegalStateException("Post-registration offload failure");
      }).when(old).doOffload();
      Request reload = fixture.request(Mode.RELOAD, DEFAULT_TABLE_CONFIG, 1,
          first.installation().installationId(), fixture._digest);
      Future<Result> publication = fixture._executor.submit(() -> fixture.execute(reload));
      SegmentDataManager held = null;
      ImmutableSegmentImpl segment = null;
      try {
        assertTrue(registered.await(10, TimeUnit.SECONDS));
        held = fixture._manager.acquireSegment(SEGMENT_NAME);
        assertNotNull(held);
        segment = (ImmutableSegmentImpl) held.getSegment();
        SegmentDataManager unregistered;
        synchronized (fixture._manager._segmentDataManagerMap) {
          unregistered = fixture._manager._segmentDataManagerMap.remove(SEGMENT_NAME);
        }
        assertSame(unregistered, held);
        fixture._manager.releaseSegment(unregistered);
        removed.countDown();
        ExecutionException failure = expectThrows(ExecutionException.class,
            () -> publication.get(10, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof IllegalStateException);
        // The map no longer contains this segment, but the query still owns its resources after publication failed.
        assertTrue(segment.tryAcquireReadLock(), "A query-retained segment must not be destroyed by action cleanup");
        try {
          assertEquals(segment.getDataSource(STRING_COLUMN).getDictionary().getStringValue(0), "A");
        } finally {
          segment.releaseReadLock();
        }
      } finally {
        removed.countDown();
        if (held != null) {
          fixture._manager.releaseSegment(held);
        }
      }
      boolean stillAlive = segment.tryAcquireReadLock();
      if (stillAlive) {
        segment.releaseReadLock();
      }
      assertFalse(stillAlive, "Normal reference-count cleanup destroys the last released segment");
    }
  }

  @Test
  public void testNativeActionFencesChangedAuthorityPointerAndMissingReload() throws Exception {
    try (NativeActionFixture fixture = new NativeActionFixture()) {
      Request absent = fixture.request(Mode.RELOAD, DEFAULT_TABLE_CONFIG, 1, null, fixture._digest);
      assertEquals(fixture.execute(absent).status(), Status.ABSENT);
      Request load = fixture.request(Mode.LOAD, DEFAULT_TABLE_CONFIG, 1, null, fixture._digest);
      fixture._onRefresh = () -> fixture.authorize(absent);
      assertEquals(fixture.execute(load).status(), Status.FENCED);
      assertNull(fixture._manager.nativeExecutor().installed(SEGMENT_NAME));
      assertFalse(fixture._manager.hasSegment(SEGMENT_NAME));
      fixture._onRefresh = () -> { };
      Request wrongDigest = fixture.request(Mode.LOAD, DEFAULT_TABLE_CONFIG, 1, null, "0".repeat(64));
      expectThrows(IOException.class, () -> fixture.execute(wrongDigest));
      assertNull(fixture._manager.nativeExecutor().installed(SEGMENT_NAME));
      Result installed = fixture.execute(load);
      Request stalePointer = fixture.request(Mode.RELOAD, DEFAULT_TABLE_CONFIG, 1, UUID.randomUUID(), fixture._digest);
      assertEquals(fixture.execute(stalePointer).status(), Status.FENCED);
      assertEquals(fixture._manager.nativeExecutor().installed(SEGMENT_NAME).installationId(),
          installed.installation().installationId());
      // Metadata snapshots alone never authorize mutations through the legacy public entry points.
      OfflineTableDataManager manager = fixture._manager;
      expectThrows(IllegalStateException.class, () -> manager.addOnlineSegment(SEGMENT_NAME));
      expectThrows(IllegalStateException.class, () -> manager.addSegment(null, null));
      expectThrows(IllegalStateException.class, () -> manager.addNewOnlineSegment(null, null));
      expectThrows(IllegalStateException.class, () -> manager.downloadAndLoadSegment(null, null));
      expectThrows(IllegalStateException.class, () -> manager.tryLoadExistingSegment(null, null));
      expectThrows(IllegalStateException.class, () -> manager.tryLoadExistingSegmentWithoutRegistering(null, null));
      expectThrows(IllegalStateException.class, () -> manager.reloadSegment(SEGMENT_NAME, false, null));
      expectThrows(IllegalStateException.class, () -> manager.reloadSegment(SEGMENT_NAME, null, null, null, false));
      expectThrows(IllegalStateException.class, () -> manager.reloadSegments(List.of(SEGMENT_NAME), false, null));
      expectThrows(IllegalStateException.class, () -> manager.reloadAllSegments(false, null));
      expectThrows(IllegalStateException.class, () -> manager.replaceSegment(SEGMENT_NAME));
      expectThrows(IllegalStateException.class, () -> manager.offloadSegment(SEGMENT_NAME));
      expectThrows(IllegalStateException.class, () -> manager.offloadSegmentUnsafe(SEGMENT_NAME));
      expectThrows(IllegalStateException.class, () -> manager.deleteSegment(SEGMENT_NAME));
      expectThrows(IllegalStateException.class, () -> manager.registerSegment(SEGMENT_NAME, null));
      expectThrows(IllegalStateException.class, () -> manager.unregisterSegment(SEGMENT_NAME));
      expectThrows(IllegalStateException.class, () -> manager.downloadSegment(null));
      expectThrows(IllegalStateException.class, () -> manager.createBackup(null));
      expectThrows(IllegalStateException.class, () -> manager.removeBackup(null));
      expectThrows(IllegalStateException.class, () -> manager.setDeleted(true));
      expectThrows(IllegalStateException.class, () -> manager.updateCachedTableConfigAndSchema(null, null));
      assertTrue(manager.hasSegment(SEGMENT_NAME));
      assertEquals(manager.nativeExecutor().installed(SEGMENT_NAME).installationId(),
          installed.installation().installationId());
    }
  }

  @Test
  public void testNativeReconcileNewSessionWithExistingAndAbsentDisk() throws Exception {
    try (NativeActionFixture fixture = new NativeActionFixture()) {
      Request absent = fixture.request(Mode.RECONCILE, DEFAULT_TABLE_CONFIG, 1, 1, null, fixture._digest, false);
      Result first = fixture.execute(absent);
      assertEquals(first.status(), Status.INSTALLED);
      SegmentDataManager held = fixture._manager.acquireSegment(SEGMENT_NAME);
      assertNotNull(held);
      assertTrue(held instanceof NativeImmutableSegmentDataManager);
      NativeImmutableSegmentDataManager oldNative = (NativeImmutableSegmentDataManager) held;
      assertEquals(oldNative.getInstallationIdentity(), absent.identity());
      File original = held.getSegment().getSegmentMetadata().getIndexDir();
      try {
        fixture.restart();
        fixture._server = new Participant("server", Role.SERVER, UUID.randomUUID(), 2);
        Files.delete(fixture._archive.toPath());
        Request reconcile = fixture.request(Mode.RECONCILE, DEFAULT_TABLE_CONFIG, 1, 2,
            first.installation().installationId(), fixture._digest, false);
        Request stalePointer = fixture.request(Mode.RECONCILE, DEFAULT_TABLE_CONFIG, 1, 2,
            UUID.randomUUID(), fixture._digest, false);
        assertEquals(fixture.execute(stalePointer).status(), Status.FENCED);
        assertFalse(fixture._manager.hasSegment(SEGMENT_NAME));
        Result recovered = fixture.execute(reconcile);
        assertEquals(recovered.status(), Status.INSTALLED);
        assertNotEquals(recovered.installation().installationId(), first.installation().installationId());
        assertNotEquals(recovered.installation().manifestSha256(), first.installation().manifestSha256());
        assertEquals(recovered.installation().identity().server(), fixture._server);
        SegmentDataManager current = fixture._manager.acquireSegment(SEGMENT_NAME);
        assertNotNull(current);
        try {
          assertTrue(current instanceof NativeImmutableSegmentDataManager);
          NativeImmutableSegmentDataManager currentNative = (NativeImmutableSegmentDataManager) current;
          assertEquals(currentNative.getInstallationIdentity(), reconcile.identity());
          assertNotEquals(currentNative.getInstallationIdentity().server(),
              oldNative.getInstallationIdentity().server());
          assertNotEquals(currentNative.getInstallationIdentity().materializationId(),
              oldNative.getInstallationIdentity().materializationId());
          assertNotEquals(current.getSegment().getSegmentMetadata().getIndexDir(), original);
          assertEquals(oldNative.getInstallationIdentity(), absent.identity());
          assertEquals(held.getReferenceCount(), 1);
        } finally {
          fixture._manager.releaseSegment(current);
        }
        assertEquals(held.getSegment().getDataSource(STRING_COLUMN).getDictionary().getStringValue(0), "A");
        fixture.restart();
        Result replay = fixture.execute(reconcile);
        assertEquals(replay.installation().installationId(), recovered.installation().installationId());
        assertEquals(replay.installation().manifestSha256(), recovered.installation().manifestSha256());
        Request staleSession = new Request(Mode.RECONCILE, absent.identity(), absent.snapshot(), false,
            recovered.installation().installationId());
        assertEquals(fixture.execute(staleSession).status(), Status.FENCED);
        Request forced = fixture.request(Mode.RECONCILE, DEFAULT_TABLE_CONFIG, 1, 2,
            recovered.installation().installationId(), fixture._digest, true);
        expectThrows(Exception.class, () -> fixture.execute(forced));
        assertEquals(fixture._manager.nativeExecutor().installed(SEGMENT_NAME).installationId(),
            recovered.installation().installationId());
      } finally {
        fixture._manager.releaseSegment(held);
      }
      assertEquals(held.getReferenceCount(), 0);
    }
  }

  @Test
  // Injects failure at the existing registration metrics call, whose compatibility API remains deprecated.
  @SuppressWarnings("deprecation")
  public void testNativeManifestRecoversInterruptedRegistrationAndRejectsCorruption() throws Exception {
    try (NativeActionFixture fixture = new NativeActionFixture()) {
      Request load = fixture.request(Mode.LOAD, DEFAULT_TABLE_CONFIG, 1, null, fixture._digest);
      AtomicBoolean failRegistration = new AtomicBoolean(true);
      doAnswer(invocation -> {
        if (failRegistration.getAndSet(false)) {
          throw new IllegalStateException("Interrupted after durable selection, before registration");
        }
        return null;
      }).when(ServerMetrics.get()).addValueToTableGauge(OFFLINE_TABLE_NAME, ServerGauge.DOCUMENT_COUNT, 5L);
      expectThrows(IllegalStateException.class, () -> fixture.execute(load));
      Installation selected = fixture._manager.nativeExecutor().installed(SEGMENT_NAME);
      assertNotNull(selected);
      assertFalse(fixture._manager.hasSegment(SEGMENT_NAME));
      fixture.restart();
      Files.delete(fixture._archive.toPath());
      Result restored = fixture.execute(load);
      assertEquals(restored.status(), Status.INSTALLED);
      assertEquals(restored.installation().installationId(), selected.installationId());
      assertEquals(restored.installation().manifestSha256(), selected.manifestSha256());
      fixture.restart();
      Path manifest;
      try (Stream<Path> paths = Files.list(new File(TABLE_DATA_DIR, ".native/active").toPath())) {
        manifest = paths.findFirst().orElseThrow();
      }
      byte[] valid = Files.readAllBytes(manifest);
      Files.writeString(manifest, "{}");
      expectThrows(IOException.class, () -> fixture.execute(load));
      assertFalse(fixture._manager.hasSegment(SEGMENT_NAME));
      Files.write(manifest, valid);
      Path installation = new File(TABLE_DATA_DIR, ".native/materializations").toPath()
          .resolve(load.identity().historyId().toString()).resolve(load.identity().table().objectId().toString())
          .resolve(load.identity().content().objectId().toString()).resolve(selected.installationId().toString())
          .resolve("segment");
      Path extra = installation.resolve("unmanifested.index");
      Files.writeString(extra, "unmanifested index bytes");
      expectThrows(IOException.class, () -> fixture.execute(load));
      assertFalse(fixture._manager.hasSegment(SEGMENT_NAME));
      Files.delete(extra);
      Files.createSymbolicLink(extra, Path.of("v3"));
      expectThrows(IOException.class, () -> fixture.execute(load));
      assertFalse(fixture._manager.hasSegment(SEGMENT_NAME));
      Files.delete(extra);
      File creationMetadata = SegmentDirectoryPaths.findCreationMetaFile(installation.toFile());
      assertNotNull(creationMetadata);
      Files.write(creationMetadata.toPath(), new byte[] {0});
      expectThrows(IOException.class, () -> fixture.execute(load));
      assertFalse(fixture._manager.hasSegment(SEGMENT_NAME));
    }
  }

  /// Uses actual segment bytes and the public native API. Local authority and component configuration are fixtures.
  private static final class NativeActionFixture implements AutoCloseable {
    private final UUID _history = UUID.randomUUID();
    private final UUID _tableId = UUID.randomUUID();
    private final UUID _schemaId = UUID.randomUUID();
    private final UUID _contentId = UUID.randomUUID();
    private final UUID _assignmentId = UUID.randomUUID();
    private Participant _server = new Participant("server", Role.SERVER, UUID.randomUUID(), 1);
    private final Owner _owner = new Owner(1, 0, 1,
        new Participant("controller", Role.CONTROLLER, UUID.randomUUID(), 1));
    private final File _archive;
    private final SegmentLoadMetadata _descriptor;
    private final String _digest;
    private final ExecutorService _executor = Executors.newSingleThreadExecutor();
    private final ReentrantReadWriteLock _authorityLock = new ReentrantReadWriteLock();
    private Identity _authorized;
    private Runnable _onRefresh = () -> { };
    private OfflineTableDataManager _manager;
    private TableDataManagerMetadata _metadata;

    private NativeActionFixture() throws Exception {
      File segment = createSegment(SegmentVersion.v3, 5);
      long crc = getCRC(segment);
      _archive = new File(TEMP_DIR, "native-content.tar.gz");
      TarCompressionUtils.createCompressedTarFile(segment, _archive);
      _digest = NativeOfflineSegmentManifest.digest(_archive.toPath());
      FileUtils.deleteDirectory(segment);
      _descriptor = new SegmentLoadMetadata(SEGMENT_NAME, crc, 0, false, _archive.toURI().toString(), null, null,
          1, 2, Map.of(), true);
      restart();
    }

    private void restart() {
      if (_manager != null) {
        _manager.shutDown();
      }
      _manager = new OfflineTableDataManager();
      _metadata = mock(TableDataManagerMetadata.class);
      when(_metadata.fetchTable(OFFLINE_TABLE_NAME)).thenReturn(new TableSnapshot(DEFAULT_TABLE_CONFIG, SCHEMA));
      InstanceDataManagerConfig config = createDefaultInstanceDataManagerConfig();
      when(config.getSegmentDirectoryLoader()).thenReturn(
          SegmentDirectoryLoaderRegistry.DEFAULT_SEGMENT_DIRECTORY_LOADER_NAME);
      _manager.initWithMetadata(config, _metadata, new SegmentLocks(), OFFLINE_TABLE_NAME,
          new SegmentReloadSemaphore(1), _executor, null, null, SEGMENT_OPERATIONS_THROTTLER, false,
          mock(ServerReloadJobStatusCache.class));
      _manager.start();
    }

    private Request request(Mode mode, TableConfig table, long version, UUID expected, String digest) {
      return request(mode, table, version, version, expected, digest, true);
    }

    private Request request(Mode mode, TableConfig table, long version, long assignmentEpoch, UUID expected,
        String digest, boolean forceDownload) {
      Identity identity = new Identity(_history, OFFLINE_TABLE_NAME, new ObjectVersion(_tableId, version),
          RAW_TABLE_NAME, new ObjectVersion(_schemaId, 1), SEGMENT_NAME, new ObjectVersion(_contentId, 1), digest,
          _server, new ObjectVersion(_assignmentId, assignmentEpoch), UUID.randomUUID(),
          new Operation(UUID.randomUUID(), 1, _owner));
      return new Request(mode, identity, new SegmentSnapshot(table, SCHEMA, _descriptor), forceDownload, expected);
    }

    private void authorize(Request request) {
      _authorityLock.writeLock().lock();
      try {
        _authorized = request.identity();
      } finally {
        _authorityLock.writeLock().unlock();
      }
    }

    private Result execute(Request request) throws Exception {
      authorize(request);
      return _manager.nativeExecutor().execute(request, new NativeOfflineSegmentAction.Authority() {
        @Override
        public void refresh() {
          _onRefresh.run();
        }

        @Override
        public NativeOfflineSegmentAction.ReadGuard acquire(Identity expected) {
          _authorityLock.readLock().lock();
          return new NativeOfflineSegmentAction.ReadGuard() {
            @Override
            public boolean isCurrent() {
              return expected.equals(_authorized);
            }

            @Override
            public void close() {
              _authorityLock.readLock().unlock();
            }
          };
        }
      });
    }

    /// Executes through the normal query planner over installed native bytes, without opening a query listener.
    private void assertCount(Identity identity) throws Exception {
      NativeQueryContext context = new NativeQueryContext(NativeQueryContext.VERSION, identity.historyId(),
          new ProcessIdentity("broker", UUID.randomUUID(), 1), identity.server(), UUID.randomUUID(), UUID.randomUUID(),
          identity.tableName(), identity.table(), identity.schemaName(), identity.schema(),
          List.of(new Segment(identity.segmentName(), identity.content(), identity.contentSha256(),
              identity.assignment(), identity.materializationId())), List.of());
      InstanceDataManager instance = mock(InstanceDataManager.class);
      when(instance.getTableDataManager(OFFLINE_TABLE_NAME)).thenReturn(_manager);
      ServerQueryExecutorV1Impl executor = new ServerQueryExecutorV1Impl();
      executor.init(new PinotConfiguration(), instance, ServerMetrics.get());
      InstanceRequest request = new InstanceRequest(1,
          CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM " + OFFLINE_TABLE_NAME));
      request.setBrokerId(context.broker().participantId());
      request.setSearchSegments(context.requiredNames());
      request.setNativeQueryContext(context.toThrift());
      try (QueryThreadContext ignored = QueryThreadContext.openForSseTest()) {
        ServerQueryRequest query = new ServerQueryRequest(request, ServerMetrics.get(), System.currentTimeMillis());
        InstanceResponseBlock response =
            executor.execute(query, QueryThreadContext.contextAwareExecutorService(_executor));
        assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
        assertTrue(response.getResultsBlock() instanceof AggregationResultsBlock);
        assertEquals(((AggregationResultsBlock) response.getResultsBlock()).getResults().get(0), 5L);
      } finally {
        executor.shutDown();
      }
    }

    @Override
    public void close() {
      _manager.shutDown();
      _executor.shutdownNow();
    }
  }

  @Test
  public void testNativeInitializationPreservesProviderConfiguration() throws Exception {
    TableConfig table = createTimestampTable(TimestampIndexGranularity.DAY);
    Schema schema = createSchemaReuseSchema();
    String originalTableJson = table.toJsonString();
    TableDataManagerMetadata metadata = mock(TableDataManagerMetadata.class);
    when(metadata.fetchTable(OFFLINE_TABLE_NAME)).thenReturn(new TableSnapshot(table, schema));
    OfflineTableDataManager manager = new OfflineTableDataManager();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    manager.initWithMetadata(createDefaultInstanceDataManagerConfig(), metadata, new SegmentLocks(), OFFLINE_TABLE_NAME,
        new SegmentReloadSemaphore(1), executor, null, null, SEGMENT_OPERATIONS_THROTTLER, false,
        mock(ServerReloadJobStatusCache.class));
    try {
      manager.start();
      assertTrue(manager.getCachedTableConfigAndSchema().getRight().hasColumn("$ts$DAY"));
      assertFalse(schema.hasColumn("$ts$DAY"));
      assertEquals(table.toJsonString(), originalTableJson);
      expectThrows(IllegalStateException.class, manager::getHelixManager);
      expectThrows(IllegalStateException.class, manager::getPropertyStore);
    } finally {
      manager.shutDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testNativeUnsupportedConfigurationRejectedBeforeFilesystemChanges() {
    TableConfig upsert = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).build();
    TableConfig dimension = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setIsDimTable(true).build();
    TableConfig realtime = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).build();
    TableConfig peer = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    peer.getValidationConfig().setPeerSegmentDownloadScheme("http");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      for (TableConfig table : List.of(upsert, dimension, realtime, peer)) {
        assertNativeInitializationRejected(new OfflineTableDataManager(), createDefaultInstanceDataManagerConfig(),
            table, executor);
      }
      InstanceDataManagerConfig peerInstance = createDefaultInstanceDataManagerConfig();
      when(peerInstance.getSegmentPeerDownloadScheme()).thenReturn("http");
      assertNativeInitializationRejected(new OfflineTableDataManager(), peerInstance, DEFAULT_TABLE_CONFIG, executor);
      assertNativeInitializationRejected(new OfflineTableDataManager() { }, createDefaultInstanceDataManagerConfig(),
          DEFAULT_TABLE_CONFIG, executor);
      assertFalse(TABLE_DATA_DIR.exists());
      assertFalse(new File(TEMP_DIR, realtime.getTableName()).exists());
    } finally {
      executor.shutdownNow();
    }
  }

  private static void assertNativeInitializationRejected(OfflineTableDataManager manager,
      InstanceDataManagerConfig instanceConfig, TableConfig table, ExecutorService executor) {
    TableDataManagerMetadata metadata = mock(TableDataManagerMetadata.class);
    when(metadata.fetchTable(table.getTableName())).thenReturn(new TableSnapshot(table, SCHEMA));
    expectThrows(IllegalArgumentException.class, () -> manager.initWithMetadata(instanceConfig, metadata,
        new SegmentLocks(), table.getTableName(), new SegmentReloadSemaphore(1), executor, null, null,
        SEGMENT_OPERATIONS_THROTTLER, false, mock(ServerReloadJobStatusCache.class)));
  }

  @Test
  public void testAddNewSegmentUseLocalCopy()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getCrc()).thenReturn(crc);
    when(zkMetadata.getDownloadUrl()).thenReturn("file://somewhere");

    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.addNewOnlineSegment(zkMetadata, new IndexLoadingConfig());
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);

    FileUtils.deleteQuietly(indexDir);
    try {
      tableDataManager.addNewOnlineSegment(zkMetadata, new IndexLoadingConfig());
      fail();
    } catch (Exception e) {
      // As expected, when local segment dir is missing, it tries to download
      // raw segment from deep store, but it would fail with bad download uri.
      assertEquals(e.getMessage(), "Operation failed after 3 attempts");
    }
  }

  @Test
  public void testAddSegmentUseLocalCopyNewTier()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getCrc()).thenReturn(crc);
    when(zkMetadata.getDownloadUrl()).thenReturn("file://somewhere");
    when(zkMetadata.getTier()).thenReturn(TIER_NAME);

    // No dataDir for coolTier, thus stay on default tier.
    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.addNewOnlineSegment(zkMetadata, createTierIndexLoadingConfig(DEFAULT_TABLE_CONFIG));
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);

    // Configured dataDir for coolTier, thus move to new dir.
    File tierDataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME, TIER_NAME, TIER_TABLE_CONFIG);
    assertFalse(tierDataDir.exists());
    // Move segDir to new tier to see if addNewOnlineSegment() can load segDir from new tier directly.
    FileUtils.moveDirectory(indexDir, tierDataDir);
    tableDataManager = createTableManager();
    tableDataManager.addNewOnlineSegment(zkMetadata, createTierIndexLoadingConfig(TIER_TABLE_CONFIG));
    assertEquals(new SegmentMetadataImpl(tierDataDir).getTotalDocs(), 5);
  }

  @Test
  public void testAddSegmentUseBackupCopy()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getCrc()).thenReturn(crc);

    BaseTableDataManager tableDataManager = createTableManager();
    File backupDir =
        tableDataManager.getSegmentDataDir(SEGMENT_NAME + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    assertTrue(indexDir.renameTo(backupDir));

    assertFalse(indexDir.exists());
    tableDataManager.addNewOnlineSegment(zkMetadata, new IndexLoadingConfig());
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);
  }

  @Test
  public void testAddSegmentStaleBackupCopy()
      throws Exception {
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);

    // Create a local segment with fewer rows, making its CRC different from the raw segment.
    // So that the raw segment is downloaded and loaded in the end.
    BaseTableDataManager tableDataManager = createTableManager();
    File indexDir = createSegment(SegmentVersion.v3, 3);
    File backupDir =
        tableDataManager.getSegmentDataDir(SEGMENT_NAME + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    assertTrue(indexDir.renameTo(backupDir));

    assertFalse(indexDir.exists());
    tableDataManager.addNewOnlineSegment(zkMetadata, new IndexLoadingConfig());
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    assertEquals(new SegmentMetadataImpl(indexDir).getTotalDocs(), 5);
  }

  @Test
  public void testAddSegmentUpConvertVersion()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v1, 5);
    long crc = getCRC(indexDir);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getCrc()).thenReturn(crc);

    // Require to use v3 format.
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);

    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.addNewOnlineSegment(zkMetadata, indexLoadingConfig);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);
  }

  @Test
  public void testAddSegmentDownConvertVersion()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getCrc()).thenReturn(crc);

    // Require to use v1 format.
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v1);

    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.addNewOnlineSegment(zkMetadata, indexLoadingConfig);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    // The existing segment preprocessing logic doesn't down convert segment format.
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    assertEquals(segmentMetadata.getVersion(), SegmentVersion.v3);
  }

  @Test
  public void testAddSegmentAddIndex()
      throws Exception {
    File indexDir = createSegment(SegmentVersion.v3, 5);
    long crc = getCRC(indexDir);
    assertFalse(hasInvertedIndex(indexDir, STRING_COLUMN));
    assertFalse(hasInvertedIndex(indexDir, LONG_COLUMN));

    // Make local and remote CRC same to skip downloading raw segment.
    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getCrc()).thenReturn(crc);

    // Require to add indices.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of(STRING_COLUMN, LONG_COLUMN)).build();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);

    BaseTableDataManager tableDataManager = createTableManager();
    tableDataManager.addNewOnlineSegment(zkMetadata, indexLoadingConfig);
    assertEquals(tableDataManager.getSegmentDataDir(SEGMENT_NAME), indexDir);
    assertTrue(indexDir.exists());
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(segmentMetadata.getTotalDocs(), 5);
    assertTrue(hasInvertedIndex(indexDir, STRING_COLUMN));
    assertTrue(hasInvertedIndex(indexDir, LONG_COLUMN));
  }

  @Test
  public void testDownloadAndDecrypt()
      throws Exception {
    File tempDir = new File(TEMP_DIR, "testDownloadAndDecrypt");
    String fileName = "tmp.txt";
    FileUtils.write(new File(tempDir, fileName), "this is from somewhere remote");
    String tarFileName = SEGMENT_NAME + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION;
    File tempTarFile = new File(TEMP_DIR, tarFileName);
    TarCompressionUtils.createCompressedTarFile(tempDir, tempTarFile);

    SegmentZKMetadata zkMetadata = mock(SegmentZKMetadata.class);
    when(zkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(zkMetadata.getDownloadUrl()).thenReturn("file://" + tempTarFile.getAbsolutePath());

    BaseTableDataManager tableDataManager = createTableManager();
    File indexDir = tableDataManager.downloadSegment(zkMetadata);
    assertEquals(FileUtils.readFileToString(new File(indexDir, fileName)), "this is from somewhere remote");

    FileUtils.deleteDirectory(indexDir);
    when(zkMetadata.getCrypterName()).thenReturn("fakePinotCrypter");
    indexDir = tableDataManager.downloadSegment(zkMetadata);
    assertEquals(FileUtils.readFileToString(new File(indexDir, fileName)), "this is from somewhere remote");

    FakePinotCrypter fakeCrypter = (FakePinotCrypter) PinotCrypterFactory.create("fakePinotCrypter");
    assertTrue(fakeCrypter._origFile.getAbsolutePath().endsWith(tarFileName + ".enc"));
    assertTrue(fakeCrypter._decFile.getAbsolutePath().endsWith(tarFileName));

    try {
      // Set maxRetry to 0 to cause retry failure immediately.
      Map<String, Object> properties = new HashMap<>();
      properties.put(BaseSegmentFetcher.RETRY_COUNT_CONFIG_KEY, 0);
      SegmentFetcherFactory.init(new PinotConfiguration(properties));
      tableDataManager.downloadSegment(zkMetadata);
      fail();
    } catch (AttemptsExceededException e) {
      assertEquals(e.getMessage(), "Operation failed after 0 attempts");
    }
  }

  @Test
  public void testUntarAndMoveSegment()
      throws IOException {
    BaseTableDataManager tableDataManager = createTableManager();
    File tempRootDir = tableDataManager.getTmpSegmentDataDir("test-untar-move");

    // All input and intermediate files are put in the tempRootDir.
    File tempTar = new File(tempRootDir, SEGMENT_NAME + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION);
    File tempInputDir = new File(tempRootDir, "input");
    FileUtils.write(new File(tempInputDir, "tmp.txt"), "this is in segment dir");
    TarCompressionUtils.createCompressedTarFile(tempInputDir, tempTar);
    FileUtils.deleteQuietly(tempInputDir);

    // The destination is the segment directory at the same level of tempRootDir.
    File untarredFile = tableDataManager.untarAndMoveSegment(SEGMENT_NAME, tempTar, tempRootDir);
    assertEquals(untarredFile, tableDataManager.getSegmentDataDir(SEGMENT_NAME));
    assertEquals(FileUtils.readFileToString(new File(untarredFile, "tmp.txt")), "this is in segment dir");

    try {
      tableDataManager.untarAndMoveSegment(SEGMENT_NAME, new File(tempRootDir, "unknown.txt"), TEMP_DIR);
      fail();
    } catch (Exception e) {
      // expected.
    }
  }

  // Regression for the same-name table recreation race on the ONLINE path: after shutdown, a stale download must not
  // replace the segment data directory, which a recreated same-name table may already own.
  @Test
  public void testMoveSegmentRejectedAfterShutdown()
      throws IOException {
    BaseTableDataManager tableDataManager = createTableManager();
    File tempRootDir = tableDataManager.getTmpSegmentDataDir("test-move-after-shutdown");

    File tempTar = new File(tempRootDir, SEGMENT_NAME + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION);
    File tempInputDir = new File(tempRootDir, "input");
    FileUtils.write(new File(tempInputDir, "tmp.txt"), "this is in segment dir", StandardCharsets.UTF_8);
    TarCompressionUtils.createCompressedTarFile(tempInputDir, tempTar);
    FileUtils.deleteQuietly(tempInputDir);

    File segmentDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    File marker = new File(segmentDir, "marker");
    FileUtils.write(marker, "recreated owner's data", StandardCharsets.UTF_8);

    tableDataManager.shutDown();
    expectThrows(IllegalStateException.class,
        () -> tableDataManager.untarAndMoveSegment(SEGMENT_NAME, tempTar, tempRootDir));
    assertEquals(FileUtils.readFileToString(marker, StandardCharsets.UTF_8), "recreated owner's data",
        "Stale download must not replace the segment data directory after shutdown");
  }

  @Test
  public void testReplaceSegmentIfCrcMismatchWhenFlagDisabledSegmentCrcMismatchShouldDownload()
      throws Exception {
    // When flag is disabled and segment CRCs don't match, should download
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);
    zkMetadata.setCrc(2048L); // Different from local
    zkMetadata.setDataCrc(99999L);
    zkMetadata.setUseDataCrc(false);

    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(SEGMENT_NAME, 1024L);
    SegmentMetadata segmentMetadata = segmentDataManager.getSegment().getSegmentMetadata();
    when(segmentMetadata.getDataCrc()).thenReturn(99999L);

    BaseTableDataManager tableDataManager = createTableManager();
    File dataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    assertFalse(dataDir.exists());

    // Should download because segment CRCs don't match (ignores data CRC)
    tableDataManager.replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, new IndexLoadingConfig());

    assertTrue(dataDir.exists());
    assertEquals(new SegmentMetadataImpl(dataDir).getTotalDocs(), 5);
  }

  @Test
  public void testReplaceSegmentIfCrcMismatchWhenFlagEnabledAndSegmentCrcMatchShouldNoDownload()
      throws Exception {
    // When flag is enabled and segment CRCs match, should not download
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);
    long segmentCrc = zkMetadata.getCrc();
    zkMetadata.setDataCrc(99999L);
    zkMetadata.setUseDataCrc(true);

    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(SEGMENT_NAME, segmentCrc);
    SegmentMetadata segmentMetadata = segmentDataManager.getSegment().getSegmentMetadata();
    when(segmentMetadata.getDataCrc()).thenReturn(11111L);

    BaseTableDataManager tableDataManager = createTableManager();
    File dataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);

    assertTrue(dataDir.mkdirs());

    // Should NOT download because segment CRCs match
    tableDataManager.replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, new IndexLoadingConfig());
  }

  @Test
  public void testReplaceSegmentIfCrcMismatchWhenFlagEnabledAndSegmentCrcMismatchWithInvalidZkDataCrc()
      throws Exception {
    // When ZK data CRC is invalid (-1), should download if segment CRCs don't match
    SegmentZKMetadata zkMetadata = createRawSegment(SegmentVersion.v3, 5);
    zkMetadata.setCrc(2048L);
    zkMetadata.setDataCrc(-1L);
    zkMetadata.setUseDataCrc(true);

    ImmutableSegmentDataManager segmentDataManager = createImmutableSegmentDataManager(SEGMENT_NAME, 1024L);
    SegmentMetadata segmentMetadata = segmentDataManager.getSegment().getSegmentMetadata();
    when(segmentMetadata.getDataCrc()).thenReturn(99999L);

    BaseTableDataManager tableDataManager = createTableManager();
    File dataDir = tableDataManager.getSegmentDataDir(SEGMENT_NAME);
    assertFalse(dataDir.exists());

    // Should download because ZK data CRC is invalid
    tableDataManager.replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, new IndexLoadingConfig());

    assertTrue(dataDir.exists());
    assertEquals(new SegmentMetadataImpl(dataDir).getTotalDocs(), 5);
  }

  @Test
  public void testOnTableConfigOrSchemaRefreshRefreshesCachedConfig()
      throws Exception {
    BaseTableDataManager tableDataManager = spy(createTableManager());
    TableConfig refreshedConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(2).build();
    Schema refreshedSchema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(STRING_COLUMN, DataType.STRING).build();
    // Stand in for the ZK fetch fetchIndexLoadingConfig performs: its contract is to refresh the cached config/schema.
    doAnswer(invocation -> {
      tableDataManager.updateCachedTableConfigAndSchema(refreshedConfig, refreshedSchema);
      return new IndexLoadingConfig();
    }).when(tableDataManager).fetchIndexLoadingConfig();

    assertSame(tableDataManager.getCachedTableConfigAndSchema().getLeft(), DEFAULT_TABLE_CONFIG);

    tableDataManager.onTableConfigOrSchemaRefresh();

    // The default callback refreshes the cache via the index-loading-config fetch.
    verify(tableDataManager).fetchIndexLoadingConfig();
    assertSame(tableDataManager.getCachedTableConfigAndSchema().getLeft(), refreshedConfig);
    assertSame(tableDataManager.getCachedTableConfigAndSchema().getRight(), refreshedSchema);
  }

  // Has to be public class for the class loader to work.
  public static class FakePinotCrypter implements PinotCrypter {
    private File _origFile;
    private File _decFile;

    @Override
    public void init(PinotConfiguration config) {
    }

    @Override
    public void encrypt(File origFile, File encFile) {
    }

    @Override
    public void decrypt(File origFile, File decFile) {
      _origFile = origFile;
      _decFile = decFile;
      origFile.renameTo(decFile);
    }
  }

  /// registerSegment must fire the post-registration lifecycle hook on the backing segment of a standalone manager.
  /// A standalone ImmutableSegmentDataManager exposes its segment via getReportableSegments() (default), while
  /// getSegments() defaults to empty — so iterating getSegments() here would silently skip the hook for the common
  /// single-segment case. This is a regression guard for that: it fails if registerSegment iterates getSegments().
  @Test
  public void testRegisterSegmentFiresOnSegmentAddedForStandaloneSegment() {
    BaseTableDataManager tableDataManager = createTableManager();
    ImmutableSegment immutableSegment = mock(ImmutableSegment.class);
    when(immutableSegment.getSegmentName()).thenReturn(SEGMENT_NAME);
    tableDataManager.registerSegment(SEGMENT_NAME, new ImmutableSegmentDataManager(immutableSegment));
    verify(immutableSegment).onSegmentAdded();
  }

  /// registerSegment must fire the hook once per reportable segment for a multi-segment manager
  @Test
  public void testRegisterSegmentFiresOnSegmentAddedForEachReportableSegment() {
    BaseTableDataManager tableDataManager = createTableManager();
    ImmutableSegment segment1 = mock(ImmutableSegment.class);
    ImmutableSegment segment2 = mock(ImmutableSegment.class);
    SegmentDataManager multiSegmentManager = mock(SegmentDataManager.class);
    when(multiSegmentManager.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(multiSegmentManager.getReportableSegments()).thenReturn(List.of(segment1, segment2));
    // registerSegment holds a reference while the hook runs; a mock returns false unless stubbed.
    when(multiSegmentManager.increaseReferenceCount()).thenReturn(true);
    tableDataManager.registerSegment(SEGMENT_NAME, multiSegmentManager);
    verify(segment1).onSegmentAdded();
    verify(segment2).onSegmentAdded();
  }

  /// The hook fires after the segment is swapped into the serving set, so registerSegment must hold a reference while
  /// it runs. A manager that has already been destroyed (reference count 0) cannot be referenced and has nothing left
  /// to notify: firing the hook there would run it against a closed SegmentDirectory.
  @Test
  public void testRegisterSegmentSkipsOnSegmentAddedForDestroyedSegment() {
    BaseTableDataManager tableDataManager = createTableManager();
    ImmutableSegment immutableSegment = mock(ImmutableSegment.class);
    when(immutableSegment.getSegmentName()).thenReturn(SEGMENT_NAME);
    ImmutableSegmentDataManager segmentDataManager = new ImmutableSegmentDataManager(immutableSegment);
    // Drop the initial reference, mirroring a concurrent replaceSegment()/unregisterSegment() that destroyed it.
    assertTrue(segmentDataManager.decreaseReferenceCount());

    tableDataManager.registerSegment(SEGMENT_NAME, segmentDataManager);

    verify(immutableSegment, never()).onSegmentAdded();
  }

  /// The hook must not leave a net reference behind: the segment stays acquirable after registration, and is not
  /// destroyed by the reference registerSegment took while firing the hook.
  @Test
  public void testRegisterSegmentRestoresReferenceCountAfterFiringHook() {
    BaseTableDataManager tableDataManager = createTableManager();
    ImmutableSegment immutableSegment = mock(ImmutableSegment.class);
    when(immutableSegment.getSegmentName()).thenReturn(SEGMENT_NAME);
    ImmutableSegmentDataManager segmentDataManager = new ImmutableSegmentDataManager(immutableSegment);

    tableDataManager.registerSegment(SEGMENT_NAME, segmentDataManager);

    verify(immutableSegment).onSegmentAdded();
    assertEquals(segmentDataManager.getReferenceCount(), 1);
    verify(immutableSegment, never()).destroy();
  }

  /// A failing hook must not fail the registration: the segment is already serving by then, and propagating the
  /// failure would abort the enclosing Helix state transition for a segment that is in fact up.
  @Test
  public void testRegisterSegmentSucceedsWhenOnSegmentAddedThrows() {
    BaseTableDataManager tableDataManager = createTableManager();
    ImmutableSegment immutableSegment = mock(ImmutableSegment.class);
    when(immutableSegment.getSegmentName()).thenReturn(SEGMENT_NAME);
    doThrow(new RuntimeException("boom")).when(immutableSegment).onSegmentAdded();
    ImmutableSegmentDataManager segmentDataManager = new ImmutableSegmentDataManager(immutableSegment);

    tableDataManager.registerSegment(SEGMENT_NAME, segmentDataManager);

    assertSame(tableDataManager.getSegmentDataManager(SEGMENT_NAME), segmentDataManager);
    assertEquals(segmentDataManager.getReferenceCount(), 1);
  }

  /// The default getReportableSegments() wraps getSegment(), so a custom manager exposing a null segment must not
  /// abort registration.
  @Test
  public void testRegisterSegmentToleratesNullReportableSegment() {
    BaseTableDataManager tableDataManager = createTableManager();
    SegmentDataManager segmentDataManager = mock(SegmentDataManager.class);
    when(segmentDataManager.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(segmentDataManager.increaseReferenceCount()).thenReturn(true);
    when(segmentDataManager.getReportableSegments()).thenReturn(Arrays.asList(null, null));

    tableDataManager.registerSegment(SEGMENT_NAME, segmentDataManager);

    assertSame(tableDataManager.getSegmentDataManager(SEGMENT_NAME), segmentDataManager);
  }

  /// An upsert replacement with a consistency mode other than NONE registers the same new segment twice: first through
  /// a DuoSegmentDataManager (whose default getReportableSegments() returns its primary, i.e. the new segment) and then
  /// directly. The hook must reach the underlying SegmentDirectory only once, since implementations are not required to
  /// be idempotent — a second call would e.g. upload a duplicate marker file.
  @Test
  public void testConsistencyModeReplacementFiresOnSegmentAddedOnce()
      throws Exception {
    BaseTableDataManager tableDataManager = createTableManager();
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn(SEGMENT_NAME);
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    // A real ImmutableSegment (not a mock) so the at-most-once guard in the implementation is exercised.
    EmptyIndexSegment newSegment = new EmptyIndexSegment(segmentMetadata, segmentDirectory);
    ImmutableSegmentDataManager newSegmentManager = new ImmutableSegmentDataManager(newSegment);
    ImmutableSegment oldSegment = mock(ImmutableSegment.class);
    when(oldSegment.getSegmentName()).thenReturn(SEGMENT_NAME);
    ImmutableSegmentDataManager oldSegmentManager = new ImmutableSegmentDataManager(oldSegment);

    // Mirrors BaseTableDataManager.replaceUpsertSegment() for a non-NONE consistency mode.
    tableDataManager.registerSegment(SEGMENT_NAME, new DuoSegmentDataManager(newSegmentManager, oldSegmentManager));
    tableDataManager.registerSegment(SEGMENT_NAME, newSegmentManager);

    verify(segmentDirectory, times(1)).onSegmentAdded();
  }

  @Test
  public void testGetCachedIndexLoadingConfigReusesSchemaAndRefreshes() {
    TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema original = createSchemaReuseSchema();
    BaseTableDataManager manager = createSchemaReuseManager(table, original);
    IndexLoadingConfig first = manager.getCachedIndexLoadingConfig();
    IndexLoadingConfig second = manager.getCachedIndexLoadingConfig();
    assertSame(first.getSchema(), original);
    assertSame(second.getSchema(), original);
    assertSame(first.getTableConfig(), second.getTableConfig());
    assertSame(first, second);
    verifyNoInteractions(manager._propertyStore);

    Schema changed = createSchemaReuseSchema();
    changed.getFieldSpecFor("id").setDefaultNullValue(-2);
    ZKMetadataProvider.setSchema(manager._propertyStore, changed);
    // Explicit refreshes still fetch the latest schema even without a separate refresh message.
    Schema refreshed = manager.fetchIndexLoadingConfig().getSchema();
    assertNotSame(refreshed, original);
    assertEquals(refreshed.getFieldSpecFor("id").getDefaultNullValue(), -2);
    assertEquals(original.getFieldSpecFor("id").getDefaultNullValue(), -1);
    assertSame(manager.getCachedTableConfigAndSchema().getRight(), refreshed);
    assertSame(manager.getCachedIndexLoadingConfig().getSchema(), refreshed);

    manager.updateCachedTableConfigAndSchema(table, changed);
    assertSame(manager.getCachedIndexLoadingConfig().getSchema(), changed);
    TableConfig indexedTable = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of("id")).build();
    manager.updateCachedTableConfigAndSchema(indexedTable, changed);
    IndexLoadingConfig indexed = manager.getCachedIndexLoadingConfig();
    assertTrue(indexed.getFieldIndexConfig("id").getConfig(StandardIndexes.inverted()).isEnabled());
    assertFalse(first.getFieldIndexConfig("id").getConfig(StandardIndexes.inverted()).isEnabled());
    assertSame(indexed.getFieldIndexConfig("id"), manager.getCachedIndexLoadingConfig().getFieldIndexConfig("id"));
  }

  @Test
  public void testGetCachedIndexLoadingConfigDerivesSegmentTier() {
    TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = createSchemaReuseSchema();
    BaseTableDataManager manager = createSchemaReuseManager(table, schema);
    IndexLoadingConfig shared = manager.getCachedIndexLoadingConfig();
    IndexLoadingConfig tierConfig = shared.withSegmentTier("cold");
    assertNotSame(tierConfig, shared);
    assertEquals(tierConfig.getSegmentTier(), "cold");
    assertNull(shared.getSegmentTier());
    assertSame(manager.getCachedIndexLoadingConfig(), shared);
    assertSame(shared.withSegmentTier(null), shared);
    verifyNoInteractions(manager._propertyStore);
  }

  @Test
  public void testGetCachedIndexLoadingConfigNormalizesTimestampBeforeReuse() {
    BaseTableDataManager manager =
        createSchemaReuseManager(createTimestampTable(TimestampIndexGranularity.DAY), createSchemaReuseSchema());
    Schema first = manager.getCachedIndexLoadingConfig().getSchema();
    IndexLoadingConfig second = manager.getCachedIndexLoadingConfig();
    assertSame(second.getSchema(), first);
    assertTrue(first.hasColumn("$ts$DAY"));
    assertTrue(second.getFieldIndexConfigByColName().get("$ts$DAY").getConfig(StandardIndexes.range()).isEnabled());
    assertEquals(second.getTableConfig().getIndexingConfig().getRangeIndexColumns(), List.of("$ts$DAY"));
    assertEquals(second.getTableConfig().getIngestionConfig().getTransformConfigs().size(), 1);

    ZKMetadataProvider.setTableConfig(manager._propertyStore, createTimestampTable(TimestampIndexGranularity.HOUR));
    manager.onTableConfigOrSchemaRefresh();
    Schema changed = manager.getCachedIndexLoadingConfig().getSchema();
    assertNotSame(changed, first);
    assertTrue(changed.hasColumn("$ts$HOUR"));
    assertFalse(changed.hasColumn("$ts$DAY"));
    assertFalse(first.hasColumn("$ts$HOUR"));
  }

  @Test
  public void testGetCachedIndexLoadingConfigConcurrentlyReusesCachedSchema()
      throws Exception {
    BaseTableDataManager manager =
        createSchemaReuseManager(createTimestampTable(TimestampIndexGranularity.DAY), createSchemaReuseSchema());
    IndexLoadingConfig shared = manager.getCachedIndexLoadingConfig();
    ExecutorService executor = Executors.newFixedThreadPool(8);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<IndexLoadingConfig>> results = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        results.add(executor.submit(() -> {
          assertTrue(start.await(10, TimeUnit.SECONDS));
          return manager.getCachedIndexLoadingConfig();
        }));
      }
      start.countDown();
      for (Future<IndexLoadingConfig> result : results) {
        assertSame(result.get(10, TimeUnit.SECONDS), shared);
      }
      verifyNoInteractions(manager._propertyStore);
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
  }

  private static BaseTableDataManager createSchemaReuseManager(TableConfig table, Schema schema) {
    BaseTableDataManager manager = new OfflineTableDataManager();
    manager._propertyStore = new FakePropertyStore();
    manager._tableNameWithType = OFFLINE_TABLE_NAME;
    ZKMetadataProvider.setTableConfig(manager._propertyStore, table);
    ZKMetadataProvider.setSchema(manager._propertyStore, schema);
    manager.updateCachedTableConfigAndSchema(table, schema);
    manager._propertyStore = spy(manager._propertyStore);
    return manager;
  }

  private static Schema createSchemaReuseSchema() {
    return new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("id", DataType.INT, -1)
        .addDateTime("ts", DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS").build();
  }

  private static TableConfig createTimestampTable(TimestampIndexGranularity granularity) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setFieldConfigList(List.of(new FieldConfig.Builder("ts")
            .withTimestampConfig(new TimestampConfig(List.of(granularity))).build())).build();
  }

  protected BaseTableDataManager createTableManager() {
    return createTableManager(createDefaultInstanceDataManagerConfig());
  }

  protected BaseTableDataManager createTableManagerWithAsyncSegmentRefreshEnabled() {
    return createTableManagerWithAsyncSegmentRefreshEnabled(createDefaultInstanceDataManagerConfig());
  }

  protected BaseTableDataManager createTableManager(InstanceDataManagerConfig instanceDataManagerConfig) {
    BaseTableDataManager tableDataManager = newTableDataManager();
    tableDataManager.init(instanceDataManagerConfig, createHelixManagerMock(), new SegmentLocks(), DEFAULT_TABLE_CONFIG,
        SCHEMA, new SegmentReloadSemaphore(1), Executors.newSingleThreadExecutor(), null, null,
        SEGMENT_OPERATIONS_THROTTLER, false, mock(ServerReloadJobStatusCache.class));
    return tableDataManager;
  }

  protected BaseTableDataManager createTableManager(SegmentReloadSemaphore segmentReloadSemaphore) {
    BaseTableDataManager tableDataManager = newTableDataManager();
    tableDataManager.init(createDefaultInstanceDataManagerConfig(), createHelixManagerMock(), new SegmentLocks(),
        DEFAULT_TABLE_CONFIG, SCHEMA, segmentReloadSemaphore, Executors.newSingleThreadExecutor(), null, null,
        SEGMENT_OPERATIONS_THROTTLER, false, mock(ServerReloadJobStatusCache.class));
    return tableDataManager;
  }

  protected BaseTableDataManager createTableManagerWithAsyncSegmentRefreshEnabled(
      InstanceDataManagerConfig instanceDataManagerConfig) {
    BaseTableDataManager tableDataManager = newTableDataManager();
    tableDataManager.init(instanceDataManagerConfig, createHelixManagerMock(), new SegmentLocks(), DEFAULT_TABLE_CONFIG,
        SCHEMA, new SegmentReloadSemaphore(1), Executors.newSingleThreadExecutor(), null, null,
        SEGMENT_OPERATIONS_THROTTLER, true, mock(ServerReloadJobStatusCache.class));
    return tableDataManager;
  }

  /// Returns the concrete [BaseTableDataManager] instance under test. Default returns a stock
  /// [OfflineTableDataManager]; subclasses override to test a different implementation while inheriting
  /// all test bodies.
  protected BaseTableDataManager newTableDataManager() {
    return new OfflineTableDataManager();
  }

  /// Returns the [HelixManager] mock wired into the TDM under test. Default returns a bare Mockito mock
  /// (no property store stubbed) — fine for the inherited test bodies which never read ZK directly. Subclasses
  /// that exercise paths reading `_propertyStore` (e.g. `fetchZKMetadata`, `fetchIndexLoadingConfig`)
  /// override to stub `helixManager.getHelixPropertyStore()` with a `FakePropertyStore` pre-seeded
  /// with table config + schema + per-segment ZK metadata.
  protected HelixManager createHelixManagerMock() {
    return mock(HelixManager.class);
  }

  protected void seedZKMetadata(BaseTableDataManager spy, String segmentName, SegmentZKMetadata zkMetadata) {
    doAnswer(invocation -> zkMetadata).when(spy).fetchZKMetadata(segmentName);
  }

  protected static InstanceDataManagerConfig createDefaultInstanceDataManagerConfig() {
    InstanceDataManagerConfig config = mock(InstanceDataManagerConfig.class);
    when(config.getInstanceDataDir()).thenReturn(TEMP_DIR.getAbsolutePath());
    // Check CRC matching on segment load time.
    when(config.shouldCheckCRCOnSegmentLoad()).thenReturn(true);
    return config;
  }

  protected static File createSegment(SegmentVersion segmentVersion, int numRows)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(DEFAULT_TABLE_CONFIG, SCHEMA);
    config.setOutDir(TABLE_DATA_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);
    config.setSegmentVersion(segmentVersion);
    List<GenericRow> rows = new ArrayList<>(3);
    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();
      row.putValue(STRING_COLUMN, STRING_VALUES[i]);
      row.putValue(LONG_COLUMN, LONG_VALUES[i]);
      rows.add(row);
    }
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(TABLE_DATA_DIR, SEGMENT_NAME);
  }

  protected static SegmentZKMetadata createRawSegment(SegmentVersion segmentVersion, int numRows)
      throws Exception {
    File indexDir = createSegment(segmentVersion, numRows);
    return makeRawSegment(indexDir,
        new File(TEMP_DIR, SEGMENT_NAME + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION), true);
  }

  protected static SegmentZKMetadata makeRawSegment(File indexDir, File rawSegmentFile, boolean deleteIndexDir)
      throws Exception {
    long crc = getCRC(indexDir);
    SegmentZKMetadata zkMetadata = new SegmentZKMetadata(SEGMENT_NAME);
    TarCompressionUtils.createCompressedTarFile(indexDir, rawSegmentFile);
    zkMetadata.setDownloadUrl("file://" + rawSegmentFile.getAbsolutePath());
    zkMetadata.setCrc(crc);
    if (deleteIndexDir) {
      FileUtils.deleteQuietly(indexDir);
    }
    return zkMetadata;
  }

  protected static long getCRC(File indexDir)
      throws IOException {
    File creationMetaFile = SegmentDirectoryPaths.findCreationMetaFile(indexDir);
    assertNotNull(creationMetaFile);
    try (DataInputStream in = new DataInputStream(new FileInputStream(creationMetaFile))) {
      return in.readLong();
    }
  }

  protected IndexLoadingConfig createTierIndexLoadingConfig(TableConfig tableConfig) {
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getSegmentDirectoryLoader()).thenReturn(TIER_SEGMENT_DIRECTORY_LOADER);
    when(instanceDataManagerConfig.getConfig()).thenReturn(new PinotConfiguration());
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(instanceDataManagerConfig, tableConfig, null);
    indexLoadingConfig.setTableDataDir(TEMP_DIR.getAbsolutePath() + File.separator + tableConfig.getTableName());
    indexLoadingConfig.setSegmentTier(TIER_NAME);
    return indexLoadingConfig;
  }

  private static ImmutableSegmentDataManager loadSegmentDataManager(File indexDir)
      throws Exception {
    return new ImmutableSegmentDataManager(
        ImmutableSegmentLoader.load(indexDir, new IndexLoadingConfig(DEFAULT_TABLE_CONFIG, SCHEMA)));
  }

  protected ImmutableSegmentDataManager createImmutableSegmentDataManager(String segmentName, long crc) {
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getCrc()).thenReturn(crc);
    return createImmutableSegmentDataManager(segmentName, segmentMetadata);
  }

  protected ImmutableSegmentDataManager createImmutableSegmentDataManager(String segmentName,
      SegmentMetadata segmentMetadata) {
    ImmutableSegmentDataManager segmentDataManager = mock(ImmutableSegmentDataManager.class);
    when(segmentDataManager.getSegmentName()).thenReturn(segmentName);
    ImmutableSegment immutableSegment = mock(ImmutableSegment.class);
    when(segmentDataManager.getSegment()).thenReturn(immutableSegment);
    when(immutableSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    return segmentDataManager;
  }

  private static boolean hasInvertedIndex(File indexDir, String columnName)
      throws IOException {
    File indexMapFile =
        new File(new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME), V1Constants.INDEX_MAP_FILE_NAME);
    return FileUtils.readFileToString(indexMapFile, StandardCharsets.UTF_8).contains(columnName + ".inverted_index");
  }

  private static class TestSegmentReloadSemaphore extends SegmentReloadSemaphore {
    private final AtomicInteger _availablePermits;
    private final AtomicBoolean _acquireAttempted = new AtomicBoolean(false);

    public TestSegmentReloadSemaphore(int permits) {
      super(permits);
      _availablePermits = new AtomicInteger(permits);
    }

    @Override
    public void acquire(String segmentName, Logger logger)
        throws InterruptedException {
      _acquireAttempted.set(true);
      super.acquire(segmentName, logger);
      _availablePermits.decrementAndGet();
    }

    @Override
    public void release() {
      _availablePermits.incrementAndGet();
      super.release();
    }

    public int availablePermits() {
      return _availablePermits.get();
    }

    public boolean isAcquireAttempted() {
      return _acquireAttempted.get();
    }
  }
}
