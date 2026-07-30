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
package org.apache.pinot.core.data.manager.offline;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.fetcher.BaseSegmentFetcher;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.LazyLoadConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for lazy segment loading: stub-on-assignment, materialize-on-first-query, concurrent dedupe, TTL eviction
 * back to stub, in-flight query protection, eager load when a local copy exists (restart warm-up), instance kill
 * switch and non-lazy regression.
 */
public class OfflineTableDataManagerLazyLoadTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "OfflineTableDataManagerLazyLoadTest");
  private static final String RAW_TABLE_NAME = "testLazyTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final File TABLE_DATA_DIR = new File(TEMP_DIR, OFFLINE_TABLE_NAME);
  private static final String SEGMENT_NAME = "testLazySegment";
  private static final String STRING_COLUMN = "col1";
  private static final String[] STRING_VALUES = {"A", "D", "E", "B", "C"};
  private static final String LONG_COLUMN = "col2";
  private static final long[] LONG_VALUES = {10000L, 20000L, 50000L, 40000L, 30000L};
  private static final int NUM_ROWS = 5;
  private static final long IDLE_EVICTION_SECONDS = 1;

  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addSingleValueDimension(STRING_COLUMN, DataType.STRING)
          .addMetric(LONG_COLUMN, DataType.LONG).build();

  // Managers created by the running test — shut down in tearDown so mmap'd segment files can be deleted (Windows
  // cannot delete open-mapped files).
  private final List<OfflineTableDataManager> _tableDataManagers = new ArrayList<>();

  @BeforeClass
  public void setUp() {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @BeforeMethod
  public void setUpMethod()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    Map<String, Object> properties = new HashMap<>();
    properties.put(BaseSegmentFetcher.RETRY_COUNT_CONFIG_KEY, 3);
    properties.put(BaseSegmentFetcher.RETRY_WAIT_MS_CONFIG_KEY, 100);
    properties.put(BaseSegmentFetcher.RETRY_DELAY_SCALE_FACTOR_CONFIG_KEY, 5);
    SegmentFetcherFactory.init(new PinotConfiguration(properties));
  }

  @AfterMethod
  public void tearDownMethod()
      throws Exception {
    for (OfflineTableDataManager tableDataManager : _tableDataManagers) {
      try {
        tableDataManager.shutDown();
      } catch (Exception e) {
        // Best effort — deletion below is what we actually need
      }
    }
    _tableDataManagers.clear();
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testStubOnAssignmentAndLoadOnAcquire()
      throws Exception {
    TableConfig tableConfig = createLazyTableConfig(IDLE_EVICTION_SECONDS, true);
    SegmentZKMetadata zkMetadata = createRawSegment(SEGMENT_NAME);
    OfflineTableDataManager tableDataManager =
        createLazyTableDataManager(tableConfig, createLazyInstanceConfig(true), zkMetadata);

    // Assignment registers a stub: routable (hasSegment) but nothing loaded and nothing on disk.
    tableDataManager.addOnlineSegment(SEGMENT_NAME);
    assertTrue(tableDataManager.hasSegment(SEGMENT_NAME));
    assertEquals(tableDataManager.getNumSegments(), 0);
    assertFalse(new File(TABLE_DATA_DIR, SEGMENT_NAME).exists());
    verify(tableDataManager, times(0)).addNewOnlineSegment(any(), any());

    // First acquire (single-segment query path) materializes the segment.
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(SEGMENT_NAME);
    assertNotNull(segmentDataManager);
    assertEquals(segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs(), NUM_ROWS);
    assertEquals(tableDataManager.getNumSegments(), 1);
    assertTrue(new File(TABLE_DATA_DIR, SEGMENT_NAME).exists());
    verify(tableDataManager, times(1)).addNewOnlineSegment(any(), any());
    tableDataManager.releaseSegment(segmentDataManager);

    // Multi-segment query path (acquireSegments) on a second stubbed segment.
    String secondSegmentName = SEGMENT_NAME + "_2";
    SegmentZKMetadata secondZkMetadata = createRawSegment(secondSegmentName);
    doReturn(secondZkMetadata).when(tableDataManager).fetchZKMetadata(secondSegmentName);
    doReturn(secondZkMetadata).when(tableDataManager).fetchZKMetadataNullable(secondSegmentName);
    tableDataManager.addOnlineSegment(secondSegmentName);
    assertEquals(tableDataManager.getNumSegments(), 1);

    List<String> missingSegments = new ArrayList<>();
    List<SegmentDataManager> segmentDataManagers =
        tableDataManager.acquireSegments(List.of(SEGMENT_NAME, secondSegmentName), missingSegments);
    assertEquals(segmentDataManagers.size(), 2);
    assertTrue(missingSegments.isEmpty());
    assertEquals(tableDataManager.getNumSegments(), 2);
    for (SegmentDataManager sdm : segmentDataManagers) {
      tableDataManager.releaseSegment(sdm);
    }
  }

  @Test
  public void testConcurrentAcquireDedupesDownload()
      throws Exception {
    TableConfig tableConfig = createLazyTableConfig(IDLE_EVICTION_SECONDS, true);
    SegmentZKMetadata zkMetadata = createRawSegment(SEGMENT_NAME);
    OfflineTableDataManager tableDataManager =
        createLazyTableDataManager(tableConfig, createLazyInstanceConfig(true), zkMetadata);
    tableDataManager.addOnlineSegment(SEGMENT_NAME);

    int numThreads = 8;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    AtomicInteger successCount = new AtomicInteger();
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(() -> {
        try {
          startLatch.await();
          SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(SEGMENT_NAME);
          if (segmentDataManager != null) {
            successCount.incrementAndGet();
            tableDataManager.releaseSegment(segmentDataManager);
          }
        } catch (Exception e) {
          // Counted as failure via successCount
        } finally {
          doneLatch.countDown();
        }
      });
    }
    startLatch.countDown();
    assertTrue(doneLatch.await(60, TimeUnit.SECONDS));
    executorService.shutdownNow();

    // All 8 concurrent cold queries succeed, but the per-segment lock dedupes them into a single download+load.
    assertEquals(successCount.get(), numThreads);
    verify(tableDataManager, times(1)).addNewOnlineSegment(any(), any());
  }

  @Test
  public void testTtlEvictionAndReloadLoop()
      throws Exception {
    TableConfig tableConfig = createLazyTableConfig(IDLE_EVICTION_SECONDS, true);
    SegmentZKMetadata zkMetadata = createRawSegment(SEGMENT_NAME);
    OfflineTableDataManager tableDataManager =
        createLazyTableDataManager(tableConfig, createLazyInstanceConfig(true), zkMetadata);
    tableDataManager.addOnlineSegment(SEGMENT_NAME);

    // Materialize.
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(SEGMENT_NAME);
    assertNotNull(segmentDataManager);
    tableDataManager.releaseSegment(segmentDataManager);
    assertEquals(tableDataManager.getNumSegments(), 1);
    File indexDir = new File(TABLE_DATA_DIR, SEGMENT_NAME);
    assertTrue(indexDir.exists());

    // Idle past the TTL, then sweep: memory freed, local dir deleted, stub re-registered, still routable.
    Thread.sleep(IDLE_EVICTION_SECONDS * 1000 + 500);
    tableDataManager.evictIdleLazySegments();
    assertEquals(tableDataManager.getNumSegments(), 0);
    assertFalse(indexDir.exists());
    assertTrue(tableDataManager.hasSegment(SEGMENT_NAME));

    // Next query refetches from the deep store and serves correctly.
    segmentDataManager = tableDataManager.acquireSegment(SEGMENT_NAME);
    assertNotNull(segmentDataManager);
    assertEquals(segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs(), NUM_ROWS);
    assertTrue(indexDir.exists());
    tableDataManager.releaseSegment(segmentDataManager);
    verify(tableDataManager, times(2)).addNewOnlineSegment(any(), any());
  }

  @Test
  public void testNoEvictionWhileQueryInFlight()
      throws Exception {
    TableConfig tableConfig = createLazyTableConfig(IDLE_EVICTION_SECONDS, true);
    SegmentZKMetadata zkMetadata = createRawSegment(SEGMENT_NAME);
    OfflineTableDataManager tableDataManager =
        createLazyTableDataManager(tableConfig, createLazyInstanceConfig(true), zkMetadata);
    tableDataManager.addOnlineSegment(SEGMENT_NAME);

    // Acquire and hold — simulates a query in flight.
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(SEGMENT_NAME);
    assertNotNull(segmentDataManager);

    Thread.sleep(IDLE_EVICTION_SECONDS * 1000 + 500);
    tableDataManager.evictIdleLazySegments();
    // Reference count > 1 blocks eviction even though the idle TTL has passed.
    assertEquals(tableDataManager.getNumSegments(), 1);
    assertTrue(new File(TABLE_DATA_DIR, SEGMENT_NAME).exists());

    tableDataManager.releaseSegment(segmentDataManager);
    // After release + idle, the sweep evicts.
    Thread.sleep(IDLE_EVICTION_SECONDS * 1000 + 500);
    tableDataManager.evictIdleLazySegments();
    assertEquals(tableDataManager.getNumSegments(), 0);
    assertTrue(tableDataManager.hasSegment(SEGMENT_NAME));
  }

  @Test
  public void testLocalCopySkipsStubOnAssignment()
      throws Exception {
    TableConfig tableConfig = createLazyTableConfig(IDLE_EVICTION_SECONDS, true);
    SegmentZKMetadata zkMetadata = createRawSegment(SEGMENT_NAME);
    OfflineTableDataManager tableDataManager =
        createLazyTableDataManager(tableConfig, createLazyInstanceConfig(true), zkMetadata);

    // Recreate the local index dir to simulate a server restart with a valid local copy: the assignment must load
    // eagerly from disk instead of resetting the segment to a stub (which would force a deep-store re-download).
    createSegment(SEGMENT_NAME);
    tableDataManager.addOnlineSegment(SEGMENT_NAME);
    assertEquals(tableDataManager.getNumSegments(), 1);
    verify(tableDataManager, times(1)).addNewOnlineSegment(any(), any());

    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(SEGMENT_NAME);
    assertNotNull(segmentDataManager);
    assertEquals(segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs(), NUM_ROWS);
    tableDataManager.releaseSegment(segmentDataManager);
  }

  @Test
  public void testInstanceKillSwitchDisablesLazyLoading()
      throws Exception {
    TableConfig tableConfig = createLazyTableConfig(IDLE_EVICTION_SECONDS, true);
    SegmentZKMetadata zkMetadata = createRawSegment(SEGMENT_NAME);
    OfflineTableDataManager tableDataManager =
        createLazyTableDataManager(tableConfig, createLazyInstanceConfig(false), zkMetadata);

    // Kill switch off: table-level lazy config is ignored, the segment loads eagerly on assignment.
    tableDataManager.addOnlineSegment(SEGMENT_NAME);
    assertEquals(tableDataManager.getNumSegments(), 1);
    assertTrue(new File(TABLE_DATA_DIR, SEGMENT_NAME).exists());
    verify(tableDataManager, times(1)).addNewOnlineSegment(any(), any());

    // Sweeps are no-ops.
    Thread.sleep(IDLE_EVICTION_SECONDS * 1000 + 500);
    tableDataManager.evictIdleLazySegments();
    assertEquals(tableDataManager.getNumSegments(), 1);
  }

  @Test
  public void testNonLazyTableUnchanged()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    SegmentZKMetadata zkMetadata = createRawSegment(SEGMENT_NAME);
    OfflineTableDataManager tableDataManager =
        createLazyTableDataManager(tableConfig, createLazyInstanceConfig(true), zkMetadata);

    // Without lazyLoadConfig the table behaves exactly like upstream: eager load on assignment.
    tableDataManager.addOnlineSegment(SEGMENT_NAME);
    assertEquals(tableDataManager.getNumSegments(), 1);
    assertTrue(new File(TABLE_DATA_DIR, SEGMENT_NAME).exists());

    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(SEGMENT_NAME);
    assertNotNull(segmentDataManager);
    assertEquals(segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs(), NUM_ROWS);
    tableDataManager.releaseSegment(segmentDataManager);

    // Sweeps never touch non-lazy tables.
    Thread.sleep(IDLE_EVICTION_SECONDS * 1000 + 500);
    tableDataManager.evictIdleLazySegments();
    assertEquals(tableDataManager.getNumSegments(), 1);

    // Unknown segments still resolve to null.
    assertNull(tableDataManager.acquireSegment("unknownSegment"));
  }

  private static TableConfig createLazyTableConfig(long idleEvictionSeconds, boolean deleteLocalOnEvict) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    tableConfig.setLazyLoadConfig(new LazyLoadConfig(true, idleEvictionSeconds, deleteLocalOnEvict));
    return tableConfig;
  }

  private static InstanceDataManagerConfig createLazyInstanceConfig(boolean lazyLoadEnabled) {
    InstanceDataManagerConfig config = mock(InstanceDataManagerConfig.class);
    when(config.getInstanceDataDir()).thenReturn(TEMP_DIR.getAbsolutePath());
    when(config.shouldCheckCRCOnSegmentLoad()).thenReturn(true);
    when(config.isLazyLoadEnabled()).thenReturn(lazyLoadEnabled);
    when(config.getLazyLoadMaterializeParallelism()).thenReturn(4);
    when(config.getLazyLoadMaterializeTimeoutSeconds()).thenReturn(300L);
    return config;
  }

  /**
   * Creates a spied OfflineTableDataManager with the ZK-touching methods stubbed out, following the
   * BaseTableDataManagerTest pattern: a real segment is built and tarred locally, and a file:// download URL acts
   * as the deep store.
   */
  private OfflineTableDataManager createLazyTableDataManager(TableConfig tableConfig,
      InstanceDataManagerConfig instanceDataManagerConfig, SegmentZKMetadata zkMetadata) {
    OfflineTableDataManager tableDataManager = spy(new OfflineTableDataManager());
    tableDataManager.init(instanceDataManagerConfig, mock(HelixManager.class), new SegmentLocks(), tableConfig,
        SCHEMA, new SegmentReloadSemaphore(1), Executors.newSingleThreadExecutor(), null, null, null, false,
        mock(ServerReloadJobStatusCache.class));
    doReturn(zkMetadata).when(tableDataManager).fetchZKMetadata(anyString());
    doReturn(zkMetadata).when(tableDataManager).fetchZKMetadataNullable(anyString());
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);
    indexLoadingConfig.setTableDataDir(TABLE_DATA_DIR.getAbsolutePath());
    doReturn(indexLoadingConfig).when(tableDataManager).fetchIndexLoadingConfig();
    _tableDataManagers.add(tableDataManager);
    return tableDataManager;
  }

  /**
   * Builds a real segment, tars it into TEMP_DIR and deletes the index dir — the file:// URL acts as the deep
   * store, so the only way for the server to serve the segment is to "download" it.
   */
  private static SegmentZKMetadata createRawSegment(String segmentName)
      throws Exception {
    File indexDir = createSegment(segmentName);
    File rawSegmentFile = new File(TEMP_DIR, segmentName + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(indexDir, rawSegmentFile);
    long crc = getCrc(indexDir);
    SegmentZKMetadata zkMetadata = new SegmentZKMetadata(segmentName);
    // NOTE: toURI() keeps this portable — a raw "file://" + absolute path is an invalid URI on Windows.
    zkMetadata.setDownloadUrl(rawSegmentFile.toURI().toString());
    zkMetadata.setCrc(crc);
    FileUtils.deleteQuietly(indexDir);
    return zkMetadata;
  }

  private static File createSegment(String segmentName)
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(TABLE_DATA_DIR.getAbsolutePath());
    config.setSegmentName(segmentName);
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(STRING_COLUMN, STRING_VALUES[i]);
      row.putValue(LONG_COLUMN, LONG_VALUES[i]);
      rows.add(row);
    }
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(TABLE_DATA_DIR, segmentName);
  }

  private static long getCrc(File indexDir)
      throws Exception {
    File creationMetaFile = SegmentDirectoryPaths.findCreationMetaFile(indexDir);
    assertNotNull(creationMetaFile);
    try (DataInputStream in = new DataInputStream(new FileInputStream(creationMetaFile))) {
      return in.readLong();
    }
  }
}
