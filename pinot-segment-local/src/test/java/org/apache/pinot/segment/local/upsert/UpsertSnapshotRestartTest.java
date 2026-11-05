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
package org.apache.pinot.segment.local.upsert;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.WatermarkUtils;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Restarts a full upsert partition with metadata TTL and snapshots enabled, using real segments, snapshot files and
/// TTL watermark file. Only the [TableDataManager] is mocked, to make segmentLock contended for chosen segments.
///
/// segA (ts=100) has pk 1, 2, 3 and the newer segB (ts=290) has pk 1, 2, so the expected valid docs are segA={2} and
/// segB={0,1}. With TTL=30, segA is out of TTL against the largest seen comparison value 290. When the snapshot pass
/// misses segA, its snapshot on disk is stale or missing, and segA must not be treated as out of TTL after a restart,
/// otherwise it is added with the stale snapshot or with all docs valid, i.e. with duplicate primary keys.
public class UpsertSnapshotRestartTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "UpsertSnapshotRestartTest");
  private static final File SEGMENTS_DIR = new File(TEMP_DIR, "segments");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension("pk", FieldSpec.DataType.INT).addMetric("ts", FieldSpec.DataType.INT)
      .setPrimaryKeyColumns(List.of("pk")).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).build();
  private static final double METADATA_TTL = 30;

  private final Set<String> _contendedSegments = ConcurrentHashMap.newKeySet();
  private TableDataManager _tableDataManager;
  private boolean _allSegmentsLoaded;

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    FileUtils.forceMkdir(TEMP_DIR);
    ServerMetrics.register(mock(ServerMetrics.class));
    _allSegmentsLoaded = true;
    _contendedSegments.clear();
    _tableDataManager = mock(TableDataManager.class);
    when(_tableDataManager.getTableDataDir()).thenReturn(TEMP_DIR);
    ConcurrentHashMap<String, ReentrantLock> segmentLocks = new ConcurrentHashMap<>();
    when(_tableDataManager.getSegmentLock(anyString())).thenAnswer(
        invocation -> segmentLocks.computeIfAbsent(invocation.getArgument(0), this::newSegmentLock));
    buildSegment("segA", new int[]{1, 2, 3}, 100);
    buildSegment("segB", new int[]{1, 2}, 290);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @DataProvider
  public static Object[][] restartModes() {
    // Whether to preload the segments with snapshot on restart, and whether to add segB before segA
    return new Object[][]{{false, false}, {false, true}, {true, false}, {true, true}};
  }

  @Test(dataProvider = "restartModes")
  public void testRestartWithoutMissedSnapshot(boolean preload, boolean addSegBFirst)
      throws Exception {
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    ImmutableSegmentImpl segA = addSegment(manager, "segA");
    ImmutableSegmentImpl segB = addSegment(manager, "segB");
    manager.takeSnapshot();
    assertEquals(persistedWatermark(manager), 290.0);
    close(manager, segA, segB);

    assertEquals(restart(preload, addSegBFirst), "{2} | {0,1}");
  }

  /// segA never got a snapshot, e.g. its segmentLock was held by a reload whenever the snapshot was taken.
  @Test(dataProvider = "restartModes")
  public void testRestartWithMissingSnapshot(boolean preload, boolean addSegBFirst)
      throws Exception {
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    ImmutableSegmentImpl segA = addSegment(manager, "segA");
    ImmutableSegmentImpl segB = addSegment(manager, "segB");
    _contendedSegments.add("segA");
    manager.takeSnapshot();
    assertFalse(hasSnapshot("segA"));
    assertTrue(hasSnapshot("segB"));
    // Capped to keep segA within TTL: 100 >= 130 - 30
    assertEquals(persistedWatermark(manager), 130.0);
    close(manager, segA, segB);

    assertEquals(restart(preload, addSegBFirst), "{2} | {0,1}");
  }

  /// segA got a snapshot while all its docs were valid, then missed the snapshot after segB invalidated 2 of them.
  @Test(dataProvider = "restartModes")
  public void testRestartWithStaleSnapshot(boolean preload, boolean addSegBFirst)
      throws Exception {
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    ImmutableSegmentImpl segA = addSegment(manager, "segA");
    manager.takeSnapshot();
    assertEquals(loadSnapshot("segA"), "{0,1,2}");
    ImmutableSegmentImpl segB = addSegment(manager, "segB");
    _contendedSegments.add("segA");
    manager.takeSnapshot();
    assertEquals(loadSnapshot("segA"), "{0,1,2}");
    // segB is not snapshotted either as the loop for segments without snapshot is skipped
    assertFalse(hasSnapshot("segB"));
    assertEquals(persistedWatermark(manager), 130.0);
    close(manager, segA, segB);

    assertEquals(restart(preload, addSegBFirst), "{2} | {0,1}");
  }

  @Test
  public void testWatermarkAdvancesOnceMissedSnapshotIsTaken()
      throws Exception {
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    ImmutableSegmentImpl segA = addSegment(manager, "segA");
    ImmutableSegmentImpl segB = addSegment(manager, "segB");
    _contendedSegments.add("segA");
    manager.takeSnapshot();
    assertEquals(persistedWatermark(manager), 130.0);
    // The watermark in memory is not capped, so the expired primary keys are still removed as before
    assertEquals(manager.getWatermark(), 290.0);

    _contendedSegments.clear();
    manager.takeSnapshot();
    assertEquals(loadSnapshot("segA"), "{2}");
    assertEquals(persistedWatermark(manager), 290.0);
    close(manager, segA, segB);
  }

  /// The just committed segment can miss its snapshot because its segmentLock is still held by the thread replacing the
  /// consuming segment. It is within TTL, so it should not hold back the persisted watermark.
  @Test
  public void testMissedSnapshotWithinTtlDoesNotCapWatermark()
      throws Exception {
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    ImmutableSegmentImpl segA = addSegment(manager, "segA");
    ImmutableSegmentImpl segB = addSegment(manager, "segB");
    _contendedSegments.add("segB");
    manager.takeSnapshot();
    assertTrue(hasSnapshot("segA"));
    assertFalse(hasSnapshot("segB"));
    assertEquals(persistedWatermark(manager), 290.0);
    close(manager, segA, segB);

    for (Object[] restartMode : restartModes()) {
      assertEquals(restart((boolean) restartMode[0], (boolean) restartMode[1]), "{2} | {0,1}");
    }
  }

  /// Stopping the metadata manager does not take a snapshot. Rows consumed into the consuming segment are not
  /// committed, and are consumed again after a restart, so the snapshots must not include the doc invalidated by them.
  /// Otherwise pk 1 would be invisible after a restart until the consuming segment catches up again.
  @Test
  public void testStopDoesNotSnapshotUncommittedUpdates()
      throws Exception {
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    ImmutableSegmentImpl segA = addSegment(manager, "segA");
    manager.takeSnapshot();
    MutableSegment consumingSegment = mock(MutableSegment.class);
    when(consumingSegment.getSegmentName()).thenReturn("segC");
    when(consumingSegment.getValidDocIds()).thenReturn(new ThreadSafeMutableRoaringBitmap());
    manager.addRecord(consumingSegment, new RecordInfo(new PrimaryKey(new Object[]{1}), 0, 200, false));
    assertEquals(segA.getValidDocIds().getMutableRoaringBitmap().toString(), "{1,2}");
    manager.stop();
    manager.close();
    segA.destroy();
    assertEquals(loadSnapshot("segA"), "{0,1,2}");

    manager = newManager();
    segA = load("segA");
    manager.preloadSegment(segA);
    assertEquals(segA.getValidDocIds().getMutableRoaringBitmap().toString(), "{0,1,2}");
    manager.stop();
    manager.close();
    segA.destroy();
  }

  /// Synthesizes the legacy format using public segment builders and the numeric watermark writer.
  private void writeLegacyState(boolean stale, boolean missing)
      throws Exception {
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    ImmutableSegmentImpl segA = addSegment(manager, "segA");
    if (!missing) {
      manager.takeSnapshot();
    }
    ImmutableSegmentImpl segB = addSegment(manager, "segB");
    if (stale || missing) {
      _contendedSegments.add("segA");
    }
    manager.takeSnapshot();
    close(manager, segA, segB);
    _contendedSegments.clear();
    // An older writer persisted the live maximum, even if a bitmap was stale or missing.
    WatermarkUtils.persistWatermark(290, new File(TEMP_DIR, V1Constants.TTL_WATERMARK_TABLE_PARTITION + 0));
  }

  @Test(dataProvider = "restartModes")
  public void testLegacyStaleSnapshotMigration(boolean preload, boolean addSegBFirst)
      throws Exception {
    writeLegacyState(true, false);
    assertEquals(loadSnapshot("segA"), "{0,1,2}");
    assertFalse(hasSnapshot("segB"));
    migrateLegacyState(preload, addSegBFirst, false);
    assertEquals(restart(preload, addSegBFirst), "{2} | {0,1}");
  }

  @Test(dataProvider = "restartModes")
  public void testLegacyMissingSnapshotMigration(boolean preload, boolean addSegBFirst)
      throws Exception {
    writeLegacyState(false, true);
    assertFalse(hasSnapshot("segA"));
    migrateLegacyState(preload, addSegBFirst, false);
    assertEquals(restart(preload, addSegBFirst), "{2} | {0,1}");
  }

  @Test(dataProvider = "restartModes")
  public void testLegacyFreshSnapshotMigration(boolean preload, boolean addSegBFirst)
      throws Exception {
    writeLegacyState(false, false);
    migrateLegacyState(preload, addSegBFirst, false);
    assertEquals(restart(preload, addSegBFirst), "{2} | {0,1}");
  }

  @Test(dataProvider = "restartModes")
  public void testLegacyMigrationWithMissedSnapshot(boolean preload, boolean addSegBFirst)
      throws Exception {
    writeLegacyState(true, false);
    migrateLegacyState(preload, addSegBFirst, true);
    assertEquals(restart(preload, addSegBFirst), "{2} | {0,1}");
  }

  @Test(dataProvider = "restartModes")
  public void testInterruptedLegacyMigration(boolean preload, boolean addSegBFirst)
      throws Exception {
    writeLegacyState(true, false);
    _allSegmentsLoaded = false;
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    ImmutableSegmentImpl segA = load("segA");
    if (preload) {
      manager.preloadSegment(segA);
    } else {
      manager.addSegment(segA);
    }
    manager.removeExpiredPrimaryKeys();
    manager.takeSnapshot();
    assertEquals(Files.size(manager.getWatermarkFile().toPath()), 8L);
    assertEquals(loadSnapshot("segA"), "{0,1,2}");
    assertTrue(manager.getLastSnapshotPass() == null);
    close(manager, segA);
    migrateLegacyState(preload, addSegBFirst, false);
    assertEquals(restart(preload, addSegBFirst), "{2} | {0,1}");
  }

  @Test
  public void testLegacyWatermarkAfterDowngrade()
      throws Exception {
    writeLegacyState(false, false);
    migrateLegacyState(true, false, false);
    // The old writer truncates the version suffix even when the numeric value happens to stay the same.
    WatermarkUtils.persistWatermark(290, new File(TEMP_DIR, V1Constants.TTL_WATERMARK_TABLE_PARTITION + 0));
    _allSegmentsLoaded = false;
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    assertFalse(manager.canSkipSegmentOutOfTTL(100));
    manager.takeSnapshot();
    assertEquals(Files.size(manager.getWatermarkFile().toPath()), 8L);
    close(manager);
  }

  @Test
  public void testCappedSnapshotBootstrapDefersCleanup()
      throws Exception {
    buildSegment("segC", new int[]{4}, 400);
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    ImmutableSegmentImpl segA = addSegment(manager, "segA");
    manager.takeSnapshot();
    ImmutableSegmentImpl segB = addSegment(manager, "segB");
    ImmutableSegmentImpl segC = addSegment(manager, "segC");
    _contendedSegments.add("segA");
    manager.takeSnapshot();
    assertEquals(persistedWatermark(manager), 130.0);
    close(manager, segA, segB, segC);
    _contendedSegments.clear();

    _allSegmentsLoaded = false;
    manager = newManager();
    segC = addSegment(manager, "segC");
    segB = addSegment(manager, "segB");
    // C advances the live watermark beyond B's TTL. B must still reconcile ownership with unloaded A.
    manager.removeExpiredPrimaryKeys();
    assertEquals(manager.getNumPrimaryKeys(), 3L);
    segA = addSegment(manager, "segA");
    assertEquals(validDocIds(segA) + " | " + validDocIds(segB), "{2} | {0,1}");
    _allSegmentsLoaded = true;
    manager.takeSnapshot();
    manager.removeExpiredPrimaryKeys();
    assertEquals(manager.getNumPrimaryKeys(), 1L);
    assertEquals(validDocIds(segA), "{2}");
    close(manager, segA, segB, segC);
    assertEquals(restart(true, false), "{2} | {0,1}");
  }

  @Test
  public void testMissingWatermarkDuringLegacyRecovery()
      throws Exception {
    writeLegacyState(true, false);
    Files.delete(new File(TEMP_DIR, V1Constants.TTL_WATERMARK_TABLE_PARTITION + 0).toPath());
    buildSegment("segC", new int[]{4}, 400);
    _allSegmentsLoaded = false;
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    ImmutableSegmentImpl segC = addSegment(manager, "segC");
    ImmutableSegmentImpl segB = addSegment(manager, "segB");
    manager.removeExpiredPrimaryKeys();
    manager.takeSnapshot();
    assertFalse(manager.getWatermarkFile().exists());
    ImmutableSegmentImpl segA = addSegment(manager, "segA");
    assertEquals(validDocIds(segA) + " | " + validDocIds(segB), "{2} | {0,1}");
    _allSegmentsLoaded = true;
    manager.takeSnapshot();
    close(manager, segA, segB, segC);
    assertEquals(restart(true, false), "{2} | {0,1}");
  }

  @Test
  public void testLegacyMigrationRequiresAssignmentEvidence()
      throws Exception {
    writeLegacyState(true, false);
    ConcurrentMapPartitionUpsertMetadataManager manager =
        new ConcurrentMapPartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0, newContext());
    manager._gotFirstConsumingSegment = true;
    assertFalse(manager.isReadyToTakeSnapshot(), "Missing Helix manager cannot certify recovery");
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    when(_tableDataManager.getHelixManager()).thenReturn(helixManager);
    when(helixManager.getHelixDataAccessor()).thenReturn(accessor);
    when(accessor.keyBuilder()).thenReturn(new PropertyKey.Builder("testCluster"));
    assertFalse(manager.isReadyToTakeSnapshot(), "Missing assignment cannot certify recovery");
    when(helixManager.getHelixDataAccessor()).thenThrow(new IllegalStateException("unavailable"));
    manager.takeSnapshot();
    assertTrue(manager.getLastSnapshotPass() == null);
    assertEquals(Files.size(manager.getWatermarkFile().toPath()), 8L);
    close(manager);
  }

  @Test
  public void testInitialLoadReadinessUsesCurrentSessionState()
      throws Exception {
    writeLegacyState(true, false);
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    when(_tableDataManager.getHelixManager()).thenReturn(helixManager);
    when(helixManager.getHelixDataAccessor()).thenReturn(accessor);
    when(helixManager.getInstanceName()).thenReturn("server");
    PropertyKey.Builder keys = new PropertyKey.Builder("testCluster");
    when(accessor.keyBuilder()).thenReturn(keys);
    IdealState assignment = new IdealState(REALTIME_TABLE_NAME);
    assignment.setPartitionState("segA", "server", "ONLINE");
    assignment.setPartitionState("segB", "server", "ONLINE");
    when(accessor.getProperty(keys.idealStates(REALTIME_TABLE_NAME))).thenReturn(assignment);
    LiveInstance instance = mock(LiveInstance.class);
    when(instance.getEphemeralOwner()).thenReturn("session");
    when(accessor.getProperty(keys.liveInstance("server"))).thenReturn(instance);
    CurrentState current = new CurrentState(REALTIME_TABLE_NAME);
    current.setState("segA", "ONLINE");
    when(accessor.getProperty(keys.currentState("server", "session", REALTIME_TABLE_NAME))).thenReturn(current);
    ConcurrentMapPartitionUpsertMetadataManager manager =
        new ConcurrentMapPartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0, newContext());
    assertFalse(manager.isReadyToTakeSnapshot());
    current.setState("segB", "ERROR");
    assertFalse(manager.isReadyToTakeSnapshot());
    current.setState("segB", "ONLINE");
    assertTrue(manager.isReadyToTakeSnapshot());
    // Initial readiness is cached; later transitions retain the normal segment lifecycle behavior.
    when(accessor.getProperty(keys.idealStates(REALTIME_TABLE_NAME))).thenReturn(null);
    assertTrue(manager.isReadyToTakeSnapshot());
    close(manager);
  }

  @Test
  public void testOffloadDuringLegacyRecovery()
      throws Exception {
    writeLegacyState(true, false);
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    ImmutableSegmentImpl segA = addSegment(manager, "segA");
    assertEquals(manager.getNumPrimaryKeys(), 3L);
    manager.removeSegment(segA);
    assertEquals(manager.getNumPrimaryKeys(), 0L, "No recovery key may reference an offloaded segment");
    close(manager, segA);
    migrateLegacyState(true, false, false);
  }

  @Test
  public void testFailedLegacyVersionPublication()
      throws Exception {
    writeLegacyState(true, false);
    File temporaryFile = new File(TEMP_DIR, V1Constants.TTL_WATERMARK_TABLE_PARTITION + "0.versioned.tmp");
    FileUtils.forceMkdir(temporaryFile);
    _allSegmentsLoaded = true;
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    ImmutableSegmentImpl segA = addSegment(manager, "segA");
    ImmutableSegmentImpl segB = addSegment(manager, "segB");
    manager.takeSnapshot();
    assertEquals(Files.size(manager.getWatermarkFile().toPath()), 8L);
    assertFalse(manager.canSkipSegmentOutOfTTL(100));
    manager.removeExpiredPrimaryKeys();
    assertEquals(manager.getNumPrimaryKeys(), 3L);
    close(manager, segA, segB);
    FileUtils.forceDelete(temporaryFile);
    migrateLegacyState(true, false, false);
    assertEquals(restart(true, false), "{2} | {0,1}");
  }

  private void migrateLegacyState(boolean preload, boolean addSegBFirst, boolean missSnapshot)
      throws Exception {
    _allSegmentsLoaded = false;
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    assertFalse(manager.canSkipSegmentOutOfTTL(100));
    ImmutableSegmentImpl segA = load("segA");
    ImmutableSegmentImpl segB = load("segB");
    List<ImmutableSegmentImpl> ordered = addSegBFirst ? List.of(segB, segA) : List.of(segA, segB);
    List<ImmutableSegmentImpl> remaining = new ArrayList<>();
    for (ImmutableSegmentImpl segment : ordered) {
      if (preload && segment.loadDocIdsFromSnapshot(V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME) != null) {
        manager.preloadSegment(segment);
        manager.removeExpiredPrimaryKeys();
      } else {
        remaining.add(segment);
      }
    }
    for (ImmutableSegmentImpl segment : remaining) {
      manager.addSegment(segment);
      manager.removeExpiredPrimaryKeys();
      manager.takeSnapshot();
      assertEquals(Files.size(manager.getWatermarkFile().toPath()), 8L);
    }
    assertEquals(validDocIds(segA) + " | " + validDocIds(segB), "{2} | {0,1}");
    assertEquals(manager.getNumPrimaryKeys(), 3L, "Keep expired ownership until recovery finishes");
    _allSegmentsLoaded = true;
    if (missSnapshot) {
      _contendedSegments.add("segA");
    }
    manager.takeSnapshot();
    assertEquals(Files.size(manager.getWatermarkFile().toPath()), 16L);
    assertEquals(persistedWatermark(manager), missSnapshot ? 130.0 : 290.0);
    manager.removeExpiredPrimaryKeys();
    assertEquals(manager.getNumPrimaryKeys(), 2L);
    assertEquals(validDocIds(segA), "{2}", "TTL removes bookkeeping, not the surviving expired row");
    close(manager, segA, segB);
    _contendedSegments.clear();
  }

  private ReentrantLock newSegmentLock(String segmentName) {
    return new ReentrantLock() {
      @Override
      public boolean tryLock() {
        return !_contendedSegments.contains(segmentName) && super.tryLock();
      }
    };
  }

  private UpsertContext newContext() {
    return new UpsertContext.Builder().setTableConfig(TABLE_CONFIG).setSchema(SCHEMA)
        .setTableDataManager(_tableDataManager).setPrimaryKeyColumns(List.of("pk"))
        .setComparisonColumns(List.of("ts")).setEnableSnapshot(true).setMetadataTTL(METADATA_TTL)
        .setTableIndexDir(TEMP_DIR).build();
  }

  private ConcurrentMapPartitionUpsertMetadataManager newManager() {
    ConcurrentMapPartitionUpsertMetadataManager manager =
        new ConcurrentMapPartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0, newContext()) {
          @Override
          protected boolean isInitialSegmentLoadComplete() {
            return _allSegmentsLoaded;
          }
        };
    manager._gotFirstConsumingSegment = true;
    return manager;
  }

  private static void buildSegment(String segmentName, int[] primaryKeys, int timestamp)
      throws Exception {
    List<GenericRow> rows = new ArrayList<>();
    for (int primaryKey : primaryKeys) {
      GenericRow row = new GenericRow();
      row.putValue("pk", primaryKey);
      row.putValue("ts", timestamp);
      rows.add(row);
    }
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(SEGMENTS_DIR.getAbsolutePath());
    config.setSegmentName(segmentName);
    config.setTableName(RAW_TABLE_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
  }

  private static ImmutableSegmentImpl load(String segmentName)
      throws Exception {
    return (ImmutableSegmentImpl) ImmutableSegmentLoader.load(new File(SEGMENTS_DIR, segmentName), ReadMode.mmap);
  }

  private static ImmutableSegmentImpl addSegment(ConcurrentMapPartitionUpsertMetadataManager manager,
      String segmentName)
      throws Exception {
    ImmutableSegmentImpl segment = load(segmentName);
    manager.addSegment(segment);
    return segment;
  }

  private static boolean hasSnapshot(String segmentName)
      throws Exception {
    ImmutableSegmentImpl segment = load(segmentName);
    try {
      return segment.loadDocIdsFromSnapshot(V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME) != null;
    } finally {
      segment.destroy();
    }
  }

  private static String loadSnapshot(String segmentName)
      throws Exception {
    ImmutableSegmentImpl segment = load(segmentName);
    try {
      return String.valueOf(segment.loadDocIdsFromSnapshot(V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME));
    } finally {
      segment.destroy();
    }
  }

  private static double persistedWatermark(ConcurrentMapPartitionUpsertMetadataManager manager) {
    double watermark = WatermarkUtils.loadWatermark(manager.getWatermarkFile(), BasePartitionUpsertMetadataManager
        .TTL_WATERMARK_NOT_SET);
    assertEquals(manager.getPersistedWatermark(), watermark, "persisted watermark in memory and on disk differ");
    return watermark;
  }

  private static void close(ConcurrentMapPartitionUpsertMetadataManager manager, ImmutableSegmentImpl... segments)
      throws Exception {
    manager.stop();
    manager.close();
    for (ImmutableSegmentImpl segment : segments) {
      segment.destroy();
    }
  }

  /// Loads the segments into a new metadata manager as a server restart does: preloads the segments with snapshot
  /// first if preload is enabled, then adds the remaining segments. Returns the valid docs as "segA | segB", where
  /// "ALL" means no validDocIds, i.e. all docs are returned by queries.
  private String restart(boolean preload, boolean addSegBFirst)
      throws Exception {
    ConcurrentMapPartitionUpsertMetadataManager manager = newManager();
    // The persisted watermark is loaded from the watermark file on restart
    persistedWatermark(manager);
    ImmutableSegmentImpl segA = load("segA");
    ImmutableSegmentImpl segB = load("segB");
    List<ImmutableSegmentImpl> segments = addSegBFirst ? List.of(segB, segA) : List.of(segA, segB);
    List<ImmutableSegmentImpl> segmentsToAdd = new ArrayList<>();
    for (ImmutableSegmentImpl segment : segments) {
      if (preload && segment.loadDocIdsFromSnapshot(V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME) != null) {
        manager.preloadSegment(segment);
      } else {
        segmentsToAdd.add(segment);
      }
    }
    for (ImmutableSegmentImpl segment : segmentsToAdd) {
      manager.addSegment(segment);
    }
    String validDocIds = validDocIds(segA) + " | " + validDocIds(segB);
    close(manager, segA, segB);
    return validDocIds;
  }

  private static String validDocIds(ImmutableSegmentImpl segment) {
    ThreadSafeMutableRoaringBitmap validDocIds = segment.getValidDocIds();
    return validDocIds != null ? validDocIds.getMutableRoaringBitmap().toString() : "ALL";
  }
}
