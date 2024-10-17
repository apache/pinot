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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.util.TestUtils;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class BasePartitionUpsertMetadataManagerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "BasePartitionUpsertMetadataManagerTest");

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(TEMP_DIR);
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.forceDelete(TEMP_DIR);
  }

  @Test
  public void testPreloadSegments()
      throws IOException {
    String realtimeTableName = "testTable_REALTIME";
    UpsertContext upsertContext = mock(UpsertContext.class);
    when(upsertContext.isSnapshotEnabled()).thenReturn(true);
    when(upsertContext.isPreloadEnabled()).thenReturn(true);
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(upsertContext.getTableDataManager()).thenReturn(tableDataManager);
    IndexLoadingConfig indexLoadingConfig = mock(IndexLoadingConfig.class);
    when(indexLoadingConfig.getTableConfig()).thenReturn(mock(TableConfig.class));

    try (DummyPartitionUpsertMetadataManager upsertMetadataManager = new DummyPartitionUpsertMetadataManager(
        realtimeTableName, 0, upsertContext)) {
      assertTrue(upsertMetadataManager.isPreloading());
      upsertMetadataManager.preloadSegments(indexLoadingConfig);
      assertFalse(upsertMetadataManager.isPreloading());
      upsertMetadataManager.stop();
    }
  }

  @Test
  public void testTakeSnapshotInOrder()
      throws IOException {
    UpsertContext upsertContext = mock(UpsertContext.class);
    when(upsertContext.isSnapshotEnabled()).thenReturn(true);
    TableDataManager tdm = mock(TableDataManager.class);
    when(upsertContext.getTableDataManager()).thenReturn(tdm);
    when(tdm.getSegmentLock(anyString())).thenReturn(new ReentrantLock());
    DummyPartitionUpsertMetadataManager upsertMetadataManager =
        new DummyPartitionUpsertMetadataManager("myTable", 0, upsertContext);

    List<String> segmentsTakenSnapshot = new ArrayList<>();

    File segDir01 = new File(TEMP_DIR, "seg01");
    ImmutableSegmentImpl seg01 = createImmutableSegment("seg01", segDir01, segmentsTakenSnapshot);
    seg01.enableUpsert(upsertMetadataManager, createValidDocIds(0, 1, 2, 3), null);
    upsertMetadataManager.addSegment(seg01);
    // seg01 has a tmp snapshot file, but no snapshot file
    FileUtils.touch(new File(segDir01, V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME + "_tmp"));

    File segDir02 = new File(TEMP_DIR, "seg02");
    ImmutableSegmentImpl seg02 = createImmutableSegment("seg02", segDir02, segmentsTakenSnapshot);
    seg02.enableUpsert(upsertMetadataManager, createValidDocIds(0, 1, 2, 3, 4, 5), null);
    upsertMetadataManager.addSegment(seg02);
    // seg02 has snapshot file, so its snapshot is taken first.
    FileUtils.touch(new File(segDir02, V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME));

    File segDir03 = new File(TEMP_DIR, "seg03");
    ImmutableSegmentImpl seg03 = createImmutableSegment("seg03", segDir03, segmentsTakenSnapshot);
    seg03.enableUpsert(upsertMetadataManager, createValidDocIds(3, 4, 7), null);
    upsertMetadataManager.addSegment(seg03);

    // The mutable segments will be skipped.
    MutableSegmentImpl seg04 = mock(MutableSegmentImpl.class);
    upsertMetadataManager.addRecord(seg04, mock(RecordInfo.class));

    upsertMetadataManager.doTakeSnapshot();
    assertEquals(segmentsTakenSnapshot.size(), 3);
    // The snapshot of seg02 was taken firstly, as it's the only segment with existing snapshot.
    assertEquals(segmentsTakenSnapshot.get(0), "seg02");
    // Set is used to track segments internally, so we can't assert the order of the other segments deterministically,
    // but all 3 segments should have taken their snapshots.
    assertTrue(segmentsTakenSnapshot.containsAll(Arrays.asList("seg01", "seg02", "seg03")));

    assertEquals(TEMP_DIR.list().length, 3);
    assertTrue(segDir01.exists());
    assertEquals(seg01.loadValidDocIdsFromSnapshot().getCardinality(), 4);
    assertTrue(segDir02.exists());
    assertEquals(seg02.loadValidDocIdsFromSnapshot().getCardinality(), 6);
    assertTrue(segDir03.exists());
    assertEquals(seg03.loadValidDocIdsFromSnapshot().getCardinality(), 3);
  }

  @Test
  public void testSkipTakeSnapshotUponRaceCondition()
      throws IOException {
    UpsertContext upsertContext = mock(UpsertContext.class);
    when(upsertContext.isSnapshotEnabled()).thenReturn(true);
    TableDataManager tdm = mock(TableDataManager.class);
    when(upsertContext.getTableDataManager()).thenReturn(tdm);
    Map<String, Lock> segmentLocks = new HashMap<>();
    segmentLocks.put("seg01", new ReentrantLock());
    segmentLocks.put("seg02", new ReentrantLock());
    segmentLocks.put("seg03", new ReentrantLock());
    segmentLocks.put("seg04", new ReentrantLock());
    when(tdm.getSegmentLock(anyString())).thenAnswer(invocation -> {
      String segmentName = invocation.getArgument(0);
      return segmentLocks.get(segmentName);
    });
    DummyPartitionUpsertMetadataManager upsertMetadataManager =
        new DummyPartitionUpsertMetadataManager("myTable", 0, upsertContext);

    List<String> segmentsTakenSnapshot = new ArrayList<>();

    File segDir01 = new File(TEMP_DIR, "seg01");
    ImmutableSegmentImpl seg01 = createImmutableSegment("seg01", segDir01, segmentsTakenSnapshot);
    seg01.enableUpsert(upsertMetadataManager, createValidDocIds(0, 1, 2, 3), null);
    upsertMetadataManager.addSegment(seg01);
    // seg01 has a tmp snapshot file, but no snapshot file
    FileUtils.touch(new File(segDir01, V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME + "_tmp"));

    File segDir02 = new File(TEMP_DIR, "seg02");
    ImmutableSegmentImpl seg02 = createImmutableSegment("seg02", segDir02, segmentsTakenSnapshot);
    seg02.enableUpsert(upsertMetadataManager, createValidDocIds(0, 1, 2, 3, 4, 5), null);
    upsertMetadataManager.addSegment(seg02);
    // seg02 has snapshot file, so its snapshot is taken first.
    FileUtils.touch(new File(segDir02, V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME));

    File segDir03 = new File(TEMP_DIR, "seg03");
    ImmutableSegmentImpl seg03 = createImmutableSegment("seg03", segDir03, segmentsTakenSnapshot);
    seg03.enableUpsert(upsertMetadataManager, createValidDocIds(3, 4, 7), null);
    upsertMetadataManager.addSegment(seg03);

    // The mutable segments will be skipped.
    MutableSegmentImpl seg04 = mock(MutableSegmentImpl.class);
    upsertMetadataManager.addRecord(seg04, mock(RecordInfo.class));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      // seg02 is the only segment with existing snapshot file, so skipping it should skip other segments w/o snapshots.
      Lock seg02Lock = segmentLocks.get("seg02");
      AtomicBoolean seg02Locked = new AtomicBoolean(false);
      ReentrantLock holderLock = new ReentrantLock();
      holderLock.lock();
      executor.submit(() -> {
        seg02Lock.lock();
        seg02Locked.set(true);
        // Block this thread a while to test if snapshots are skipped.
        holderLock.lock();
        seg02Lock.unlock();
      });
      // Make sure the bg thread has acquired the seg02Lock before testing to avoid flakiness.
      TestUtils.waitForCondition(aVoid -> seg02Locked.get(), 1000L, "Failed to acquire seg02Lock in time");
      // Since seg02 is skipped, no snapshots would be taken.
      upsertMetadataManager.doTakeSnapshot();
      assertEquals(segmentsTakenSnapshot.size(), 0);

      // Unblock the bg thread so that it releases the segmentLock.
      holderLock.unlock();
      // Acquire the segmentLock once, in case the bg thread is not running in time and causes flakiness.
      seg02Lock.lock();
      seg02Lock.unlock();
      // Now the segmentLock can be acquired for sure, and snapshots should be taken.
      upsertMetadataManager.doTakeSnapshot();
      assertEquals(segmentsTakenSnapshot.size(), 3);
      assertTrue(segDir01.exists());
      assertEquals(seg01.loadValidDocIdsFromSnapshot().getCardinality(), 4);
      assertTrue(segDir02.exists());
      assertEquals(seg02.loadValidDocIdsFromSnapshot().getCardinality(), 6);
      assertTrue(segDir03.exists());
      assertEquals(seg03.loadValidDocIdsFromSnapshot().getCardinality(), 3);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testTakeSnapshotInOrderBasedOnUpdates()
      throws IOException {
    UpsertContext upsertContext = mock(UpsertContext.class);
    when(upsertContext.isSnapshotEnabled()).thenReturn(true);
    TableDataManager tdm = mock(TableDataManager.class);
    when(upsertContext.getTableDataManager()).thenReturn(tdm);
    when(tdm.getSegmentLock(anyString())).thenReturn(new ReentrantLock());
    DummyPartitionUpsertMetadataManager upsertMetadataManager =
        new DummyPartitionUpsertMetadataManager("myTable", 0, upsertContext);

    List<String> segmentsTakenSnapshot = new ArrayList<>();

    File segDir01 = new File(TEMP_DIR, "seg01");
    ImmutableSegmentImpl seg01 = createImmutableSegment("seg01", segDir01, segmentsTakenSnapshot);
    seg01.enableUpsert(upsertMetadataManager, createValidDocIds(0, 1, 2, 3), null);
    upsertMetadataManager.addSegment(seg01);
    // seg01 has a tmp snapshot file, but no snapshot file
    FileUtils.touch(new File(segDir01, V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME + "_tmp"));

    File segDir02 = new File(TEMP_DIR, "seg02");
    ImmutableSegmentImpl seg02 = createImmutableSegment("seg02", segDir02, segmentsTakenSnapshot);
    seg02.enableUpsert(upsertMetadataManager, createValidDocIds(0, 1, 2, 3, 4, 5), null);
    upsertMetadataManager.addSegment(seg02);
    // seg02 has snapshot file, so its snapshot is taken first.
    FileUtils.touch(new File(segDir02, V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME));

    File segDir03 = new File(TEMP_DIR, "seg03");
    ImmutableSegmentImpl seg03 = createImmutableSegment("seg03", segDir03, segmentsTakenSnapshot);
    seg03.enableUpsert(upsertMetadataManager, createValidDocIds(3, 4, 7), null);
    // just track it but not mark it as updated.
    upsertMetadataManager.trackSegment(seg03);

    // The mutable segments will be skipped.
    MutableSegmentImpl seg04 = mock(MutableSegmentImpl.class);
    upsertMetadataManager.addRecord(seg04, mock(RecordInfo.class));

    upsertMetadataManager.doTakeSnapshot();
    assertEquals(segmentsTakenSnapshot.size(), 2);
    // The snapshot of seg02 was taken firstly, as it's the only segment with existing snapshot.
    assertEquals(segmentsTakenSnapshot.get(0), "seg02");
    // Set is used to track segments internally, so we can't assert the order of the other segments deterministically,
    // but all 3 segments should have taken their snapshots.
    assertTrue(segmentsTakenSnapshot.containsAll(Arrays.asList("seg01", "seg02")));

    assertEquals(TEMP_DIR.list().length, 3);
    assertTrue(segDir01.exists());
    assertEquals(seg01.loadValidDocIdsFromSnapshot().getCardinality(), 4);
    assertTrue(segDir02.exists());
    assertEquals(seg02.loadValidDocIdsFromSnapshot().getCardinality(), 6);
    assertTrue(segDir03.exists());
    assertNull(seg03.loadValidDocIdsFromSnapshot());
  }

  // Losing segment is the one that lost all comparisons with docs from other segments, thus becomes empty of valid
  // docs. Previously such segment was not tracked for snapshotting, but we should take snapshot of its empty bitmap.
  @Test
  public void testTakeSnapshotWithLosingSegment()
      throws IOException {
    UpsertContext upsertContext = mock(UpsertContext.class);
    when(upsertContext.isSnapshotEnabled()).thenReturn(true);
    TableDataManager tdm = mock(TableDataManager.class);
    when(upsertContext.getTableDataManager()).thenReturn(tdm);
    when(tdm.getSegmentLock(anyString())).thenReturn(new ReentrantLock());
    DummyPartitionUpsertMetadataManager upsertMetadataManager =
        new DummyPartitionUpsertMetadataManager("myTable", 0, upsertContext);

    List<String> segmentsTakenSnapshot = new ArrayList<>();

    File segDir01 = new File(TEMP_DIR, "seg01");
    ImmutableSegmentImpl seg01 = createImmutableSegment("seg01", segDir01, segmentsTakenSnapshot);
    seg01.enableUpsert(upsertMetadataManager, createValidDocIds(0, 1, 2, 3), null);
    // addSegment() would track seg, and mark it as updated for snapshotting.
    upsertMetadataManager.addSegment(seg01);
    // seg01 has a tmp snapshot file, but no snapshot file
    FileUtils.touch(new File(segDir01, V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME + "_tmp"));

    File segDir02 = new File(TEMP_DIR, "seg02");
    ImmutableSegmentImpl seg02 = createImmutableSegment("seg02", segDir02, segmentsTakenSnapshot);
    seg02.enableUpsert(upsertMetadataManager, createValidDocIds(0, 1, 2, 3, 4, 5), null);
    upsertMetadataManager.addSegment(seg02);
    // seg02 has snapshot file, so its snapshot is taken first.
    FileUtils.touch(new File(segDir02, V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME));

    File segDir03 = new File(TEMP_DIR, "seg03");
    ImmutableSegmentImpl seg03 = createImmutableSegment("seg03", segDir03, segmentsTakenSnapshot);
    seg03.enableUpsert(upsertMetadataManager, createValidDocIds(3, 4, 7), null);
    upsertMetadataManager.addSegment(seg03);

    // The mutable segments will be skipped.
    MutableSegmentImpl seg04 = mock(MutableSegmentImpl.class);
    upsertMetadataManager.addRecord(seg04, mock(RecordInfo.class));

    upsertMetadataManager.doTakeSnapshot();
    assertEquals(segmentsTakenSnapshot.size(), 3);
    // The snapshot of seg02 was taken firstly, as it's the only segment with existing snapshot.
    assertEquals(segmentsTakenSnapshot.get(0), "seg02");
    // Set is used to track segments internally, so we can't assert the order of the other segments deterministically,
    // but all 3 segments should have taken their snapshots.
    assertTrue(segmentsTakenSnapshot.containsAll(Arrays.asList("seg01", "seg02", "seg03")));

    assertEquals(TEMP_DIR.list().length, 3);
    assertTrue(segDir01.exists());
    assertEquals(seg01.loadValidDocIdsFromSnapshot().getCardinality(), 4);
    assertTrue(segDir02.exists());
    assertEquals(seg02.loadValidDocIdsFromSnapshot().getCardinality(), 6);
    assertTrue(segDir03.exists());
    assertEquals(seg03.loadValidDocIdsFromSnapshot().getCardinality(), 3);
  }

  @Test
  public void testConsistencyModeSync()
      throws Exception {
    UpsertContext upsertContext = mock(UpsertContext.class);
    when(upsertContext.getConsistencyMode()).thenReturn(UpsertConfig.ConsistencyMode.SYNC);
    DummyPartitionUpsertMetadataManager upsertMetadataManager =
        new DummyPartitionUpsertMetadataManager("myTable", 0, upsertContext);

    CountDownLatch latch = new CountDownLatch(1);
    Map<IndexSegment, ThreadSafeMutableRoaringBitmap> segmentQueryableDocIdsMap = new HashMap<>();
    IndexSegment seg01 = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap validDocIds01 = createThreadSafeMutableRoaringBitmap(10);
    AtomicBoolean called = new AtomicBoolean(false);
    when(seg01.getValidDocIds()).then(invocationOnMock -> {
      called.set(true);
      latch.await();
      return validDocIds01;
    });
    upsertMetadataManager.trackSegmentForUpsertView(seg01);
    segmentQueryableDocIdsMap.put(seg01, validDocIds01);

    IndexSegment seg02 = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap validDocIds02 = createThreadSafeMutableRoaringBitmap(11);
    when(seg02.getValidDocIds()).thenReturn(validDocIds02);
    upsertMetadataManager.trackSegmentForUpsertView(seg02);
    segmentQueryableDocIdsMap.put(seg02, validDocIds02);

    IndexSegment seg03 = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap validDocIds03 = createThreadSafeMutableRoaringBitmap(12);
    when(seg03.getValidDocIds()).thenReturn(validDocIds03);
    upsertMetadataManager.trackSegmentForUpsertView(seg03);
    segmentQueryableDocIdsMap.put(seg03, validDocIds03);

    List<SegmentContext> segmentContexts = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      // This thread does replaceDocId and takes WLock first.
      executor.submit(() -> {
        RecordInfo recordInfo = new RecordInfo(null, 5, null, false);
        upsertMetadataManager.replaceDocId(seg03, validDocIds03, null, seg01, 0, 12, recordInfo);
      });
      // This thread gets segment contexts, but it's blocked until the first thread finishes replaceDocId.
      long startMs = System.currentTimeMillis();
      Future<Long> future = executor.submit(() -> {
        // Check called flag to let the first thread do replaceDocId, thus get WLock, first.
        while (!called.get()) {
          Thread.sleep(10);
        }
        segmentQueryableDocIdsMap.forEach((k, v) -> segmentContexts.add(new SegmentContext(k)));
        upsertMetadataManager.getUpsertViewManager().setSegmentContexts(segmentContexts, new HashMap<>());
        return System.currentTimeMillis() - startMs;
      });
      // The first thread can only finish after the latch is counted down after 2s.
      // So the 2nd thread getting segment contexts will be blocked for 2s+.
      Thread.sleep(2000);
      latch.countDown();
      long duration = future.get(3, TimeUnit.SECONDS);
      assertTrue(duration >= 2000, duration + " was less than expected");
    } finally {
      executor.shutdownNow();
    }
    assertEquals(segmentContexts.size(), 3);
    for (SegmentContext sc : segmentContexts) {
      ThreadSafeMutableRoaringBitmap validDocIds = segmentQueryableDocIdsMap.get(sc.getIndexSegment());
      assertNotNull(validDocIds);
      // SegmentContext holds a clone of the original queryableDocIds held by the segment object.
      assertNotSame(sc.getQueryableDocIdsSnapshot(), validDocIds.getMutableRoaringBitmap());
      assertEquals(sc.getQueryableDocIdsSnapshot(), validDocIds.getMutableRoaringBitmap());
      // docId=0 in seg01 got invalidated.
      if (sc.getIndexSegment() == seg01) {
        assertFalse(sc.getQueryableDocIdsSnapshot().contains(0));
      }
      // docId=12 in seg03 was newly added.
      if (sc.getIndexSegment() == seg03) {
        assertTrue(sc.getQueryableDocIdsSnapshot().contains(12));
      }
    }
  }

  @Test
  public void testConsistencyModeSnapshot()
      throws Exception {
    UpsertContext upsertContext = mock(UpsertContext.class);
    when(upsertContext.getConsistencyMode()).thenReturn(UpsertConfig.ConsistencyMode.SNAPSHOT);
    when(upsertContext.getUpsertViewRefreshIntervalMs()).thenReturn(5L);
    DummyPartitionUpsertMetadataManager upsertMetadataManager =
        new DummyPartitionUpsertMetadataManager("myTable", 0, upsertContext);

    CountDownLatch latch = new CountDownLatch(1);
    Map<IndexSegment, ThreadSafeMutableRoaringBitmap> segmentQueryableDocIdsMap = new HashMap<>();
    IndexSegment seg01 = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap validDocIds01 = createThreadSafeMutableRoaringBitmap(10);
    upsertMetadataManager.trackSegmentForUpsertView(seg01);
    segmentQueryableDocIdsMap.put(seg01, validDocIds01);

    IndexSegment seg02 = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap validDocIds02 = createThreadSafeMutableRoaringBitmap(11);
    when(seg02.getValidDocIds()).thenReturn(validDocIds02);
    upsertMetadataManager.trackSegmentForUpsertView(seg02);
    segmentQueryableDocIdsMap.put(seg02, validDocIds02);

    IndexSegment seg03 = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap validDocIds03 = createThreadSafeMutableRoaringBitmap(12);
    when(seg03.getValidDocIds()).thenReturn(validDocIds03);
    upsertMetadataManager.trackSegmentForUpsertView(seg03);
    segmentQueryableDocIdsMap.put(seg03, validDocIds03);

    List<SegmentContext> segmentContexts = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(2);

    // Set up the awaiting logic here to not block the trackSegmentForUpsertView methods above, as they refresh the
    // upsert view.
    AtomicBoolean called = new AtomicBoolean(false);
    when(seg01.getValidDocIds()).then(invocationOnMock -> {
      called.set(true);
      latch.await();
      return validDocIds01;
    });
    try {
      // This thread does replaceDocId and takes WLock first, and it'll refresh the upsert view Sleep a bit here to
      // make the upsert view stale before doing the replaceDocId, to force it to refresh the upsert view.
      Thread.sleep(10);
      executor.submit(() -> {
        RecordInfo recordInfo = new RecordInfo(null, 5, null, false);
        upsertMetadataManager.replaceDocId(seg03, validDocIds03, null, seg01, 0, 12, recordInfo);
      });
      // This thread gets segment contexts, but it's blocked until the first thread finishes replaceDocId.
      long startMs = System.currentTimeMillis();
      Future<Long> future = executor.submit(() -> {
        // Check called flag to let the first thread do replaceDocId, thus get WLock, first.
        while (!called.get()) {
          Thread.sleep(10);
        }
        segmentQueryableDocIdsMap.forEach((k, v) -> segmentContexts.add(new SegmentContext(k)));
        // This thread reuses the upsert view refreshed by the first thread above.
        upsertMetadataManager.getUpsertViewManager().setSegmentContexts(segmentContexts, new HashMap<>());
        return System.currentTimeMillis() - startMs;
      });
      // The first thread can only finish after the latch is counted down after 2s.
      // So the 2nd thread getting segment contexts will be blocked for 2s+.
      Thread.sleep(2000);
      latch.countDown();
      long duration = future.get(3, TimeUnit.SECONDS);
      assertTrue(duration >= 2000, duration + " was less than expected");
    } finally {
      executor.shutdownNow();
    }
    assertEquals(segmentContexts.size(), 3);
    assertEquals(upsertMetadataManager.getUpsertViewManager().getSegmentQueryableDocIdsMap().size(), 3);
    assertTrue(upsertMetadataManager.getUpsertViewManager().getUpdatedSegmentsSinceLastRefresh().isEmpty());

    // Get the upsert view again, and the existing bitmap objects should be set in segment contexts.
    // The segmentContexts initialized above holds the same bitmaps objects as from the upsert view.
    List<SegmentContext> reuseSegmentContexts = new ArrayList<>();
    segmentQueryableDocIdsMap.forEach((k, v) -> reuseSegmentContexts.add(new SegmentContext(k)));
    upsertMetadataManager.getUpsertViewManager().setSegmentContexts(reuseSegmentContexts, new HashMap<>());
    for (SegmentContext reuseSC : reuseSegmentContexts) {
      for (SegmentContext sc : segmentContexts) {
        if (reuseSC.getIndexSegment() == sc.getIndexSegment()) {
          assertSame(reuseSC.getQueryableDocIdsSnapshot(), sc.getQueryableDocIdsSnapshot());
        }
      }
      ThreadSafeMutableRoaringBitmap validDocIds = segmentQueryableDocIdsMap.get(reuseSC.getIndexSegment());
      assertNotNull(validDocIds);
      // The upsert view holds a clone of the original queryableDocIds held by the segment object.
      assertNotSame(reuseSC.getQueryableDocIdsSnapshot(), validDocIds.getMutableRoaringBitmap());
      assertEquals(reuseSC.getQueryableDocIdsSnapshot(), validDocIds.getMutableRoaringBitmap());
      // docId=0 in seg01 got invalidated.
      if (reuseSC.getIndexSegment() == seg01) {
        assertFalse(reuseSC.getQueryableDocIdsSnapshot().contains(0));
      }
      // docId=12 in seg03 was newly added.
      if (reuseSC.getIndexSegment() == seg03) {
        assertTrue(reuseSC.getQueryableDocIdsSnapshot().contains(12));
      }
    }

    // Force refresh the upsert view when getting it, so different bitmap objects should be set in segment contexts.
    upsertMetadataManager.getUpsertViewManager().getUpdatedSegmentsSinceLastRefresh()
        .addAll(Arrays.asList(seg01, seg02, seg03));
    List<SegmentContext> refreshSegmentContexts = new ArrayList<>();
    segmentQueryableDocIdsMap.forEach((k, v) -> refreshSegmentContexts.add(new SegmentContext(k)));
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("upsertViewFreshnessMs", "0");
    upsertMetadataManager.getUpsertViewManager().setSegmentContexts(refreshSegmentContexts, queryOptions);
    for (SegmentContext refreshSC : refreshSegmentContexts) {
      for (SegmentContext sc : segmentContexts) {
        if (refreshSC.getIndexSegment() == sc.getIndexSegment()) {
          assertNotSame(refreshSC.getQueryableDocIdsSnapshot(), sc.getQueryableDocIdsSnapshot());
        }
      }
      ThreadSafeMutableRoaringBitmap validDocIds = segmentQueryableDocIdsMap.get(refreshSC.getIndexSegment());
      assertNotNull(validDocIds);
      // The upsert view holds a clone of the original queryableDocIds held by the segment object.
      assertNotSame(refreshSC.getQueryableDocIdsSnapshot(), validDocIds.getMutableRoaringBitmap());
      assertEquals(refreshSC.getQueryableDocIdsSnapshot(), validDocIds.getMutableRoaringBitmap());
      // docId=0 in seg01 got invalidated.
      if (refreshSC.getIndexSegment() == seg01) {
        assertFalse(refreshSC.getQueryableDocIdsSnapshot().contains(0));
      }
      // docId=12 in seg03 was newly added.
      if (refreshSC.getIndexSegment() == seg03) {
        assertTrue(refreshSC.getQueryableDocIdsSnapshot().contains(12));
      }
    }
  }

  @Test
  public void testConsistencyModeSnapshotWithUntrackedSegments()
      throws Exception {
    UpsertContext upsertContext = mock(UpsertContext.class);
    when(upsertContext.getConsistencyMode()).thenReturn(UpsertConfig.ConsistencyMode.SNAPSHOT);
    when(upsertContext.getUpsertViewRefreshIntervalMs()).thenReturn(3000L);
    DummyPartitionUpsertMetadataManager upsertMetadataManager =
        new DummyPartitionUpsertMetadataManager("myTable", 0, upsertContext);

    Map<IndexSegment, ThreadSafeMutableRoaringBitmap> segmentQueryableDocIdsMap = new HashMap<>();
    IndexSegment seg01 = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap validDocIds01 = createThreadSafeMutableRoaringBitmap(10);
    when(seg01.getValidDocIds()).thenReturn(validDocIds01);
    upsertMetadataManager.trackSegmentForUpsertView(seg01);
    segmentQueryableDocIdsMap.put(seg01, validDocIds01);

    IndexSegment seg02 = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap validDocIds02 = createThreadSafeMutableRoaringBitmap(11);
    when(seg02.getValidDocIds()).thenReturn(validDocIds02);
    upsertMetadataManager.trackSegmentForUpsertView(seg02);
    segmentQueryableDocIdsMap.put(seg02, validDocIds02);

    IndexSegment seg03 = mock(IndexSegment.class);
    ThreadSafeMutableRoaringBitmap validDocIds03 = createThreadSafeMutableRoaringBitmap(12);
    when(seg03.getValidDocIds()).thenReturn(validDocIds03);
    upsertMetadataManager.trackSegmentForUpsertView(seg03);
    segmentQueryableDocIdsMap.put(seg03, validDocIds03);

    RecordInfo recordInfo = new RecordInfo(null, 5, null, false);
    upsertMetadataManager.replaceDocId(seg03, validDocIds03, null, seg01, 0, 12, recordInfo);

    List<SegmentContext> segmentContexts = new ArrayList<>();
    segmentQueryableDocIdsMap.forEach((k, v) -> segmentContexts.add(new SegmentContext(k)));
    upsertMetadataManager.getUpsertViewManager().setSegmentContexts(segmentContexts, new HashMap<>());
    assertEquals(segmentContexts.size(), 3);
    assertEquals(upsertMetadataManager.getUpsertViewManager().getSegmentQueryableDocIdsMap().size(), 3);

    // We can force to refresh the upsert view by clearing up the current upsert view, even though there are no updated
    // segments tracked in _updatedSegmentsSinceLastRefresh.
    assertEquals(upsertMetadataManager.getUpsertViewManager().getUpdatedSegmentsSinceLastRefresh().size(), 2);
    assertTrue(upsertMetadataManager.getUpsertViewManager().getUpdatedSegmentsSinceLastRefresh().contains(seg01));
    assertTrue(upsertMetadataManager.getUpsertViewManager().getUpdatedSegmentsSinceLastRefresh().contains(seg03));
    upsertMetadataManager.getUpsertViewManager().getSegmentQueryableDocIdsMap().clear();

    List<SegmentContext> refreshSegmentContexts = new ArrayList<>();
    segmentQueryableDocIdsMap.forEach((k, v) -> refreshSegmentContexts.add(new SegmentContext(k)));
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("upsertViewFreshnessMs", "0");
    upsertMetadataManager.getUpsertViewManager().setSegmentContexts(refreshSegmentContexts, queryOptions);
    for (SegmentContext refreshSC : refreshSegmentContexts) {
      for (SegmentContext sc : segmentContexts) {
        if (refreshSC.getIndexSegment() == sc.getIndexSegment()) {
          assertNotSame(refreshSC.getQueryableDocIdsSnapshot(), sc.getQueryableDocIdsSnapshot());
        }
      }
      ThreadSafeMutableRoaringBitmap validDocIds = segmentQueryableDocIdsMap.get(refreshSC.getIndexSegment());
      assertNotNull(validDocIds);
      // The upsert view holds a clone of the original queryableDocIds held by the segment object.
      assertNotSame(refreshSC.getQueryableDocIdsSnapshot(), validDocIds.getMutableRoaringBitmap());
      assertEquals(refreshSC.getQueryableDocIdsSnapshot(), validDocIds.getMutableRoaringBitmap());
      assertNotNull(refreshSC.getQueryableDocIdsSnapshot());
      // docId=0 in seg01 got invalidated.
      if (refreshSC.getIndexSegment() == seg01) {
        assertFalse(refreshSC.getQueryableDocIdsSnapshot().contains(0));
      }
      // docId=12 in seg03 was newly added.
      if (refreshSC.getIndexSegment() == seg03) {
        assertTrue(refreshSC.getQueryableDocIdsSnapshot().contains(12));
      }
    }
  }

  @Test
  public void testTrackNewlyAddedSegments() {
    UpsertContext upsertContext = mock(UpsertContext.class);
    when(upsertContext.getNewSegmentTrackingTimeMs()).thenReturn(0L);
    DummyPartitionUpsertMetadataManager upsertMetadataManager =
        new DummyPartitionUpsertMetadataManager("myTable", 0, upsertContext);

    upsertMetadataManager.trackNewlyAddedSegment("seg1");
    upsertMetadataManager.trackNewlyAddedSegment("seg2");
    assertEquals(upsertMetadataManager.getNewlyAddedSegments().size(), 0);

    when(upsertContext.getNewSegmentTrackingTimeMs()).thenReturn(100L);
    upsertMetadataManager = new DummyPartitionUpsertMetadataManager("myTable", 0, upsertContext);
    upsertMetadataManager.trackNewlyAddedSegment("seg1");
    upsertMetadataManager.trackNewlyAddedSegment("seg2");
    assertEquals(upsertMetadataManager.getNewlyAddedSegments().size(), 2);
    // There is 100ms delay before removal of stale segments.
    assertEquals(upsertMetadataManager.getNewlyAddedSegments().size(), 2);
    DummyPartitionUpsertMetadataManager finalUpsertMetadataManager = upsertMetadataManager;
    TestUtils.waitForCondition(aVoid -> finalUpsertMetadataManager.getNewlyAddedSegments().isEmpty(), 300L,
        "Failed to remove stale segments");
  }

  @Test
  public void testResolveComparisonTies() {
    // Build a record info list for testing
    int[] primaryKeys = new int[]{0, 1, 2, 0, 1, 0};
    int[] timestamps = new int[]{0, 0, 0, 0, 0, 0};
    int numRecords = primaryKeys.length;
    List<RecordInfo> recordInfoList = new ArrayList<>();
    for (int docId = 0; docId < numRecords; docId++) {
      recordInfoList.add(new RecordInfo(makePrimaryKey(primaryKeys[docId]), docId, timestamps[docId], false));
    }
    // Resolve comparison ties
    Iterator<RecordInfo> deDuplicatedRecords =
        BasePartitionUpsertMetadataManager.resolveComparisonTies(recordInfoList.iterator(), HashFunction.NONE);
    // Ensure we have only 1 record for each unique primary key
    Map<PrimaryKey, RecordInfo> recordsByPrimaryKeys = new HashMap<>();
    while (deDuplicatedRecords.hasNext()) {
      RecordInfo recordInfo = deDuplicatedRecords.next();
      assertFalse(recordsByPrimaryKeys.containsKey(recordInfo.getPrimaryKey()));
      recordsByPrimaryKeys.put(recordInfo.getPrimaryKey(), recordInfo);
    }
    assertEquals(recordsByPrimaryKeys.size(), 3);
    // Ensure that to resolve ties, we pick the last docId
    assertEquals(recordsByPrimaryKeys.get(makePrimaryKey(0)).getDocId(), 5);
    assertEquals(recordsByPrimaryKeys.get(makePrimaryKey(1)).getDocId(), 4);
    assertEquals(recordsByPrimaryKeys.get(makePrimaryKey(2)).getDocId(), 2);
  }

  private static ThreadSafeMutableRoaringBitmap createThreadSafeMutableRoaringBitmap(int docCnt) {
    ThreadSafeMutableRoaringBitmap bitmap = new ThreadSafeMutableRoaringBitmap();
    for (int i = 0; i < docCnt; i++) {
      bitmap.add(i);
    }
    return bitmap;
  }

  private static ThreadSafeMutableRoaringBitmap createValidDocIds(int... docIds) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    return new ThreadSafeMutableRoaringBitmap(bitmap);
  }

  private static ImmutableSegmentImpl createImmutableSegment(String segName, File segDir,
      List<String> segmentsTakenSnapshot)
      throws IOException {
    FileUtils.forceMkdir(segDir);
    SegmentMetadataImpl meta = mock(SegmentMetadataImpl.class);
    when(meta.getName()).thenReturn(segName);
    when(meta.getIndexDir()).thenReturn(segDir);
    return new ImmutableSegmentImpl(mock(SegmentDirectory.class), meta, new HashMap<>(), null) {
      public void persistValidDocIdsSnapshot() {
        segmentsTakenSnapshot.add(segName);
        super.persistValidDocIdsSnapshot();
      }
    };
  }

  private static PrimaryKey makePrimaryKey(int value) {
    return new PrimaryKey(new Object[]{value});
  }

  private static class DummyPartitionUpsertMetadataManager extends BasePartitionUpsertMetadataManager {

    protected DummyPartitionUpsertMetadataManager(String tableNameWithType, int partitionId, UpsertContext context) {
      super(tableNameWithType, partitionId, context);
    }

    public void trackSegment(IndexSegment seg) {
      _trackedSegments.add(seg);
    }

    @Override
    protected long getNumPrimaryKeys() {
      return 0;
    }

    @Override
    protected void doAddOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
        @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator,
        @Nullable IndexSegment oldSegment, @Nullable MutableRoaringBitmap validDocIdsForOldSegment) {
    }

    @Override
    protected boolean doAddRecord(MutableSegment segment, RecordInfo recordInfo) {
      return false;
    }

    @Override
    protected void removeSegment(IndexSegment segment, MutableRoaringBitmap validDocIds) {
    }

    @Override
    protected GenericRow doUpdateRecord(GenericRow record, RecordInfo recordInfo) {
      return null;
    }

    @Override
    protected void doRemoveExpiredPrimaryKeys() {
    }
  }
}
