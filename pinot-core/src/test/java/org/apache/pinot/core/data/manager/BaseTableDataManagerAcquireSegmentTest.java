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

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.data.manager.TableDataManagerParams;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class BaseTableDataManagerAcquireSegmentTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String SEGMENT_PREFIX = "segment";
  private static final int DELETED_SEGMENTS_CACHE_SIZE = 100;
  private static final int DELETED_SEGMENTS_TTL_MINUTES = 2;

  // Set once for the suite
  private File _tmpDir;
  private Random _random;

  // Set once for every test
  private volatile int _nDestroys;
  private volatile boolean _closing;
  private Set<ImmutableSegment> _allSegments = new HashSet<>();
  private Set<SegmentDataManager> _accessedSegManagers =
      Collections.newSetFromMap(new ConcurrentHashMap<SegmentDataManager, Boolean>());
  private Set<SegmentDataManager> _allSegManagers =
      Collections.newSetFromMap(new ConcurrentHashMap<SegmentDataManager, Boolean>());
  private AtomicInteger _numQueries = new AtomicInteger(0);
  private Map<String, ImmutableSegmentDataManager> _internalSegMap;
  private Throwable _exception;
  private Thread _masterThread;
  // Segment numbers in place.
  // When we add a segment, we add hi+1, and bump _hi.
  // When we remove a segment, we remove _lo and bump _lo
  // When we replace a segment, we pick a number between _hi and _lo (inclusive)
  private volatile int _lo;
  private volatile int _hi;

  @BeforeSuite
  public void setUp()
      throws Exception {
    _tmpDir = new File(FileUtils.getTempDirectory(), "OfflineTableDataManagerTest");
    TestUtils.ensureDirectoriesExistAndEmpty(_tmpDir);
    _tmpDir.deleteOnExit();

    long seed = System.currentTimeMillis();
    _random = new Random(seed);
    System.out.printf("Record random seed: %d to reproduce test results upon failure\n", seed);
  }

  @AfterSuite
  public void tearDown() {
    if (_tmpDir != null) {
      org.apache.commons.io.FileUtils.deleteQuietly(_tmpDir);
    }
  }

  @BeforeMethod
  public void beforeMethod() {
    _nDestroys = 0;
    _closing = false;
    _allSegments.clear();
    _accessedSegManagers.clear();
    _allSegManagers.clear();
    _numQueries.set(0);
    _exception = null;
    _masterThread = null;
  }

  private TableDataManager makeTestableManager()
      throws Exception {
    TableDataManager tableDataManager = new OfflineTableDataManager();
    TableDataManagerConfig config;
    {
      config = mock(TableDataManagerConfig.class);
      when(config.getTableName()).thenReturn(OFFLINE_TABLE_NAME);
      when(config.getDataDir()).thenReturn(_tmpDir.getAbsolutePath());
      when(config.getAuthConfig()).thenReturn(new MapConfiguration(new HashMap<>()));
      when(config.getTableDeletedSegmentsCacheSize()).thenReturn(DELETED_SEGMENTS_CACHE_SIZE);
      when(config.getTableDeletedSegmentsCacheTtlMinutes()).thenReturn(DELETED_SEGMENTS_TTL_MINUTES);
    }
    tableDataManager.init(config, "dummyInstance", mock(ZkHelixPropertyStore.class),
        new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), mock(HelixManager.class), null, null,
        new TableDataManagerParams(0, false, -1));
    tableDataManager.start();
    Field segsMapField = BaseTableDataManager.class.getDeclaredField("_segmentDataManagerMap");
    segsMapField.setAccessible(true);
    _internalSegMap = (Map<String, ImmutableSegmentDataManager>) segsMapField.get(tableDataManager);
    return tableDataManager;
  }

  private ImmutableSegment makeImmutableSegment(String segmentName, int totalDocs) {
    ImmutableSegment immutableSegment = mock(ImmutableSegment.class);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(immutableSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(immutableSegment.getSegmentName()).thenReturn(segmentName);
    when(immutableSegment.getSegmentMetadata().getTotalDocs()).thenReturn(totalDocs);
    doAnswer(invocation -> {
      _nDestroys++;
      return null;
    }).when(immutableSegment).destroy();
    _allSegments.add(immutableSegment);
    return immutableSegment;
  }

  @Test
  public void basicTest()
      throws Exception {
    TableDataManager tableDataManager = makeTestableManager();
    Assert.assertEquals(tableDataManager.getNumSegments(), 0);
    final String segmentName = "TestSegment";
    final int totalDocs = 23456;
    // Add the segment, get it for use, remove the segment, and then return it.
    // Make sure that the segment is not destroyed before return.
    ImmutableSegment immutableSegment = makeImmutableSegment(segmentName, totalDocs);
    tableDataManager.addSegment(immutableSegment);
    Assert.assertEquals(tableDataManager.getNumSegments(), 1);
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    Assert.assertEquals(segmentDataManager.getReferenceCount(), 2);
    tableDataManager.removeSegment(segmentName);
    Assert.assertEquals(tableDataManager.getNumSegments(), 0);
    Assert.assertEquals(segmentDataManager.getReferenceCount(), 1);
    Assert.assertEquals(_nDestroys, 0);
    tableDataManager.releaseSegment(segmentDataManager);
    Assert.assertEquals(segmentDataManager.getReferenceCount(), 0);
    Assert.assertEquals(_nDestroys, 1);

    // Now the segment should not be available for use.Also, returning a null reader is fine
    segmentDataManager = tableDataManager.acquireSegment(segmentName);
    Assert.assertNull(segmentDataManager);
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    Assert.assertEquals(segmentDataManagers.size(), 0);

    // If a caller tries to acquire the deleted segment using acquireSegments, it will be returned in
    // notAcquiredSegments. The isSegmentDeletedRecently method should return true.
    List<String> notAcquiredSegments = new ArrayList<>();
    tableDataManager.acquireSegments(ImmutableList.of(segmentName), notAcquiredSegments);
    Assert.assertEquals(notAcquiredSegments.size(), 1);
    Assert.assertTrue(tableDataManager.isSegmentDeletedRecently(segmentName));

    // Adding and removing the segment again is fine. After adding the segment back, isSegmentDeletedRecently should
    // return false.
    tableDataManager.addSegment(immutableSegment);
    Assert.assertFalse(tableDataManager.isSegmentDeletedRecently(segmentName));
    tableDataManager.removeSegment(segmentName);

    // Removing the segment again is fine.
    tableDataManager.removeSegment(segmentName);
    Assert.assertEquals(tableDataManager.getNumSegments(), 0);

    // Add a new segment and remove it in order this time.
    final String anotherSeg = "AnotherSegment";
    ImmutableSegment ix1 = makeImmutableSegment(anotherSeg, totalDocs);
    tableDataManager.addSegment(ix1);
    Assert.assertEquals(tableDataManager.getNumSegments(), 1);
    SegmentDataManager sdm1 = tableDataManager.acquireSegment(anotherSeg);
    Assert.assertNotNull(sdm1);
    Assert.assertEquals(sdm1.getReferenceCount(), 2);
    // acquire all segments
    List<SegmentDataManager> segmentDataManagersList = tableDataManager.acquireAllSegments();
    Assert.assertEquals(segmentDataManagersList.size(), 1);
    Assert.assertEquals(sdm1.getReferenceCount(), 3);
    for (SegmentDataManager dataManager : segmentDataManagersList) {
      tableDataManager.releaseSegment(dataManager);
    }
    // count is back to original
    Assert.assertEquals(sdm1.getReferenceCount(), 2);
    tableDataManager.releaseSegment(sdm1);
    Assert.assertEquals(sdm1.getReferenceCount(), 1);
    // Now replace the segment with another one.
    ImmutableSegment ix2 = makeImmutableSegment(anotherSeg, totalDocs + 1);
    tableDataManager.addSegment(ix2);
    Assert.assertEquals(tableDataManager.getNumSegments(), 1);
    // Now the previous one should have been destroyed, and
    Assert.assertEquals(sdm1.getReferenceCount(), 0);
    verify(ix1, times(1)).destroy();
    // Delete ix2 without accessing it.
    SegmentDataManager sdm2 = _internalSegMap.get(anotherSeg);
    Assert.assertEquals(sdm2.getReferenceCount(), 1);
    tableDataManager.removeSegment(anotherSeg);
    Assert.assertEquals(tableDataManager.getNumSegments(), 0);
    Assert.assertEquals(sdm2.getReferenceCount(), 0);
    verify(ix2, times(1)).destroy();
    tableDataManager.shutDown();
  }

  /*
   * These tests simulate the access of segments via OfflineTableDataManager.
   *
   * It creates 31 segments (0..30) to start with and adds them to the tableDataManager (hi = 30, lo = 0)
   * It spawns 10 "query" threads, and one "helix" thread.
   *
   * The query threads pick up a random of 70% the segments and 'get' them, wait a random period of time (5 to 80ms)
   * and then 'release' the segments back, and does this in a continuous loop.
   *
   * The helix thread decides to do one of the following:
   * - Add a segment (hi+1), and bumps hi by 1 (does this 20% of the time)
   * - Remove a segment (lo) and bumps up lo by 1 (does this 20% of the time)
   * - Replaces a segment (a randomm one between (lo,hi), 60% of the time)
   * and then waits for a random of 50-300ms before attempting one of the ops again.
   */

  @Test
  public void testReplace()
      throws Exception {
    _lo = 0;
    _hi = 30;   // Total number of segments we have in the server.
    final int numQueryThreads = 10;
    final int runTimeSec = 20;
    // With the current parameters, 3k ops take about 15 seconds, create about 90 segments and drop about half of them
    // Running with coverage, it provides complete coverage of the (relevant) lines in OfflineTableDataManager

    TableDataManager tableDataManager = makeTestableManager();

    for (int i = _lo; i <= _hi; i++) {
      final String segName = SEGMENT_PREFIX + i;
      tableDataManager.addSegment(makeImmutableSegment(segName, _random.nextInt()));
      _allSegManagers.add(_internalSegMap.get(segName));
    }

    runStorageServer(numQueryThreads, runTimeSec, tableDataManager);  // replaces segments while online
    tableDataManager.shutDown();
  }

  private void runStorageServer(int numQueryThreads, int runTimeSec, TableDataManager tableDataManager)
      throws Exception {
    // Start 1 helix worker thread and as many query threads as configured.
    List<Thread> queryThreads = new ArrayList<>(numQueryThreads);
    for (int i = 0; i < numQueryThreads; i++) {
      BaseTableDataManagerAcquireSegmentTest.TestSegmentUser segUser =
          new BaseTableDataManagerAcquireSegmentTest.TestSegmentUser(tableDataManager);
      Thread segUserThread = new Thread(segUser);
      queryThreads.add(segUserThread);
      segUserThread.start();
    }

    BaseTableDataManagerAcquireSegmentTest.TestHelixWorker helixWorker =
        new BaseTableDataManagerAcquireSegmentTest.TestHelixWorker(tableDataManager);
    Thread helixWorkerThread = new Thread(helixWorker);
    helixWorkerThread.start();
    _masterThread = Thread.currentThread();

    try {
      Thread.sleep(runTimeSec * 1000);
    } catch (InterruptedException e) {
    }
    _closing = true;

    helixWorkerThread.join();
    for (Thread t : queryThreads) {
      t.join();
    }

    if (_exception != null) {
      Assert.fail("One of the threads failed", _exception);
    }

    // tableDataManager should be quiescent now.

    // All segments we ever created must have a corresponding segment manager.
    Assert.assertEquals(_allSegManagers.size(), _allSegments.size());

    final int nSegsAcccessed = _accessedSegManagers.size();
    for (SegmentDataManager segmentDataManager : _internalSegMap.values()) {
      Assert.assertEquals(segmentDataManager.getReferenceCount(), 1);
      // We should never have called destroy on these segments. Remove it from the list of accessed segments.
      verify(segmentDataManager.getSegment(), never()).destroy();
      _allSegManagers.remove(segmentDataManager);
      _accessedSegManagers.remove(segmentDataManager);
    }

    // For the remaining segments in accessed list, destroy must have been called exactly once.
    for (SegmentDataManager segmentDataManager : _allSegManagers) {
      verify(segmentDataManager.getSegment(), times(1)).destroy();
      // Also their count should be 0
      Assert.assertEquals(segmentDataManager.getReferenceCount(), 0);
    }

    // The number of segments we accessed must be <= total segments created.
    Assert.assertTrue(nSegsAcccessed <= _allSegments.size(),
        "Accessed=" + nSegsAcccessed + ",created=" + _allSegments.size());
    // The number of segments we have seen and that are not there anymore, must be <= number destroyed.
    Assert.assertTrue(_accessedSegManagers.size() <= _nDestroys,
        "SeenButUnavailableNow=" + _accessedSegManagers.size() + ",Destroys=" + _nDestroys);

    // The current number of segments must be the as expected (hi-lo+1)
    Assert.assertEquals(_internalSegMap.size(), _hi - _lo + 1);
  }

  private class TestSegmentUser implements Runnable {
    private static final double ACQUIRE_ALL_PROBABILITY = 0.20;
    private final int _minUseTimeMs = 5;
    private final int _maxUseTimeMs = 80;
    private final int _nSegsPercent = 70; // We use 70% of the segments for any query.
    private final TableDataManager _tableDataManager;

    private TestSegmentUser(TableDataManager tableDataManager) {
      _tableDataManager = tableDataManager;
    }

    @Override
    public void run() {
      while (!_closing) {
        try {
          List<SegmentDataManager> segmentDataManagers = null;
          double probability = _random.nextDouble();
          if (probability <= ACQUIRE_ALL_PROBABILITY) {
            segmentDataManagers = _tableDataManager.acquireAllSegments();
          } else {
            Set<Integer> segmentIds = pickSegments();
            List<String> segmentList = new ArrayList<>(segmentIds.size());
            for (Integer segmentId : segmentIds) {
              segmentList.add(SEGMENT_PREFIX + segmentId);
            }
            segmentDataManagers = _tableDataManager.acquireSegments(segmentList, new ArrayList<>());
          }
          // Some of them may be rejected, but that is OK.

          // Keep track of all segment data managers we ever accessed.
          for (SegmentDataManager segmentDataManager : segmentDataManagers) {
            _accessedSegManagers.add(segmentDataManager);
          }
          // To simulate real use case, may be we can add a small percent that is returned right away after pruning?
          try {
            int sleepTime = _random.nextInt(_maxUseTimeMs - _minUseTimeMs + 1) + _minUseTimeMs;
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            _closing = true;
          }
          for (SegmentDataManager segmentDataManager : segmentDataManagers) {
            _tableDataManager.releaseSegment(segmentDataManager);
          }
        } catch (Throwable t) {
          _masterThread.interrupt();
          _exception = t;
        }
      }
    }

    private Set<Integer> pickSegments() {
      int hi = _hi;
      int lo = _lo;
      int totalSegs = hi - lo + 1;
      Set<Integer> segmentIds = new HashSet<>(totalSegs);
      final int nSegments = totalSegs * _nSegsPercent / 100;
      while (segmentIds.size() != nSegments) {
        segmentIds.add(_random.nextInt(totalSegs) + lo);
      }
      return segmentIds;
    }
  }

  private class TestHelixWorker implements Runnable {
    private final int _removePercent;
    private final int _replacePercent;
    private final int _addPercent;
    private final int _minSleepMs;
    private final int _maxSleepMs;
    private final TableDataManager _tableDataManager;

    private TestHelixWorker(TableDataManager tableDataManager) {
      _tableDataManager = tableDataManager;

      _removePercent = 20;
      _addPercent = 20;
      _replacePercent = 60;
      _minSleepMs = 50;
      _maxSleepMs = 300;
    }

    @Override
    public void run() {
      while (!_closing) {
        try {
          int nextInt = _random.nextInt(100);
          if (nextInt < _removePercent) {
            removeSegment();
          } else if (nextInt < _removePercent + _replacePercent) {
            replaceSegment();
          } else {
            addSegment();
          }
          try {
            int sleepTime = _random.nextInt(_maxSleepMs - _minSleepMs + 1) + _minSleepMs;
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            _closing = true;
          }
        } catch (Throwable t) {
          _masterThread.interrupt();
          _exception = t;
        }
      }
    }

    // Add segment _hi + 1,bump hi.
    private void addSegment() {
      final int segmentToAdd = _hi + 1;
      final String segName = SEGMENT_PREFIX + segmentToAdd;
      _tableDataManager.addSegment(makeImmutableSegment(segName, _random.nextInt()));
      _allSegManagers.add(_internalSegMap.get(segName));
      _hi = segmentToAdd;
    }

    // Replace a segment between _lo and _hi
    private void replaceSegment() {
      int segToReplace = _random.nextInt(_hi - _lo + 1) + _lo;
      final String segName = SEGMENT_PREFIX + segToReplace;
      _tableDataManager.addSegment(makeImmutableSegment(segName, _random.nextInt()));
      _allSegManagers.add(_internalSegMap.get(segName));
    }

    // Remove the segment _lo and then bump _lo
    private void removeSegment() {
      // Keep at least one segment in place.
      if (_hi > _lo) {
        _tableDataManager.removeSegment(SEGMENT_PREFIX + _lo);
        _lo++;
      } else {
        addSegment();
      }
    }
  }
}
