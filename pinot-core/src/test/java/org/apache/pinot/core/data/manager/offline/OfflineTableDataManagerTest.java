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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.mockito.InOrder;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Tests that [OfflineTableDataManager] takes the upsert snapshot and removes the expired primary keys in the
/// background after a segment is added to an upsert partition, in the same order as a REALTIME segment commit, and
/// runs one such task per partition at a time.
public class OfflineTableDataManagerTest {
  private static final String TABLE_NAME_WITH_TYPE = "testTable_OFFLINE";

  private final List<Runnable> _tasks = new ArrayList<>();

  @Test
  public void testSnapshotBeforeRemovingExpiredKeysWithMetadataTTL() {
    PartitionUpsertMetadataManager partitionManager = mockPartitionManager(30);
    newTableDataManager(true).onUpsertSegmentAdded(0, partitionManager);
    runTasks();
    InOrder inOrder = inOrder(partitionManager);
    inOrder.verify(partitionManager).takeSnapshot();
    inOrder.verify(partitionManager).removeExpiredPrimaryKeys();
  }

  @Test
  public void testRemoveExpiredKeysBeforeSnapshotWithoutMetadataTTL() {
    PartitionUpsertMetadataManager partitionManager = mockPartitionManager(0);
    newTableDataManager(true).onUpsertSegmentAdded(0, partitionManager);
    runTasks();
    InOrder inOrder = inOrder(partitionManager);
    inOrder.verify(partitionManager).removeExpiredPrimaryKeys();
    inOrder.verify(partitionManager).takeSnapshot();
  }

  /// Before the partition's segments are loaded, e.g. while the server starts, the run is skipped and counted.
  @Test
  public void testSkipBeforePartitionIsLoaded() {
    PartitionUpsertMetadataManager partitionManager = mockPartitionManager(30);
    ServerMetrics previousServerMetrics = ServerMetrics.get();
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    ServerMetrics.deregister();
    ServerMetrics.register(serverMetrics);
    OfflineTableDataManager tableDataManager;
    try {
      tableDataManager = newTableDataManager(false);
    } finally {
      ServerMetrics.deregister();
      ServerMetrics.register(previousServerMetrics);
    }
    tableDataManager.onUpsertSegmentAdded(0, partitionManager);
    runTasks();
    verify(partitionManager, never()).takeSnapshot();
    verify(partitionManager, never()).removeExpiredPrimaryKeys();
    verify(serverMetrics).addMeteredTableValue(TABLE_NAME_WITH_TYPE,
        ServerMeter.UPSERT_SNAPSHOT_SKIPPED_PARTITION_NOT_LOADED, 1L);
  }

  /// Nothing is scheduled while the server loads its segments at startup, or while the partition preloads, so the
  /// partition isn't checked as loaded against ZK for every segment still loading.
  @Test
  public void testNoTaskDuringStartupOrPreload() {
    PartitionUpsertMetadataManager partitionManager = mockPartitionManager(30);
    AtomicBoolean serverReady = new AtomicBoolean();
    OfflineTableDataManager tableDataManager = newTableDataManager(true, serverReady::get);
    tableDataManager.onUpsertSegmentAdded(0, partitionManager);
    assertTrue(_tasks.isEmpty());

    serverReady.set(true);
    when(partitionManager.isPreloading()).thenReturn(true);
    tableDataManager.onUpsertSegmentAdded(0, partitionManager);
    assertTrue(_tasks.isEmpty());

    when(partitionManager.isPreloading()).thenReturn(false);
    tableDataManager.onUpsertSegmentAdded(0, partitionManager);
    runTasks();
    verify(partitionManager).takeSnapshot();
  }

  /// Adds while the task is scheduled share it, and adds while it runs make it run once more.
  @Test
  public void testAddsToPartitionShareOneTask() {
    PartitionUpsertMetadataManager partitionManager = mockPartitionManager(30);
    OfflineTableDataManager tableDataManager = newTableDataManager(true);
    for (int i = 0; i < 3; i++) {
      tableDataManager.onUpsertSegmentAdded(0, partitionManager);
    }
    assertEquals(_tasks.size(), 1);
    // A segment added during the first run
    doAnswer(invocation -> {
      tableDataManager.onUpsertSegmentAdded(0, partitionManager);
      return null;
    }).doNothing().when(partitionManager).takeSnapshot();
    runTasks();
    assertEquals(_tasks.size(), 0);
    verify(partitionManager, times(2)).takeSnapshot();
    verify(partitionManager, times(2)).removeExpiredPrimaryKeys();
    // The partition is idle again, so the next add schedules a new task
    tableDataManager.onUpsertSegmentAdded(0, partitionManager);
    assertEquals(_tasks.size(), 1);
  }

  @Test
  public void testPartitionsHaveSeparateTasks() {
    OfflineTableDataManager tableDataManager = newTableDataManager(true);
    tableDataManager.onUpsertSegmentAdded(0, mockPartitionManager(30));
    tableDataManager.onUpsertSegmentAdded(1, mockPartitionManager(30));
    assertEquals(_tasks.size(), 2);
  }

  /// The segment is already added, so a failure must not fail the task, and later adds take the snapshot again.
  @Test
  public void testFailureDoesNotStopLaterRuns() {
    PartitionUpsertMetadataManager partitionManager = mockPartitionManager(30);
    doThrow(new RuntimeException("test")).when(partitionManager).removeExpiredPrimaryKeys();
    OfflineTableDataManager tableDataManager = newTableDataManager(true);
    tableDataManager.onUpsertSegmentAdded(0, partitionManager);
    runTasks();
    tableDataManager.onUpsertSegmentAdded(0, partitionManager);
    runTasks();
    verify(partitionManager, times(2)).takeSnapshot();
  }

  @Test
  public void testRejectedTaskIsScheduledOnNextAdd() {
    PartitionUpsertMetadataManager partitionManager = mockPartitionManager(30);
    _tasks.clear();
    ExecutorService executor = mock(ExecutorService.class);
    doThrow(new RejectedExecutionException("test")).doAnswer(invocation -> {
      _tasks.add(invocation.getArgument(0));
      return null;
    }).when(executor).execute(any());
    OfflineTableDataManager tableDataManager = new TestOfflineTableDataManager(true, executor, () -> true);
    tableDataManager.onUpsertSegmentAdded(0, partitionManager);
    assertTrue(_tasks.isEmpty());
    verify(partitionManager, never()).takeSnapshot();
    tableDataManager.onUpsertSegmentAdded(0, partitionManager);
    runTasks();
    verify(partitionManager).takeSnapshot();
  }

  private void runTasks() {
    while (!_tasks.isEmpty()) {
      _tasks.remove(0).run();
    }
  }

  private static PartitionUpsertMetadataManager mockPartitionManager(double metadataTTL) {
    UpsertContext context = mock(UpsertContext.class);
    when(context.getMetadataTTL()).thenReturn(metadataTTL);
    PartitionUpsertMetadataManager partitionManager = mock(PartitionUpsertMetadataManager.class);
    when(partitionManager.getContext()).thenReturn(context);
    return partitionManager;
  }

  /// Returns a table data manager whose background tasks are queued in [#_tasks] until [#runTasks()].
  private OfflineTableDataManager newTableDataManager(boolean partitionLoaded) {
    return newTableDataManager(partitionLoaded, () -> true);
  }

  private OfflineTableDataManager newTableDataManager(boolean partitionLoaded, BooleanSupplier serverReady) {
    _tasks.clear();
    ExecutorService executor = mock(ExecutorService.class);
    doAnswer(invocation -> {
      _tasks.add(invocation.getArgument(0));
      return null;
    }).when(executor).execute(any());
    return new TestOfflineTableDataManager(partitionLoaded, executor, serverReady);
  }

  private static class TestOfflineTableDataManager extends OfflineTableDataManager {
    final boolean _partitionLoaded;

    TestOfflineTableDataManager(boolean partitionLoaded, ExecutorService refreshExecutor, BooleanSupplier serverReady) {
      super(serverReady);
      _partitionLoaded = partitionLoaded;
      _tableNameWithType = TABLE_NAME_WITH_TYPE;
      _segmentRefreshExecutor = refreshExecutor;
      _logger = LoggerFactory.getLogger(OfflineTableDataManagerTest.class);
    }

    @Override
    boolean isPartitionLoaded(int partitionId) {
      return _partitionLoaded;
    }
  }
}
