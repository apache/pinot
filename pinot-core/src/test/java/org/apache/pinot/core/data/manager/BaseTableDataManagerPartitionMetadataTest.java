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

import com.google.common.cache.CacheBuilder;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.UpsertConfig.ConsistencyMode;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


/// Verifies partition metadata initialization at publication and offload boundaries used by concurrent consumers.
/// Metadata callbacks run synchronously so each test observes the exact boundary without scheduling assumptions.
public class BaseTableDataManagerPartitionMetadataTest {
  private static final String SEGMENT_NAME = "testTable__0__0__1";

  @Test
  public void testOrdinarySegmentReadyBeforePublicationAndReplacementOffload() {
    BaseTableDataManager table = createTableManager(null);
    ImmutableSegment oldSegment = createSegment();
    table.addSegment(oldSegment, null);
    assertTrue(getImmutableManager(table).isPartitionMetadataInitialized());

    ImmutableSegment replacement = createSegment();
    doAnswer(invocation -> {
      assertSame(getImmutableManager(table).getSegment(), replacement);
      assertTrue(getImmutableManager(table).isPartitionMetadataInitialized());
      return null;
    }).when(replacement).onSegmentAdded();
    doAnswer(invocation -> {
      assertSame(getImmutableManager(table).getSegment(), replacement);
      assertTrue(getImmutableManager(table).isPartitionMetadataInitialized());
      return null;
    }).when(oldSegment).offload();

    table.addSegment(replacement, null);

    verify(replacement).onSegmentAdded();
    verify(oldSegment).offload();
  }

  @DataProvider
  public Object[][] initializationFailure() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "initializationFailure")
  public void testNewUpsertSegmentReadyOnlyAfterMetadataLoads(boolean failInitialization) {
    PartitionUpsertMetadataManager partition = mock(PartitionUpsertMetadataManager.class);
    BaseTableDataManager table = createTableManager(partition);
    ImmutableSegment segment = createSegment();
    IllegalStateException failure = new IllegalStateException("Metadata initialization failed");
    doAnswer(invocation -> {
      assertFalse(getImmutableManager(table).isPartitionMetadataInitialized());
      return null;
    }).when(segment).onSegmentAdded();
    doAnswer(invocation -> {
      assertSame(getImmutableManager(table).getSegment(), segment);
      assertFalse(getImmutableManager(table).isPartitionMetadataInitialized());
      if (failInitialization) {
        throw failure;
      }
      return null;
    }).when(partition).addSegment(segment);

    if (failInitialization) {
      assertSame(expectThrows(IllegalStateException.class, () -> table.addSegment(segment, null)), failure);
      assertFalse(getImmutableManager(table).isPartitionMetadataInitialized());
    } else {
      table.addSegment(segment, null);
      assertTrue(getImmutableManager(table).isPartitionMetadataInitialized());
    }
    verify(segment).onSegmentAdded();
    verify(partition).addSegment(segment);
    verifyUpsertSnapshotScheduled(table, failInitialization ? 0 : 1);
  }

  @Test(dataProvider = "initializationFailure")
  public void testUpsertPreloadReadyBeforePublication(boolean failInitialization) {
    PartitionUpsertMetadataManager partition = mock(PartitionUpsertMetadataManager.class);
    when(partition.isPreloading()).thenReturn(true);
    BaseTableDataManager table = createTableManager(partition);
    ImmutableSegment segment = createSegment();
    IllegalStateException failure = new IllegalStateException("Metadata preload failed");
    doAnswer(invocation -> {
      assertNull(table.getSegmentDataManager(SEGMENT_NAME));
      if (failInitialization) {
        throw failure;
      }
      return null;
    }).when(partition).preloadSegment(segment);
    doAnswer(invocation -> {
      assertTrue(getImmutableManager(table).isPartitionMetadataInitialized());
      return null;
    }).when(segment).onSegmentAdded();

    if (failInitialization) {
      assertSame(expectThrows(IllegalStateException.class, () -> table.addSegment(segment, null)), failure);
      assertNull(table.getSegmentDataManager(SEGMENT_NAME));
      verify(segment, never()).onSegmentAdded();
    } else {
      table.addSegment(segment, null);
      assertTrue(getImmutableManager(table).isPartitionMetadataInitialized());
      verify(segment).onSegmentAdded();
    }
    verify(partition).preloadSegment(segment);
    // Preloaded segments come with their snapshot, so they don't schedule one
    verifyUpsertSnapshotScheduled(table, 0);
  }

  @DataProvider
  public Object[][] replacementModes() {
    return new Object[][]{{ConsistencyMode.NONE, false}, {ConsistencyMode.NONE, true},
        {ConsistencyMode.SYNC, false}, {ConsistencyMode.SYNC, true},
        {ConsistencyMode.SNAPSHOT, false}, {ConsistencyMode.SNAPSHOT, true}};
  }

  @Test(dataProvider = "replacementModes")
  public void testUpsertReplacementReadyBeforeOldSegmentOffload(ConsistencyMode consistencyMode,
      boolean failInitialization) {
    PartitionUpsertMetadataManager partition = mock(PartitionUpsertMetadataManager.class);
    BaseTableDataManager table = createTableManager(partition);
    UpsertContext context = mock(UpsertContext.class);
    when(context.getConsistencyMode()).thenReturn(consistencyMode);
    when(table._tableUpsertMetadataManager.getContext()).thenReturn(context);
    ImmutableSegment oldSegment = createSegment();
    ImmutableSegmentDataManager oldManager = new ImmutableSegmentDataManager(oldSegment);
    table.registerSegment(SEGMENT_NAME, oldManager);
    ImmutableSegment replacement = createSegment();
    ImmutableSegmentDataManager newManager = new ImmutableSegmentDataManager(replacement);
    IllegalStateException failure = new IllegalStateException("Metadata replacement failed");
    doAnswer(invocation -> {
      assertFalse(newManager.isPartitionMetadataInitialized());
      if (consistencyMode == ConsistencyMode.NONE) {
        assertSame(table.getSegmentDataManager(SEGMENT_NAME), oldManager);
      } else {
        assertTrue(table.getSegmentDataManager(SEGMENT_NAME) instanceof DuoSegmentDataManager);
      }
      if (failInitialization) {
        throw failure;
      }
      return null;
    }).when(partition).replaceSegment(replacement, oldSegment);
    doAnswer(invocation -> {
      assertSame(table.getSegmentDataManager(SEGMENT_NAME), newManager);
      assertTrue(newManager.isPartitionMetadataInitialized());
      return null;
    }).when(oldSegment).offload();

    if (failInitialization) {
      assertSame(expectThrows(IllegalStateException.class,
          () -> table.replaceUpsertSegment(SEGMENT_NAME, oldManager, newManager, partition)), failure);
      assertFalse(newManager.isPartitionMetadataInitialized());
      verify(oldSegment, never()).offload();
    } else {
      table.replaceUpsertSegment(SEGMENT_NAME, oldManager, newManager, partition);
      assertTrue(newManager.isPartitionMetadataInitialized());
      verify(oldSegment).offload();
    }
    verify(partition).replaceSegment(replacement, oldSegment);
  }

  @Test
  public void testUpsertReplacementSchedulesSnapshot() {
    PartitionUpsertMetadataManager partition = mock(PartitionUpsertMetadataManager.class);
    BaseTableDataManager table = createTableManager(partition);
    UpsertContext context = mock(UpsertContext.class);
    when(context.getConsistencyMode()).thenReturn(ConsistencyMode.NONE);
    when(table._tableUpsertMetadataManager.getContext()).thenReturn(context);
    ImmutableSegment oldSegment = createSegment();
    table.registerSegment(SEGMENT_NAME, new ImmutableSegmentDataManager(oldSegment));

    ImmutableSegment replacement = createSegment();
    table.addSegment(replacement, null);
    verify(partition).replaceSegment(replacement, oldSegment);
    verifyUpsertSnapshotScheduled(table, 1);
  }

  /// Verifies how many times the OFFLINE upsert snapshot was scheduled. It's scheduled after a segment is added to or
  /// replaced in its partition, and not when that fails or the segment is preloaded.
  private static void verifyUpsertSnapshotScheduled(BaseTableDataManager table, int numScheduled) {
    verify(table._segmentRefreshExecutor, times(numScheduled)).execute(any(Runnable.class));
  }

  private static BaseTableDataManager createTableManager(PartitionUpsertMetadataManager partition) {
    BaseTableDataManager table = new OfflineTableDataManager();
    table._tableNameWithType = "testTable_REALTIME";
    table._logger = LoggerFactory.getLogger(BaseTableDataManagerPartitionMetadataTest.class);
    table._recentlyDeletedSegments = CacheBuilder.newBuilder().build();
    // Receives the OFFLINE upsert snapshot tasks, which these tests do not run
    table._segmentRefreshExecutor = mock(ExecutorService.class);
    if (partition != null) {
      table._tableUpsertMetadataManager = mock(TableUpsertMetadataManager.class);
      when(table._tableUpsertMetadataManager.getOrCreatePartitionManager(0)).thenReturn(partition);
    }
    return table;
  }

  private static ImmutableSegment createSegment() {
    ImmutableSegment segment = mock(ImmutableSegment.class);
    when(segment.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(segment.getSegmentMetadata()).thenReturn(mock(SegmentMetadata.class));
    return segment;
  }

  private static ImmutableSegmentDataManager getImmutableManager(BaseTableDataManager table) {
    return (ImmutableSegmentDataManager) table.getSegmentDataManager(SEGMENT_NAME);
  }
}
