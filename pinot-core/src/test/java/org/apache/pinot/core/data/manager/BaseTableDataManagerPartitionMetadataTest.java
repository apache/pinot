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
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig.ConsistencyMode;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.mockito.InOrder;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


/// Verifies partition metadata initialization at publication and offload boundaries used by concurrent consumers, and
/// its rebuild when the ONLINE transition is retried after a failed initialization.
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
  }

  @DataProvider
  public Object[][] onlineRetries() {
    return new Object[][]{{TableType.OFFLINE, false}, {TableType.OFFLINE, true},
        {TableType.REALTIME, false}, {TableType.REALTIME, true}};
  }

  @Test(dataProvider = "onlineRetries")
  public void testOnlineRetryRebuildsOnlyUninitializedUpsertSegment(TableType tableType, boolean failInitialization)
      throws Exception {
    PartitionUpsertMetadataManager partition = mock(PartitionUpsertMetadataManager.class);
    long crc = 1234L;
    SegmentZKMetadata zkMetadata = new SegmentZKMetadata(SEGMENT_NAME);
    zkMetadata.setCrc(crc);
    if (tableType == TableType.REALTIME) {
      // The rebuild does not wait for a pauseless segment to complete its commit
      zkMetadata.setStatus(Status.COMMITTING);
    }
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    BaseTableDataManager table = createOnlineRetryTableManager(tableType, ConsistencyMode.NONE, partition, zkMetadata,
        indexLoadingConfig);

    ImmutableSegment segment = createSegment();
    SegmentMetadata segmentMetadata = segment.getSegmentMetadata();
    when(segmentMetadata.getCrc()).thenReturn(crc);
    doReturn(segment).when(table).tryLoadExistingSegmentWithoutRegistering(zkMetadata, indexLoadingConfig);
    if (failInitialization) {
      IllegalStateException failure = new IllegalStateException("Metadata initialization failed");
      doThrow(failure).when(partition).addSegment(segment);
      assertSame(expectThrows(IllegalStateException.class, () -> table.addOnlineSegment(SEGMENT_NAME)), failure);
    } else {
      table.addOnlineSegment(SEGMENT_NAME);
    }
    ImmutableSegmentDataManager loadedManager = getImmutableManager(table);
    assertSame(loadedManager.getSegment(), segment);
    assertEquals(loadedManager.isPartitionMetadataInitialized(), !failInitialization);

    // The ERROR -> OFFLINE reset leaves the segment registered, so the ONLINE retry finds it with a matching CRC.
    ImmutableSegment reloadedSegment = createSegment();
    doAnswer(invocation -> {
      table.addSegment(reloadedSegment, zkMetadata);
      return null;
    }).when(table).reloadSegment(anyString(), any(), any(), any(), anyBoolean());
    table.addOnlineSegment(SEGMENT_NAME);

    ImmutableSegmentDataManager onlineManager = getImmutableManager(table);
    assertTrue(onlineManager.isPartitionMetadataInitialized());
    if (failInitialization) {
      verify(table).reloadSegment(SEGMENT_NAME, indexLoadingConfig, zkMetadata, segmentMetadata, false);
      // The segment is tracked as newly added again before the replacement moves any keys to it
      InOrder inOrder = inOrder(partition);
      inOrder.verify(partition).trackNewlyAddedSegment(SEGMENT_NAME);
      inOrder.verify(partition).addSegment(segment);
      inOrder.verify(partition).trackNewlyAddedSegment(SEGMENT_NAME);
      inOrder.verify(partition).replaceSegment(reloadedSegment, segment);
      assertNotSame(onlineManager, loadedManager);
      assertSame(onlineManager.getSegment(), reloadedSegment);
      verify(segment).offload();
    } else {
      verify(table, never()).reloadSegment(anyString(), any(), any(), any(), anyBoolean());
      verify(partition).trackNewlyAddedSegment(SEGMENT_NAME);
      assertSame(onlineManager, loadedManager);
    }
  }

  @DataProvider
  public Object[][] consistencyModes() {
    return new Object[][]{{ConsistencyMode.NONE}, {ConsistencyMode.SYNC}, {ConsistencyMode.SNAPSHOT}};
  }

  @Test(dataProvider = "consistencyModes")
  public void testOnlineRetryRebuildsAgainAfterFailedRebuild(ConsistencyMode consistencyMode)
      throws Exception {
    PartitionUpsertMetadataManager partition = mock(PartitionUpsertMetadataManager.class);
    long crc = 1234L;
    SegmentZKMetadata zkMetadata = new SegmentZKMetadata(SEGMENT_NAME);
    zkMetadata.setCrc(crc);
    zkMetadata.setStatus(Status.COMMITTING);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    BaseTableDataManager table = createOnlineRetryTableManager(TableType.REALTIME, consistencyMode, partition,
        zkMetadata, indexLoadingConfig);

    ImmutableSegment segment = createSegment();
    when(segment.getSegmentMetadata().getCrc()).thenReturn(crc);
    doReturn(segment).when(table).tryLoadExistingSegmentWithoutRegistering(zkMetadata, indexLoadingConfig);
    IllegalStateException addFailure = new IllegalStateException("Metadata initialization failed");
    doThrow(addFailure).when(partition).addSegment(segment);
    assertSame(expectThrows(IllegalStateException.class, () -> table.addOnlineSegment(SEGMENT_NAME)), addFailure);
    ImmutableSegmentDataManager loadedManager = getImmutableManager(table);

    // The first retry fails to rebuild the upsert metadata
    ImmutableSegment failedSegment = createSegment();
    ImmutableSegment rebuiltSegment = createSegment();
    doAnswer(invocation -> {
      table.addSegment(failedSegment, zkMetadata);
      return null;
    }).doAnswer(invocation -> {
      table.addSegment(rebuiltSegment, zkMetadata);
      return null;
    }).when(table).reloadSegment(anyString(), any(), any(), any(), anyBoolean());
    IllegalStateException replaceFailure = new IllegalStateException("Metadata replacement failed");
    doThrow(replaceFailure).when(partition).replaceSegment(failedSegment, segment);
    assertSame(expectThrows(IllegalStateException.class, () -> table.addOnlineSegment(SEGMENT_NAME)), replaceFailure);
    assertSame(getImmutableManager(table), loadedManager);
    assertFalse(loadedManager.isPartitionMetadataInitialized());
    if (consistencyMode != ConsistencyMode.NONE) {
      verify(partition).untrackSegmentForUpsertView(failedSegment);
    }
    // The upsert metadata can still point at the failed segment
    verify(failedSegment, never()).destroy();
    verify(segment, never()).offload();

    // The second retry rebuilds the upsert metadata
    table.addOnlineSegment(SEGMENT_NAME);
    ImmutableSegmentDataManager onlineManager = getImmutableManager(table);
    assertSame(onlineManager.getSegment(), rebuiltSegment);
    assertTrue(onlineManager.isPartitionMetadataInitialized());
    verify(partition).replaceSegment(rebuiltSegment, segment);
    verify(segment).offload();
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
    oldManager.markPartitionMetadataInitialized();
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
      // Only a failed rebuild of uninitialized upsert metadata puts the old segment back on its own
      if (consistencyMode == ConsistencyMode.NONE) {
        assertSame(table.getSegmentDataManager(SEGMENT_NAME), oldManager);
      } else {
        assertTrue(table.getSegmentDataManager(SEGMENT_NAME) instanceof DuoSegmentDataManager);
      }
      verify(oldSegment, never()).offload();
    } else {
      table.replaceUpsertSegment(SEGMENT_NAME, oldManager, newManager, partition);
      assertTrue(newManager.isPartitionMetadataInitialized());
      verify(oldSegment).offload();
    }
    verify(partition).replaceSegment(replacement, oldSegment);
  }

  private static BaseTableDataManager createTableManager(PartitionUpsertMetadataManager partition) {
    return createTableManager(TableType.OFFLINE, partition);
  }

  private static BaseTableDataManager createTableManager(TableType tableType,
      PartitionUpsertMetadataManager partition) {
    BaseTableDataManager table = tableType == TableType.OFFLINE
        ? new OfflineTableDataManager()
        : new RealtimeTableDataManager(null);
    // A realtime table name lets the partition id come from the LLC segment name, without ZK, for both table types
    table._tableNameWithType = "testTable_REALTIME";
    table._logger = LoggerFactory.getLogger(BaseTableDataManagerPartitionMetadataTest.class);
    table._recentlyDeletedSegments = CacheBuilder.newBuilder().build();
    if (partition != null) {
      table._tableUpsertMetadataManager = mock(TableUpsertMetadataManager.class);
      when(table._tableUpsertMetadataManager.getOrCreatePartitionManager(0)).thenReturn(partition);
    }
    return table;
  }

  /// Returns a spied upsert table manager that serves ONLINE transitions of the segment from the given ZK metadata and
  /// index loading config.
  private static BaseTableDataManager createOnlineRetryTableManager(TableType tableType,
      ConsistencyMode consistencyMode, PartitionUpsertMetadataManager partition, SegmentZKMetadata zkMetadata,
      IndexLoadingConfig indexLoadingConfig) {
    BaseTableDataManager table = spy(createTableManager(tableType, partition));
    table._segmentLocks = new SegmentLocks();
    UpsertContext context = mock(UpsertContext.class);
    when(context.getConsistencyMode()).thenReturn(consistencyMode);
    when(table._tableUpsertMetadataManager.getContext()).thenReturn(context);
    doReturn(zkMetadata).when(table).fetchZKMetadata(SEGMENT_NAME);
    doReturn(indexLoadingConfig).when(table).getCachedIndexLoadingConfig();
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
