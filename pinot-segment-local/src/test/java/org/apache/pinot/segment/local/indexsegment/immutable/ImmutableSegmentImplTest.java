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
package org.apache.pinot.segment.local.indexsegment.immutable;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests for the post-registration lifecycle hook of [ImmutableSegmentImpl].
public class ImmutableSegmentImplTest {

  /// The hook must reach the directory at most once per segment instance: the same segment can be registered more than
  /// once (e.g. an upsert replacement with a consistency mode other than NONE registers it through a
  /// DuoSegmentDataManager and then directly), and implementations are not required to be idempotent.
  @Test
  public void testOnSegmentAddedNotifiesDirectoryAtMostOnce()
      throws Exception {
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    ImmutableSegmentImpl segment = createSegment(segmentDirectory);

    segment.onSegmentAdded();
    segment.onSegmentAdded();
    segment.onSegmentAdded();

    verify(segmentDirectory, times(1)).onSegmentAdded();
  }

  /// The hook fires after the segment is already serving, so a directory failure must not propagate out of it.
  @Test
  public void testOnSegmentAddedIsBestEffort()
      throws Exception {
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    doThrow(new RuntimeException("boom")).when(segmentDirectory).onSegmentAdded();
    ImmutableSegmentImpl segment = createSegment(segmentDirectory);

    // Must not throw.
    segment.onSegmentAdded();

    verify(segmentDirectory).onSegmentAdded();
  }

  /// A failed attempt consumes the single notification: the directory was already told, and retry semantics are the
  /// implementation's business, not the caller's.
  @Test
  public void testFailedOnSegmentAddedIsNotRetried()
      throws Exception {
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    doThrow(new RuntimeException("boom")).when(segmentDirectory).onSegmentAdded();
    ImmutableSegmentImpl segment = createSegment(segmentDirectory);

    segment.onSegmentAdded();
    segment.onSegmentAdded();

    verify(segmentDirectory, times(1)).onSegmentAdded();
  }

  @Test
  public void testDestroyMarksSegmentDestroyedBeforeClosingIndexes()
      throws Exception {
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    ColumnIndexContainer indexContainer = mock(ColumnIndexContainer.class);
    ImmutableSegmentImpl segment = createSegment(segmentDirectory, Map.of("col", indexContainer));
    // Readers that see the flag skip the segment, so it must be set before any index is closed
    doAnswer(invocation -> {
      assertTrue(segment.isDestroyed());
      assertFalse(segment.tryAcquireReadLock());
      return null;
    }).when(indexContainer).close();

    segment.destroy();

    verify(indexContainer).close();
    verify(segmentDirectory).close();
  }

  @Test
  public void testDestroyWaitsForReadLock()
      throws Exception {
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    ColumnIndexContainer indexContainer = mock(ColumnIndexContainer.class);
    ImmutableSegmentImpl segment = createSegment(segmentDirectory, Map.of("col", indexContainer));
    assertTrue(segment.tryAcquireReadLock());

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> destroyer = executor.submit(segment::destroy);
      // A reader holds the read lock: destroy must not close anything
      assertThrows(TimeoutException.class, () -> destroyer.get(500, TimeUnit.MILLISECONDS));
      assertFalse(segment.isDestroyed());
      verify(indexContainer, never()).close();

      segment.releaseReadLock();
      destroyer.get(10, TimeUnit.SECONDS);
      assertTrue(segment.isDestroyed());
      assertFalse(segment.tryAcquireReadLock());
      verify(indexContainer).close();
      verify(segmentDirectory).close();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testDestroyDoesNotWaitOnAnotherSegmentsReader()
      throws Exception {
    // The guard is per segment, so a reader pinning one segment must not hold up destroy of an unrelated one. Under
    // a lock shared across the partition this times out.
    ImmutableSegmentImpl reading = createSegment(mock(SegmentDirectory.class));
    SegmentDirectory destroyedDirectory = mock(SegmentDirectory.class);
    ImmutableSegmentImpl destroyed = createSegment(destroyedDirectory);
    assertTrue(reading.tryAcquireReadLock());

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(destroyed::destroy).get(10, TimeUnit.SECONDS);
      assertTrue(destroyed.isDestroyed());
      verify(destroyedDirectory).close();
      assertFalse(reading.isDestroyed());
    } finally {
      reading.releaseReadLock();
      executor.shutdownNow();
    }
  }

  private static ImmutableSegmentImpl createSegment(SegmentDirectory segmentDirectory) {
    return createSegment(segmentDirectory, Map.of());
  }

  private static ImmutableSegmentImpl createSegment(SegmentDirectory segmentDirectory,
      Map<String, ColumnIndexContainer> columnIndexContainerMap) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("seg");
    // getColumnMetadataMap() is declared as a TreeMap, so an immutable Map.of() will not do here.
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(new TreeMap<>());
    return new ImmutableSegmentImpl(segmentDirectory, segmentMetadata, columnIndexContainerMap, null);
  }
}
