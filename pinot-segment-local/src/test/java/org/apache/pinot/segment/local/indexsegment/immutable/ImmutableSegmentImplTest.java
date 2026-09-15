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
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


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

  private static ImmutableSegmentImpl createSegment(SegmentDirectory segmentDirectory) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("seg");
    // getColumnMetadataMap() is declared as a TreeMap, so an immutable Map.of() will not do here.
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(new TreeMap<>());
    return new ImmutableSegmentImpl(segmentDirectory, segmentMetadata, Map.of(), null);
  }
}
