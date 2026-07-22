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

import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class EmptyIndexSegmentTest {

  /**
   * An empty (0-doc) segment loaded from a directory must still run the post-registration lifecycle hook and own
   * closing the directory, mirroring the non-empty ImmutableSegmentImpl path.
   */
  @Test
  public void testOnSegmentAddedDelegatesToDirectoryAndDestroyCloses()
      throws Exception {
    SegmentMetadataImpl metadata = mock(SegmentMetadataImpl.class);
    when(metadata.getName()).thenReturn("seg");
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    EmptyIndexSegment segment = new EmptyIndexSegment(metadata, segmentDirectory);

    segment.onSegmentAdded();
    verify(segmentDirectory).onSegmentAdded();

    segment.destroy();
    verify(segmentDirectory).close();
  }

  /**
   * The hook fires after the segment is already serving, so a directory failure must not propagate out of it.
   */
  @Test
  public void testOnSegmentAddedIsBestEffort()
      throws Exception {
    SegmentMetadataImpl metadata = mock(SegmentMetadataImpl.class);
    when(metadata.getName()).thenReturn("seg");
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    doThrow(new RuntimeException("boom")).when(segmentDirectory).onSegmentAdded();
    EmptyIndexSegment segment = new EmptyIndexSegment(metadata, segmentDirectory);

    // Must not throw.
    segment.onSegmentAdded();
  }

  /**
   * An empty segment loaded without a directory (e.g. the local File path) must treat the lifecycle callbacks as safe
   * no-ops.
   */
  @Test
  public void testMetadataOnlySegmentHasNoDirectorySideEffects() {
    SegmentMetadataImpl metadata = mock(SegmentMetadataImpl.class);
    when(metadata.getName()).thenReturn("seg");
    EmptyIndexSegment segment = new EmptyIndexSegment(metadata);

    // No directory: both must be safe no-ops.
    segment.onSegmentAdded();
    segment.destroy();
  }
}
