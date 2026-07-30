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
package org.apache.pinot.controller.helix.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class PinotHelixResourceManagerLastLLCSegmentsTest {

  private static final String TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);

  /**
   * A realtime table can contain non-LLC-named segments (e.g. uploaded via batch ingestion) sitting in DONE state
   * alongside the LLC-named consuming/committed segments. {@code getLastLLCCompletedSegments} must skip those
   * uploaded segments rather than NPE when {@code LLCSegmentName.of(name)} returns {@code null}.
   */
  @Test
  public void testGetLastLLCCompletedSegmentsSkipsNonLLCNamedSegments() {
    long now = System.currentTimeMillis();
    int partitionId = 3;

    List<SegmentZKMetadata> segments = new ArrayList<>();
    // Two LLC-named DONE segments — sequence 0 and 1 for the same partition; sequence 1 is the latest.
    LLCSegmentName seq0 = new LLCSegmentName(TABLE_NAME, partitionId, 0, now);
    LLCSegmentName seq1 = new LLCSegmentName(TABLE_NAME, partitionId, 1, now);
    segments.add(doneSegment(seq0.getSegmentName()));
    segments.add(doneSegment(seq1.getSegmentName()));
    // An uploaded (non-LLC-named) segment in DONE state — must be ignored, not crash the method.
    segments.add(doneSegment("uploaded_segment_0"));

    PinotHelixResourceManager rm = mock(PinotHelixResourceManager.class);
    when(rm.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(segments);
    when(rm.getLastLLCCompletedSegments(REALTIME_TABLE_NAME)).thenCallRealMethod();
    when(rm.getLastLLCCompletedSegments(anyList())).thenCallRealMethod();

    Collection<String> lastCompleted = rm.getLastLLCCompletedSegments(REALTIME_TABLE_NAME);
    Set<String> actual = new HashSet<>(lastCompleted);
    assertEquals(actual, Set.of(seq1.getSegmentName()));
  }

  private static SegmentZKMetadata doneSegment(String name) {
    SegmentZKMetadata md = new SegmentZKMetadata(name);
    md.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    return md;
  }
}
