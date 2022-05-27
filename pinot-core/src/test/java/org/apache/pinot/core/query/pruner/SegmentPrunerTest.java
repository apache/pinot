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
package org.apache.pinot.core.query.pruner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class SegmentPrunerTest {

  SegmentPruner _segmentPruner;

  @BeforeMethod
  public void setUp() {
    _segmentPruner = mock(SegmentPruner.class);
    when(_segmentPruner.prune(anyList(), any())).thenCallRealMethod();
    when(_segmentPruner.prune(any(IndexSegment.class), any())).thenReturn(false);
  }

  @Test
  public void testPrunePruningOne() {
    int capacity = 5;
    List<IndexSegment> segments = new ArrayList<>(capacity);
    for (int i = 0; i < capacity; i++) {
      segments.add(mock(IndexSegment.class));
    }
    QueryContext queryContext = mock(QueryContext.class);
    when(_segmentPruner.prune(segments.get(3), queryContext)).thenReturn(true);

    List<IndexSegment> pruned = _segmentPruner.prune(segments, queryContext);

    ArrayList<IndexSegment> expected = new ArrayList<>(segments);
    expected.remove(segments.get(3));
    assertEquals(pruned, expected);
  }

  @Test
  public void testPrunePruningFirst() {
    int capacity = 5;
    List<IndexSegment> segments = new ArrayList<>(capacity);
    for (int i = 0; i < capacity; i++) {
      segments.add(mock(IndexSegment.class));
    }
    QueryContext queryContext = mock(QueryContext.class);
    when(_segmentPruner.prune(segments.get(0), queryContext)).thenReturn(true);

    List<IndexSegment> pruned = _segmentPruner.prune(segments, queryContext);

    ArrayList<IndexSegment> expected = new ArrayList<>(segments);
    expected.remove(segments.get(0));
    assertEquals(pruned, expected);
  }

  @Test
  public void testPrunePruningLast() {
    int capacity = 5;
    List<IndexSegment> segments = new ArrayList<>(capacity);
    for (int i = 0; i < capacity; i++) {
      segments.add(mock(IndexSegment.class));
    }
    QueryContext queryContext = mock(QueryContext.class);
    when(_segmentPruner.prune(segments.get(4), queryContext)).thenReturn(true);

    List<IndexSegment> pruned = _segmentPruner.prune(segments, queryContext);

    ArrayList<IndexSegment> expected = new ArrayList<>(segments);
    expected.remove(segments.get(4));
    assertEquals(pruned, expected);
  }

  @Test
  public void testPrunePruningSome() {
    int capacity = 5;
    List<IndexSegment> segments = new ArrayList<>(capacity);
    for (int i = 0; i < capacity; i++) {
      segments.add(mock(IndexSegment.class));
    }
    QueryContext queryContext = mock(QueryContext.class);
    when(_segmentPruner.prune(segments.get(1), queryContext)).thenReturn(true);
    when(_segmentPruner.prune(segments.get(3), queryContext)).thenReturn(true);

    List<IndexSegment> pruned = _segmentPruner.prune(segments, queryContext);

    ArrayList<IndexSegment> expected = new ArrayList<>(segments);
    expected.remove(segments.get(1));
    expected.remove(segments.get(3));
    assertEquals(pruned, expected);
  }

  @Test
  public void testPruneNotPruning() {
    int capacity = 5;
    List<IndexSegment> segments = new ArrayList<>(capacity);
    for (int i = 0; i < capacity; i++) {
      segments.add(mock(IndexSegment.class));
    }
    QueryContext queryContext = mock(QueryContext.class);

    List<IndexSegment> pruned = _segmentPruner.prune(segments, queryContext);

    assertEquals(pruned, segments);
  }

  @Test
  public void testPrunePruningAll() {
    int capacity = 5;
    List<IndexSegment> segments = new ArrayList<>(capacity);
    for (int i = 0; i < capacity; i++) {
      segments.add(mock(IndexSegment.class));
    }
    QueryContext queryContext = mock(QueryContext.class);
    when(_segmentPruner.prune(any(IndexSegment.class), any())).thenReturn(true);

    List<IndexSegment> pruned = _segmentPruner.prune(segments, queryContext);

    assertEquals(pruned, Collections.emptyList());
  }
}
