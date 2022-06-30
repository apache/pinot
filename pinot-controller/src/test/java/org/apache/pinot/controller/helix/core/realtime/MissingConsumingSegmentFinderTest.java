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
package org.apache.pinot.controller.helix.core.realtime;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.LongMsgOffsetFactory;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class MissingConsumingSegmentFinderTest {

  private StreamPartitionMsgOffsetFactory _offsetFactory = new LongMsgOffsetFactory();

  @Test
  public void noMissingConsumingSegmentsScenario1() {
    // scenario 1: no missing segments, but connecting to stream throws exception
    // only ideal state info is used

    Map<String, Map<String, String>> idealStateMap = new HashMap<>();
    // partition 0
    idealStateMap.put("tableA__0__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 1
    idealStateMap.put("tableA__1__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__1__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__1__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 2
    idealStateMap.put("tableA__2__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__2__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__2__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 3
    idealStateMap.put("tableA__3__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__3__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__3__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));

    Instant now = Instant.parse("2022-06-01T18:00:00.00Z");
    MissingConsumingSegmentFinder finder = new MissingConsumingSegmentFinder("tableA", null, new HashMap<>(), null);
    MissingConsumingSegmentFinder.MissingSegmentInfo info = finder.findMissingSegments(idealStateMap, now);
    assertEquals(info._totalCount, 0);
    assertEquals(info._newPartitionGroupCount, 0);
    assertEquals(info._maxDurationInMinutes, 0);
  }

  @Test
  public void noMissingConsumingSegmentsScenario2() {
    // scenario 2: no missing segments and there's no exception in connecting to stream

    Map<String, Map<String, String>> idealStateMap = new HashMap<>();
    // partition 0
    idealStateMap.put("tableA__0__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 1
    idealStateMap.put("tableA__1__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__1__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__1__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 2
    idealStateMap.put("tableA__2__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__2__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__2__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 3
    idealStateMap.put("tableA__3__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__3__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__3__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));

    Map<Integer, StreamPartitionMsgOffset> partitionGroupIdToLargestStreamOffsetMap = ImmutableMap.of(
        0, new LongMsgOffset(1000),
        1, new LongMsgOffset(1001),
        2, new LongMsgOffset(1002),
        3, new LongMsgOffset(1003)
    );

    Instant now = Instant.parse("2022-06-01T18:00:00.00Z");
    MissingConsumingSegmentFinder finder =
        new MissingConsumingSegmentFinder("tableA", null, partitionGroupIdToLargestStreamOffsetMap, null);
    MissingConsumingSegmentFinder.MissingSegmentInfo info = finder.findMissingSegments(idealStateMap, now);
    assertEquals(info._totalCount, 0);
    assertEquals(info._newPartitionGroupCount, 0);
    assertEquals(info._maxDurationInMinutes, 0);
  }

  @Test
  public void noMissingConsumingSegmentsScenario3() {
    // scenario 3: no missing segments and there's no exception in connecting to stream
    // two partitions have reached end of life

    Map<String, Map<String, String>> idealStateMap = new HashMap<>();
    // partition 0
    idealStateMap.put("tableA__0__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 1 (has reached end of life)
    idealStateMap.put("tableA__1__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__1__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    // partition 2
    idealStateMap.put("tableA__2__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__2__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__2__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 3 (has reached end of life)
    idealStateMap.put("tableA__3__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__3__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));

    Map<Integer, StreamPartitionMsgOffset> partitionGroupIdToLargestStreamOffsetMap = ImmutableMap.of(
        0, new LongMsgOffset(1000),
        1, new LongMsgOffset(701),
        2, new LongMsgOffset(1002),
        3, new LongMsgOffset(703)
    );

    // setup segment metadata fetcher
    SegmentZKMetadata m1 = mock(SegmentZKMetadata.class);
    when(m1.getEndOffset()).thenReturn("701");
    SegmentZKMetadata m3 = mock(SegmentZKMetadata.class);
    when(m3.getEndOffset()).thenReturn("703");
    MissingConsumingSegmentFinder.SegmentMetadataFetcher metadataFetcher =
        mock(MissingConsumingSegmentFinder.SegmentMetadataFetcher.class);
    when(metadataFetcher.fetchSegmentZkMetadata("tableA", "tableA__1__1__20220601T1200Z")).thenReturn(m1);
    when(metadataFetcher.fetchSegmentZkMetadata("tableA", "tableA__3__1__20220601T1200Z")).thenReturn(m3);

    Instant now = Instant.parse("2022-06-01T18:00:00.00Z");
    MissingConsumingSegmentFinder finder =
        new MissingConsumingSegmentFinder("tableA", metadataFetcher, partitionGroupIdToLargestStreamOffsetMap,
            _offsetFactory);
    MissingConsumingSegmentFinder.MissingSegmentInfo info = finder.findMissingSegments(idealStateMap, now);
    assertEquals(info._totalCount, 0);
    assertEquals(info._newPartitionGroupCount, 0);
    assertEquals(info._maxDurationInMinutes, 0);
  }

  @Test
  public void noMissingConsumingSegmentsScenario4() {
    // scenario 4: no missing segments, but connecting to stream throws exception
    // two partitions have reached end of life
    // since there's no way to detect if the partitions have reached end of life, those partitions are reported as
    // missing consuming segments

    Map<String, Map<String, String>> idealStateMap = new HashMap<>();
    // partition 0
    idealStateMap.put("tableA__0__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 1 (has reached end of life)
    idealStateMap.put("tableA__1__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__1__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    // partition 2
    idealStateMap.put("tableA__2__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__2__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__2__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 3
    idealStateMap.put("tableA__3__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__3__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__3__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 4 (has reached end of life)
    idealStateMap.put("tableA__4__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    // partition 5
    idealStateMap.put("tableA__5__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__5__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__5__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));

    // setup segment metadata fetcher
    MissingConsumingSegmentFinder.SegmentMetadataFetcher metadataFetcher =
        mock(MissingConsumingSegmentFinder.SegmentMetadataFetcher.class);
    when(metadataFetcher.fetchSegmentCompletionTime("tableA", "tableA__1__1__20220601T1200Z"))
        .thenReturn(Instant.parse("2022-06-01T15:00:00.00Z").toEpochMilli());
    when(metadataFetcher.fetchSegmentCompletionTime("tableA", "tableA__4__0__20220601T0900Z"))
        .thenReturn(Instant.parse("2022-06-01T12:00:00.00Z").toEpochMilli());

    Instant now = Instant.parse("2022-06-01T18:00:00.00Z");
    MissingConsumingSegmentFinder finder =
        new MissingConsumingSegmentFinder("tableA", metadataFetcher, new HashMap<>(), null);
    MissingConsumingSegmentFinder.MissingSegmentInfo info = finder.findMissingSegments(idealStateMap, now);
    assertEquals(info._totalCount, 2);
    assertEquals(info._newPartitionGroupCount, 0);
    assertEquals(info._maxDurationInMinutes, 6 * 60); // (18:00:00 - 12:00:00) in minutes
  }

  @Test
  public void missingConsumingSegments() {

    Map<String, Map<String, String>> idealStateMap = new HashMap<>();
    // partition 0
    idealStateMap.put("tableA__0__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 1 (missing consuming segment)
    idealStateMap.put("tableA__1__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__1__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    // partition 2
    idealStateMap.put("tableA__2__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__2__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__2__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 3
    idealStateMap.put("tableA__3__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__3__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__3__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 4 (missing consuming segment)
    idealStateMap.put("tableA__4__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    // partition 5
    idealStateMap.put("tableA__5__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__5__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__5__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 6 is a new partition and there's no consuming segment in ideal states for it

    Map<Integer, StreamPartitionMsgOffset> partitionGroupIdToLargestStreamOffsetMap = new HashMap<>();
    partitionGroupIdToLargestStreamOffsetMap.put(0, new LongMsgOffset(1000));
    partitionGroupIdToLargestStreamOffsetMap.put(1, new LongMsgOffset(1001));
    partitionGroupIdToLargestStreamOffsetMap.put(2, new LongMsgOffset(1002));
    partitionGroupIdToLargestStreamOffsetMap.put(3, new LongMsgOffset(1003));
    partitionGroupIdToLargestStreamOffsetMap.put(4, new LongMsgOffset(1004));
    partitionGroupIdToLargestStreamOffsetMap.put(5, new LongMsgOffset(1005));
    partitionGroupIdToLargestStreamOffsetMap.put(6, new LongMsgOffset(16));

    // setup segment metadata fetcher
    SegmentZKMetadata m1 = mock(SegmentZKMetadata.class);
    when(m1.getEndOffset()).thenReturn("701");
    when(m1.getCreationTime()).thenReturn(Instant.parse("2022-06-01T15:00:00.00Z").toEpochMilli());
    SegmentZKMetadata m4 = mock(SegmentZKMetadata.class);
    when(m4.getEndOffset()).thenReturn("704");
    when(m4.getCreationTime()).thenReturn(Instant.parse("2022-06-01T12:00:00.00Z").toEpochMilli());
    MissingConsumingSegmentFinder.SegmentMetadataFetcher metadataFetcher =
        mock(MissingConsumingSegmentFinder.SegmentMetadataFetcher.class);
    when(metadataFetcher.fetchSegmentZkMetadata("tableA", "tableA__1__1__20220601T1200Z")).thenReturn(m1);
    when(metadataFetcher.fetchSegmentZkMetadata("tableA", "tableA__4__0__20220601T0900Z")).thenReturn(m4);

    Instant now = Instant.parse("2022-06-01T18:00:00.00Z");
    MissingConsumingSegmentFinder finder =
        new MissingConsumingSegmentFinder("tableA", metadataFetcher, partitionGroupIdToLargestStreamOffsetMap,
            _offsetFactory);
    MissingConsumingSegmentFinder.MissingSegmentInfo info = finder.findMissingSegments(idealStateMap, now);
    assertEquals(info._totalCount, 3);
    assertEquals(info._newPartitionGroupCount, 1);
    assertEquals(info._maxDurationInMinutes, 6 * 60); // (18:00:00 - 12:00:00) in minutes
  }

  @Test
  public void missingConsumingSegmentsWithStreamConnectionIssue() {

    Map<String, Map<String, String>> idealStateMap = new HashMap<>();
    // partition 0
    idealStateMap.put("tableA__0__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 1 (missing consuming segment)
    idealStateMap.put("tableA__1__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__1__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    // partition 2
    idealStateMap.put("tableA__2__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__2__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__2__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 3
    idealStateMap.put("tableA__3__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__3__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__3__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 4 (missing consuming segment)
    idealStateMap.put("tableA__4__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    // partition 5
    idealStateMap.put("tableA__5__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__5__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__5__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 6 is a new partition and there's no consuming segment in ideal states for it

    // setup segment metadata fetcher
    MissingConsumingSegmentFinder.SegmentMetadataFetcher metadataFetcher =
        mock(MissingConsumingSegmentFinder.SegmentMetadataFetcher.class);
    when(metadataFetcher.fetchSegmentCompletionTime("tableA", "tableA__1__1__20220601T1200Z"))
        .thenReturn(Instant.parse("2022-06-01T15:00:00.00Z").toEpochMilli());
    when(metadataFetcher.fetchSegmentCompletionTime("tableA", "tableA__4__0__20220601T0900Z"))
        .thenReturn(Instant.parse("2022-06-01T12:00:00.00Z").toEpochMilli());

    Instant now = Instant.parse("2022-06-01T18:00:00.00Z");
    MissingConsumingSegmentFinder finder =
        new MissingConsumingSegmentFinder("tableA", metadataFetcher, new HashMap<>(), _offsetFactory);
    MissingConsumingSegmentFinder.MissingSegmentInfo info = finder.findMissingSegments(idealStateMap, now);
    assertEquals(info._totalCount, 2);
    assertEquals(info._newPartitionGroupCount, 0);
    assertEquals(info._maxDurationInMinutes, 6 * 60); // (18:00:00 - 12:00:00) in minutes
  }
}
