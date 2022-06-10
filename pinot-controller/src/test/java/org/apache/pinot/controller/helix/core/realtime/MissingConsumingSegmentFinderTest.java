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
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class MissingConsumingSegmentFinderTest {

  MissingConsumingSegmentFinder.SegmentMetadataFetcher _metadataFetcher =
      mock(MissingConsumingSegmentFinder.SegmentMetadataFetcher.class);

  @Test
  public void testFindMissingSegments() {
    Map<String, Map<String, String>> idealStateMap = new HashMap<>();
    // partition 0
    idealStateMap.put("tableA__0__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__0__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 1 (no consuming segment)
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
    // partition 4 (no consuming segment)
    idealStateMap.put("tableA__4__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    // partition 5
    idealStateMap.put("tableA__5__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__5__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableA__5__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));

    when(_metadataFetcher.fetchZNodeModificationTime("tableA", "tableA__1__1__20220601T1200Z"))
        .thenReturn(Instant.parse("2022-06-01T15:00:00.00Z").toEpochMilli());
    when(_metadataFetcher.fetchZNodeModificationTime("tableA", "tableA__4__0__20220601T0900Z"))
        .thenReturn(Instant.parse("2022-06-01T12:00:00.00Z").toEpochMilli());

    Instant now = Instant.parse("2022-06-01T18:00:00.00Z");
    MissingConsumingSegmentFinder finder = new MissingConsumingSegmentFinder("tableA", _metadataFetcher);
    MissingConsumingSegmentFinder.MissingSegmentInfo info = finder.findMissingSegments(idealStateMap, now);
    assertEquals(info._count, 2);
    assertEquals(info._maxDurationInMinutes, 6 * 60); // (18:00:00 - 12:00:00) in minutes
  }

  @Test
  public void testFindMissingSegmentsWithNoMissingSegment() {
    Map<String, Map<String, String>> idealStateMap = new HashMap<>();
    // partition 0
    idealStateMap.put("tableB__0__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableB__0__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableB__0__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 1
    idealStateMap.put("tableB__1__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableB__1__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableB__1__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 2
    idealStateMap.put("tableB__2__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableB__2__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableB__2__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));
    // partition 3
    idealStateMap.put("tableB__3__0__20220601T0900Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableB__3__1__20220601T1200Z", ImmutableMap.of("ServerX", "ONLINE", "ServerY", "ONLINE"));
    idealStateMap.put("tableB__3__2__20220601T1500Z", ImmutableMap.of("ServerX", "CONSUMING", "ServerY", "CONSUMING"));

    Instant now = Instant.parse("2022-06-01T18:00:00.00Z");
    MissingConsumingSegmentFinder finder = new MissingConsumingSegmentFinder("tableB", _metadataFetcher);
    MissingConsumingSegmentFinder.MissingSegmentInfo info = finder.findMissingSegments(idealStateMap, now);
    assertEquals(info._count, 0);
    assertEquals(info._maxDurationInMinutes, 0);
  }
}
