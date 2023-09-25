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

package org.apache.pinot.server.starter.helix;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class OffsetBasedConsumptionStatusCheckerTest {

  @Test
  public void regularCase() {

    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Set<String> consumingSegments = ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    OffsetBasedConsumptionStatusChecker statusChecker =
        new OffsetBasedConsumptionStatusChecker(instanceDataManager, consumingSegments);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    RealtimeSegmentDataManager segMngrA0 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrA1 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrB0 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);
    when(segMngrA0.getLatestStreamOffsetAtStartupTime()).thenReturn(new LongMsgOffset(15));
    when(segMngrA1.getLatestStreamOffsetAtStartupTime()).thenReturn(new LongMsgOffset(150));
    when(segMngrB0.getLatestStreamOffsetAtStartupTime()).thenReturn(new LongMsgOffset(1500));

    //           latest ingested offset    latest stream offset
    // segA0              10                       15
    // segA1              100                      150
    // segB0              1000                     1500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(100));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(1000));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    //           latest ingested offset     latest stream offset
    // segA0              20                       15
    // segA1              200                      150
    // segB0              2000                     1500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(200));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(2000));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }

  @Test
  public void dataMangersBeingSetup() {

    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Set<String> consumingSegments = ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);

    OffsetBasedConsumptionStatusChecker statusChecker =
        new OffsetBasedConsumptionStatusChecker(instanceDataManager, consumingSegments);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup some SegmentDataManagers
    RealtimeSegmentDataManager segMngrA0 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrA1 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);

    //           latest ingested offset    latest stream offset
    // segA0               10                     15
    // segA1               100                    150
    // segB0           not setup yet              1500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(100));
    when(segMngrA0.getLatestStreamOffsetAtStartupTime()).thenReturn(new LongMsgOffset(15));
    when(segMngrA1.getLatestStreamOffsetAtStartupTime()).thenReturn(new LongMsgOffset(150));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup the remaining SegmentDataManager
    RealtimeSegmentDataManager segMngrB0 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    //           latest ingested offset     latest stream offset
    // segA0               20                      15
    // segA1               200                     150
    // segB0               1000                    1500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(200));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(1000));
    when(segMngrB0.getLatestStreamOffsetAtStartupTime()).thenReturn(new LongMsgOffset(1500));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 1);

    //           latest ingested offset     latest stream offset
    // segA0               30                      15
    // segA1               300                     150
    // segB0               2000                    1500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(30));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(300));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(2000));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }

  @Test
  public void segmentsBeingCommitted() {

    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Set<String> consumingSegments = ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    OffsetBasedConsumptionStatusChecker statusChecker =
        new OffsetBasedConsumptionStatusChecker(instanceDataManager, consumingSegments);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    RealtimeSegmentDataManager segMngrA0 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrA1 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrB0 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    //           latest ingested offset    latest stream offset
    // segA0              10                       15
    // segA1              100                      150
    // segB0              1000                     1500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(100));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(1000));
    when(segMngrA0.getLatestStreamOffsetAtStartupTime()).thenReturn(new LongMsgOffset(15));
    when(segMngrA1.getLatestStreamOffsetAtStartupTime()).thenReturn(new LongMsgOffset(150));
    when(segMngrB0.getLatestStreamOffsetAtStartupTime()).thenReturn(new LongMsgOffset(1500));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // segB0 is now committed; ImmutableSegmentDataManager is returned by table data manager
    ImmutableSegmentDataManager immSegMngrB0 = mock(ImmutableSegmentDataManager.class);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(immSegMngrB0);

    //           latest ingested offset     latest stream offset
    // segA0              20                        15
    // segA1              200                       150
    // segB0        committed at 1200               1500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(200));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }

  @Test
  public void cannotGetLatestStreamOffset() {

    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Set<String> consumingSegments = ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    OffsetBasedConsumptionStatusChecker statusChecker =
        new OffsetBasedConsumptionStatusChecker(instanceDataManager, consumingSegments);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    RealtimeSegmentDataManager segMngrA0 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrA1 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrB0 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    //           latest ingested offset    latest stream offset
    // segA0              10                       15
    // segA1              100                      150
    // segB0              1000                     null - could not get the latest offset from stream at startup
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(100));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(1000));
    when(segMngrA0.getLatestStreamOffsetAtStartupTime()).thenReturn(new LongMsgOffset(15));
    when(segMngrA1.getLatestStreamOffsetAtStartupTime()).thenReturn(new LongMsgOffset(150));
    when(segMngrB0.getLatestStreamOffsetAtStartupTime()).thenReturn(null);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    //           latest ingested offset     latest stream offset
    // segA0              20                        15
    // segA1              200                       150
    // segB0              2000                      null - could not get the latest offset from stream at startup
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(200));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(2000));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 1);

    //           latest ingested offset     latest stream offset
    // segA0              30                        15
    // segA1              300                       150
    // segB0              3000                      null - could not get the latest offset from stream at startup
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(30));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(300));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(3000));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 1);
  }
}
