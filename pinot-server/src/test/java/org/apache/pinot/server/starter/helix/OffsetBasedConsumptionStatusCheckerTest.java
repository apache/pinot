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
import java.util.function.Supplier;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class OffsetBasedConsumptionStatusCheckerTest {

  @Test
  public void regularCase() {

    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Supplier<Set<String>> consumingSegmentProvider = () -> ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    OffsetBasedConsumptionStatusChecker statusChecker =
        new OffsetBasedConsumptionStatusChecker(instanceDataManager, consumingSegmentProvider);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    LLRealtimeSegmentDataManager segMngrA0 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrA1 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrB0 = mock(LLRealtimeSegmentDataManager.class);
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
    when(segMngrA0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(15));
    when(segMngrA1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(150));
    when(segMngrB0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(1500));
    assertFalse(statusChecker.haveAllConsumingSegmentsReachedStreamLatestOffset());

    //           latest ingested offset     latest stream offset     already observed latest stream offset
    // segA0              20                       25                         15
    // segA1              200                      250                        150
    // segB0              2000                     2500                       1500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(200));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(2000));
    when(segMngrA0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(25));
    when(segMngrA1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(250));
    when(segMngrB0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(2500));
    assertTrue(statusChecker.haveAllConsumingSegmentsReachedStreamLatestOffset());
  }

  @Test
  public void dataMangersBeingSetup() {

    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Supplier<Set<String>> consumingSegmentProvider = () -> ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);

    OffsetBasedConsumptionStatusChecker statusChecker =
        new OffsetBasedConsumptionStatusChecker(instanceDataManager, consumingSegmentProvider);

    // TableDataManager is not set up yet
    assertFalse(statusChecker.haveAllConsumingSegmentsReachedStreamLatestOffset());

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup some SegmentDataManagers
    LLRealtimeSegmentDataManager segMngrA0 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrA1 = mock(LLRealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);

    //           latest ingested offset    latest stream offset
    // segA0               10                     15
    // segA1               100                    150
    // segB0           not setup yet              1500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(100));
    when(segMngrA0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(15));
    when(segMngrA1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(150));
    assertFalse(statusChecker.haveAllConsumingSegmentsReachedStreamLatestOffset());

    // setup the remaining SegmentDataManager
    LLRealtimeSegmentDataManager segMngrB0 = mock(LLRealtimeSegmentDataManager.class);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    //           latest ingested offset     latest stream offset     already observed latest stream offset
    // segA0           20                           25                         15
    // segA1           200                          250                        150
    // segB0           2000                         2500                       -
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(200));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(2000));
    when(segMngrA0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(25));
    when(segMngrA1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(250));
    when(segMngrB0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(2500));
    assertFalse(statusChecker.haveAllConsumingSegmentsReachedStreamLatestOffset());

    //           latest ingested offset     latest stream offset     already observed latest stream offset
    // segA0              30                        35                         15
    // segA1              300                       350                        150
    // segB0              3000                      3500                       2500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(30));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(300));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(3000));
    when(segMngrA0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(35));
    when(segMngrA1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(350));
    when(segMngrB0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(3500));
    assertTrue(statusChecker.haveAllConsumingSegmentsReachedStreamLatestOffset());
  }

  @Test
  public void segmentsBeingCommitted() {

    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Supplier<Set<String>> consumingSegmentProvider = () -> ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    OffsetBasedConsumptionStatusChecker statusChecker =
        new OffsetBasedConsumptionStatusChecker(instanceDataManager, consumingSegmentProvider);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    LLRealtimeSegmentDataManager segMngrA0 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrA1 = mock(LLRealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);

    // segB0 is committed; ImmutableSegmentDataManager is returned by table data manager
    ImmutableSegmentDataManager segMngrB0 = mock(ImmutableSegmentDataManager.class);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    //           latest ingested offset    latest stream offset
    // segA0              10                       15
    // segA1              100                      150
    // segB0              1000                     1500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(100));
    when(segMngrA0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(15));
    when(segMngrA1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(150));
    assertFalse(statusChecker.haveAllConsumingSegmentsReachedStreamLatestOffset());

    // segB1 replaces segB0 in set of consuming segments
    String segB1 = "tableB__0__1__123Z";
    statusChecker.setConsumingSegmentFinder(() -> ImmutableSet.of(segA0, segA1, segB1));
    LLRealtimeSegmentDataManager segMngrB1 = mock(LLRealtimeSegmentDataManager.class);
    when(tableDataManagerB.acquireSegment(segB1)).thenReturn(segMngrB1);

    //           latest ingested offset     latest stream offset     already observed latest stream offset
    // segA0              20                        25                         15
    // segA1              200                       250                        150
    // segB0              1000                      2500                       -
    // segB1              2000                      2500                       -
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(200));
    when(segMngrB1.getCurrentOffset()).thenReturn(new LongMsgOffset(2000));
    when(segMngrA0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(25));
    when(segMngrA1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(250));
    when(segMngrB1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(2500));
    assertFalse(statusChecker.haveAllConsumingSegmentsReachedStreamLatestOffset());

    //           latest ingested offset     latest stream offset     already observed latest stream offset
    // segA0              30                        35                         15
    // segA1              300                       350                        150
    // segB0              1000                      3500                       -
    // segB1              3000                      3500                       2500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(30));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(300));
    when(segMngrB1.getCurrentOffset()).thenReturn(new LongMsgOffset(3000));
    when(segMngrA0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(35));
    when(segMngrA1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(350));
    when(segMngrB1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(3500));
    assertTrue(statusChecker.haveAllConsumingSegmentsReachedStreamLatestOffset());
  }

  @Test
  public void cannotGetLatestStreamOffset() {

    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Supplier<Set<String>> consumingSegmentProvider = () -> ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    OffsetBasedConsumptionStatusChecker statusChecker =
        new OffsetBasedConsumptionStatusChecker(instanceDataManager, consumingSegmentProvider);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    LLRealtimeSegmentDataManager segMngrA0 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrA1 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrB0 = mock(LLRealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    //           latest ingested offset    latest stream offset
    // segA0              10                       15
    // segA1              100                      150
    // segB0              1000                     null - transiently cannot get the latest offset from stream
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(100));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(1000));
    when(segMngrA0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(15));
    when(segMngrA1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(150));
    when(segMngrB0.fetchLatestStreamOffset()).thenReturn(null);
    assertFalse(statusChecker.haveAllConsumingSegmentsReachedStreamLatestOffset());

    //           latest ingested offset     latest stream offset     already observed latest stream offset
    // segA0              20                        25                         15
    // segA1              200                       250                        150
    // segB0              2000                      2500                       -
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(200));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(2000));
    when(segMngrA0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(25));
    when(segMngrA1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(250));
    when(segMngrB0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(2500));
    assertFalse(statusChecker.haveAllConsumingSegmentsReachedStreamLatestOffset());

    //           latest ingested offset     latest stream offset     already observed latest stream offset
    // segA0              30                        35                         15
    // segA1              300                       350                        150
    // segB0              3000                      3500                       2500
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(30));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(300));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(3000));
    when(segMngrA0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(35));
    when(segMngrA1.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(350));
    when(segMngrB0.fetchLatestStreamOffset()).thenReturn(new LongMsgOffset(3500));
    assertTrue(statusChecker.haveAllConsumingSegmentsReachedStreamLatestOffset());
  }
}