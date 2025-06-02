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
package org.apache.pinot.common.tier;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TierSegmentSelectorTest {

  @Test
  public void testTimeBasedSegmentSelector() {

    long now = System.currentTimeMillis();

    // offline segment
    String segmentName = "segment_0";
    String tableNameWithType = "myTable_OFFLINE";
    SegmentZKMetadata offlineSegmentZKMetadata = new SegmentZKMetadata(segmentName);
    offlineSegmentZKMetadata.setStartTime((now - TimeUnit.DAYS.toMillis(9)));
    offlineSegmentZKMetadata.setEndTime((now - TimeUnit.DAYS.toMillis(8)));
    offlineSegmentZKMetadata.setTimeUnit(TimeUnit.MILLISECONDS);

    // segment is 8 days old. selected by 7d
    TimeBasedTierSegmentSelector segmentSelector = new TimeBasedTierSegmentSelector("7d");
    Assert.assertEquals(segmentSelector.getType(), TierFactory.TIME_SEGMENT_SELECTOR_TYPE);
    Assert.assertEquals(segmentSelector.getSegmentAgeMillis(), TimeUnit.DAYS.toMillis(7));
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, offlineSegmentZKMetadata));

    // not selected by 30d
    segmentSelector = new TimeBasedTierSegmentSelector("30d");
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, offlineSegmentZKMetadata));

    // not selected by 10d
    segmentSelector = new TimeBasedTierSegmentSelector("240h");
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, offlineSegmentZKMetadata));

    // selected by 4h
    segmentSelector = new TimeBasedTierSegmentSelector("240m");
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, offlineSegmentZKMetadata));

    // realtime segment
    segmentName = "myTable__4__1__" + now;
    tableNameWithType = "myTable_REALTIME";
    SegmentZKMetadata realtimeSegmentZKMetadata = new SegmentZKMetadata(segmentName);
    realtimeSegmentZKMetadata.setStartTime(TimeUnit.MILLISECONDS.toHours(now - TimeUnit.DAYS.toMillis(3)));
    realtimeSegmentZKMetadata.setEndTime(TimeUnit.MILLISECONDS.toHours((now - TimeUnit.DAYS.toMillis(2))));
    realtimeSegmentZKMetadata.setTimeUnit(TimeUnit.HOURS);
    realtimeSegmentZKMetadata.setStatus(Status.DONE);

    // segment is 2 days old. not selected by 7d
    segmentSelector = new TimeBasedTierSegmentSelector("7d");
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, realtimeSegmentZKMetadata));

    // selected by 1d
    segmentSelector = new TimeBasedTierSegmentSelector("1d");
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, realtimeSegmentZKMetadata));

    // selected by 10h
    segmentSelector = new TimeBasedTierSegmentSelector("600m");
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, realtimeSegmentZKMetadata));

    // not selected by 5d
    segmentSelector = new TimeBasedTierSegmentSelector("120h");
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, realtimeSegmentZKMetadata));
  }

  @Test
  public void testRealTimeConsumingSegmentShouldNotBeRelocated() {

    long now = System.currentTimeMillis();

    String segmentName = "myTable__4__1__" + now;
    String tableNameWithType = "myTable_REALTIME";
    SegmentZKMetadata realtimeSegmentZKMetadata = new SegmentZKMetadata(segmentName);
    realtimeSegmentZKMetadata.setStatus(Status.IN_PROGRESS);

    TimeBasedTierSegmentSelector segmentSelector = new TimeBasedTierSegmentSelector("7d");
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, realtimeSegmentZKMetadata));
  }

  @Test
  public void testFixedSegmentSelector() {

    // offline segment
    String segmentName = "segment_0";
    String tableNameWithType = "myTable_OFFLINE";
    SegmentZKMetadata offlineSegmentZKMetadata = new SegmentZKMetadata(segmentName);

    FixedTierSegmentSelector segmentSelector = new FixedTierSegmentSelector(Set.of());
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, offlineSegmentZKMetadata));

    segmentSelector = new FixedTierSegmentSelector(Set.of("segment_1", "segment_2"));
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, offlineSegmentZKMetadata));

    segmentSelector = new FixedTierSegmentSelector(Set.of("SEGMENT_0", "segment_1", "segment_2"));
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, offlineSegmentZKMetadata));

    segmentSelector = new FixedTierSegmentSelector(Set.of("segment_0", "segment_1", "segment_2"));
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, offlineSegmentZKMetadata));

    segmentSelector = new FixedTierSegmentSelector(Set.of("segment %", "segment_2"));
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, offlineSegmentZKMetadata));
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, new SegmentZKMetadata("segment %")));

    // realtime segment
    segmentName = "myTable__4__1__" + 123456789;
    tableNameWithType = "myTable_REALTIME";
    SegmentZKMetadata realtimeSegmentZKMetadata = new SegmentZKMetadata(segmentName);
    realtimeSegmentZKMetadata.setStartTime(System.currentTimeMillis() - 10000);
    realtimeSegmentZKMetadata.setEndTime(System.currentTimeMillis() - 5000);
    realtimeSegmentZKMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    realtimeSegmentZKMetadata.setStatus(Status.DONE);

    segmentSelector = new FixedTierSegmentSelector(Set.of(segmentName, "foo", "bar"));
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, realtimeSegmentZKMetadata));

    realtimeSegmentZKMetadata.setStatus(Status.IN_PROGRESS);
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, realtimeSegmentZKMetadata));
  }
}
