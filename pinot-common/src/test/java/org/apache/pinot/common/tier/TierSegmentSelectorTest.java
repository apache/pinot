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

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


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
    ZNRecord segmentZKMetadataZNRecord = offlineSegmentZKMetadata.toZNRecord();

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore
        .get(eq(ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName)), any(),
            anyInt())).thenReturn(segmentZKMetadataZNRecord);

    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

    // segment is 8 days old. selected by 7d
    TimeBasedTierSegmentSelector segmentSelector = new TimeBasedTierSegmentSelector(helixManager, "7d");
    Assert.assertEquals(segmentSelector.getType(), TierFactory.TIME_SEGMENT_SELECTOR_TYPE);
    Assert.assertEquals(segmentSelector.getSegmentAgeMillis(), TimeUnit.DAYS.toMillis(7));
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, segmentName));

    // not selected by 30d
    segmentSelector = new TimeBasedTierSegmentSelector(helixManager, "30d");
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, segmentName));

    // not selected by 10d
    segmentSelector = new TimeBasedTierSegmentSelector(helixManager, "240h");
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, segmentName));

    // selected by 4h
    segmentSelector = new TimeBasedTierSegmentSelector(helixManager, "240m");
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, segmentName));

    // realtime segment
    segmentName = "myTable__4__1__" + now;
    tableNameWithType = "myTable_REALTIME";
    SegmentZKMetadata realtimeSegmentZKMetadata = new SegmentZKMetadata(segmentName);
    realtimeSegmentZKMetadata.setStartTime(TimeUnit.MILLISECONDS.toHours(now - TimeUnit.DAYS.toMillis(3)));
    realtimeSegmentZKMetadata.setEndTime(TimeUnit.MILLISECONDS.toHours((now - TimeUnit.DAYS.toMillis(2))));
    realtimeSegmentZKMetadata.setTimeUnit(TimeUnit.HOURS);
    realtimeSegmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    realtimeSegmentZKMetadata.setNumReplicas(1);
    segmentZKMetadataZNRecord = realtimeSegmentZKMetadata.toZNRecord();

    when(propertyStore
        .get(eq(ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName)), any(),
            anyInt())).thenReturn(segmentZKMetadataZNRecord);

    helixManager = mock(HelixManager.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

    // segment is 2 days old. not selected by 7d
    segmentSelector = new TimeBasedTierSegmentSelector(helixManager, "7d");
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, segmentName));

    // selected by 1d
    segmentSelector = new TimeBasedTierSegmentSelector(helixManager, "1d");
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, segmentName));

    // selected by 10h
    segmentSelector = new TimeBasedTierSegmentSelector(helixManager, "600m");
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, segmentName));

    // not selected by 5d
    segmentSelector = new TimeBasedTierSegmentSelector(helixManager, "120h");
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, segmentName));
  }

  @Test
  public void testRealTimeConsumingSegmentShouldNotBeRelocated() {

    long now = System.currentTimeMillis();

    String segmentName = "myTable__4__1__" + now;
    String tableNameWithType = "myTable_REALTIME";
    SegmentZKMetadata realtimeSegmentZKMetadata = new SegmentZKMetadata(segmentName);
    realtimeSegmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);

    ZNRecord segmentZKMetadataZNRecord = realtimeSegmentZKMetadata.toZNRecord();

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore
            .get(eq(ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName)), any(),
                    anyInt())).thenReturn(segmentZKMetadataZNRecord);

    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

    TimeBasedTierSegmentSelector segmentSelector = new TimeBasedTierSegmentSelector(helixManager, "7d");
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, segmentName));
  }

  @Test
  public void testFixedSegmentSelector() {

    // offline segment
    String segmentName = "segment_0";
    String tableNameWithType = "myTable_OFFLINE";

    FixedTierSegmentSelector segmentSelector = new FixedTierSegmentSelector(null, Collections.emptySet());
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, segmentName));

    segmentSelector = new FixedTierSegmentSelector(null, Sets.newHashSet("segment_1", "segment_2"));
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, segmentName));

    segmentSelector = new FixedTierSegmentSelector(null, Sets.newHashSet("SEGMENT_0", "segment_1", "segment_2"));
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, segmentName));

    segmentSelector = new FixedTierSegmentSelector(null, Sets.newHashSet("segment_0", "segment_1", "segment_2"));
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, segmentName));

    segmentSelector = new FixedTierSegmentSelector(null, Sets.newHashSet("segment %", "segment_2"));
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, segmentName));
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, "segment %"));

    // realtime segment
    segmentName = "myTable__4__1__" + 123456789;
    tableNameWithType = "myTable_REALTIME";
    SegmentZKMetadata realtimeSegmentZKMetadata = new SegmentZKMetadata(segmentName);
    realtimeSegmentZKMetadata.setStartTime(System.currentTimeMillis() - 10000);
    realtimeSegmentZKMetadata.setEndTime(System.currentTimeMillis() - 5000);
    realtimeSegmentZKMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    realtimeSegmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    realtimeSegmentZKMetadata.setNumReplicas(1);
    ZNRecord segmentZKMetadataZNRecord = realtimeSegmentZKMetadata.toZNRecord();

    HelixManager helixManager = mock(HelixManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore
        .get(eq(ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName)), any(),
            anyInt())).thenReturn(segmentZKMetadataZNRecord);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    segmentSelector = new FixedTierSegmentSelector(helixManager, Sets.newHashSet(segmentName, "foo", "bar"));
    Assert.assertTrue(segmentSelector.selectSegment(tableNameWithType, segmentName));

    realtimeSegmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    segmentZKMetadataZNRecord = realtimeSegmentZKMetadata.toZNRecord();
    when(propertyStore
        .get(eq(ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName)), any(),
            anyInt())).thenReturn(segmentZKMetadataZNRecord);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    Assert.assertFalse(segmentSelector.selectSegment(tableNameWithType, segmentName));
  }
}
