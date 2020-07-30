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
package org.apache.pinot.broker.routing.segmentselector;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.util.FakePropertyStore;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.lineage.SegmentLineageUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;


public class SegmentPreSelectorTest {

  @Test
  public void testSegmentLineageBasedSegmentPreSelector() {
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();
    String offlineTableName = "testTable_OFFLINE";
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, String> onlineInstanceStateMap = Collections.singletonMap("server", ONLINE);
    Set<String> onlineSegments = new HashSet<>();

    int numOfflineSegments = 5;
    for (int i = 0 ; i< numOfflineSegments; i++ ) {
      String segmentName = "segment_" + i;
      externalView.setStateMap(segmentName, onlineInstanceStateMap);
      onlineSegments.add(segmentName);
    }

    // Initialize segment pre-selector
    SegmentLineageBasedSegmentPreSelector segmentPreSelector =
        new SegmentLineageBasedSegmentPreSelector(offlineTableName, propertyStore);

    // Check the case where there's no segment lineage metadata for a table
    Assert.assertEquals(segmentPreSelector.preSelect(new HashSet<>(onlineSegments)), onlineSegments);

    // Update the segment lineage
    SegmentLineage segmentLineage = new SegmentLineage(offlineTableName);
    String lineageEntryId = SegmentLineageUtils.generateLineageEntryId();
    segmentLineage.addLineageEntry(lineageEntryId,
        new LineageEntry(Arrays.asList("segment_0", "segment_1", "segment_2"), Arrays.asList("merged_0", "merged_1"),
            LineageEntryState.IN_PROGRESS, System.currentTimeMillis()));
    SegmentLineageAccessHelper.writeSegmentLineage(propertyStore, segmentLineage, -1);

    Assert.assertEquals(segmentPreSelector.preSelect(new HashSet<>(onlineSegments)),
        new HashSet<>(Arrays.asList("segment_0", "segment_1", "segment_2", "segment_3", "segment_4")));

    // merged_0 is added
    externalView.setStateMap("merged_0", onlineInstanceStateMap);
    onlineSegments.add("merged_0");
    Assert.assertEquals(segmentPreSelector.preSelect(new HashSet<>(onlineSegments)),
        new HashSet<>(Arrays.asList("segment_0", "segment_1", "segment_2", "segment_3", "segment_4")));

    // merged_1 is added
    externalView.setStateMap("merged_1", onlineInstanceStateMap);
    onlineSegments.add("merged_1");
    Assert.assertEquals(segmentPreSelector.preSelect(new HashSet<>(onlineSegments)),
        new HashSet<>(Arrays.asList("segment_0", "segment_1", "segment_2", "segment_3", "segment_4")));

    // Lineage entry gets updated to "COMPLETED"
    LineageEntry lineageEntry = segmentLineage.getLineageEntry(lineageEntryId);
    segmentLineage.updateLineageEntry(lineageEntryId,
        new LineageEntry(lineageEntry.getSegmentsFrom(), lineageEntry.getSegmentsTo(), LineageEntryState.COMPLETED,
            System.currentTimeMillis()));
    SegmentLineageAccessHelper.writeSegmentLineage(propertyStore, segmentLineage, -1);

    Assert.assertEquals(segmentPreSelector.preSelect(new HashSet<>(onlineSegments)),
        new HashSet<>(Arrays.asList("segment_3", "segment_4", "merged_0", "merged_1")));
  }
}
