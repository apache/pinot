/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.broker.routing.selector;

import com.linkedin.pinot.broker.util.FakePropertyStore;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.lineage.SegmentMergeLineage;
import com.linkedin.pinot.common.lineage.SegmentMergeLineageAccessHelper;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MergedSegmentSelectorTest {
  private static final String TEST_TABLE_NAME = "test_OFFLINE";

  @Test
  public void testMergedSegmentSelector() throws Exception {
    MergedSegmentSelector segmentSelector = new MergedSegmentSelector();

    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TEST_TABLE_NAME).build();
    ZkHelixPropertyStore<ZNRecord> fakePropertyStore = new FakePropertyStore();
    segmentSelector.init(tableConfig, fakePropertyStore);

    // Add G0 (segment0), G1 (segment1), G2 (segment2) that represent 3 base segments
    SegmentMergeLineage segmentMergeLineage = new SegmentMergeLineage(TEST_TABLE_NAME);
    List<String> childrenGroups = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      String groupId = "G" + i;
      segmentMergeLineage.addSegmentGroup(groupId, Arrays.asList(new String[]{"segment" + i}), null);
      childrenGroups.add(groupId);
    }

    // Update segment merge lineage in the property store and update segment selector
    SegmentMergeLineageAccessHelper.writeSegmentMergeLineage(fakePropertyStore, segmentMergeLineage, 0);
    segmentSelector.computeOnExternalViewChange();

    // Test the result of segment selection
    Set<String> segmentsToQuery = new HashSet<>(Arrays.asList(
        new String[]{"segment0", "segment1", "segment2", "segment3"}));
    Set<String> selectedSegments = segmentSelector.selectSegments(null, segmentsToQuery);
    Assert.assertEquals(selectedSegments,
        new HashSet<>(Arrays.asList(new String[]{"segment0", "segment1", "segment2", "segment3"})));

    // Add G3 (merged_0, merged_1) that was merged from G0, G1, G2
    String mergedGroupId = "G3";
    segmentMergeLineage.addSegmentGroup(mergedGroupId,
        Arrays.asList(new String[]{"merged0", "merged1"}), childrenGroups);

    // Update segment merge lineage in the property store and update segment selector
    SegmentMergeLineageAccessHelper.writeSegmentMergeLineage(fakePropertyStore, segmentMergeLineage, 0);
    segmentSelector.computeOnExternalViewChange();

    // Test the result of segment selection
    segmentsToQuery = new HashSet<>(Arrays.asList(
        new String[]{"segment0", "segment1", "segment2", "merged0", "merged1"}));
    selectedSegments = segmentSelector.selectSegments(null, segmentsToQuery);
    Assert.assertEquals(selectedSegments,
        new HashSet<>(Arrays.asList(new String[]{"merged0", "merged1"})));

    // Test the result of segment selection when the external view does not have one of merged segments
    segmentsToQuery = new HashSet<>(Arrays.asList(
        new String[]{"segment0", "segment1", "segment2", "merged0"}));
    selectedSegments = segmentSelector.selectSegments(null, segmentsToQuery);
    Assert.assertEquals(selectedSegments,
        new HashSet<>(Arrays.asList(new String[]{"segment0", "segment1", "segment2"})));

    // Add G4 (segment4)
    segmentMergeLineage.addSegmentGroup("G4", Arrays.asList(new String[]{"segment3"}), null);
    // Update segment merge lineage in the property store and update segment selector
    SegmentMergeLineageAccessHelper.writeSegmentMergeLineage(fakePropertyStore, segmentMergeLineage, 0);
    segmentSelector.computeOnExternalViewChange();

    // Test the result of segment selection
    segmentsToQuery = new HashSet<>(Arrays.asList(
        new String[]{"segment0", "segment1", "segment2", "segment3", "merged0", "merged1"}));
    selectedSegments = segmentSelector.selectSegments(null, segmentsToQuery);
    Assert.assertEquals(selectedSegments,
        new HashSet<>(Arrays.asList(new String[]{"merged0", "merged1", "segment3"})));
  }
}
