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
package org.apache.pinot.broker.routing.selector;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.util.FakePropertyStore;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.lineage.SegmentMergeLineage;
import org.apache.pinot.common.lineage.SegmentMergeLineageAccessHelper;
import org.apache.pinot.common.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MergedSegmentSelectorTest {
  private static final String TEST_TABLE_NAME = "test_OFFLINE";

  @Test
  public void testMergedSegmentSelector()
      throws Exception {
    MergedSegmentSelector segmentSelector = new MergedSegmentSelector();

    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TEST_TABLE_NAME).build();
    ZkHelixPropertyStore<ZNRecord> fakePropertyStore = new FakePropertyStore();
    segmentSelector.init(tableConfig, fakePropertyStore);

    // Add G0 (segment0), G1 (segment1), G2 (segment2), G3 (segment3) that represent 4 base segments
    SegmentMergeLineage segmentMergeLineage = new SegmentMergeLineage(TEST_TABLE_NAME);
    for (int i = 0; i < 4; i++) {
      String groupId = "G" + i;
      segmentMergeLineage.addSegmentGroup(groupId, Arrays.asList(new String[]{"segment" + i}), null);
    }

    // Update segment merge lineage in the property store and update segment selector
    SegmentMergeLineageAccessHelper.writeSegmentMergeLineage(fakePropertyStore, segmentMergeLineage, 0);
    segmentSelector.computeOnExternalViewChange();

    // Test the result of segment selection
    testSegmentSelector(segmentSelector, new String[]{"segment0", "segment1", "segment2", "segment3"},
        new String[]{"segment0", "segment1", "segment2", "segment3"});

    // Add G4 (merged_0, merged_1) that was merged from G0, G1, G2
    String mergedGroupId = "G4";
    List<String> childrenGroups = Arrays.asList(new String[]{"G0", "G1", "G2"});
    segmentMergeLineage
        .addSegmentGroup(mergedGroupId, Arrays.asList(new String[]{"merged0", "merged1"}), childrenGroups);

    // Update segment merge lineage in the property store and update segment selector
    SegmentMergeLineageAccessHelper.writeSegmentMergeLineage(fakePropertyStore, segmentMergeLineage, 0);
    segmentSelector.computeOnExternalViewChange();

    // Test the result of segment selection
    testSegmentSelector(segmentSelector,
        new String[]{"segment0", "segment1", "segment2", "segment3", "merged0", "merged1"},
        new String[]{"merged0", "merged1", "segment3"});

    // Test the result of segment selection when the external view does not have one of merged segments
    testSegmentSelector(segmentSelector, new String[]{"segment0", "segment1", "segment2", "segment3", "merged0"},
        new String[]{"segment0", "segment1", "segment2", "segment3"});

    // Test the result of segment selection when one of the base segment is missing.
    testSegmentSelector(segmentSelector, new String[]{"segment0", "segment2", "segment3", "merged0", "merged1"},
        new String[]{"merged0", "merged1", "segment3"});

    // Add G5 (segment4)
    segmentMergeLineage.addSegmentGroup("G5", Arrays.asList(new String[]{"segment4"}), null);

    // Update segment merge lineage in the property store and update segment selector
    SegmentMergeLineageAccessHelper.writeSegmentMergeLineage(fakePropertyStore, segmentMergeLineage, 0);
    segmentSelector.computeOnExternalViewChange();

    // Test the result of segment selection
    testSegmentSelector(segmentSelector,
        new String[]{"segment0", "segment1", "segment2", "segment3", "segment4", "merged0", "merged1"},
        new String[]{"merged0", "merged1", "segment3", "segment4"});

    // Add G6 (merged3) that is merged from G3 (segment3), G5 (segment4)
    segmentMergeLineage
        .addSegmentGroup("G6", Arrays.asList(new String[]{"merged2"}), Arrays.asList(new String[]{"G3", "G5"}));

    // Update segment merge lineage in the property store and update segment selector
    SegmentMergeLineageAccessHelper.writeSegmentMergeLineage(fakePropertyStore, segmentMergeLineage, 0);
    segmentSelector.computeOnExternalViewChange();

    testSegmentSelector(segmentSelector,
        new String[]{"segment0", "segment1", "segment2", "segment3", "segment4", "merged0", "merged1", "merged2"},
        new String[]{"merged0", "merged1", "merged2"});

    testSegmentSelector(segmentSelector,
        new String[]{"segment0", "segment1", "segment2", "segment3", "segment4", "merged1", "merged2"},
        new String[]{"segment0", "segment1", "segment2", "merged2"});

    testSegmentSelector(segmentSelector,
        new String[]{"segment0", "segment1", "segment2", "segment3", "segment4", "merged1"},
        new String[]{"segment0", "segment1", "segment2", "segment3", "segment4"});

    // Add G7 (merged4) that is merged from G3, G6
    segmentMergeLineage
        .addSegmentGroup("G7", Arrays.asList(new String[]{"merged3"}), Arrays.asList(new String[]{"G4", "G6"}));

    // Update segment merge lineage in the property store and update segment selector
    SegmentMergeLineageAccessHelper.writeSegmentMergeLineage(fakePropertyStore, segmentMergeLineage, 0);
    segmentSelector.computeOnExternalViewChange();

    testSegmentSelector(segmentSelector,
        new String[]{"segment0", "segment1", "segment2", "segment3", "segment4", "merged0", "merged1", "merged2", "merged3"},
        new String[]{"merged3"});

    testSegmentSelector(segmentSelector,
        new String[]{"segment0", "segment1", "segment2", "segment3", "segment4", "merged0", "merged2", "merged3"},
        new String[]{"merged3"});

    testSegmentSelector(segmentSelector,
        new String[]{"segment0", "segment1", "segment2", "segment3", "segment4", "merged0", "merged1", "merged2"},
        new String[]{"merged0", "merged1", "merged2"});

    testSegmentSelector(segmentSelector,
        new String[]{"segment0", "segment1", "segment2", "segment3", "segment4", "merged0", "merged2"},
        new String[]{"merged2", "segment0", "segment1", "segment2"});
  }

  private void testSegmentSelector(SegmentSelector segmentSelector, String[] segmentsToQuery, String[] expectedResult) {
    Set<String> segmentsToQuerySet = new HashSet<>(Arrays.asList(segmentsToQuery));
    Set<String> selectedSegments = segmentSelector.selectSegments(null, segmentsToQuerySet);
    Assert.assertEquals(selectedSegments, new HashSet<>(Arrays.asList(expectedResult)));
  }
}
