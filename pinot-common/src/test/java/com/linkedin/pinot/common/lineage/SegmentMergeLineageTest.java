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
package com.linkedin.pinot.common.lineage;

import com.linkedin.pinot.common.exception.InvalidConfigException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentMergeLineageTest {

  @Test
  public void testSegmentMergeLineage() throws Exception {
    SegmentMergeLineage segmentMergeLineage = new SegmentMergeLineage("test_OFFLINE");
    String groupId1 = "G1";
    List<String> groupSegments1 = Arrays.asList(new String[]{"segment1", "segment2", "segment3"});
    segmentMergeLineage.addSegmentGroup(groupId1, groupSegments1, null);
    Assert.assertEquals(segmentMergeLineage.getSegmentsForGroup(groupId1), groupSegments1);

    String groupId2 = "G2";
    List<String> groupSegments2 = Arrays.asList(new String[]{"segment4", "segment5"});
    segmentMergeLineage.addSegmentGroup(groupId2, groupSegments2, null);
    Assert.assertEquals(segmentMergeLineage.getSegmentsForGroup(groupId2), groupSegments2);

    String groupId3 = "G3";
    List<String> groupSegments3 = Arrays.asList(new String[]{"segment6"});
    segmentMergeLineage.addSegmentGroup(groupId3, groupSegments3, Arrays.asList(new String[]{groupId1, groupId2}));
    Assert.assertEquals(segmentMergeLineage.getSegmentsForGroup(groupId3), groupSegments3);

    // Check available APIs
    Assert.assertEquals(segmentMergeLineage.getTableName(), "test_OFFLINE");
    Assert.assertEquals(segmentMergeLineage.getChildrenForGroup(groupId3),
        Arrays.asList(new String[]{groupId1, groupId2}));
    Assert.assertEquals(segmentMergeLineage.getAllGroupLevels(), Arrays.asList(new Integer[]{0, 1}));
    Assert.assertTrue(segmentMergeLineage.equals(SegmentMergeLineage.fromZNRecord(segmentMergeLineage.toZNRecord())));
    Assert.assertEquals(segmentMergeLineage.getGroupIdsForGroupLevel(0),
        Arrays.asList(new String[]{groupId1, groupId2}));
    Assert.assertEquals(segmentMergeLineage.getGroupIdsForGroupLevel(1),
        Arrays.asList(new String[]{groupId3}));
    validateSegmentGroup(segmentMergeLineage);

    // Check ZNRecord conversion
    Assert.assertEquals(segmentMergeLineage, SegmentMergeLineage.fromZNRecord(segmentMergeLineage.toZNRecord()));

    // Test removing groups
    segmentMergeLineage.removeSegmentGroup(groupId1);
    Assert.assertNull(segmentMergeLineage.getChildrenForGroup(groupId1));
    Assert.assertNull(segmentMergeLineage.getSegmentsForGroup(groupId1));
    Assert.assertFalse(segmentMergeLineage.getGroupIdsForGroupLevel(0).contains(groupId1));
  }

  @Test(expectedExceptions = InvalidConfigException.class)
  public void testUpdateWithDuplicateGroupId() throws Exception {
    SegmentMergeLineage segmentMergeLineage = new SegmentMergeLineage("test_OFFLINE");
    String groupId1 = "G1";
    List<String> groupSegments1 = Arrays.asList(new String[]{"segment1, segment2, segment3"});
    segmentMergeLineage.addSegmentGroup(groupId1, groupSegments1, null);
    Assert.assertEquals(segmentMergeLineage.getSegmentsForGroup(groupId1), groupSegments1);

    List<String> groupSegments2 = Arrays.asList(new String[]{"segment4, segment5, segment6"});
    segmentMergeLineage.addSegmentGroup(groupId1, groupSegments2, null);
  }

  private void validateSegmentGroup(SegmentMergeLineage segmentMergeLineage) {
    SegmentGroup segmentGroup = segmentMergeLineage.getMergeLineageRootSegmentGroup();
    for (SegmentGroup child : segmentGroup.getChildrenGroups()) {
      validateSegmentGroupNode(child, segmentMergeLineage);
    }
  }

  private void validateSegmentGroupNode(SegmentGroup segmentGroup, SegmentMergeLineage segmentMergeLineage) {
    String groupId = segmentGroup.getGroupId();
    Assert.assertEquals(segmentGroup.getSegments(), new HashSet<>(segmentMergeLineage.getSegmentsForGroup(groupId)));
    Assert.assertTrue(segmentMergeLineage.getGroupIdsForGroupLevel(segmentGroup.getGroupLevel()).contains(groupId));

    List<SegmentGroup> childrenGroups = segmentGroup.getChildrenGroups();
    if (childrenGroups != null) {
      List<String> childrenGroupIds = new ArrayList<>();
      for (SegmentGroup child : childrenGroups) {
        childrenGroupIds.add(child.getGroupId());
      }
      Assert.assertEquals(childrenGroupIds, segmentMergeLineage.getChildrenForGroup(groupId));

      for (SegmentGroup child : segmentGroup.getChildrenGroups()) {
        validateSegmentGroupNode(child, segmentMergeLineage);
      }
    }
  }
}
