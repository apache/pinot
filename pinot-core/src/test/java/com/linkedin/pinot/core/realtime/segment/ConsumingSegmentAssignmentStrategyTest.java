/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.realtime.segment;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test for verification of correct assignment of segments given a partition assignment
 */
public class ConsumingSegmentAssignmentStrategyTest {

  /**
   * Verifies that segments in segment assignment matches input list
   * Verifies that segment assignment is as expected given the partition assignment
   * @param newSegments
   * @param partitionAssignment
   * @param exception
   */
  private void verifyAssignment(List<String> newSegments, PartitionAssignment partitionAssignment, boolean exception,
      List<String> expectedSegmentsInSegmentAssignment) {

    ConsumingSegmentAssignmentStrategy strategy = new ConsumingSegmentAssignmentStrategy();
    try {
      Map<String, List<String>> segmentAssignment = strategy.assign(newSegments, partitionAssignment);
      Assert.assertFalse(exception);
      Assert.assertEquals(segmentAssignment.keySet().size(), expectedSegmentsInSegmentAssignment.size());
      Assert.assertTrue(segmentAssignment.keySet().containsAll(expectedSegmentsInSegmentAssignment));
      for (String segmentName : expectedSegmentsInSegmentAssignment) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        int partitionId = llcSegmentName.getPartitionId();
        List<String> expectedInstances = partitionAssignment.getInstancesListForPartition(String.valueOf(partitionId));
        List<String> assignedInstances = segmentAssignment.get(segmentName);
        Assert.assertEquals(expectedInstances.size(), assignedInstances.size());
        Assert.assertTrue(expectedInstances.containsAll(assignedInstances));
      }
    } catch (Exception e) {
      Assert.assertTrue(exception);
    }
  }

  /**
   * Tests various scenarios of how segment assignment will be invoked
   */
  @Test
  public void testAssign() {
    String tableName = "aTableToTest_REALTIME";

    List<String> newSegments;
    PartitionAssignment partitionAssignment;

    // empty new segments list
    newSegments = new ArrayList<>();
    partitionAssignment = new PartitionAssignment(tableName);
    verifyAssignment(newSegments, partitionAssignment, false, newSegments);

    // non empty new segments list, empty partition assignment
    LLCSegmentName llcSegmentName0 = new LLCSegmentName(tableName, 0, 0, System.currentTimeMillis());
    LLCSegmentName llcSegmentName1 = new LLCSegmentName(tableName, 1, 0, System.currentTimeMillis());
    LLCSegmentName llcSegmentName2 = new LLCSegmentName(tableName, 2, 0, System.currentTimeMillis());
    newSegments.add(llcSegmentName0.getSegmentName());
    newSegments.add(llcSegmentName1.getSegmentName());
    newSegments.add(llcSegmentName2.getSegmentName());
    verifyAssignment(newSegments, partitionAssignment, true, newSegments);

    // non empty new segments list, non empty partition assignment, partitions match
    partitionAssignment.addPartition("0", Lists.newArrayList("s1", "s2"));
    partitionAssignment.addPartition("1", Lists.newArrayList("s3", "s1"));
    partitionAssignment.addPartition("2", Lists.newArrayList("s2", "s3"));
    verifyAssignment(newSegments, partitionAssignment, false, newSegments);

    // partition for a segment missing in partition assignment
    LLCSegmentName llcSegmentName3 = new LLCSegmentName(tableName, 3, 0, System.currentTimeMillis());
    newSegments.add(llcSegmentName3.getSegmentName());
    verifyAssignment(newSegments, partitionAssignment, true, newSegments);

    // extra partitions in partition assignment than needed
    partitionAssignment.addPartition("3", Lists.newArrayList("s1", "s2"));
    partitionAssignment.addPartition("4", Lists.newArrayList("s3", "s1"));
    verifyAssignment(newSegments, partitionAssignment, false, newSegments);

    // non llc segment name
    List<String> goodSegments = Lists.newArrayList(newSegments);
    newSegments.add("nonLLCSegmentName");
    verifyAssignment(newSegments, partitionAssignment, false, goodSegments);
  }
}