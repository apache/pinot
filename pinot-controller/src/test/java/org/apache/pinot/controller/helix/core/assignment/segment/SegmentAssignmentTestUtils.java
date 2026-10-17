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
package org.apache.pinot.controller.helix.core.assignment.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;


public class SegmentAssignmentTestUtils {
  private SegmentAssignmentTestUtils() {
  }

  /// Returns a list of names.
  public static List<String> getNameList(String namePrefix, int numNames) {
    List<String> names = new ArrayList<>(numNames);
    for (int nameId = 0; nameId < numNames; nameId++) {
      names.add(namePrefix + nameId);
    }
    return names;
  }

  /// Returns an ideal state holding the given segment assignment (the same map, not a copy) and no partition
  /// assignment.
  private static IdealState toIdealState(Map<String, Map<String, String>> segmentAssignment) {
    IdealState idealState = new IdealState("testTable");
    idealState.getRecord().setMapFields(segmentAssignment);
    return idealState;
  }

  /// Assigns the segment with the given current segment assignment and no partition assignment.
  public static List<String> assignSegment(SegmentAssignment segmentAssignment, String segmentName,
      Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    return segmentAssignment.assignSegment(segmentName, toIdealState(currentAssignment), instancePartitionsMap);
  }

  /// Rebalances the table with the given current segment assignment and no partition assignment, and returns the
  /// rebalanced segment assignment.
  public static Map<String, Map<String, String>> rebalanceTable(SegmentAssignment segmentAssignment,
      Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, RebalanceConfig config) {
    return segmentAssignment.rebalanceTable(toIdealState(currentAssignment), instancePartitionsMap, sortedTiers,
        tierInstancePartitionsMap, config).getSegmentAssignment();
  }
}
