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
package org.apache.pinot.integration.tests.realtime.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.utils.CommonConstants;

import static org.testng.Assert.assertEquals;


public class PauselessRealtimeTestUtils {

  private PauselessRealtimeTestUtils() {
  }

  public static void verifyIdealState(String tableName, int numSegmentsExpected, HelixManager helixManager) {
    IdealState idealState = HelixHelper.getTableIdealState(helixManager, tableName);
    Map<String, Map<String, String>> segmentAssignment = idealState.getRecord().getMapFields();
    assertEquals(segmentAssignment.size(), numSegmentsExpected);
  }

  /// Marks all current segments of the given table as exceeding the max segment completion time, making them
  /// immediately eligible for repair by the next validation run while keeping their in-flight commit attempts
  /// rejected. Segments created after this call are not affected. No-op when the cluster is not started with a
  /// [FailureInjectingPinotLLCRealtimeSegmentManager].
  public static void forceExpireSegments(PinotHelixResourceManager helixResourceManager, String tableNameWithType) {
    PinotLLCRealtimeSegmentManager realtimeSegmentManager = helixResourceManager.getRealtimeSegmentManager();
    if (realtimeSegmentManager instanceof FailureInjectingPinotLLCRealtimeSegmentManager) {
      List<String> segmentNames = new ArrayList<>();
      for (SegmentZKMetadata segmentZKMetadata : helixResourceManager.getSegmentsZKMetadata(tableNameWithType)) {
        segmentNames.add(segmentZKMetadata.getSegmentName());
      }
      ((FailureInjectingPinotLLCRealtimeSegmentManager) realtimeSegmentManager).forceExpireSegments(segmentNames);
    }
  }

  /// Clears the force-expired segments so that segments re-activated by the repair can complete normally.
  public static void clearForceExpiredSegments(PinotHelixResourceManager helixResourceManager) {
    PinotLLCRealtimeSegmentManager realtimeSegmentManager = helixResourceManager.getRealtimeSegmentManager();
    if (realtimeSegmentManager instanceof FailureInjectingPinotLLCRealtimeSegmentManager) {
      ((FailureInjectingPinotLLCRealtimeSegmentManager) realtimeSegmentManager).clearForceExpiredSegments();
    }
  }

  public static boolean assertUrlPresent(List<SegmentZKMetadata> segmentZKMetadataList) {
    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      if (segmentZKMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.COMMITTING
          && segmentZKMetadata.getDownloadUrl() == null) {
        return false;
      }
    }
    return true;
  }

  public static void compareZKMetadataForSegments(List<SegmentZKMetadata> segmentsZKMetadata,
      List<SegmentZKMetadata> segmentsZKMetadata1) {
    Map<String, SegmentZKMetadata> segmentZKMetadataMap =
        getPartitionSegmentNumberToMetadataMap(segmentsZKMetadata);
    Map<String, SegmentZKMetadata> segmentZKMetadataMap1 =
        getPartitionSegmentNumberToMetadataMap(segmentsZKMetadata1);

    segmentZKMetadataMap.forEach((segmentKey, segmentZKMetadata) -> {
      SegmentZKMetadata segmentZKMetadata1 = segmentZKMetadataMap1.get(segmentKey);
      compareSegmentZkMetadata(segmentZKMetadata, segmentZKMetadata1);
    });
  }

  private static Map<String, SegmentZKMetadata> getPartitionSegmentNumberToMetadataMap(
      List<SegmentZKMetadata> segmentsZKMetadata) {
    Map<String, SegmentZKMetadata> segmentZKMetadataMap = new HashMap<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentZKMetadata.getSegmentName());
      String segmentKey = llcSegmentName.getPartitionGroupId() + "_" + llcSegmentName.getSequenceNumber();
      segmentZKMetadataMap.put(segmentKey, segmentZKMetadata);
    }
    return segmentZKMetadataMap;
  }

  private static void compareSegmentZkMetadata(SegmentZKMetadata segmentZKMetadata,
      SegmentZKMetadata segmentZKMetadata1) {
    if (segmentZKMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.IN_PROGRESS) {
      return;
    }
    assertEquals(segmentZKMetadata.getStatus(), segmentZKMetadata1.getStatus());
    assertEquals(segmentZKMetadata.getStartOffset(), segmentZKMetadata1.getStartOffset());
    assertEquals(segmentZKMetadata.getEndOffset(), segmentZKMetadata1.getEndOffset());
    assertEquals(segmentZKMetadata.getTotalDocs(), segmentZKMetadata1.getTotalDocs());
    assertEquals(segmentZKMetadata.getStartTimeMs(), segmentZKMetadata1.getStartTimeMs());
    assertEquals(segmentZKMetadata.getEndTimeMs(), segmentZKMetadata1.getEndTimeMs());
  }
}
