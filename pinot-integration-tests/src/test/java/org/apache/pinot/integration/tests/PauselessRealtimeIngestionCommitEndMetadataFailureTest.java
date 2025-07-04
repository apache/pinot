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
package org.apache.pinot.integration.tests;

import java.util.List;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.util.FailureInjectionUtils;
import org.apache.pinot.integration.tests.realtime.utils.PauselessRealtimeTestUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;


public class PauselessRealtimeIngestionCommitEndMetadataFailureTest
    extends BasePauselessRealtimeIngestionTest {

  // The total number of real-time segments is NUM_REALTIME_SEGMENTS.
  // Two segments are in the CONSUMING state, meaning they are still ingesting data.
  // The remaining segments (NUM_REALTIME_SEGMENTS - 2) are in the committing state,
  private static final int NUM_REALTIME_SEGMENTS_IN_COMMITTING_STATE = 46;

  @Override
  protected String getFailurePoint() {
    return FailureInjectionUtils.FAULT_BEFORE_COMMIT_END_METADATA;
  }

  @Override
  protected int getExpectedSegmentsWithFailure() {
    return NUM_REALTIME_SEGMENTS;  // All segments still appear in ideal state
  }

  @Override
  protected int getExpectedZKMetadataWithFailure() {
    return NUM_REALTIME_SEGMENTS;
  }

  @Override
  protected long getCountStarResultWithFailure() {
    return DEFAULT_COUNT_STAR_RESULT;
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    PauselessRealtimeTestUtils.verifyIdealState(tableNameWithType, NUM_REALTIME_SEGMENTS, _helixManager);

    // the setup() function only waits till all the documents have been loaded
    // this has lead to race condition in the commit protocol and validation manager run leading to test failures
    // checking the completion of the commit protocol i.e. segment marked COMMITTING before triggering
    // validation manager in the tests prevents this.

    // All the segments should be in COMMITTING state for pauseless table having commitEndMetadata failure
    TestUtils.waitForCondition((aVoid) -> {
      List<SegmentZKMetadata> segmentZKMetadataList =
          _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
      return segmentZKMetadataList.stream()
          .filter(
              segmentZKMetadata -> segmentZKMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.COMMITTING)
          .count() == NUM_REALTIME_SEGMENTS_IN_COMMITTING_STATE;
    }, 1000, 100000, "Some segments are still IN_PROGRESS");

    List<SegmentZKMetadata> segmentZKMetadataList = _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
    for (SegmentZKMetadata metadata : segmentZKMetadataList) {
      assertNull(metadata.getDownloadUrl());
    }

    runValidationAndVerify();
  }
}
