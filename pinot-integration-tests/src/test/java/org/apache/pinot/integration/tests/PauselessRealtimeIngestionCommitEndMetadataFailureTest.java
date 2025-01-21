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
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;


public class PauselessRealtimeIngestionCommitEndMetadataFailureTest
    extends BasePauselessRealtimeIngestionTest {

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

    List<SegmentZKMetadata> segmentZKMetadataList = _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
    for (SegmentZKMetadata metadata : segmentZKMetadataList) {
      assertNull(metadata.getDownloadUrl());
    }

    runValidationAndVerify();
  }
}
