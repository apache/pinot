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

import org.apache.pinot.controller.helix.core.util.FailureInjectionUtils;
import org.testng.annotations.Test;


public class PauselessRealtimeIngestionNewSegmentMetadataCreationFailureTest
    extends BasePauselessRealtimeIngestionTest {

  private static final int NUM_REALTIME_SEGMENTS_WITH_FAILURE = 2;
  private static final int NUM_REALTIME_SEGMENTS_ZK_METADATA_WITH_FAILURE = 2;
  private static final long DEFAULT_COUNT_STAR_RESULT_WITH_FAILURE = 5000;

  @Override
  protected String getFailurePoint() {
    return FailureInjectionUtils.FAULT_BEFORE_NEW_SEGMENT_METADATA_CREATION;
  }

  @Override
  protected int getExpectedSegmentsWithFailure() {
    return NUM_REALTIME_SEGMENTS_WITH_FAILURE;
  }

  @Override
  protected int getExpectedZKMetadataWithFailure() {
    return NUM_REALTIME_SEGMENTS_ZK_METADATA_WITH_FAILURE;
  }

  @Override
  protected long getCountStarResultWithFailure() {
    return DEFAULT_COUNT_STAR_RESULT_WITH_FAILURE;
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    runValidationAndVerify();
  }
}
