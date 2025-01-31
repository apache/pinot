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

import org.testng.annotations.Test;


public class PauselessRealtimeIngestionIntegrationTest extends BasePauselessRealtimeIngestionTest {

  @Override
  protected String getFailurePoint() {
    return null;  // No failure point for basic test
  }

  @Override
  protected int getExpectedSegmentsWithFailure() {
    return NUM_REALTIME_SEGMENTS;  // Always expect full segments
  }

  @Override
  protected int getExpectedZKMetadataWithFailure() {
    return NUM_REALTIME_SEGMENTS;  // Always expect full metadata
  }

  @Override
  protected long getCountStarResultWithFailure() {
    return DEFAULT_COUNT_STAR_RESULT;  // Always expect full count
  }

  @Override
  protected void injectFailure() {
    // Do nothing - no failure to inject
  }

  @Override
  protected void disableFailure() {
    // Do nothing - no failure to disable
  }

  @Test(description = "Ensure that all the segments are ingested, built and uploaded when pauseless consumption is "
      + "enabled")
  public void testSegmentAssignment() {
    testBasicSegmentAssignment();
  }
}
