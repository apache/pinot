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
package org.apache.pinot.segment.local.realtime;

import org.apache.pinot.segment.local.realtime.DecodeRetryTracker.Outcome;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class DecodeRetryTrackerTest {
  private static final long MINUTE = DecodeRetryTracker.WARN_INTERVAL_MS;

  @Test
  public void testReportsOnceThenRemindsEveryMinute() {
    DecodeRetryTracker tracker = new DecodeRetryTracker(Long.MAX_VALUE, 10_000);
    assertEquals(tracker.onRetriableFailure(0), Outcome.RETRY_AND_REPORT);
    assertEquals(tracker.onRetriableFailure(1_000), Outcome.RETRY);
    assertEquals(tracker.onRetriableFailure(MINUTE), Outcome.RETRY_AND_REMIND);
    assertEquals(tracker.onRetriableFailure(MINUTE + 1_000), Outcome.RETRY);
    assertEquals(tracker.getNumRetries(), 4);
    assertEquals(tracker.onMessageDone(), 4);
    assertFalse(tracker.isRetrying());
    // The next failure after a quiet minute is reported afresh, even though the outage has no limit
    assertEquals(tracker.onRetriableFailure(3 * MINUTE), Outcome.RETRY_AND_REPORT);
  }

  @Test
  public void testGivesUpOnceTheOutageReachesTheLimit() {
    DecodeRetryTracker tracker = new DecodeRetryTracker(5_000, 10_000);
    assertEquals(tracker.onRetriableFailure(0), Outcome.RETRY_AND_REPORT);
    assertEquals(tracker.onRetriableFailure(5_000), Outcome.GIVE_UP_AND_REPORT);
    assertFalse(tracker.isRetrying());
    // Failures less than the limit apart continue the outage, so they give up at once
    assertEquals(tracker.onRetriableFailure(6_000), Outcome.GIVE_UP);
    // A failure after a quiet period at least the limit long starts a new outage
    assertEquals(tracker.onRetriableFailure(12_000), Outcome.RETRY_AND_REPORT);
    assertEquals(tracker.getOutageDurationMs(13_000), 1_000);
  }

  @Test
  public void testZeroLimitNeverRetries() {
    DecodeRetryTracker tracker = new DecodeRetryTracker(0, 10_000);
    assertEquals(tracker.onRetriableFailure(0), Outcome.GIVE_UP);
    assertEquals(tracker.onRetriableFailure(1), Outcome.GIVE_UP);
  }

  @Test
  public void testBackoffDoublesUpToTheCapAndRestartsPerMessage() {
    DecodeRetryTracker tracker = new DecodeRetryTracker(Long.MAX_VALUE, 500);
    assertTrue(tracker.onRetriableFailure(0).isRetry());
    assertEquals(tracker.nextBackoffMs(), 100);
    assertEquals(tracker.nextBackoffMs(), 200);
    assertEquals(tracker.nextBackoffMs(), 400);
    assertEquals(tracker.nextBackoffMs(), 500);
    assertEquals(tracker.nextBackoffMs(), 500);
    tracker.onMessageDone();
    assertTrue(tracker.onRetriableFailure(1_000).isRetry());
    assertEquals(tracker.nextBackoffMs(), 100);
  }
}
