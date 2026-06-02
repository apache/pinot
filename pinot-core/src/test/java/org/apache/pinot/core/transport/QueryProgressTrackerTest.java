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
package org.apache.pinot.core.transport;

import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryProgressStats;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;


public class QueryProgressTrackerTest {
  @Test
  public void testTracksActiveAndCompletedProgress() {
    QueryProgressTracker tracker = new QueryProgressTracker(true);
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();
    executionContext.addTotalSegmentsToProcess(3);
    executionContext.incrementProcessedSegments();

    tracker.register("query", executionContext);
    QueryProgressStats active = tracker.getProgressStats("query");
    assertEquals(active.getProcessedSegments(), 1);
    assertEquals(active.getTotalSegmentsToProcess(), 3);

    tracker.complete("query", executionContext, true);
    QueryProgressStats completed = tracker.getProgressStats("query");
    assertEquals(completed.getProcessedSegments(), 3);
    assertEquals(completed.getProcessedWorkUnits(), 3);
    assertEquals(completed.getProgressPercent(), 100.0);
  }

  @Test
  public void testCompletedProgressClampsProcessedCountsToKnownTotals() {
    QueryProgressTracker tracker = new QueryProgressTracker(true);
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();
    executionContext.addTotalSegmentsToProcess(1);
    executionContext.incrementProcessedSegments();
    executionContext.incrementProcessedSegments();

    tracker.register("query", executionContext);
    tracker.complete("query", executionContext, true);

    QueryProgressStats completed = tracker.getProgressStats("query");
    assertEquals(completed.getProcessedSegments(), 1);
    assertEquals(completed.getTotalSegmentsToProcess(), 1);
    assertEquals(completed.getProcessedWorkUnits(), 1);
    assertEquals(completed.getTotalWorkUnits(), 1);
  }

  @Test
  public void testLateCompletionDoesNotRemoveReusedQueryId() {
    QueryProgressTracker tracker = new QueryProgressTracker(true);
    QueryExecutionContext first = QueryExecutionContext.forSseTest();
    QueryExecutionContext second = QueryExecutionContext.forSseTest();
    second.addTotalSegmentsToProcess(5);

    tracker.register("query", first);
    tracker.register("query", second);
    tracker.complete("query", second, true);
    tracker.complete("query", first, false);

    QueryProgressStats progressStats = tracker.getProgressStats("query");
    assertEquals(progressStats.getProcessedSegments(), 5);
    assertEquals(progressStats.getTotalSegmentsToProcess(), 5);
  }

  @Test
  public void testDisabledTrackerIsNoOp() {
    QueryProgressTracker tracker = new QueryProgressTracker(false);
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();

    tracker.register("query", executionContext);
    tracker.complete("query", executionContext, true);

    assertFalse(tracker.isEnabled());
    assertNull(tracker.getProgressStats("query"));
  }
}
