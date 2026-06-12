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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


/// Verifies that MV segment names are unique across task retries for the same window.
///
/// Bug: before the fix, names were `table_startMs_endMs_segIdx`. Two attempts of the
/// same window produced identical names. When attempt-1 uploaded segments but did not finish
/// `startReplaceSegments`, attempt-2 would regenerate the same names and the controller
/// would reject the new lineage entry because the segment names already existed.
///
/// Fix: names now include a per-invocation `attemptId` (a fresh UUID generated at the
/// start of each `executeTask` call): `table_startMs_endMs_attemptId_segIdx`.
/// The Helix subtask id is NOT used here because Helix reuses the same subtask id across retries
/// of a job, which would reproduce identical names and trigger the same collision.
public class MaterializedViewSegmentNameTest {

  private static final String TABLE = "orders_mv_OFFLINE";
  private static final long START_MS = 1_700_000_000_000L;
  private static final long END_MS = 1_700_086_400_000L;

  @Test
  public void testSegmentNameIncludesAttemptId() {
    String attemptId = "550e8400-e29b-41d4-a716-446655440000";
    String name = MaterializedViewTaskExecutor.buildSegmentName(TABLE, START_MS, END_MS, attemptId, 0);
    assertTrue(name.contains(attemptId),
        "Segment name must contain attemptId to be unique across retries: " + name);
  }

  @Test
  public void testDifferentAttemptIdsProduceDifferentNames() {
    String name1 = MaterializedViewTaskExecutor.buildSegmentName(TABLE, START_MS, END_MS, "uuid-attempt-1", 0);
    String name2 = MaterializedViewTaskExecutor.buildSegmentName(TABLE, START_MS, END_MS, "uuid-attempt-2", 0);
    assertNotEquals(name1, name2,
        "Same window, same segIdx but different attemptIds must produce different names");
  }

  @Test
  public void testSameAttemptIdSameWindowProducesSameName() {
    String attemptId = "550e8400-e29b-41d4-a716-446655440000";
    String name1 = MaterializedViewTaskExecutor.buildSegmentName(TABLE, START_MS, END_MS, attemptId, 0);
    String name2 = MaterializedViewTaskExecutor.buildSegmentName(TABLE, START_MS, END_MS, attemptId, 0);
    assertEquals(name1, name2,
        "Same attemptId and same segIdx must produce stable names within one attempt");
  }

  @Test
  public void testDifferentSegIdxProduceDifferentNames() {
    String attemptId = "550e8400-e29b-41d4-a716-446655440000";
    String name0 = MaterializedViewTaskExecutor.buildSegmentName(TABLE, START_MS, END_MS, attemptId, 0);
    String name1 = MaterializedViewTaskExecutor.buildSegmentName(TABLE, START_MS, END_MS, attemptId, 1);
    assertNotEquals(name0, name1,
        "Different segIdx values must produce different names");
  }

  @Test
  public void testNameStartsWithWindowPrefix() {
    String attemptId = "550e8400-e29b-41d4-a716-446655440000";
    String name = MaterializedViewTaskExecutor.buildSegmentName(TABLE, START_MS, END_MS, attemptId, 0);
    String expectedPrefix = TABLE + "_" + START_MS + "_" + END_MS;
    assertTrue(name.startsWith(expectedPrefix),
        "Segment name must start with table_startMs_endMs so the window prefix scan matches it: " + name);
  }
}
