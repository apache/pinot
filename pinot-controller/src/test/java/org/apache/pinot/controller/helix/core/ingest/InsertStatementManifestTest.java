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
package org.apache.pinot.controller.helix.core.ingest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/**
 * Unit tests for {@link InsertStatementManifest} JSON serialization round-trip.
 */
public class InsertStatementManifestTest {

  @Test
  public void testJsonRoundTrip()
      throws IOException {
    InsertStatementManifest original = new InsertStatementManifest(
        "stmt-1", "req-1", "hash-abc", "testTable_OFFLINE",
        InsertType.ROW, InsertStatementState.PREPARED,
        1000L, 2000L, Arrays.asList("seg-1", "seg-2"), null, null);

    String json = original.toJsonString();
    InsertStatementManifest deserialized = InsertStatementManifest.fromJsonString(json);

    assertEquals(deserialized.getStatementId(), "stmt-1");
    assertEquals(deserialized.getRequestId(), "req-1");
    assertEquals(deserialized.getPayloadHash(), "hash-abc");
    assertEquals(deserialized.getTableNameWithType(), "testTable_OFFLINE");
    assertEquals(deserialized.getInsertType(), InsertType.ROW);
    assertEquals(deserialized.getState(), InsertStatementState.PREPARED);
    assertEquals(deserialized.getCreatedTimeMs(), 1000L);
    assertEquals(deserialized.getLastUpdatedTimeMs(), 2000L);
    assertEquals(deserialized.getSegmentNames(), Arrays.asList("seg-1", "seg-2"));
    assertNull(deserialized.getErrorMessage());
  }

  @Test
  public void testJsonRoundTripWithError()
      throws IOException {
    InsertStatementManifest original = new InsertStatementManifest(
        "stmt-2", null, null, "testTable_REALTIME",
        InsertType.FILE, InsertStatementState.ABORTED,
        3000L, 4000L, Collections.emptyList(), "Something went wrong", null);

    String json = original.toJsonString();
    InsertStatementManifest deserialized = InsertStatementManifest.fromJsonString(json);

    assertEquals(deserialized.getStatementId(), "stmt-2");
    assertNull(deserialized.getRequestId());
    assertNull(deserialized.getPayloadHash());
    assertEquals(deserialized.getTableNameWithType(), "testTable_REALTIME");
    assertEquals(deserialized.getInsertType(), InsertType.FILE);
    assertEquals(deserialized.getState(), InsertStatementState.ABORTED);
    assertEquals(deserialized.getErrorMessage(), "Something went wrong");
  }

  @Test
  public void testStateTransition() {
    InsertStatementManifest manifest = new InsertStatementManifest(
        "stmt-3", null, null, "testTable_OFFLINE",
        InsertType.ROW, InsertStatementState.NEW,
        1000L, 1000L, Collections.emptyList(), null, null);

    assertEquals(manifest.getState(), InsertStatementState.NEW);

    manifest.setState(InsertStatementState.ACCEPTED);
    assertEquals(manifest.getState(), InsertStatementState.ACCEPTED);

    manifest.setState(InsertStatementState.PREPARED);
    assertEquals(manifest.getState(), InsertStatementState.PREPARED);

    manifest.setSegmentNames(Arrays.asList("seg-a"));
    assertEquals(manifest.getSegmentNames(), Arrays.asList("seg-a"));

    manifest.setState(InsertStatementState.COMMITTED);
    assertEquals(manifest.getState(), InsertStatementState.COMMITTED);
  }
}
