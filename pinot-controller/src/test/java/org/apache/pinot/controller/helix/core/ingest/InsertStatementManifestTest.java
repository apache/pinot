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

import java.util.List;
import org.apache.pinot.spi.ingest.InsertErrorCode;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Tests persistence of successful and failed synchronous ROW results.
public class InsertStatementManifestTest {
  @Test
  public void testFailedResultRoundTrip() throws Exception {
    InsertStatementManifest original = new InsertStatementManifest("s1", "r1", "hash", "t_OFFLINE", InsertType.ROW,
        InsertStatementState.ABORTED, 10, 20, List.of("partial"), "upload failed",
        InsertErrorCode.SEGMENT_UPLOAD_FAILED_PARTIAL);
    InsertStatementManifest restored = InsertStatementManifest.fromJsonString(original.toJsonString());
    assertEquals(restored.getStatementId(), "s1");
    assertEquals(restored.getRequestId(), "r1");
    assertEquals(restored.getPayloadHash(), "hash");
    assertEquals(restored.getTableNameWithType(), "t_OFFLINE");
    assertEquals(restored.getState(), InsertStatementState.ABORTED);
    assertEquals(restored.getCreatedTimeMs(), 10L);
    assertEquals(restored.getLastUpdatedTimeMs(), 20L);
    assertEquals(restored.getSegmentNames(), List.of("partial"));
    assertEquals(restored.getErrorMessage(), "upload failed");
    assertEquals(restored.getErrorCode(), InsertErrorCode.SEGMENT_UPLOAD_FAILED_PARTIAL);
  }

  @Test
  public void testAcceptedResultWithoutRequestId() throws Exception {
    InsertStatementManifest original = new InsertStatementManifest("s1", null, null, "t_REALTIME", InsertType.ROW,
        InsertStatementState.ACCEPTED, 10, 10, null, null, null);
    InsertStatementManifest restored = InsertStatementManifest.fromJsonString(original.toJsonString());
    assertNull(restored.getRequestId());
    assertNull(restored.getErrorCode());
    assertEquals(restored.getSegmentNames(), List.of());
    assertEquals(restored.getState(), InsertStatementState.ACCEPTED);
  }
}
