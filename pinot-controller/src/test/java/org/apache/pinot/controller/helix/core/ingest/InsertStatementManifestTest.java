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


/// Unit tests for {@link InsertStatementManifest} JSON serialization round-trip.
public class InsertStatementManifestTest {

  @Test
  public void testJsonRoundTrip()
      throws IOException {
    InsertStatementManifest original = new InsertStatementManifest(
        "stmt-1", "req-1", "hash-abc", "testTable_OFFLINE",
        InsertType.ROW, InsertStatementState.ACCEPTED,
        1000L, 2000L, Arrays.asList("seg-1", "seg-2"), null, null, null);

    String json = original.toJsonString();
    InsertStatementManifest deserialized = InsertStatementManifest.fromJsonString(json);

    assertEquals(deserialized.getStatementId(), "stmt-1");
    assertEquals(deserialized.getRequestId(), "req-1");
    assertEquals(deserialized.getPayloadHash(), "hash-abc");
    assertEquals(deserialized.getTableNameWithType(), "testTable_OFFLINE");
    assertEquals(deserialized.getInsertType(), InsertType.ROW);
    assertEquals(deserialized.getState(), InsertStatementState.ACCEPTED);
    assertEquals(deserialized.getCreatedTimeMs(), 1000L);
    assertEquals(deserialized.getLastUpdatedTimeMs(), 2000L);
    assertEquals(deserialized.getSegmentNames(), Arrays.asList("seg-1", "seg-2"));
    assertNull(deserialized.getErrorMessage());
    assertNull(deserialized.getLineageEntryId());
    assertNull(deserialized.getMinionTaskName());
  }

  @Test
  public void testJsonRoundTripAllFields()
      throws IOException {
    InsertStatementManifest original = new InsertStatementManifest(
        "stmt-full", "req-full", "hash-full", "fullTable_OFFLINE",
        InsertType.FILE, InsertStatementState.VISIBLE,
        5000L, 6000L, Arrays.asList("seg-x", "seg-y", "seg-z"),
        "completed normally", "lineage-entry-42", "Task_SegmentGenerationAndPushTask_stmt-full_12345");

    String json = original.toJsonString();
    InsertStatementManifest deserialized = InsertStatementManifest.fromJsonString(json);

    assertEquals(deserialized.getStatementId(), original.getStatementId());
    assertEquals(deserialized.getRequestId(), original.getRequestId());
    assertEquals(deserialized.getPayloadHash(), original.getPayloadHash());
    assertEquals(deserialized.getTableNameWithType(), original.getTableNameWithType());
    assertEquals(deserialized.getInsertType(), original.getInsertType());
    assertEquals(deserialized.getState(), original.getState());
    assertEquals(deserialized.getCreatedTimeMs(), original.getCreatedTimeMs());
    assertEquals(deserialized.getLastUpdatedTimeMs(), original.getLastUpdatedTimeMs());
    assertEquals(deserialized.getSegmentNames(), original.getSegmentNames());
    assertEquals(deserialized.getErrorMessage(), original.getErrorMessage());
    assertEquals(deserialized.getLineageEntryId(), original.getLineageEntryId());
    assertEquals(deserialized.getMinionTaskName(), original.getMinionTaskName());
    assertNull(deserialized.getInformationalMessage(), "informationalMessage should be null when not set");
  }

  /// Verifies the new `informationalMessage` field round-trips through JSON and is independent
  /// of `errorMessage`. The sweep auto-complete path stamps `informationalMessage` on a
  /// VISIBLE manifest; UI/JDBC clients must see both fields after deserialization so they can
  /// present the advisory without confusing it with an error.
  @Test
  public void testJsonRoundTripWithInformationalMessage()
      throws IOException {
    InsertStatementManifest original = new InsertStatementManifest(
        "stmt-info", "req-info", "hash-info", "infoTable_OFFLINE",
        InsertType.FILE, InsertStatementState.VISIBLE,
        7000L, 8000L, Collections.emptyList(),
        null /* no errorMessage */, "lineage-info", "Task_SegGenPushTask_stmt-info_9999");
    original.setInformationalMessage("Auto-completed via cleanup sweep; segmentNames not surfaced.");

    String json = original.toJsonString();
    InsertStatementManifest deserialized = InsertStatementManifest.fromJsonString(json);

    assertNull(deserialized.getErrorMessage(), "errorMessage must remain null when only informational is set");
    assertEquals(deserialized.getInformationalMessage(),
        "Auto-completed via cleanup sweep; segmentNames not surfaced.");
    /// State must remain VISIBLE — the advisory does not change the lifecycle.
    assertEquals(deserialized.getState(), InsertStatementState.VISIBLE);
  }

  @Test
  public void testJsonRoundTripWithError()
      throws IOException {
    InsertStatementManifest original = new InsertStatementManifest(
        "stmt-2", null, null, "testTable_REALTIME",
        InsertType.FILE, InsertStatementState.ABORTED,
        3000L, 4000L, Collections.emptyList(), "Something went wrong", null, null);

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

  /// Verifies backward compatibility: a ZK blob persisted by old code (before minionTaskName
  /// was added) must deserialize cleanly with minionTaskName == null. This is the rolling-upgrade
  /// scenario where a new controller leader reads manifests written by the previous leader.
  @Test
  public void testDeserializeOldJsonWithoutMinionTaskName()
      throws IOException {
    String oldJson = "{\"statementId\":\"s1\",\"requestId\":\"r1\",\"payloadHash\":\"h1\","
        + "\"tableNameWithType\":\"t_OFFLINE\",\"insertType\":\"FILE\","
        + "\"state\":\"ACCEPTED\",\"createdTimeMs\":1000,\"lastUpdatedTimeMs\":1000,"
        + "\"segmentNames\":[]}";
    InsertStatementManifest m = InsertStatementManifest.fromJsonString(oldJson);
    assertEquals(m.getStatementId(), "s1");
    assertEquals(m.getState(), InsertStatementState.ACCEPTED);
    assertNull(m.getMinionTaskName(), "minionTaskName must be null when absent in old JSON");
  }

  /// Verifies that a manifest created via the convenience 12-arg constructor carries the current
  /// schema version, and that JSON round-trip preserves it.
  @Test
  public void testSchemaVersionDefaultAndRoundTrip()
      throws IOException {
    InsertStatementManifest m = new InsertStatementManifest(
        "s-sv", null, null, "t_OFFLINE", InsertType.FILE, InsertStatementState.ACCEPTED,
        1000L, 1000L, Collections.emptyList(), null, null, null);
    assertEquals(m.getSchemaVersion(), InsertStatementManifest.CURRENT_SCHEMA_VERSION);
    InsertStatementManifest round = InsertStatementManifest.fromJsonString(m.toJsonString());
    assertEquals(round.getSchemaVersion(), InsertStatementManifest.CURRENT_SCHEMA_VERSION);
  }

  /// Verifies that a persisted blob without schemaVersion deserializes as schema version 1,
  /// so rolling-upgrade readers keep working.
  @Test
  public void testDeserializeOldJsonWithoutSchemaVersion()
      throws IOException {
    String oldJson = "{\"statementId\":\"s-old\",\"requestId\":null,\"payloadHash\":null,"
        + "\"tableNameWithType\":\"t_OFFLINE\",\"insertType\":\"FILE\","
        + "\"state\":\"ACCEPTED\",\"createdTimeMs\":1,\"lastUpdatedTimeMs\":1,"
        + "\"segmentNames\":[]}";
    InsertStatementManifest m = InsertStatementManifest.fromJsonString(oldJson);
    assertEquals(m.getSchemaVersion(), 1);
  }

  /// Verifies that a manifest with a forward-only schemaVersion is rejected at deserialization.
  /// A newer controller writing v2 must NOT be silently accepted by an older controller — silent
  /// partial reads of unknown fields are how distributed-state bugs hide.
  @Test(expectedExceptions = IOException.class,
      expectedExceptionsMessageRegExp = ".*schemaVersion=99.*exceeds max supported.*")
  public void testDeserializeRejectsForwardIncompatibleSchemaVersion()
      throws IOException {
    String forwardJson = "{\"schemaVersion\":99,\"statementId\":\"s-future\",\"requestId\":null,"
        + "\"payloadHash\":null,\"tableNameWithType\":\"t_OFFLINE\",\"insertType\":\"FILE\","
        + "\"state\":\"ACCEPTED\",\"createdTimeMs\":1,\"lastUpdatedTimeMs\":1,\"segmentNames\":[]}";
    InsertStatementManifest.fromJsonString(forwardJson);
  }

  /// Verifies that an unknown JSON field is silently ignored — the @JsonAnySetter captures unknown
  /// properties so older controllers skip new fields cleanly during a rolling upgrade where a newer
  /// leader writes additional metadata at the same schemaVersion.
  @Test
  public void testDeserializeIgnoresUnknownProperties()
      throws IOException {
    String jsonWithExtra = "{\"statementId\":\"s-extra\",\"requestId\":null,\"payloadHash\":null,"
        + "\"tableNameWithType\":\"t_OFFLINE\",\"insertType\":\"FILE\","
        + "\"state\":\"ACCEPTED\",\"createdTimeMs\":1,\"lastUpdatedTimeMs\":1,"
        + "\"segmentNames\":[],\"futureField\":\"v2-only\"}";
    InsertStatementManifest m = InsertStatementManifest.fromJsonString(jsonWithExtra);
    assertEquals(m.getStatementId(), "s-extra");
  }

  /// Forward-compat regression: a v1 controller reading a v2-written manifest with an extra field
  /// must preserve that field through CAS read-modify-write. Without @JsonAnyGetter+@JsonAnySetter,
  /// a v1 controller would silently strip the v2 field on the next write, corrupting state during
  /// rolling upgrades.
  @Test
  public void testRoundTripPreservesUnknownFieldsAcrossCas()
      throws IOException {
    /// v2 controller writes a manifest with a future field (and a nested object).
    String v2Json = "{\"statementId\":\"s-future\",\"requestId\":\"r-future\",\"payloadHash\":\"h\","
        + "\"tableNameWithType\":\"t_OFFLINE\",\"insertType\":\"ROW\","
        + "\"state\":\"ACCEPTED\",\"createdTimeMs\":1,\"lastUpdatedTimeMs\":1,"
        + "\"segmentNames\":[],"
        + "\"v2ScalarField\":\"future-value\","
        + "\"v2NestedField\":{\"key\":\"value\",\"list\":[1,2,3]}}";

    /// v1 controller deserializes (unknown fields captured), mutates state, re-serializes.
    InsertStatementManifest m = InsertStatementManifest.fromJsonString(v2Json);
    assertEquals(m.getStatementId(), "s-future");
    m.setState(InsertStatementState.VISIBLE);  /// simulate CAS mutation
    String roundTripped = m.toJsonString();

    /// The unknown fields must survive: a future v2 reader reading this string must still see them.
    org.testng.Assert.assertTrue(roundTripped.contains("v2ScalarField"),
        "v2 scalar field must round-trip: " + roundTripped);
    org.testng.Assert.assertTrue(roundTripped.contains("future-value"),
        "v2 scalar value must round-trip: " + roundTripped);
    org.testng.Assert.assertTrue(roundTripped.contains("v2NestedField"),
        "v2 nested field must round-trip: " + roundTripped);
    org.testng.Assert.assertTrue(roundTripped.contains("\"key\":\"value\""),
        "nested object content must round-trip: " + roundTripped);
    org.testng.Assert.assertTrue(roundTripped.contains("[1,2,3]"),
        "nested array must round-trip: " + roundTripped);
  }

  /// Verifies that omitted optional fields deserialize cleanly with the right defaults — covers the
  /// backward-readable direction of the rolling-upgrade protocol.
  @Test
  public void testDeserializeMissingOptionalFieldsUsesDefaults()
      throws IOException {
    /// No minionTaskName, lineageEntryId, errorMessage, segmentNames, schemaVersion, requestId,
    /// or payloadHash — only the fields the @JsonCreator constructor cannot synthesize.
    String minimalJson = "{\"statementId\":\"s-min\",\"tableNameWithType\":\"t_OFFLINE\","
        + "\"insertType\":\"FILE\",\"state\":\"ACCEPTED\",\"createdTimeMs\":1,\"lastUpdatedTimeMs\":1}";
    InsertStatementManifest m = InsertStatementManifest.fromJsonString(minimalJson);
    assertEquals(m.getStatementId(), "s-min");
    assertEquals(m.getSchemaVersion(), 1);  /// missing schemaVersion defaults to 1
    assertEquals(m.getSegmentNames(), Collections.emptyList());
    assertEquals(m.getMinionTaskName(), null);
    assertEquals(m.getLineageEntryId(), null);
    assertEquals(m.getErrorMessage(), null);
  }

  @Test
  public void testStateTransition() {
    InsertStatementManifest manifest = new InsertStatementManifest(
        "stmt-3", null, null, "testTable_OFFLINE",
        InsertType.ROW, InsertStatementState.ACCEPTED,
        1000L, 1000L, Collections.emptyList(), null, null, null);

    assertEquals(manifest.getState(), InsertStatementState.ACCEPTED);

    manifest.setSegmentNames(Arrays.asList("seg-a"));
    assertEquals(manifest.getSegmentNames(), Arrays.asList("seg-a"));

    manifest.setState(InsertStatementState.VISIBLE);
    assertEquals(manifest.getState(), InsertStatementState.VISIBLE);
  }
}
