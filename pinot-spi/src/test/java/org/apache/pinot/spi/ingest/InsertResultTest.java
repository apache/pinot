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
package org.apache.pinot.spi.ingest;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// JSON round-trip and validation tests for {@link InsertResult}. These guard the wire contract
/// used between broker, controller, and JDBC/REST clients.
public class InsertResultTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testRoundTripAllFields()
      throws Exception {
    InsertResult original = new InsertResult.Builder()
        .setStatementId("stmt-rt")
        .setState(InsertStatementState.VISIBLE)
        .setMessage("rows applied")
        .setInformationalMessage("Auto-completed via cleanup sweep")
        .setSegmentNames(Arrays.asList("seg-a", "seg-b"))
        .setErrorCode(null)
        .build();

    String json = OBJECT_MAPPER.writeValueAsString(original);
    InsertResult round = OBJECT_MAPPER.readValue(json, InsertResult.class);

    assertEquals(round.getStatementId(), "stmt-rt");
    assertEquals(round.getState(), InsertStatementState.VISIBLE);
    assertEquals(round.getMessage(), "rows applied");
    assertEquals(round.getInformationalMessage(), "Auto-completed via cleanup sweep");
    assertEquals(round.getSegmentNames(), Arrays.asList("seg-a", "seg-b"));
    assertNull(round.getErrorCode());
    /// The segmentNames list must be immutable so callers can't mutate the result's internal state.
    expectThrows(UnsupportedOperationException.class, () -> round.getSegmentNames().add("seg-c"));
  }

  /// Forward-compat: a wire blob lacking `informationalMessage` must deserialize cleanly with
  /// the field set to null. Old controllers / old clients have no notion of this field; new code
  /// must tolerate its absence.
  @Test
  public void testDeserializeOldJsonWithoutInformationalMessage()
      throws Exception {
    String oldJson = "{\"statementId\":\"s1\",\"state\":\"VISIBLE\",\"message\":\"ok\",\"segmentNames\":[]}";
    InsertResult result = OBJECT_MAPPER.readValue(oldJson, InsertResult.class);
    assertEquals(result.getStatementId(), "s1");
    assertEquals(result.getState(), InsertStatementState.VISIBLE);
    assertEquals(result.getMessage(), "ok");
    assertNull(result.getInformationalMessage(),
        "informationalMessage must be null when absent in old JSON");
  }

  /// `@JsonCreator` must throw on null state. Jackson wraps the `IllegalArgumentException`
  /// the @JsonCreator throws into a {@link JsonMappingException}; assert that specific type AND that
  /// the message identifies the field by name so a future refactor that swallows the guard cannot
  /// silently pass this test.
  @Test
  public void testJsonCreatorRejectsNullState() {
    String badJson = "{\"statementId\":\"s2\",\"message\":\"x\"}";
    JsonMappingException ex = expectThrows(JsonMappingException.class,
        () -> OBJECT_MAPPER.readValue(badJson, InsertResult.class));
    assertTrue(ex.getMessage().contains("state is required"),
        "Expected null-state error to mention 'state is required'; got: " + ex.getMessage());
  }

  /// Symmetric guard for `@JsonCreator` rejecting null `statementId`.
  @Test
  public void testJsonCreatorRejectsNullStatementId() {
    String badJson = "{\"state\":\"VISIBLE\",\"message\":\"x\"}";
    JsonMappingException ex = expectThrows(JsonMappingException.class,
        () -> OBJECT_MAPPER.readValue(badJson, InsertResult.class));
    assertTrue(ex.getMessage().contains("statementId is required"),
        "Expected null-statementId error to mention 'statementId is required'; got: " + ex.getMessage());
  }

  /// Backward-compat constructor (5-arg, no `informationalMessage`) must produce a valid
  /// result identical to the full constructor's null-default behavior. This protects external
  /// plugins that compiled against the pre-round-6 surface.
  @Test
  public void testBackwardCompatConstructor() {
    InsertResult r = new InsertResult("stmt-bc", InsertStatementState.ACCEPTED, "msg",
        Arrays.asList("s1"), "EC");
    assertEquals(r.getStatementId(), "stmt-bc");
    assertEquals(r.getState(), InsertStatementState.ACCEPTED);
    assertEquals(r.getMessage(), "msg");
    assertNull(r.getInformationalMessage(), "5-arg constructor must default informationalMessage to null");
    assertEquals(r.getSegmentNames(), Arrays.asList("s1"));
    assertEquals(r.getErrorCode(), "EC");
  }

  /// The 5-arg back-compat constructor delegates to the full constructor, which enforces non-null
  /// state. A refactor that broke the delegate chain (e.g., introducing an alternate path that
  /// skips the check) must not silently allow null state through.
  @Test
  public void testBackwardCompatConstructorRejectsNullState() {
    expectThrows(IllegalArgumentException.class,
        () -> new InsertResult("stmt-bc-null", null, "m", Arrays.asList("s1"), "EC"));
  }

  @Test
  public void testBuilderRejectsNullState() {
    InsertResult.Builder b = new InsertResult.Builder().setStatementId("s3");
    expectThrows(IllegalStateException.class, b::build);
  }

  @Test
  public void testBuilderRejectsNullStatementId() {
    InsertResult.Builder b = new InsertResult.Builder().setState(InsertStatementState.VISIBLE);
    expectThrows(IllegalStateException.class, b::build);
  }
}
