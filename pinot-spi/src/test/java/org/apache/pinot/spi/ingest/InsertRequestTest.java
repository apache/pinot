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
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Constructor-invariant tests for {@link InsertRequest}. Locks the symmetric enforcement between
/// the wire-deserialized `@JsonCreator` path and the in-process `Builder.build()` path.
public class InsertRequestTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testPayloadHashIncludesLogicalNullFields() {
    GenericRow logicalNull = new GenericRow();
    logicalNull.putDefaultNullValue("id", Integer.MIN_VALUE);
    GenericRow sentinelValue = new GenericRow();
    sentinelValue.putValue("id", Integer.MIN_VALUE);
    InsertRequest first = new InsertRequest.Builder().setTableName("t").setInsertType(InsertType.ROW)
        .setRows(List.of(logicalNull)).build();
    InsertRequest second = new InsertRequest.Builder().setTableName("t").setInsertType(InsertType.ROW)
        .setRows(List.of(sentinelValue)).build();
    assertNotEquals(first.computePayloadHash(), second.computePayloadHash());
  }

  @Test
  public void testJsonCreatorRejectsMissingTableName() {
    String badJson = "{\"insertType\":\"ROW\"}";
    JsonMappingException ex = expectThrows(JsonMappingException.class,
        () -> OBJECT_MAPPER.readValue(badJson, InsertRequest.class));
    assertTrue(ex.getMessage().contains("tableName is required"),
        "Expected missing-tableName error; got: " + ex.getMessage());
  }

  @Test
  public void testJsonCreatorRejectsEmptyTableName() {
    String badJson = "{\"tableName\":\"\",\"insertType\":\"ROW\"}";
    JsonMappingException ex = expectThrows(JsonMappingException.class,
        () -> OBJECT_MAPPER.readValue(badJson, InsertRequest.class));
    assertTrue(ex.getMessage().contains("tableName is required"),
        "Expected empty-tableName error; got: " + ex.getMessage());
  }

  @Test
  public void testJsonCreatorRejectsMissingInsertType() {
    String badJson = "{\"tableName\":\"t\"}";
    JsonMappingException ex = expectThrows(JsonMappingException.class,
        () -> OBJECT_MAPPER.readValue(badJson, InsertRequest.class));
    assertTrue(ex.getMessage().contains("insertType is required"),
        "Expected missing-insertType error; got: " + ex.getMessage());
  }


  @Test
  public void testJsonCreatorAcceptsRowInsert() throws Exception {
    String json = "{\"tableName\":\"t\",\"insertType\":\"ROW\"}";
    InsertRequest r = OBJECT_MAPPER.readValue(json, InsertRequest.class);
    assertEquals(r.getTableName(), "t");
    assertEquals(r.getInsertType(), InsertType.ROW);
  }



  @Test
  public void testBuilderRejectsMissingTableName() {
    InsertRequest.Builder b = new InsertRequest.Builder().setInsertType(InsertType.ROW);
    IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, b::build);
    assertTrue(ex.getMessage().contains("tableName is required"));
  }

  @Test
  public void testBuilderRejectsMissingInsertType() {
    InsertRequest.Builder b = new InsertRequest.Builder().setTableName("t");
    IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, b::build);
    assertTrue(ex.getMessage().contains("insertType is required"));
  }


  /// The two enforcement paths (wire-deserialized and Builder) must throw with the identical
  /// message text so log-greps catch both code paths exhaustively. Locks all three invariants:
  /// missing-tableName and missing-insertType.
  @Test
  public void testBuilderAndJsonCreatorThrowSameMessages() {
    /// Missing tableName.
    assertCanonicalPhraseOnBothPaths(
        () -> new InsertRequest.Builder().setInsertType(InsertType.ROW).build(),
        "{\"insertType\":\"ROW\"}",
        "tableName is required for InsertRequest");
    /// Missing insertType.
    assertCanonicalPhraseOnBothPaths(
        () -> new InsertRequest.Builder().setTableName("t").build(),
        "{\"tableName\":\"t\"}",
        "insertType is required for InsertRequest");
  }

  private static void assertCanonicalPhraseOnBothPaths(
      Runnable builderThrowingCall, String jsonThrowingBody, String canonicalPhrase) {
    IllegalArgumentException builderEx = expectThrows(IllegalArgumentException.class, builderThrowingCall::run);
    JsonMappingException jsonEx = expectThrows(JsonMappingException.class,
        () -> OBJECT_MAPPER.readValue(jsonThrowingBody, InsertRequest.class));
    assertTrue(builderEx.getMessage().contains(canonicalPhrase),
        "Builder message must contain '" + canonicalPhrase + "'; got: " + builderEx.getMessage());
    assertTrue(jsonEx.getMessage().contains(canonicalPhrase),
        "JsonCreator message must contain '" + canonicalPhrase + "'; got: " + jsonEx.getMessage());
  }

  /// statementId and requestId become ZK znode names and segment-name components; both must be
  /// rejected on any construction path when they contain characters outside the strict ID pattern.
  @Test
  public void testIdsOutsidePatternRejectedOnBothPaths() {
    InsertRequest.Builder badStatementId = new InsertRequest.Builder()
        .setTableName("t").setInsertType(InsertType.ROW).setStatementId("a/b");
    IllegalArgumentException ex1 = expectThrows(IllegalArgumentException.class, badStatementId::build);
    assertTrue(ex1.getMessage().contains("statementId must match"), ex1.getMessage());

    InsertRequest.Builder badRequestId = new InsertRequest.Builder()
        .setTableName("t").setInsertType(InsertType.ROW).setRequestId("../etc");
    IllegalArgumentException ex2 = expectThrows(IllegalArgumentException.class, badRequestId::build);
    assertTrue(ex2.getMessage().contains("requestId must match"), ex2.getMessage());

    JsonMappingException ex3 = expectThrows(JsonMappingException.class,
        () -> OBJECT_MAPPER.readValue(
            "{\"tableName\":\"t\",\"insertType\":\"ROW\",\"statementId\":\"a/b\"}",
            InsertRequest.class));
    assertTrue(ex3.getMessage().contains("statementId must match"), ex3.getMessage());
  }

  /// The controller REST endpoint never trusts a client-supplied statementId; this helper is what
  /// it uses to replace it while keeping everything else intact.
  @Test
  public void testWithServerGeneratedStatementIdReplacesClientValue() {
    InsertRequest request = new InsertRequest.Builder()
        .setTableName("t").setInsertType(InsertType.ROW).setStatementId("client-chosen")
        .setRequestId("req-1").build();
    InsertRequest regenerated = request.withServerGeneratedStatementId();
    assertNotEquals(regenerated.getStatementId(), "client-chosen");
    assertTrue(InsertRequest.ID_PATTERN.matcher(regenerated.getStatementId()).matches());
    assertEquals(regenerated.getRequestId(), "req-1");
    assertEquals(regenerated.getTableName(), "t");
  }

  /// The payload hash is computed server-side for idempotency conflict detection: identical
  /// payloads must hash identically, a changed row value must change the hash, and control options
  /// (requestId, tableType) must not affect it.
  @Test
  public void testComputePayloadHashStableAndSensitive() {
    GenericRow row = new GenericRow();
    row.putValue("id", 1);
    row.putValue("name", "x");
    InsertRequest a = new InsertRequest.Builder()
        .setTableName("t").setInsertType(InsertType.ROW).setRows(List.of(row))
        .setOptions(Map.of("requestId", "r1")).build();
    InsertRequest b = new InsertRequest.Builder()
        .setTableName("t").setInsertType(InsertType.ROW).setRows(List.of(row))
        .setOptions(Map.of("requestId", "r2")).build();
    assertEquals(a.computePayloadHash(), b.computePayloadHash(),
        "control options must not affect the payload hash");

    GenericRow differentRow = new GenericRow();
    differentRow.putValue("id", 2);
    differentRow.putValue("name", "x");
    InsertRequest c = new InsertRequest.Builder()
        .setTableName("t").setInsertType(InsertType.ROW).setRows(List.of(differentRow)).build();
    assertNotEquals(a.computePayloadHash(), c.computePayloadHash(),
        "a changed row value must change the payload hash");
  }

  private static String hashFields(Map<String, Object> fields) {
    GenericRow row = new GenericRow();
    fields.forEach(row::putValue);
    return new InsertRequest.Builder().setTableName("t").setInsertType(InsertType.ROW)
        .setRows(List.of(row)).build().computePayloadHash();
  }

  @Test
  public void testPayloadHashSeparatesMultiValueShapeAndEmbeddedDelimiters() {
    assertNotEquals(hashFields(Map.of("tags", List.of("a", "b"))),
        hashFields(Map.of("tags", List.of("a, b"))));
    assertNotEquals(hashFields(Map.of("x", "a\u0000y\u0000Sb")),
        hashFields(Map.of("x", "a", "y", "b")));
    assertNotEquals(hashFields(Map.of("x\u0000Sa", "b")), hashFields(Map.of("x", "a\u0000Sb")));
    assertNotEquals(hashFields(Map.of("tags", List.of(List.of("a", "b")))),
        hashFields(Map.of("tags", List.of("[a, b]"))));
  }

  @Test
  public void testPayloadHashCanonicalizesRowFieldsSequencesAndIntegerBoxing() {
    Map<String, Object> reversed = new LinkedHashMap<>();
    reversed.put("b", 2L);
    reversed.put("a", List.of(1L, 2L));
    assertEquals(hashFields(reversed), hashFields(Map.of("a", new Integer[]{1, 2}, "b", 2)));
    assertNotEquals(hashFields(Map.of("value", 2)), hashFields(Map.of("value", "2")));
    // STRING columns store integer 1 and double 1.0 differently, despite numeric equality.
    assertNotEquals(hashFields(Map.of("value", 1)), hashFields(Map.of("value", 1.0)));
    assertNotEquals(hashFields(Map.of("value", new BigDecimal("2.00"))), hashFields(Map.of("value", 2)));
    assertNotEquals(hashFields(Map.of("value", new BigDecimal("2.00"))),
        hashFields(Map.of("value", new BigDecimal("2.0"))));
    assertNotEquals(hashFields(Map.of("value", 0.1f)), hashFields(Map.of("value", 0.1d)));
  }

  @Test
  public void testPayloadHashPreservesNestedMapValueOrder() {
    Map<String, Object> first = new LinkedHashMap<>();
    first.put("a", "first");
    first.put("b", "second");
    Map<String, Object> reversed = new LinkedHashMap<>();
    reversed.put("b", "second");
    reversed.put("a", "first");
    // Ingestion standardizes a column-value Map to its values in iteration order.
    assertNotEquals(hashFields(Map.of("tags", first)), hashFields(Map.of("tags", reversed)));
  }
}
