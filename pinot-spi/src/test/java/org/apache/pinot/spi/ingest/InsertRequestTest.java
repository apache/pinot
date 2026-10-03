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

  /// FILE inserts must carry a fileUri. The check mirrors {@link InsertRequest.Builder#build()} so
  /// the wire-deserialized and in-process paths reject this case with the same message.
  @Test
  public void testJsonCreatorRejectsFileWithoutFileUri() {
    String badJson = "{\"tableName\":\"t\",\"insertType\":\"FILE\"}";
    JsonMappingException ex = expectThrows(JsonMappingException.class,
        () -> OBJECT_MAPPER.readValue(badJson, InsertRequest.class));
    assertTrue(ex.getMessage().contains("fileUri is required"),
        "Expected missing-fileUri error; got: " + ex.getMessage());
  }

  @Test
  public void testJsonCreatorAcceptsRowInsertWithoutFileUri() throws Exception {
    String json = "{\"tableName\":\"t\",\"insertType\":\"ROW\"}";
    InsertRequest r = OBJECT_MAPPER.readValue(json, InsertRequest.class);
    assertEquals(r.getTableName(), "t");
    assertEquals(r.getInsertType(), InsertType.ROW);
  }

  @Test
  public void testJsonCreatorAcceptsFileInsertWithFileUri() throws Exception {
    String json = "{\"tableName\":\"t\",\"insertType\":\"FILE\",\"fileUri\":\"s3://bucket/path\"}";
    InsertRequest r = OBJECT_MAPPER.readValue(json, InsertRequest.class);
    assertEquals(r.getTableName(), "t");
    assertEquals(r.getInsertType(), InsertType.FILE);
    assertEquals(r.getFileUri(), "s3://bucket/path");
  }

  @Test
  public void testBuilderAcceptsFileInsertWithFileUri() {
    InsertRequest r = new InsertRequest.Builder()
        .setTableName("t").setInsertType(InsertType.FILE).setFileUri("s3://bucket/path").build();
    assertEquals(r.getTableName(), "t");
    assertEquals(r.getInsertType(), InsertType.FILE);
    assertEquals(r.getFileUri(), "s3://bucket/path");
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

  @Test
  public void testBuilderRejectsFileWithoutFileUri() {
    InsertRequest.Builder b = new InsertRequest.Builder().setTableName("t").setInsertType(InsertType.FILE);
    IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, b::build);
    assertTrue(ex.getMessage().contains("fileUri is required"));
  }

  /// The two enforcement paths (wire-deserialized and Builder) must throw with the identical
  /// message text so log-greps catch both code paths exhaustively. Locks all three invariants:
  /// missing-tableName, missing-insertType, and FILE-without-fileUri.
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
    /// FILE without fileUri.
    assertCanonicalPhraseOnBothPaths(
        () -> new InsertRequest.Builder().setTableName("t").setInsertType(InsertType.FILE).build(),
        "{\"tableName\":\"t\",\"insertType\":\"FILE\"}",
        "fileUri is required for FILE insert");
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
}
