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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
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
}
