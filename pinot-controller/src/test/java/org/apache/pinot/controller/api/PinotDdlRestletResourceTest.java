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
package org.apache.pinot.controller.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// End-to-end integration tests for `POST /sql/ddl`.
///
/// Each test uses a unique table name prefix so failures do not cascade through the shared
/// controller test instance.
public class PinotDdlRestletResourceTest extends ControllerTest {

  private static final String TBL_BASIC = "ddlBasicOffline";
  private static final String TBL_DRY_RUN = "ddlDryRunOffline";
  private static final String TBL_IF_NOT_EXISTS = "ddlIfNotExistsOffline";
  private static final String TBL_DROP = "ddlDropOffline";

  @BeforeClass
  public void setUp()
      throws Exception {
    DEFAULT_INSTANCE.setupSharedStateAndValidate();
  }

  @AfterClass
  public void tearDown() {
    DEFAULT_INSTANCE.cleanup();
  }

  // -------------------------------------------------------------------------------------------
  // CREATE TABLE
  // -------------------------------------------------------------------------------------------

  @Test
  public void createOfflineTableRoundTrip()
      throws IOException {
    String sql = "CREATE TABLE " + TBL_BASIC + " ("
        + "  id INT NOT NULL DIMENSION,"
        + "  ts LONG DATETIME FORMAT '1:MILLISECONDS:EPOCH' GRANULARITY '1:MILLISECONDS'"
        + ") TABLE_TYPE = OFFLINE PROPERTIES ("
        + "  'timeColumnName' = 'ts',"
        + "  'replication' = '1'"
        + ")";

    JsonNode response = postDdl(sql, false);
    assertEquals(response.get("operation").asText(), "CREATE_TABLE");
    assertEquals(response.get("tableName").asText(), TBL_BASIC + "_OFFLINE");
    assertEquals(response.get("tableType").asText(), "OFFLINE");
    assertNotNull(response.get("schema"));
    assertNotNull(response.get("tableConfig"));
    assertFalse(response.get("dryRun").asBoolean());

    // Verify it actually persisted: the table should now show up in the existing tables list.
    String listResponse = sendGetRequest(DEFAULT_INSTANCE.getControllerBaseApiUrl() + "/tables");
    assertTrue(listResponse.contains(TBL_BASIC),
        "Created table should appear in /tables; got " + listResponse);
  }

  @Test
  public void dryRunDoesNotPersist()
      throws IOException {
    String sql = "CREATE TABLE " + TBL_DRY_RUN + " (id INT) TABLE_TYPE = OFFLINE";
    JsonNode response = postDdl(sql, true);
    assertTrue(response.get("dryRun").asBoolean());
    assertNotNull(response.get("schema"));
    assertNotNull(response.get("tableConfig"));

    // Verify nothing was persisted: the table should NOT be in the listing.
    String listResponse = sendGetRequest(DEFAULT_INSTANCE.getControllerBaseApiUrl() + "/tables");
    assertFalse(listResponse.contains(TBL_DRY_RUN),
        "Dry-run table must not be persisted; got " + listResponse);
  }

  @Test
  public void createIfNotExistsIsIdempotent()
      throws IOException {
    String sql = "CREATE TABLE IF NOT EXISTS " + TBL_IF_NOT_EXISTS
        + " (id INT) TABLE_TYPE = OFFLINE";

    // First call: creates.
    postDdl(sql, false);
    // Second call: succeeds without error.
    JsonNode second = postDdl(sql, false);
    assertEquals(second.get("operation").asText(), "CREATE_TABLE");
    assertTrue(second.get("message").asText().toLowerCase().contains("exist"),
        "Expected idempotent message, got: " + second.get("message").asText());
  }

  @Test
  public void createWithoutIfNotExistsConflicts()
      throws IOException {
    String sql = "CREATE TABLE conflictTable (id INT) TABLE_TYPE = OFFLINE";
    postDdl(sql, false); // first time — succeeds
    int status = postDdlExpectFailure(sql);
    assertEquals(status, 409, "Expected 409 Conflict on duplicate create");
  }

  // -------------------------------------------------------------------------------------------
  // DROP TABLE
  // -------------------------------------------------------------------------------------------

  @Test
  public void dropTableSucceeds()
      throws IOException {
    postDdl("CREATE TABLE " + TBL_DROP + " (id INT) TABLE_TYPE = OFFLINE", false);

    JsonNode response = postDdl("DROP TABLE " + TBL_DROP, false);
    assertEquals(response.get("operation").asText(), "DROP_TABLE");
    JsonNode deleted = response.get("deletedTables");
    assertNotNull(deleted);
    assertTrue(deleted.toString().contains(TBL_DROP),
        "Expected " + TBL_DROP + " in deletedTables; got " + deleted);
  }

  @Test
  public void dropMissingTableWithoutIfExistsReturns404()
      throws IOException {
    int status = postDdlExpectFailure("DROP TABLE noSuchTableEverCreated");
    assertEquals(status, 404);
  }

  @Test
  public void dropMissingTableWithIfExistsSucceeds()
      throws IOException {
    JsonNode response = postDdl("DROP TABLE IF EXISTS noSuchTableEverCreated2", false);
    assertEquals(response.get("operation").asText(), "DROP_TABLE");
    assertTrue(response.get("ifExists").asBoolean());
  }

  // -------------------------------------------------------------------------------------------
  // SHOW TABLES
  // -------------------------------------------------------------------------------------------

  @Test
  public void showTablesListsExistingTables()
      throws IOException {
    // Make sure at least one DDL-created table exists for this test.
    String tbl = "ddlShowList";
    postDdl("CREATE TABLE " + tbl + " (id INT) TABLE_TYPE = OFFLINE", false);

    JsonNode response = postDdl("SHOW TABLES", false);
    assertEquals(response.get("operation").asText(), "SHOW_TABLES");
    assertNotNull(response.get("tableNames"));
    assertTrue(response.get("tableNames").toString().contains(tbl),
        "Expected " + tbl + " in tableNames; got " + response.get("tableNames"));
  }

  @Test
  public void showCreateRendersCanonicalDdlAndRoundTrips()
      throws IOException {
    String tbl = "ddlShowCreateRoundtrip";
    String createSql = "CREATE TABLE " + tbl + " ("
        + "  id INT NOT NULL DIMENSION,"
        + "  ts LONG DATETIME FORMAT '1:MILLISECONDS:EPOCH' GRANULARITY '1:MILLISECONDS'"
        + ") TABLE_TYPE = OFFLINE PROPERTIES ("
        + "  'timeColumnName' = 'ts',"
        + "  'replication' = '2',"
        + "  'brokerTenant' = 'DefaultTenant'"
        + ")";
    postDdl(createSql, false);

    JsonNode response = postDdl("SHOW CREATE TABLE " + tbl, false);
    assertEquals(response.get("operation").asText(), "SHOW_CREATE_TABLE");
    assertEquals(response.get("tableName").asText(), tbl + "_OFFLINE");
    String ddl = response.get("ddl").asText();
    // Canonical clause order: column block, TABLE_TYPE, PROPERTIES.
    assertTrue(ddl.startsWith("CREATE TABLE " + tbl + " ("), ddl);
    assertTrue(ddl.contains("TABLE_TYPE = OFFLINE"), ddl);
    assertTrue(ddl.contains("'replication' = '2'"), ddl);
    assertTrue(ddl.contains("'timeColumnName' = 'ts'"), ddl);
    // Properties are emitted lexicographically: brokerTenant < replication < timeColumnName.
    int brokerIdx = ddl.indexOf("'brokerTenant'");
    int replicationIdx = ddl.indexOf("'replication'");
    int timeColIdx = ddl.indexOf("'timeColumnName'");
    assertTrue(brokerIdx < replicationIdx && replicationIdx < timeColIdx,
        "Properties not in lex order:\n" + ddl);
  }

  /// Cluster-level regression for the recommended TIMESTAMP-typed time column: CREATE persists,
  /// SHOW CREATE renders the canonical short-form FORMAT 'TIMESTAMP' (DateTimeFieldSpec normalizes
  /// the format token implicitly for TIMESTAMP), and the re-issued DDL compiles on the same
  /// controller. This pins the end-to-end path against PinotHelixResourceManager.addTable /
  /// addSchema, the validation pipeline, and the reverse emitter — the layers that the
  /// in-process compiler/emitter unit tests do not exercise.
  @Test
  public void createWithTimestampTimeColumnRoundTrips()
      throws IOException {
    String tbl = "ddlTimestampTimeRoundtrip";
    String createSql = "CREATE TABLE " + tbl + " ("
        + "  id INT NOT NULL DIMENSION,"
        + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:MILLISECONDS'"
        + ") TABLE_TYPE = OFFLINE PROPERTIES ("
        + "  'timeColumnName' = 'ts',"
        + "  'replication' = '1'"
        + ")";
    JsonNode createResp = postDdl(createSql, false);
    assertEquals(createResp.get("operation").asText(), "CREATE_TABLE");
    assertEquals(createResp.get("tableName").asText(), tbl + "_OFFLINE");
    // The stored Schema must carry the normalized TIMESTAMP format token, not the verbose
    // 1:MILLISECONDS:TIMESTAMP form the user typed — DateTimeFieldSpec rewrites it because the
    // format is implicit for TIMESTAMP.
    JsonNode schema = createResp.get("schema");
    JsonNode tsField = null;
    for (JsonNode f : schema.get("dateTimeFieldSpecs")) {
      if ("ts".equals(f.get("name").asText())) {
        tsField = f;
        break;
      }
    }
    assertNotNull(tsField, "Schema must declare ts as a DateTimeFieldSpec: " + schema);
    assertEquals(tsField.get("dataType").asText(), "TIMESTAMP");
    assertEquals(tsField.get("format").asText(), "TIMESTAMP",
        "DateTimeFieldSpec must normalize TIMESTAMP format to short form; got " + tsField);
    assertEquals(tsField.get("granularity").asText(), "1:MILLISECONDS");

    // SHOW CREATE TABLE must render the short form and be idempotent on re-compile.
    JsonNode showResp = postDdl("SHOW CREATE TABLE " + tbl, false);
    String ddl = showResp.get("ddl").asText();
    assertTrue(ddl.contains("ts TIMESTAMP DATETIME FORMAT 'TIMESTAMP' GRANULARITY '1:MILLISECONDS'"),
        "Canonical DDL must emit TIMESTAMP short-form format; got:\n" + ddl);
    // DROP cleans up so the next test starts fresh.
    postDdl("DROP TABLE " + tbl, false);
  }

  @Test
  public void showCreateOnMissingTableReturns404()
      throws IOException {
    int status = postDdlExpectFailure("SHOW CREATE TABLE noSuchTableEverDeclared");
    assertEquals(status, 404);
  }

  @Test
  public void showCreateWithDatabaseHeaderEmitsDatabaseQualifiedDdl()
      throws IOException {
    // Regression: executeShowCreate() was passing show.getDatabaseName() (always null when the
    // SQL uses a bare table name) into CanonicalDdlEmitter instead of the resolved `database`
    // that incorporates the Database: header. The emitted DDL would therefore carry no qualifier,
    // making the statement non-idempotent when replayed without the same header.
    String tbl = "ddlShowCreateWithHeader";
    postDdl("CREATE TABLE " + tbl + " (id INT) TABLE_TYPE = OFFLINE", false);

    String url = DEFAULT_INSTANCE.getControllerBaseApiUrl() + "/sql/ddl";
    String body = "{\"sql\": \"SHOW CREATE TABLE " + tbl + "\"}";
    Map<String, String> hdrs = new LinkedHashMap<>();
    hdrs.put("Content-Type", "application/json");
    // Pass database via header with no SQL db. qualifier — the resolved database must appear in
    // the emitted DDL so replaying without the header still targets the correct tenant.
    hdrs.put("database", "default");
    String raw = sendPostRequest(url, body, hdrs);
    JsonNode response = JsonUtils.stringToJsonNode(raw);
    assertEquals(response.get("operation").asText(), "SHOW_CREATE_TABLE");
    String ddl = response.get("ddl").asText();
    // The database name "default" is a SQL reserved keyword, so the canonical emitter must
    // double-quote it to round-trip. The qualifier must still be present — otherwise replaying
    // the DDL without the Database header would target the wrong tenant.
    assertTrue(ddl.startsWith("CREATE TABLE \"default\"." + tbl),
        "Expected DDL to carry db-qualified name; got:\n" + ddl);
  }

  @Test
  public void databaseQualifiedDropTargetsCorrectTable()
      throws IOException {
    // Regression: DROP TABLE was previously discarding the SQL `db.` qualifier and silently
    // targeting the default-database table of the same bare name. With no Database header, the
    // qualified DROP must report "table not found in <db>.<name>", not a stale 200 against
    // some unrelated default-DB table.
    String bareName = "ddlDbQualifiedDropTarget";
    postDdl("CREATE TABLE " + bareName + " (id INT) TABLE_TYPE = OFFLINE", false);

    // Drop with a fake DB qualifier that does not match the table's actual (default) database
    // should NOT delete the default-DB copy. Without IF EXISTS, expect 404.
    int status = postDdlExpectFailure("DROP TABLE noSuchDb." + bareName);
    assertEquals(status, 404);

    // The default-DB table should still be there.
    String listResponse = sendGetRequest(DEFAULT_INSTANCE.getControllerBaseApiUrl() + "/tables");
    assertTrue(listResponse.contains(bareName),
        "Default-DB table must not be silently dropped by a qualified DROP; got " + listResponse);
  }

  // -------------------------------------------------------------------------------------------
  // Negative parse / compile cases
  // -------------------------------------------------------------------------------------------

  @Test
  public void emptyBodyReturnsBadRequest()
      throws IOException {
    int status = postRawExpectFailure("{}");
    assertEquals(status, 400);
  }

  @Test
  public void parseErrorReturnsBadRequest()
      throws IOException {
    int status = postDdlExpectFailure("CREATE NOT_A_THING (id INT) TABLE_TYPE = OFFLINE");
    assertEquals(status, 400);
  }

  @Test
  public void semanticErrorReturnsBadRequest()
      throws IOException {
    int status = postDdlExpectFailure(
        "CREATE TABLE foo (id INT) TABLE_TYPE = OFFLINE PROPERTIES ('timeColumnName' = 'missing')");
    assertEquals(status, 400);
  }

  @Test
  public void successfulCreateReturns201()
      throws IOException {
    String tbl = "ddlCreate201Test";
    String url = DEFAULT_INSTANCE.getControllerBaseApiUrl() + "/sql/ddl";
    String body = "{\"sql\": \"CREATE TABLE " + tbl + " (id INT) TABLE_TYPE = OFFLINE\"}";
    Pair<Integer, String> result = postRequestWithStatusCode(url, body);
    assertEquals(result.getLeft().intValue(), 201, "Expected HTTP 201 for successful CREATE TABLE");
    JsonNode response = JsonUtils.stringToJsonNode(result.getRight());
    assertEquals(response.get("operation").asText(), "CREATE_TABLE");
  }

  @Test
  public void oversizedInputReturnsBadRequest()
      throws IOException {
    // Any SQL that exceeds 256 KB must be rejected before parsing to prevent unbounded allocations.
    String oversized = "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE /* " + StringUtils.repeat("x", 256 * 1024) + " */";
    int status = postDdlExpectFailure(oversized);
    assertEquals(status, 400, "Expected 400 for input exceeding MAX_DDL_SQL_CHARS");
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static JsonNode postDdl(String sql, boolean dryRun)
      throws IOException {
    String url = DEFAULT_INSTANCE.getControllerBaseApiUrl() + "/sql/ddl?dryRun=" + dryRun;
    String body = "{\"sql\": " + JsonUtils.objectToString(sql) + "}";
    String response = sendPostRequest(url, body,
        Collections.singletonMap("Content-Type", "application/json"));
    return JsonUtils.stringToJsonNode(response);
  }

  private static int postDdlExpectFailure(String sql) {
    String url = DEFAULT_INSTANCE.getControllerBaseApiUrl() + "/sql/ddl";
    String body;
    try {
      body = "{\"sql\": " + JsonUtils.objectToString(sql) + "}";
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return postRawExpectFailureBody(url, body);
  }

  private static int postRawExpectFailure(String body) {
    return postRawExpectFailureBody(
        DEFAULT_INSTANCE.getControllerBaseApiUrl() + "/sql/ddl", body);
  }

  private static int postRawExpectFailureBody(String url, String body) {
    // Use postRequestWithStatusCode which does not throw on error — the HTTP status code is
    // returned directly from the underlying response. Scanning exception messages for numeric
    // substrings is fragile because the error body may itself contain integers that look like
    // status codes (e.g. the echoed request body or a column count).
    try {
      Pair<Integer, String> result = postRequestWithStatusCode(url, body);
      int status = result.getLeft();
      if (status >= 200 && status < 300) {
        fail("Expected request to fail with HTTP error but got status " + status);
      }
      return status;
    } catch (IOException e) {
      throw new RuntimeException("Unexpected IO error contacting controller", e);
    }
  }
}
