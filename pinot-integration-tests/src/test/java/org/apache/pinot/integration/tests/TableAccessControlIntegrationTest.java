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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Broker-side table-level access control over the single-stage engine: which tables a query is allowed to read, and
/// how that interacts with row-level filters. Add cases here for any table a query reaches other than through its
/// FROM clause.
///
/// The cluster has a regular table and a dimension table, and three principals with different access to them, so a
/// case can pick the principal it needs instead of starting another cluster. Only the broker runs with access
/// control; the controller and server are left open so table setup and segment upload need no credentials.
public class TableAccessControlIntegrationTest extends BaseClusterIntegrationTest {
  private static final String TABLE = "someTable";
  private static final String DIM_TABLE = "someDimTable";

  private static final String TABLE_KEY = "tableKey";
  private static final String DIM_KEY = "dimKey";
  private static final String DIM_VALUE = "dimValue";

  private static final int NUM_ROWS = 10;

  /// Authorized for every table
  private static final Map<String, String> ADMIN_HEADER = Map.of("Authorization", "Basic YWRtaW46dmVyeXNlY3JldA==");
  /// Authorized for [#TABLE] only
  private static final Map<String, String> USER_HEADER = Map.of("Authorization", "Basic dXNlcjpzZWNyZXQ=");
  /// Authorized for both tables, but with a row-level filter on [#DIM_TABLE]
  private static final Map<String, String> RLS_USER_HEADER =
      Map.of("Authorization", "Basic cmxzVXNlcjpybHNTZWNyZXQ=");

  private static final String LOOKUP_QUERY =
      "SELECT lookup('" + DIM_TABLE + "', '" + DIM_VALUE + "', '" + DIM_KEY + "', " + TABLE_KEY + ") FROM " + TABLE
          + " ORDER BY " + TABLE_KEY;

  private static String dimValueFor(long key) {
    return "value-" + key;
  }

  @Override
  public String getTableName() {
    return TABLE;
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty("pinot.broker.enable.row.column.level.auth", "true");
    brokerConf.setProperty("pinot.broker.access.control.class",
        "org.apache.pinot.broker.broker.BasicAuthAccessControlFactory");
    brokerConf.setProperty("pinot.broker.access.control.principals", "admin,user,rlsUser");
    brokerConf.setProperty("pinot.broker.access.control.principals.admin.password", "verysecret");
    brokerConf.setProperty("pinot.broker.access.control.principals.user.password", "secret");
    brokerConf.setProperty("pinot.broker.access.control.principals.user.tables", TABLE);
    brokerConf.setProperty("pinot.broker.access.control.principals.rlsUser.password", "rlsSecret");
    brokerConf.setProperty("pinot.broker.access.control.principals.rlsUser.tables", TABLE + "," + DIM_TABLE);
    brokerConf.setProperty("pinot.broker.access.control.principals.rlsUser." + DIM_TABLE + ".rls",
        DIM_KEY + " = 1");
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(TABLE)
        .addSingleValueDimension(TABLE_KEY, FieldSpec.DataType.LONG)
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).build();
  }

  private Schema createDimSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(DIM_TABLE)
        .addSingleValueDimension(DIM_KEY, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DIM_VALUE, FieldSpec.DataType.STRING)
        .setPrimaryKeyColumns(List.of(DIM_KEY))
        .build();
  }

  private TableConfig createDimTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(DIM_TABLE)
        .setDimensionTableConfig(new DimensionTableConfig(false, false))
        .setIsDimTable(true)
        .build();
  }

  private static Field avroField(String name, Type type) {
    return new Field(name, org.apache.avro.Schema.create(type), null, null);
  }

  /// Writes `NUM_ROWS` records, applying `fillRecord` to each one to set the column values for row `i`.
  private File createAvroFile(String name, List<Field> fields, BiConsumer<GenericData.Record, Integer> fillRecord)
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord(name, null, null, false);
    avroSchema.setFields(fields);
    File file = new File(_tempDir, name + ".avro");
    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      writer.create(avroSchema, file);
      for (int i = 0; i < NUM_ROWS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        fillRecord.accept(record, i);
        writer.append(record);
      }
    }
    return file;
  }

  private void addTable(String tableName, Schema schema, TableConfig tableConfig, File avroFile)
      throws Exception {
    addSchema(schema);
    addTableConfig(tableConfig);
    File segmentDir = new File(_segmentDir, tableName);
    File tarDir = new File(_tarDir, tableName);
    TestUtils.ensureDirectoriesExistAndEmpty(segmentDir, tarDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(List.of(avroFile), tableConfig, schema, 0, segmentDir, tarDir);
    uploadSegments(tableName, tarDir);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    File tableAvro = createAvroFile("table", List.of(avroField(TABLE_KEY, Type.LONG)),
        (record, i) -> record.put(TABLE_KEY, (long) i));
    addTable(TABLE, createSchema(), createOfflineTableConfig(), tableAvro);

    File dimAvro = createAvroFile("dim", List.of(avroField(DIM_KEY, Type.LONG), avroField(DIM_VALUE, Type.STRING)),
        (record, i) -> {
          record.put(DIM_KEY, (long) i);
          record.put(DIM_VALUE, dimValueFor(i));
        });
    addTable(DIM_TABLE, createDimSchema(), createDimTableConfig(), dimAvro);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode table = postQuery("SELECT COUNT(*) FROM " + TABLE, ADMIN_HEADER);
        JsonNode dim = postQuery("SELECT COUNT(*) FROM " + DIM_TABLE, ADMIN_HEADER);
        return table.get("resultTable").get("rows").get(0).get(0).asLong() == NUM_ROWS
            && dim.get("resultTable").get("rows").get(0).get(0).asLong() == NUM_ROWS;
      } catch (Exception e) {
        return false;
      }
    }, 100L, 60_000L, "Failed to load data into both tables");
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  private void assertForbidden(String query, Map<String, String> headers) {
    try {
      postQuery(query, headers);
      fail("Expected 403 for query: " + query);
    } catch (Exception e) {
      Throwable cause = e.getCause() instanceof HttpErrorStatusException ? e.getCause() : e;
      assertTrue(cause instanceof HttpErrorStatusException, "expected HttpErrorStatusException, got: " + cause);
      assertEquals(((HttpErrorStatusException) cause).getStatusCode(), 403);
    }
  }

  /// The dimension table named inside `lookup()` is subject to access control even though it is not the FROM table.
  @Test
  public void testLookupDeniedForUnauthorizedDimensionTable() {
    assertForbidden(LOOKUP_QUERY, USER_HEADER);
  }

  /// A principal authorized for both tables still gets the looked-up values.
  @Test
  public void testLookupAllowedForAuthorizedDimensionTable()
      throws Exception {
    JsonNode response = postQuery(LOOKUP_QUERY, ADMIN_HEADER);
    assertNoError(response);
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      assertEquals(rows.get(i).get(0).asText(), dimValueFor(i));
    }
  }

  /// `lookup()` resolves a row by primary key and never evaluates a filter against the dimension table, so a
  /// row-level filter on that table cannot be applied and the query has to be rejected rather than returning
  /// unfiltered rows.
  @Test
  public void testLookupRejectedWhenDimensionTableHasRowLevelFilter()
      throws Exception {
    // Querying the dimension table directly works and returns only the row the filter allows. That pins the
    // rejection below to the filter rather than to a missing grant on the table.
    JsonNode response = postQuery("SELECT " + DIM_VALUE + " FROM " + DIM_TABLE, RLS_USER_HEADER);
    assertNoError(response);
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asText(), dimValueFor(1));

    assertForbidden(LOOKUP_QUERY, RLS_USER_HEADER);
  }

  /// A query that reads only its FROM table is unaffected.
  @Test
  public void testQueryWithoutLookupUnaffected()
      throws Exception {
    JsonNode response = postQuery("SELECT COUNT(*) FROM " + TABLE, USER_HEADER);
    assertNoError(response);
    assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_ROWS);
  }
}
