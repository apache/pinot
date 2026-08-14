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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// End-to-end coverage for aggregating, grouping and de-duplicating a UUID column.
///
/// These run through a real broker reduce, which is the point: the group-key conversion in
/// `GroupByDataTableReducer#getConvertedKey` is only reachable when the broker reduces a *single* data table, and the
/// unit-level `BaseQueriesTest` harness always reduces two. A `case UUID` there that returns the stored `byte[]`
/// instead of the converted `java.util.UUID` makes `GROUP BY ... HAVING` over a UUID column fail with
/// `ClassCastException: class [B cannot be cast to class java.util.UUID`, and only a query-level test catches it.
@Test(suiteName = "CustomClusterIntegrationTest")
public class UuidAggregationTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "UuidAggregationTest";
  private static final String UUID_COLUMN = "uuidColumn";
  private static final String UUID_0 = "550e8400-e29b-41d4-a716-446655440000";
  private static final String UUID_0_HEX = "550e8400e29b41d4a716446655440000";
  private static final String UUID_1 = "550e8400-e29b-41d4-a716-446655440001";
  private static final String UUID_2 = "550e8400-e29b-41d4-a716-446655440002";

  /// `UUID_0` appears twice so grouping and distinct are distinguishable from a plain row count.
  private static final List<String> ROWS = List.of(UUID_0, UUID_0, UUID_1, UUID_2);
  private static final int NUM_DISTINCT = 3;

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return ROWS.size();
  }

  @Override
  public int getNumAvroFiles() {
    return 1;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(UUID_COLUMN, DataType.UUID)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("uuidRecord", null, null, false);
    avroSchema.setFields(List.of(new org.apache.avro.Schema.Field(UUID_COLUMN,
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      DataFileWriter<GenericData.Record> writer = avroFilesAndWriters.getWriters().get(0);
      for (String uuid : ROWS) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(UUID_COLUMN, uuid);
        writer.append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test
  public void testGroupByUuidColumn()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode rows = query(
        String.format("SELECT %s, COUNT(*) FROM %s GROUP BY %s ORDER BY %s", UUID_COLUMN, getTableName(), UUID_COLUMN,
            UUID_COLUMN));
    assertEquals(rows.size(), NUM_DISTINCT, rows.toPrettyString());

    // Group keys must come back as canonical UUIDs, not hex and not a byte-array rendering.
    List<String> keys = new ArrayList<>();
    for (JsonNode row : rows) {
      keys.add(row.get(0).asText());
    }
    assertEquals(keys, List.of(UUID_0, UUID_1, UUID_2), rows.toPrettyString());
    assertEquals(rows.get(0).get(1).asLong(), 2, rows.toPrettyString());
    assertEquals(rows.get(1).get(1).asLong(), 1, rows.toPrettyString());
  }

  /// The regression that motivated this class: `GROUP BY` a UUID column with a `HAVING` predicate on that same
  /// column runs the group key through `getConvertedKey` and then straight into `PredicateRowMatcher`, which casts
  /// to `java.util.UUID`.
  @Test
  public void testGroupByUuidColumnWithHaving()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode rows = query(
        String.format("SELECT %s, COUNT(*) FROM %s GROUP BY %s HAVING %s = '%s'", UUID_COLUMN, getTableName(),
            UUID_COLUMN, UUID_COLUMN, UUID_0_HEX));
    assertEquals(rows.size(), 1, rows.toPrettyString());
    assertEquals(rows.get(0).get(0).asText(), UUID_0, rows.toPrettyString());
    assertEquals(rows.get(0).get(1).asLong(), 2, rows.toPrettyString());

    // Same thing via an explicit CAST of the canonical form.
    rows = query(String.format("SELECT %s, COUNT(*) FROM %s GROUP BY %s HAVING %s = CAST('%s' AS UUID)", UUID_COLUMN,
        getTableName(), UUID_COLUMN, UUID_COLUMN, UUID_1));
    assertEquals(rows.size(), 1, rows.toPrettyString());
    assertEquals(rows.get(0).get(0).asText(), UUID_1, rows.toPrettyString());
    assertEquals(rows.get(0).get(1).asLong(), 1, rows.toPrettyString());
  }

  /// The `serverReturnFinalResult` variant. This is the option that gates the single-data-table branch in
  /// `GroupByDataTableReducer#reduceAndSetResults` (`isServerReturnFinalResult() && dataTables.size() == 1`), so it
  /// covers a different reduce branch from the test above. Verified by reverting the `getConvertedKey` fix: both
  /// this and the plain `GROUP BY ... HAVING` test fail with
  /// `ClassCastException: class [B cannot be cast to class java.util.UUID`, and both pass with it.
  @Test
  public void testGroupByUuidColumnWithHavingReturningFinalResult()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode rows = query(
        String.format("SELECT %s, COUNT(*) FROM %s GROUP BY %s HAVING %s = '%s' OPTION(serverReturnFinalResult=true)",
            UUID_COLUMN, getTableName(), UUID_COLUMN, UUID_COLUMN, UUID_0_HEX));
    assertEquals(rows.size(), 1, rows.toPrettyString());
    assertEquals(rows.get(0).get(0).asText(), UUID_0, rows.toPrettyString());
    assertEquals(rows.get(0).get(1).asLong(), 2, rows.toPrettyString());
  }

  /// Group keys must also render canonically on the `getConvertedKey` path, not as hex.
  @Test
  public void testGroupByUuidColumnReturningFinalResult()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode rows = query(
        String.format("SELECT %s, COUNT(*) FROM %s GROUP BY %s ORDER BY %s OPTION(serverReturnFinalResult=true)",
            UUID_COLUMN, getTableName(), UUID_COLUMN, UUID_COLUMN));
    assertEquals(rows.size(), NUM_DISTINCT, rows.toPrettyString());
    List<String> keys = new ArrayList<>();
    for (JsonNode row : rows) {
      keys.add(row.get(0).asText());
    }
    assertEquals(keys, List.of(UUID_0, UUID_1, UUID_2), rows.toPrettyString());
  }

  @Test
  public void testDistinctOnUuidColumn()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode rows =
        query(String.format("SELECT DISTINCT %s FROM %s ORDER BY %s", UUID_COLUMN, getTableName(), UUID_COLUMN));
    assertEquals(rows.size(), NUM_DISTINCT, rows.toPrettyString());

    // BytesDistinctTable used to hard-code toHexString(); a UUID column must render canonically.
    List<String> values = new ArrayList<>();
    for (JsonNode row : rows) {
      values.add(row.get(0).asText());
    }
    assertEquals(values, List.of(UUID_0, UUID_1, UUID_2), rows.toPrettyString());
  }

  @Test
  public void testDistinctCountOnUuidColumn()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    for (String function : List.of("DISTINCTCOUNT", "DISTINCTCOUNTHLL", "DISTINCTCOUNTBITMAP",
        "DISTINCTCOUNTTHETASKETCH", "DISTINCTCOUNTCPCSKETCH")) {
      JsonNode rows = query(String.format("SELECT %s(%s) FROM %s", function, UUID_COLUMN, getTableName()));
      assertEquals(rows.get(0).get(0).asLong(), NUM_DISTINCT, function + ": " + rows.toPrettyString());
    }
  }

  private JsonNode query(String sql)
      throws Exception {
    JsonNode response = postQuery(sql);
    assertTrue(response.path("exceptions").isEmpty(), sql + " -> " + response.toPrettyString());
    return response.path("resultTable").path("rows");
  }
}
