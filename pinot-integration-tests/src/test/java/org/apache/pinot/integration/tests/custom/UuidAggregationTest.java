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


/// End-to-end UUID aggregation coverage over dictionary-encoded and raw SV/MV columns.
@Test(suiteName = "CustomClusterIntegrationTest")
public class UuidAggregationTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "UuidAggregationTest";
  private static final String UUID_DICT_SV_COLUMN = "uuidDictSv";
  private static final String UUID_DICT_MV_COLUMN = "uuidDictMv";
  private static final String UUID_RAW_SV_COLUMN = "uuidRawSv";
  private static final String UUID_RAW_MV_COLUMN = "uuidRawMv";

  private static final String UUID_0 = "550e8400-e29b-41d4-a716-446655440000";
  private static final String UUID_0_HEX = "550e8400e29b41d4a716446655440000";
  private static final String UUID_1 = "550e8400-e29b-41d4-a716-446655440001";
  private static final String UUID_2 = "550e8400-e29b-41d4-a716-446655440002";
  private static final String UUID_3 = "550e8400-e29b-41d4-a716-446655440003";

  private static final List<String> UUID_SV_VALUES = List.of(UUID_0, UUID_0, UUID_1, UUID_2);
  private static final List<List<String>> UUID_MV_VALUES =
      List.of(List.of(UUID_0, UUID_1), List.of(UUID_1, UUID_2), List.of(UUID_0), List.of(UUID_3));

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return UUID_SV_VALUES.size();
  }

  @Override
  public int getNumAvroFiles() {
    return 1;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setNoDictionaryColumns(List.of(UUID_RAW_SV_COLUMN, UUID_RAW_MV_COLUMN)).build();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(UUID_DICT_SV_COLUMN, DataType.UUID)
        .addMultiValueDimension(UUID_DICT_MV_COLUMN, DataType.UUID)
        .addSingleValueDimension(UUID_RAW_SV_COLUMN, DataType.UUID)
        .addMultiValueDimension(UUID_RAW_MV_COLUMN, DataType.UUID)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema uuidSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("uuidRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(UUID_DICT_SV_COLUMN, uuidSchema, null, null),
        new org.apache.avro.Schema.Field(UUID_DICT_MV_COLUMN, org.apache.avro.Schema.createArray(uuidSchema), null,
            null),
        new org.apache.avro.Schema.Field(UUID_RAW_SV_COLUMN, uuidSchema, null, null),
        new org.apache.avro.Schema.Field(UUID_RAW_MV_COLUMN, org.apache.avro.Schema.createArray(uuidSchema), null,
            null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      DataFileWriter<GenericData.Record> writer = avroFilesAndWriters.getWriters().get(0);
      for (int i = 0; i < UUID_SV_VALUES.size(); i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(UUID_DICT_SV_COLUMN, UUID_SV_VALUES.get(i));
        record.put(UUID_DICT_MV_COLUMN, UUID_MV_VALUES.get(i));
        record.put(UUID_RAW_SV_COLUMN, UUID_SV_VALUES.get(i));
        record.put(UUID_RAW_MV_COLUMN, UUID_MV_VALUES.get(i));
        writer.append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test
  public void testGroupByHavingReturningFinalResult()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode rows = query(String.format(
        "SELECT %1$s, COUNT(*) FROM %2$s GROUP BY %1$s HAVING %1$s = '%3$s' "
            + "OPTION(serverReturnFinalResult=true)",
        UUID_DICT_SV_COLUMN, getTableName(), UUID_0_HEX));

    assertEquals(rows.size(), 1, rows.toPrettyString());
    assertEquals(rows.get(0).get(0).asText(), UUID_0, rows.toPrettyString());
    assertEquals(rows.get(0).get(1).asLong(), 2L, rows.toPrettyString());
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDistinctOnUuidColumn(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode rows = query(String.format("SELECT DISTINCT %1$s FROM %2$s ORDER BY %1$s", UUID_DICT_SV_COLUMN,
        getTableName()));

    assertEquals(rows.size(), 3, rows.toPrettyString());
    for (int i = 0; i < rows.size(); i++) {
      assertEquals(rows.get(i).get(0).asText(), List.of(UUID_0, UUID_1, UUID_2).get(i), rows.toPrettyString());
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testMultiStageUuidLiteralPredicate(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode rows = query(String.format(
        "SELECT COUNT(*) FROM %1$s WHERE %2$s = CAST('%3$s' AS UUID)",
        getTableName(), UUID_RAW_SV_COLUMN, UUID_0));
    assertCounts(rows.get(0), 2L);
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testMultiStageUuidEqualityJoin(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode rows = query(String.format("SELECT COUNT(*) FROM %2$s a JOIN %2$s b ON a.%1$s = b.%3$s",
        UUID_DICT_SV_COLUMN, getTableName(), UUID_RAW_SV_COLUMN));
    assertCounts(rows.get(0), 6L);
  }

  @Test
  public void testDistinctCountOnUuidColumns()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    for (String function : List.of("DISTINCTCOUNT", "DISTINCTCOUNTHLL", "DISTINCTCOUNTHLLPLUS",
        "DISTINCTCOUNTBITMAP", "DISTINCTCOUNTTHETASKETCH", "DISTINCTCOUNTCPCSKETCH", "DISTINCTCOUNTULL")) {
      JsonNode rows = query(String.format("SELECT %1$s(%2$s), %1$s(%3$s), %1$s(%4$s) FROM %5$s", function,
          UUID_DICT_SV_COLUMN, UUID_RAW_SV_COLUMN, UUID_RAW_MV_COLUMN, getTableName()));
      assertCounts(rows.get(0), 3L, 3L, 4L);
    }

    // A dictionary-encoded multi-value column collects dictionary ids rather than reading values, which is a
    // separate path from the raw multi-value column above
    JsonNode rows =
        query(String.format("SELECT DISTINCTCOUNTULL(%s) FROM %s", UUID_DICT_MV_COLUMN, getTableName()));
    assertCounts(rows.get(0), 4L);

    rows = query(String.format(
        "SELECT DISTINCTCOUNTTHETASKETCH(%1$s, '', '%1$s = ''%3$s''', '$1'), "
            + "DISTINCTCOUNTTHETASKETCH(%2$s, '', '%2$s = ''%3$s''', '$1') FROM %4$s",
        UUID_RAW_SV_COLUMN, UUID_RAW_MV_COLUMN, UUID_0, getTableName()));
    assertCounts(rows.get(0), 1L, 2L);
  }

  @Test
  public void testCpcAndThetaGroupByUuidColumns()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode rows = queryGroupBy(UUID_RAW_SV_COLUMN);
    assertEquals(rows.size(), 3, rows.toPrettyString());
    assertGroupRow(rows.get(0), UUID_0, 1L, 3L, 1L, 3L);
    assertGroupRow(rows.get(1), UUID_1, 1L, 1L, 1L, 1L);
    assertGroupRow(rows.get(2), UUID_2, 1L, 1L, 1L, 1L);

    // UUID multi-value group keys require a dictionary, while the aggregate input remains raw.
    rows = queryGroupBy(UUID_DICT_MV_COLUMN);
    assertEquals(rows.size(), 4, rows.toPrettyString());
    assertGroupRow(rows.get(0), UUID_0, 2L, 2L, 2L, 2L);
    assertGroupRow(rows.get(1), UUID_1, 1L, 3L, 1L, 3L);
    assertGroupRow(rows.get(2), UUID_2, 1L, 2L, 1L, 2L);
    assertGroupRow(rows.get(3), UUID_3, 1L, 1L, 1L, 1L);
  }

  private JsonNode queryGroupBy(String groupByColumn)
      throws Exception {
    return query(String.format(
        "SELECT %1$s, DISTINCTCOUNTCPCSKETCH(%2$s), DISTINCTCOUNTCPCSKETCH(%3$s), "
            + "DISTINCTCOUNTTHETASKETCH(%2$s), DISTINCTCOUNTTHETASKETCH(%3$s) "
            + "FROM %4$s GROUP BY %1$s ORDER BY %1$s",
        groupByColumn, UUID_RAW_SV_COLUMN, UUID_RAW_MV_COLUMN, getTableName()));
  }

  private JsonNode query(String sql)
      throws Exception {
    JsonNode response = postQuery(sql);
    assertTrue(response.path("exceptions").isEmpty(), sql + " -> " + response.toPrettyString());
    return response.path("resultTable").path("rows");
  }

  private static void assertGroupRow(JsonNode row, String groupKey, long... expectedCounts) {
    assertEquals(row.get(0).asText(), groupKey, row.toPrettyString());
    for (int i = 0; i < expectedCounts.length; i++) {
      assertEquals(row.get(i + 1).asLong(), expectedCounts[i], row.toPrettyString());
    }
  }

  private static void assertCounts(JsonNode row, long... expectedCounts) {
    assertEquals(row.size(), expectedCounts.length, row.toPrettyString());
    for (int i = 0; i < expectedCounts.length; i++) {
      assertEquals(row.get(i).asLong(), expectedCounts[i], row.toPrettyString());
    }
  }
}
