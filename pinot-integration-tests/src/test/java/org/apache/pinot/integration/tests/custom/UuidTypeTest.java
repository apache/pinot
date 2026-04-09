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
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.create;
import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class UuidTypeTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "UuidTypeTest";
  private static final String ID_COLUMN = "id";
  private static final String UUID_FROM_STRING_COLUMN = "uuidFromString";
  private static final String UUID_FROM_BYTES_COLUMN = "uuidFromBytes";
  private static final String TIME_COLUMN = "ts";
  private static final List<String> UUID_VALUES = List.of(
      "550e8400-e29b-41d4-a716-446655440000",
      "550e8400-e29b-41d4-a716-446655440001",
      "550e8400-e29b-41d4-a716-446655440000",
      "550e8400-e29b-41d4-a716-446655440002",
      "550e8400-e29b-41d4-a716-446655440001",
      "550e8400-e29b-41d4-a716-446655440003");
  private static final List<String> DISTINCT_UUID_VALUES = List.of(
      "550e8400-e29b-41d4-a716-446655440000",
      "550e8400-e29b-41d4-a716-446655440001",
      "550e8400-e29b-41d4-a716-446655440002",
      "550e8400-e29b-41d4-a716-446655440003");
  private static final long BASE_TIMESTAMP_MILLIS = 1_700_000_000_000L;

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public String getTimeColumnName() {
    return TIME_COLUMN;
  }

  @Override
  protected long getCountStarResult() {
    return UUID_VALUES.size();
  }

  @Override
  public int getNumAvroFiles() {
    return 1;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    return 2;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(ID_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(UUID_FROM_STRING_COLUMN, FieldSpec.DataType.UUID)
        .addSingleValueDimension(UUID_FROM_BYTES_COLUMN, FieldSpec.DataType.UUID)
        .addDateTimeField(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("uuidRecord", null, null, false);
    avroSchema.setFields(List.of(
        new Field(ID_COLUMN, create(Type.INT), null, null),
        new Field(UUID_FROM_STRING_COLUMN, create(Type.STRING), null, null),
        new Field(UUID_FROM_BYTES_COLUMN, create(Type.BYTES), null, null),
        new Field(TIME_COLUMN, create(Type.LONG), null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < UUID_VALUES.size(); i++) {
        String uuidValue = UUID_VALUES.get(i);
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID_COLUMN, i);
        record.put(UUID_FROM_STRING_COLUMN, uuidValue);
        record.put(UUID_FROM_BYTES_COLUMN, ByteBuffer.wrap(UuidUtils.toBytes(uuidValue)));
        record.put(TIME_COLUMN, BASE_TIMESTAMP_MILLIS + i);
        writers.get(0).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSelectAndFilterQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode response = postQuery(String.format(
        "SELECT %s, %s, %s FROM %s ORDER BY %s", ID_COLUMN, UUID_FROM_STRING_COLUMN, UUID_FROM_BYTES_COLUMN,
        getTableName(), ID_COLUMN));
    assertNoExceptions(response);

    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), UUID_VALUES.size());
    for (int i = 0; i < UUID_VALUES.size(); i++) {
      assertEquals(rows.get(i).get(0).asInt(), i);
      assertEquals(rows.get(i).get(1).asText(), UUID_VALUES.get(i));
      assertEquals(rows.get(i).get(2).asText(), UUID_VALUES.get(i));
    }

    response = postQuery(String.format(
        "SELECT %s FROM %s WHERE %s = CAST('%s' AS UUID) ORDER BY %s", ID_COLUMN, getTableName(),
        UUID_FROM_STRING_COLUMN, UUID_VALUES.get(1), ID_COLUMN));
    assertNoExceptions(response);
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0).get(0).asInt(), 1);
    assertEquals(rows.get(1).get(0).asInt(), 4);

    response = postQuery(String.format(
        "SELECT %s FROM %s WHERE %s IN (CAST('%s' AS UUID), CAST('%s' AS UUID)) ORDER BY %s", ID_COLUMN,
        getTableName(), UUID_FROM_BYTES_COLUMN, DISTINCT_UUID_VALUES.get(0), DISTINCT_UUID_VALUES.get(2), ID_COLUMN));
    assertNoExceptions(response);
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0).get(0).asInt(), 0);
    assertEquals(rows.get(1).get(0).asInt(), 2);
    assertEquals(rows.get(2).get(0).asInt(), 3);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGroupByDistinctAndOrderByQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode response = postQuery(String.format(
        "SELECT %s, COUNT(*) FROM %s GROUP BY %s ORDER BY %s", UUID_FROM_STRING_COLUMN, getTableName(),
        UUID_FROM_STRING_COLUMN, UUID_FROM_STRING_COLUMN));
    assertNoExceptions(response);

    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), DISTINCT_UUID_VALUES.size());
    assertEquals(rows.get(0).get(0).asText(), DISTINCT_UUID_VALUES.get(0));
    assertEquals(rows.get(0).get(1).asInt(), 2);
    assertEquals(rows.get(1).get(0).asText(), DISTINCT_UUID_VALUES.get(1));
    assertEquals(rows.get(1).get(1).asInt(), 2);
    assertEquals(rows.get(2).get(0).asText(), DISTINCT_UUID_VALUES.get(2));
    assertEquals(rows.get(2).get(1).asInt(), 1);
    assertEquals(rows.get(3).get(0).asText(), DISTINCT_UUID_VALUES.get(3));
    assertEquals(rows.get(3).get(1).asInt(), 1);

    response = postQuery(String.format(
        "SELECT DISTINCT %s FROM %s ORDER BY %s", UUID_FROM_BYTES_COLUMN, getTableName(), UUID_FROM_BYTES_COLUMN));
    assertNoExceptions(response);
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), DISTINCT_UUID_VALUES.size());
    for (int i = 0; i < DISTINCT_UUID_VALUES.size(); i++) {
      assertEquals(rows.get(i).get(0).asText(), DISTINCT_UUID_VALUES.get(i));
    }

    response = postQuery(String.format(
        "SELECT %s, %s FROM %s ORDER BY %s, %s", UUID_FROM_STRING_COLUMN, ID_COLUMN, getTableName(),
        UUID_FROM_STRING_COLUMN, ID_COLUMN));
    assertNoExceptions(response);
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), UUID_VALUES.size());
    List<String> sortedUuidValues = List.of(
        DISTINCT_UUID_VALUES.get(0),
        DISTINCT_UUID_VALUES.get(0),
        DISTINCT_UUID_VALUES.get(1),
        DISTINCT_UUID_VALUES.get(1),
        DISTINCT_UUID_VALUES.get(2),
        DISTINCT_UUID_VALUES.get(3));
    for (int i = 0; i < sortedUuidValues.size(); i++) {
      assertEquals(rows.get(i).get(0).asText(), sortedUuidValues.get(i));
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testUuidEqualityJoin(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode response = postQuery(String.format(
        "SELECT a.%s, b.%s, a.%s "
            + "FROM (SELECT %s, %s FROM %s WHERE %s < 5) a "
            + "JOIN (SELECT %s, %s FROM %s WHERE %s > 0) b "
            + "ON a.%s = b.%s "
            + "WHERE a.%s < b.%s "
            + "ORDER BY a.%s, b.%s",
        ID_COLUMN, ID_COLUMN, UUID_FROM_BYTES_COLUMN, ID_COLUMN, UUID_FROM_BYTES_COLUMN, getTableName(), ID_COLUMN,
        ID_COLUMN, UUID_FROM_BYTES_COLUMN, getTableName(), ID_COLUMN, UUID_FROM_BYTES_COLUMN, UUID_FROM_BYTES_COLUMN,
        ID_COLUMN, ID_COLUMN, ID_COLUMN, ID_COLUMN));
    assertNoExceptions(response);

    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0).get(0).asInt(), 0);
    assertEquals(rows.get(0).get(1).asInt(), 2);
    assertEquals(rows.get(0).get(2).asText(), DISTINCT_UUID_VALUES.get(0));
    assertEquals(rows.get(1).get(0).asInt(), 1);
    assertEquals(rows.get(1).get(1).asInt(), 4);
    assertEquals(rows.get(1).get(2).asText(), DISTINCT_UUID_VALUES.get(1));
  }

  private static void assertNoExceptions(JsonNode response) {
    assertEquals(response.get("exceptions").size(), 0, response.toPrettyString());
  }
}
