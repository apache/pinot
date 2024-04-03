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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class ArrayTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "ArrayTest";
  private static final String BOOLEAN_COLUMN = "boolCol";
  private static final String INT_COLUMN = "intCol";
  private static final String LONG_COLUMN = "longCol";
  private static final String FLOAT_COLUMN = "floatCol";
  private static final String DOUBLE_COLUMN = "doubleCol";
  private static final String STRING_COLUMN = "stringCol";
  private static final String TIMESTAMP_COLUMN = "timestampCol";
  private static final String GROUP_BY_COLUMN = "groupKey";

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testArrayAggQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "arrayAgg(boolCol, 'BOOLEAN'), "
            + "arrayAgg(intCol, 'INT'), "
            + "arrayAgg(longCol, 'LONG'), "
            // NOTE: FLOAT array is auto converted to DOUBLE array
            + (useMultiStageQueryEngine ? "arrayAgg(floatCol, 'DOUBLE'), " : "arrayAgg(floatCol, 'FLOAT'), ")
            + "arrayAgg(doubleCol, 'DOUBLE'), "
            + "arrayAgg(stringCol, 'STRING'), "
            + "arrayAgg(timestampCol, 'TIMESTAMP') "
            + "FROM %s LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 7);
    assertEquals(row.get(0).size(), getCountStarResult());
    assertEquals(row.get(1).size(), getCountStarResult());
    assertEquals(row.get(2).size(), getCountStarResult());
    assertEquals(row.get(3).size(), getCountStarResult());
    assertEquals(row.get(4).size(), getCountStarResult());
    assertEquals(row.get(5).size(), getCountStarResult());
    assertEquals(row.get(6).size(), getCountStarResult());
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testArrayAggGroupByQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "arrayAgg(boolCol, 'BOOLEAN'), "
            + "arrayAgg(intCol, 'INT'), "
            + "arrayAgg(longCol, 'LONG'), "
            // NOTE: FLOAT array is auto converted to DOUBLE array
            + (useMultiStageQueryEngine ? "arrayAgg(floatCol, 'DOUBLE'), " : "arrayAgg(floatCol, 'FLOAT'), ")
            + "arrayAgg(doubleCol, 'DOUBLE'), "
            + "arrayAgg(stringCol, 'STRING'), "
            + "arrayAgg(timestampCol, 'TIMESTAMP'), "
            + "groupKey "
            + "FROM %s "
            + "GROUP BY groupKey "
            + "LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 10);
    for (int i = 0; i < 10; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 8);
      assertEquals(row.get(0).size(), getCountStarResult() / 10);
      assertEquals(row.get(1).size(), getCountStarResult() / 10);
      assertEquals(row.get(2).size(), getCountStarResult() / 10);
      assertEquals(row.get(3).size(), getCountStarResult() / 10);
      assertEquals(row.get(4).size(), getCountStarResult() / 10);
      assertEquals(row.get(5).size(), getCountStarResult() / 10);
      assertEquals(row.get(6).size(), getCountStarResult() / 10);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testArrayAggDistinctQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "arrayAgg(boolCol, 'BOOLEAN', true), "
            + "arrayAgg(intCol, 'INT', true), "
            + "arrayAgg(longCol, 'LONG', true), "
            // NOTE: FLOAT array is auto converted to DOUBLE array
            + (useMultiStageQueryEngine ? "arrayAgg(floatCol, 'DOUBLE', true), "
            : "arrayAgg(floatCol, 'FLOAT', true), ")
            + "arrayAgg(doubleCol, 'DOUBLE', true), "
            + "arrayAgg(stringCol, 'STRING', true), "
            + "arrayAgg(timestampCol, 'TIMESTAMP', true) "
            + "FROM %s LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 7);
    assertEquals(row.get(0).size(), 2);
    assertEquals(row.get(1).size(), getCountStarResult() / 10);
    assertEquals(row.get(2).size(), getCountStarResult() / 10);
    assertEquals(row.get(3).size(), getCountStarResult() / 10);
    assertEquals(row.get(4).size(), getCountStarResult() / 10);
    assertEquals(row.get(5).size(), getCountStarResult() / 10);
    assertEquals(row.get(6).size(), getCountStarResult() / 10);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testArrayAggDistinctGroupByQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "arrayAgg(boolCol, 'BOOLEAN', true), "
            + "arrayAgg(intCol, 'INT', true), "
            + "arrayAgg(longCol, 'LONG', true), "
            // NOTE: FLOAT array is auto converted to DOUBLE array
            + (useMultiStageQueryEngine ? "arrayAgg(floatCol, 'DOUBLE', true), "
            : "arrayAgg(floatCol, 'FLOAT', true), ")
            + "arrayAgg(doubleCol, 'DOUBLE', true), "
            + "arrayAgg(stringCol, 'STRING', true), "
            + "arrayAgg(timestampCol, 'TIMESTAMP', true), "
            + "groupKey "
            + "FROM %s "
            + "GROUP BY groupKey "
            + "LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 10);
    for (int i = 0; i < 10; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 8);
      assertEquals(row.get(0).size(), 2);
      assertEquals(row.get(1).size(), getCountStarResult() / 100);
      assertEquals(row.get(2).size(), getCountStarResult() / 100);
      assertEquals(row.get(3).size(), getCountStarResult() / 100);
      assertEquals(row.get(4).size(), getCountStarResult() / 100);
      assertEquals(row.get(5).size(), getCountStarResult() / 100);
      assertEquals(row.get(6).size(), getCountStarResult() / 100);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testStringSplitFunction(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "split('t1,t2,t3', ',') "
            + "FROM %s LIMIT 1", getTableName());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 1);
    assertEquals(row.get(0).size(), 3);
    assertEquals(row.get(0).get(0).asText(), "t1");
    assertEquals(row.get(0).get(1).asText(), "t2");
    assertEquals(row.get(0).get(2).asText(), "t3");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testIntArrayLiteral(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (String arrayLiteral : List.of("ARRAY[1,2,3]", "ARRAY'{1,2,3}'")) {
      for (boolean withFrom : new boolean[]{true, false}) {
        String query = withFrom ? String.format("SELECT %s FROM %s LIMIT 1", arrayLiteral, getTableName())
            : "SELECT " + arrayLiteral;
        JsonNode result = postQuery(query).get("resultTable");
        // TODO: Check data schema
        JsonNode rows = result.get("rows");
        assertEquals(rows.size(), 1);
        JsonNode row = rows.get(0);
        assertEquals(row.size(), 1);
        assertEquals(row.get(0).size(), 3);
        assertEquals(row.get(0).get(0).asInt(), 1);
        assertEquals(row.get(0).get(1).asInt(), 2);
        assertEquals(row.get(0).get(2).asInt(), 3);
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testLongArrayLiteral(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (String arrayLiteral : List.of("ARRAY[2147483648,2147483649,2147483650]",
        "ARRAY'{2147483648,2147483649,2147483650}'")) {
      for (boolean withFrom : new boolean[]{true, false}) {
        String query = withFrom ? String.format("SELECT %s FROM %s LIMIT 1", arrayLiteral, getTableName())
            : "SELECT " + arrayLiteral;
        JsonNode result = postQuery(query).get("resultTable");
        assertEquals(result.get("dataSchema").get("columnDataTypes").get(0).textValue(), "LONG_ARRAY");
        JsonNode rows = result.get("rows");
        assertEquals(rows.size(), 1);
        JsonNode row = rows.get(0);
        assertEquals(row.size(), 1);
        assertEquals(row.get(0).size(), 3);
        assertEquals(row.get(0).get(0).longValue(), 2147483648L);
        assertEquals(row.get(0).get(1).longValue(), 2147483649L);
        assertEquals(row.get(0).get(2).longValue(), 2147483650L);
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFloatArrayLiteral(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (String arrayLiteral : List.of("ARRAY[0.1,0.2,0.3]", "ARRAY'{0.1,0.2,0.3}'")) {
      for (boolean withFrom : new boolean[]{true, false}) {
        String query = withFrom ? String.format("SELECT %s FROM %s LIMIT 1", arrayLiteral, getTableName())
            : "SELECT " + arrayLiteral;
        JsonNode result = postQuery(query).get("resultTable");
        // TODO: Check data schema
        JsonNode rows = result.get("rows");
        assertEquals(rows.size(), 1);
        JsonNode row = rows.get(0);
        assertEquals(row.size(), 1);
        assertEquals(row.get(0).size(), 3);
        assertEquals(row.get(0).get(0).asDouble(), 0.1);
        assertEquals(row.get(0).get(1).asDouble(), 0.2);
        assertEquals(row.get(0).get(2).asDouble(), 0.3);
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDoubleArrayLiteral(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String arrayLiteral = "ARRAY[CAST(0.1 AS DOUBLE),CAST(0.2 AS DOUBLE),CAST(0.3 AS DOUBLE)]";
    for (boolean withFrom : new boolean[]{true, false}) {
      String query = withFrom ? String.format("SELECT %s FROM %s LIMIT 1", arrayLiteral, getTableName())
          : "SELECT " + arrayLiteral;
      JsonNode result = postQuery(query).get("resultTable");
      // TODO: Check data schema
      JsonNode rows = result.get("rows");
      assertEquals(rows.size(), 1);
      JsonNode row = rows.get(0);
      assertEquals(row.size(), 1);
      assertEquals(row.get(0).size(), 3);
      assertEquals(row.get(0).get(0).asDouble(), 0.1);
      assertEquals(row.get(0).get(1).asDouble(), 0.2);
      assertEquals(row.get(0).get(2).asDouble(), 0.3);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testStringArrayLiteral(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (String arrayLiteral : List.of("ARRAY['a','bb','ccc']", "ARRAY'{\"a\",\"bb\",\"ccc\"}'")) {
      for (boolean withFrom : new boolean[]{true, false}) {
        String query = withFrom ? String.format("SELECT %s FROM %s LIMIT 1", arrayLiteral, getTableName())
            : "SELECT " + arrayLiteral;
        JsonNode result = postQuery(query).get("resultTable");
        assertEquals(result.get("dataSchema").get("columnDataTypes").get(0).textValue(), "STRING_ARRAY");
        JsonNode rows = result.get("rows");
        assertEquals(rows.size(), 1);
        JsonNode row = rows.get(0);
        assertEquals(row.size(), 1);
        assertEquals(row.get(0).size(), 3);
        assertEquals(row.get(0).get(0).textValue(), "a");
        assertEquals(row.get(0).get(1).textValue(), "bb");
        assertEquals(row.get(0).get(2).textValue(), "ccc");
      }
    }
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(BOOLEAN_COLUMN, FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
        .addSingleValueDimension(FLOAT_COLUMN, FieldSpec.DataType.FLOAT)
        .addSingleValueDimension(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(TIMESTAMP_COLUMN, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(GROUP_BY_COLUMN, FieldSpec.DataType.STRING)
        .build();
  }

  @Override
  public File createAvroFile()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(BOOLEAN_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN),
            null, null),
        new org.apache.avro.Schema.Field(INT_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null),
        new org.apache.avro.Schema.Field(LONG_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(FLOAT_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT),
            null, null),
        new org.apache.avro.Schema.Field(DOUBLE_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(STRING_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(TIMESTAMP_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(GROUP_BY_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null)
    ));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    Cache<Integer, GenericData.Record> recordCache = CacheBuilder.newBuilder().build();
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < getCountStarResult(); i++) {
        // add avro record to file
        int finalI = i;
        fileWriter.append(recordCache.get((int) (i % (getCountStarResult() / 10)), () -> {
              // create avro record
              GenericData.Record record = new GenericData.Record(avroSchema);
              record.put(BOOLEAN_COLUMN, finalI % 4 == 0 || finalI % 4 == 1);
              record.put(INT_COLUMN, finalI);
              record.put(LONG_COLUMN, finalI);
              record.put(FLOAT_COLUMN, finalI + RANDOM.nextFloat());
              record.put(DOUBLE_COLUMN, finalI + RANDOM.nextDouble());
              record.put(STRING_COLUMN, RandomStringUtils.random(finalI));
              record.put(TIMESTAMP_COLUMN, finalI);
              record.put(GROUP_BY_COLUMN, String.valueOf(finalI % 10));
              return record;
            }
        ));
      }
    }
    return avroFile;
  }
}
