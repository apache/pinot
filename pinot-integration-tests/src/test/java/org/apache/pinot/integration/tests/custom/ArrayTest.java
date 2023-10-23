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
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


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
    Assert.assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    Assert.assertEquals(row.size(), 7);
    Assert.assertEquals(row.get(0).size(), getCountStarResult());
    Assert.assertEquals(row.get(1).size(), getCountStarResult());
    Assert.assertEquals(row.get(2).size(), getCountStarResult());
    Assert.assertEquals(row.get(3).size(), getCountStarResult());
    Assert.assertEquals(row.get(4).size(), getCountStarResult());
    Assert.assertEquals(row.get(5).size(), getCountStarResult());
    Assert.assertEquals(row.get(6).size(), getCountStarResult());
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
    Assert.assertEquals(rows.size(), 10);
    for (int i = 0; i < 10; i++) {
      JsonNode row = rows.get(i);
      Assert.assertEquals(row.size(), 8);
      Assert.assertEquals(row.get(0).size(), getCountStarResult() / 10);
      Assert.assertEquals(row.get(1).size(), getCountStarResult() / 10);
      Assert.assertEquals(row.get(2).size(), getCountStarResult() / 10);
      Assert.assertEquals(row.get(3).size(), getCountStarResult() / 10);
      Assert.assertEquals(row.get(4).size(), getCountStarResult() / 10);
      Assert.assertEquals(row.get(5).size(), getCountStarResult() / 10);
      Assert.assertEquals(row.get(6).size(), getCountStarResult() / 10);
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
    Assert.assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    Assert.assertEquals(row.size(), 7);
    Assert.assertEquals(row.get(0).size(), 2);
    Assert.assertEquals(row.get(1).size(), getCountStarResult() / 10);
    Assert.assertEquals(row.get(2).size(), getCountStarResult() / 10);
    Assert.assertEquals(row.get(3).size(), getCountStarResult() / 10);
    Assert.assertEquals(row.get(4).size(), getCountStarResult() / 10);
    Assert.assertEquals(row.get(5).size(), getCountStarResult() / 10);
    Assert.assertEquals(row.get(6).size(), getCountStarResult() / 10);
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
    Assert.assertEquals(rows.size(), 10);
    for (int i = 0; i < 10; i++) {
      JsonNode row = rows.get(i);
      Assert.assertEquals(row.size(), 8);
      Assert.assertEquals(row.get(0).size(), 2);
      Assert.assertEquals(row.get(1).size(), getCountStarResult() / 100);
      Assert.assertEquals(row.get(2).size(), getCountStarResult() / 100);
      Assert.assertEquals(row.get(3).size(), getCountStarResult() / 100);
      Assert.assertEquals(row.get(4).size(), getCountStarResult() / 100);
      Assert.assertEquals(row.get(5).size(), getCountStarResult() / 100);
      Assert.assertEquals(row.get(6).size(), getCountStarResult() / 100);
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
              record.put(BOOLEAN_COLUMN, RANDOM.nextBoolean());
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
