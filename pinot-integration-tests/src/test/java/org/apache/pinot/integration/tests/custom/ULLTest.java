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

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.local.utils.UltraLogLogUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@Test(suiteName = "CustomClusterIntegrationTest")
public class ULLTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "ULLSketchTest";
  private static final String ID = "id";
  private static final String COLUMN = "ullBytes";

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT DISTINCT_COUNT_ULL(%s), DISTINCT_COUNT_RAW_ULL(%s) FROM %s",
       COLUMN, COLUMN, getTableName());
    JsonNode jsonNode = postQuery(query);
    long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    byte[] rawSketchBytes = Base64.getDecoder().decode(jsonNode.get("resultTable").get("rows").get(0).get(1).asText());
    UltraLogLog ull = UltraLogLog.wrap(rawSketchBytes);

    assertTrue(distinctCount > 0);
    assertEquals(Math.round(ull.getDistinctCountEstimate()), distinctCount);
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testUnionWithSketchQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = String.format(
        "SELECT " + "DISTINCT_COUNT_ULL(%s), " + "DISTINCT_COUNT_RAW_ULL(%s) " + "FROM " + "("
            + "SELECT %s FROM %s WHERE %s = 4 " + "UNION ALL " + "SELECT %s FROM %s WHERE %s = 5 " + "UNION ALL "
            + "SELECT %s FROM %s WHERE %s = 6 " + "UNION ALL " + "SELECT %s FROM %s WHERE %s = 7 " + ")",
        COLUMN, COLUMN, COLUMN, getTableName(), ID, COLUMN,
        getTableName(), ID, COLUMN, getTableName(), ID, COLUMN, getTableName(), ID);
    JsonNode jsonNode = postQuery(query);
    long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    byte[] rawSketchBytes = Base64.getDecoder().decode(jsonNode.get("resultTable").get("rows").get(0).get(1).asText());
    UltraLogLog ull = UltraLogLog.wrap(rawSketchBytes);

    assertTrue(distinctCount > 0);
    assertEquals(Math.round(ull.getDistinctCountEstimate()), distinctCount);
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testJoinWithSketchQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT " + "DISTINCT_COUNT_ULL(a.%s), " + "DISTINCT_COUNT_RAW_ULL(a.%s), "
            + "DISTINCT_COUNT_ULL(b.%s), " + "DISTINCT_COUNT_RAW_ULL(b.%s) " + "FROM "
            + "(SELECT * FROM %s WHERE %s < 8 ) a " + "JOIN " + "(SELECT * FROM %s WHERE %s > 3 ) b "
            + "ON a.%s = b.%s", COLUMN, COLUMN, COLUMN, COLUMN,
        getTableName(), ID, getTableName(), ID, ID, ID);
    JsonNode jsonNode = postQuery(query);
    long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    byte[] rawSketchBytes = Base64.getDecoder().decode(jsonNode.get("resultTable").get("rows").get(0).get(1).asText());
    UltraLogLog ull = UltraLogLog.wrap(rawSketchBytes);

    assertTrue(distinctCount > 0);
    assertEquals(Math.round(ull.getDistinctCountEstimate()), distinctCount);

    distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(2).asLong();
    rawSketchBytes = Base64.getDecoder().decode(jsonNode.get("resultTable").get("rows").get(0).get(3).asText());
    ull = UltraLogLog.wrap(rawSketchBytes);

    assertTrue(distinctCount > 0);
    assertEquals(Math.round(ull.getDistinctCountEstimate()), distinctCount);
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName()).addSingleValueDimension(ID, FieldSpec.DataType.INT)
        .addMetric(COLUMN, FieldSpec.DataType.BYTES).build();
  }

  @Override
  public File createAvroFile()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(ID, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null,
            null), new org.apache.avro.Schema.Field(COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES), null, null)));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < getCountStarResult(); i++) {
        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID, RandomUtils.nextInt(0, 10));
        record.put(COLUMN, ByteBuffer.wrap(getRandomRawValue()));
        // add avro record to file
        fileWriter.append(record);
      }
    }
    return avroFile;
  }

  private byte[] getRandomRawValue() {
    UltraLogLog ull = UltraLogLog.create(4); // test on a smaller P to save some ram & ensure we support custom P values
    UltraLogLogUtils.hashObject(RANDOM.nextInt(100))
        .ifPresent(ull::add);
    return ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.serialize(ull);
  }
}
