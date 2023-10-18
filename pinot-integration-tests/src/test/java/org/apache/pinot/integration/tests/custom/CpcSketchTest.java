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
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@Test(suiteName = "CustomClusterIntegrationTest")
public class CpcSketchTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "CpcSketchTest";
  private static final String ID = "id";
  private static final String MET_CPC_SKETCH_BYTES = "metCpcSketchBytes";

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT DISTINCT_COUNT_CPC_SKETCH(%s), DISTINCT_COUNT_RAW_CPC_SKETCH(%s) FROM %s",
        MET_CPC_SKETCH_BYTES, MET_CPC_SKETCH_BYTES, getTableName());
    JsonNode jsonNode = postQuery(query);
    long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    byte[] rawSketchBytes = Base64.getDecoder().decode(jsonNode.get("resultTable").get("rows").get(0).get(1).asText());
    CpcSketch deserializedSketch = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(rawSketchBytes);

    assertTrue(distinctCount > 0);
    assertEquals(Math.round(deserializedSketch.getEstimate()), distinctCount);
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testCpcUnionQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (int i = 0; i < 10; i++) {
      String query = "SELECT " + "DISTINCT_COUNT_CPC_SKETCH(metCpcSketchBytes), "
          + "GET_CPC_SKETCH_ESTIMATE(DISTINCT_COUNT_RAW_CPC_SKETCH(metCpcSketchBytes)) " + "FROM " + getTableName()
          + " WHERE id=" + i;
      JsonNode jsonNode = postQuery(query);
      long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
      assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(1).asLong(), distinctCount);
      query = "SELECT " + "GET_CPC_SKETCH_ESTIMATE(DISTINCT_COUNT_RAW_CPC_SKETCH(metCpcSketchBytes) "
          + "FILTER (WHERE id = " + i + ")) " + "FROM " + getTableName();
      jsonNode = postQuery(query);
      assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asLong(), distinctCount);
    }

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        // Query Type 1:
        String query = "SELECT DISTINCT_COUNT_CPC_SKETCH(metCpcSketchBytes), "
            + "GET_CPC_SKETCH_ESTIMATE(DISTINCT_COUNT_RAW_CPC_SKETCH(metCpcSketchBytes)) " + "FROM " + getTableName()
            + " WHERE id=" + i + " OR id=" + j;
        JsonNode jsonNode = postQuery(query);
        long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
        assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(1).asLong(), distinctCount);

        // Query Type 2:
        query = "SELECT " + "GET_CPC_SKETCH_ESTIMATE(DISTINCT_COUNT_RAW_CPC_SKETCH(metCpcSketchBytes) "
            + "FILTER (WHERE id = " + i + " OR id = " + j + ")) " + "FROM " + getTableName();
        jsonNode = postQuery(query);
        assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asLong(), distinctCount);

        // Query Type 3:
        query = "SELECT " + "GET_CPC_SKETCH_ESTIMATE(CPC_SKETCH_UNION( "
            + "DISTINCT_COUNT_RAW_CPC_SKETCH(metCpcSketchBytes) FILTER (WHERE id = " + i + "),"
            + "DISTINCT_COUNT_RAW_CPC_SKETCH(metCpcSketchBytes) FILTER (WHERE id = " + j + ")))" + " FROM "
            + getTableName();

        jsonNode = postQuery(query);
        assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asLong(), distinctCount);
      }
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testUnionWithSketchQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = String.format(
        "SELECT " + "DISTINCT_COUNT_CPC_SKETCH(%s), " + "DISTINCT_COUNT_RAW_CPC_SKETCH(%s) " + "FROM " + "("
            + "SELECT %s FROM %s WHERE %s = 4 " + "UNION ALL " + "SELECT %s FROM %s WHERE %s = 5 " + "UNION ALL "
            + "SELECT %s FROM %s WHERE %s = 6 " + "UNION ALL " + "SELECT %s FROM %s WHERE %s = 7 " + ")",
        MET_CPC_SKETCH_BYTES, MET_CPC_SKETCH_BYTES, MET_CPC_SKETCH_BYTES, getTableName(), ID, MET_CPC_SKETCH_BYTES,
        getTableName(), ID, MET_CPC_SKETCH_BYTES, getTableName(), ID, MET_CPC_SKETCH_BYTES, getTableName(), ID);
    JsonNode jsonNode = postQuery(query);
    long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    byte[] rawSketchBytes = Base64.getDecoder().decode(jsonNode.get("resultTable").get("rows").get(0).get(1).asText());
    CpcSketch deserializedSketch = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(rawSketchBytes);

    assertTrue(distinctCount > 0);
    assertEquals(Math.round(deserializedSketch.getEstimate()), distinctCount);
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testJoinWithSketchQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT " + "DISTINCT_COUNT_CPC_SKETCH(a.%s), " + "DISTINCT_COUNT_RAW_CPC_SKETCH(a.%s), "
            + "DISTINCT_COUNT_CPC_SKETCH(b.%s), " + "DISTINCT_COUNT_RAW_CPC_SKETCH(b.%s) " + "FROM "
            + "(SELECT * FROM %s WHERE %s < 8 ) a " + "JOIN " + "(SELECT * FROM %s WHERE %s > 3 ) b "
            + "ON a.%s = b.%s", MET_CPC_SKETCH_BYTES, MET_CPC_SKETCH_BYTES, MET_CPC_SKETCH_BYTES, MET_CPC_SKETCH_BYTES,
        getTableName(), ID, getTableName(), ID, ID, ID);
    JsonNode jsonNode = postQuery(query);
    long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    byte[] rawSketchBytes = Base64.getDecoder().decode(jsonNode.get("resultTable").get("rows").get(0).get(1).asText());
    CpcSketch deserializedSketch = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(rawSketchBytes);
    assertTrue(distinctCount > 0);
    assertEquals(Math.round(deserializedSketch.getEstimate()), distinctCount);

    distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(2).asLong();
    rawSketchBytes = Base64.getDecoder().decode(jsonNode.get("resultTable").get("rows").get(0).get(3).asText());
    deserializedSketch = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(rawSketchBytes);
    assertTrue(distinctCount > 0);
    assertEquals(Math.round(deserializedSketch.getEstimate()), distinctCount);
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName()).addSingleValueDimension(ID, FieldSpec.DataType.INT)
        .addMetric(MET_CPC_SKETCH_BYTES, FieldSpec.DataType.BYTES).build();
  }

  @Override
  public File createAvroFile()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(ID, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null,
            null), new org.apache.avro.Schema.Field(MET_CPC_SKETCH_BYTES,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES), null, null)));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < getCountStarResult(); i++) {
        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID, RandomUtils.nextInt(10));
        record.put(MET_CPC_SKETCH_BYTES, ByteBuffer.wrap(getRandomRawValue()));
        // add avro record to file
        fileWriter.append(record);
      }
    }
    return avroFile;
  }

  private byte[] getRandomRawValue() {
    CpcSketch sketch = new CpcSketch(4);
    sketch.update(RANDOM.nextInt(100));
    return ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(sketch);
  }
}
