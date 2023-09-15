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
import org.apache.datasketches.tuple.Intersection;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.aninteger.IntegerSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@Test(suiteName = "CustomClusterIntegrationTest")
public class TupleSketchTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "TupleSketchTest";
  private static final String ID = "id";
  private static final String MET_TUPLE_SKETCH_BYTES = "metTupleSketchBytes";

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format(
            "SELECT DISTINCT_COUNT_TUPLE_SKETCH(%s), DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(%s), "
                + "SUM_VALUES_INTEGER_SUM_TUPLE_SKETCH(%s), AVG_VALUE_INTEGER_SUM_TUPLE_SKETCH(%s) FROM %s",
            MET_TUPLE_SKETCH_BYTES, MET_TUPLE_SKETCH_BYTES, MET_TUPLE_SKETCH_BYTES, MET_TUPLE_SKETCH_BYTES,
            getTableName());
    JsonNode jsonNode = postQuery(query);
    long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    byte[] rawSketchBytes = Base64.getDecoder().decode(jsonNode.get("resultTable").get("rows").get(0).get(1).asText());
    Sketch<IntegerSummary> deserializedSketch =
        ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(rawSketchBytes);

    assertTrue(distinctCount > 0);
    assertEquals(Double.valueOf(deserializedSketch.getEstimate()).longValue(), distinctCount);
    assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(2).asLong() > 0);
    assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(3).asLong() > 0);
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testTupleUnionQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (int i = 0; i < 10; i++) {
      String query = "SELECT "
          + "DISTINCT_COUNT_TUPLE_SKETCH(metTupleSketchBytes), "
          + "GET_INT_TUPLE_SKETCH_ESTIMATE(DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(metTupleSketchBytes)) "
          + "FROM " + getTableName()
          + " WHERE id=" + i;
      JsonNode jsonNode = postQuery(query);
      long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
      assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(1).asLong(), distinctCount);
      query = "SELECT "
          + "GET_INT_TUPLE_SKETCH_ESTIMATE(DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(metTupleSketchBytes) "
          + "FILTER (WHERE id = " + i + ")) "
          + "FROM " + getTableName();
      jsonNode = postQuery(query);
      assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asLong(), distinctCount);
    }

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        // Query Type 1:
        String query = "SELECT DISTINCT_COUNT_TUPLE_SKETCH(metTupleSketchBytes), "
            + "GET_INT_TUPLE_SKETCH_ESTIMATE(DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(metTupleSketchBytes)) "
            + "FROM " + getTableName()
            + " WHERE id=" + i + " OR id=" + j;
        JsonNode jsonNode = postQuery(query);
        long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
        assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(1).asLong(), distinctCount);

        // Query Type 2:
        query = "SELECT "
            + "GET_INT_TUPLE_SKETCH_ESTIMATE(DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(metTupleSketchBytes) "
            + "FILTER (WHERE id = " + i + " OR id = " + j + ")) "
            + "FROM " + getTableName();
        jsonNode = postQuery(query);
        assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asLong(), distinctCount);

        // Query Type 3:
        query = "SELECT "
            + "GET_INT_TUPLE_SKETCH_ESTIMATE(INT_SUM_TUPLE_SKETCH_UNION( "
            + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(metTupleSketchBytes) FILTER (WHERE id = " + i + "),"
            + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(metTupleSketchBytes) FILTER (WHERE id = " + j + ")))"
            + " FROM " + getTableName();

        jsonNode = postQuery(query);
        assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asLong(), distinctCount);
      }
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testTupleIntersectionQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    for (int i = 1; i < 9; i++) { // Query with Intersection
      String query = "SELECT "
          + "GET_INT_TUPLE_SKETCH_ESTIMATE(INT_SUM_TUPLE_SKETCH_INTERSECT( "
          + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(metTupleSketchBytes) "
          + "FILTER (WHERE id = " + (i - 1) + " OR id = " + i + "),"
          + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(metTupleSketchBytes) "
          + "FILTER (WHERE id = " + i + " OR id = " + (i + 1) + "))),"

          + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(metTupleSketchBytes) "
          + "FILTER (WHERE id = " + (i - 1) + " OR id = " + i + "),"

          + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(metTupleSketchBytes) "
          + "FILTER (WHERE id = " + (i + 1) + " OR id = " + i + ")"
          + " FROM " + getTableName();
      JsonNode jsonNode = postQuery(query);

      String sketch1 = jsonNode.get("resultTable").get("rows").get(0).get(1).asText();
      String sketch2 = jsonNode.get("resultTable").get("rows").get(0).get(2).asText();
      Sketch<IntegerSummary> deserializedSketch1 =
          ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(Base64.getDecoder().decode(sketch1));
      Sketch<IntegerSummary> deserializedSketch2 =
          ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(Base64.getDecoder().decode(sketch2));
      Intersection<IntegerSummary> intersection = new Intersection<>(new IntegerSummarySetOperations(
          IntegerSummary.Mode.Sum, IntegerSummary.Mode.Sum));
      intersection.intersect(deserializedSketch1);
      intersection.intersect(deserializedSketch2);
      long estimate = (long) intersection.getResult().getEstimate();
      assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asLong(), estimate);
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testUnionWithSketchQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format(
            "SELECT "
                + "DISTINCT_COUNT_TUPLE_SKETCH(%s), "
                + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(%s), "
                + "SUM_VALUES_INTEGER_SUM_TUPLE_SKETCH(%s), "
                + "AVG_VALUE_INTEGER_SUM_TUPLE_SKETCH(%s) "
                + "FROM "
                + "("
                + "SELECT %s FROM %s WHERE %s = 4 "
                + "UNION ALL "
                + "SELECT %s FROM %s WHERE %s = 5 "
                + "UNION ALL "
                + "SELECT %s FROM %s WHERE %s = 6 "
                + "UNION ALL "
                + "SELECT %s FROM %s WHERE %s = 7 "
                + ")",
            MET_TUPLE_SKETCH_BYTES, MET_TUPLE_SKETCH_BYTES, MET_TUPLE_SKETCH_BYTES, MET_TUPLE_SKETCH_BYTES,
            MET_TUPLE_SKETCH_BYTES, getTableName(), ID, MET_TUPLE_SKETCH_BYTES, getTableName(), ID,
            MET_TUPLE_SKETCH_BYTES, getTableName(), ID, MET_TUPLE_SKETCH_BYTES, getTableName(), ID);
    JsonNode jsonNode = postQuery(query);
    long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    byte[] rawSketchBytes = Base64.getDecoder().decode(jsonNode.get("resultTable").get("rows").get(0).get(1).asText());
    Sketch<IntegerSummary> deserializedSketch =
        ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(rawSketchBytes);

    assertTrue(distinctCount > 0);
    assertEquals(Double.valueOf(deserializedSketch.getEstimate()).longValue(), distinctCount);
    assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(2).asLong() > 0);
    assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(3).asLong() > 0);
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testJoinWithSketchQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format(
            "SELECT "
                + "DISTINCT_COUNT_TUPLE_SKETCH(a.%s), "
                + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(a.%s), "
                + "SUM_VALUES_INTEGER_SUM_TUPLE_SKETCH(a.%s), "
                + "AVG_VALUE_INTEGER_SUM_TUPLE_SKETCH(a.%s), "
                + "DISTINCT_COUNT_TUPLE_SKETCH(b.%s), "
                + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(b.%s), "
                + "SUM_VALUES_INTEGER_SUM_TUPLE_SKETCH(b.%s), "
                + "AVG_VALUE_INTEGER_SUM_TUPLE_SKETCH(b.%s) "
                + "FROM "
                + "(SELECT * FROM %s WHERE %s < 8 ) a "
                + "JOIN "
                + "(SELECT * FROM %s WHERE %s > 3 ) b "
                + "ON a.%s = b.%s",
            MET_TUPLE_SKETCH_BYTES, MET_TUPLE_SKETCH_BYTES, MET_TUPLE_SKETCH_BYTES, MET_TUPLE_SKETCH_BYTES,
            MET_TUPLE_SKETCH_BYTES, MET_TUPLE_SKETCH_BYTES, MET_TUPLE_SKETCH_BYTES, MET_TUPLE_SKETCH_BYTES,
            getTableName(), ID, getTableName(), ID, ID, ID);
    JsonNode jsonNode = postQuery(query);
    long distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    byte[] rawSketchBytes = Base64.getDecoder().decode(jsonNode.get("resultTable").get("rows").get(0).get(1).asText());
    Sketch<IntegerSummary> deserializedSketch =
        ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(rawSketchBytes);
    assertTrue(distinctCount > 0);
    assertEquals(Double.valueOf(deserializedSketch.getEstimate()).longValue(), distinctCount);
    assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(2).asLong() > 0);
    assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(3).asLong() > 0);

    distinctCount = jsonNode.get("resultTable").get("rows").get(0).get(4).asLong();
    rawSketchBytes = Base64.getDecoder().decode(jsonNode.get("resultTable").get("rows").get(0).get(5).asText());
    deserializedSketch = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(rawSketchBytes);
    assertTrue(distinctCount > 0);
    assertEquals(Double.valueOf(deserializedSketch.getEstimate()).longValue(), distinctCount);
    assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(6).asLong() > 0);
    assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(7).asLong() > 0);
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testJoinAndIntersectionWithSketchQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        "SELECT "
            + "GET_INT_TUPLE_SKETCH_ESTIMATE(INT_SUM_TUPLE_SKETCH_INTERSECT( "
            + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(a." + MET_TUPLE_SKETCH_BYTES + "), "
            + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(b." + MET_TUPLE_SKETCH_BYTES + "))), "
            + "GET_INT_TUPLE_SKETCH_ESTIMATE(INT_SUM_TUPLE_SKETCH_UNION( "
            + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(a." + MET_TUPLE_SKETCH_BYTES + "), "
            + "DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH(b." + MET_TUPLE_SKETCH_BYTES + "))) "
            + "FROM "
            + "(SELECT * FROM " + getTableName() + " WHERE id < 8 ) a "
            + "JOIN "
            + "(SELECT * FROM " + getTableName() + " WHERE id > 3 ) b "
            + "ON a.id = b.id";
    JsonNode jsonNode = postQuery(query);
    long distinctCountIntersection = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    long distinctCountUnion = jsonNode.get("resultTable").get("rows").get(0).get(1).asLong();
    assertTrue(distinctCountIntersection <= distinctCountUnion);
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(ID, FieldSpec.DataType.INT)
        .addMetric(MET_TUPLE_SKETCH_BYTES, FieldSpec.DataType.BYTES)
        .build();
  }

  @Override
  public File createAvroFile()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(ID, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null,
            null),
        new org.apache.avro.Schema.Field(MET_TUPLE_SKETCH_BYTES, org.apache.avro.Schema.create(
            org.apache.avro.Schema.Type.BYTES), null, null)));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < getCountStarResult(); i++) {
        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID, RandomUtils.nextInt(10));
        record.put(MET_TUPLE_SKETCH_BYTES, ByteBuffer.wrap(getRandomRawValue()));
        // add avro record to file
        fileWriter.append(record);
      }
    }
    return avroFile;
  }

  private byte[] getRandomRawValue() {
    IntegerSketch is = new IntegerSketch(4, IntegerSummary.Mode.Sum);
    is.update(RANDOM.nextInt(100), RANDOM.nextInt(100));
    return ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(is.compact());
  }
}
