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
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.aninteger.IntegerSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@Test(suiteName = "CustomClusterIntegrationTest")
public class TupleSketchTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "TupleSketchTest";
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

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addMetric(MET_TUPLE_SKETCH_BYTES, FieldSpec.DataType.BYTES)
        .build();
  }

  @Override
  public File createAvroFile()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(MET_TUPLE_SKETCH_BYTES, org.apache.avro.Schema.create(
            org.apache.avro.Schema.Type.BYTES), null, null)));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < getCountStarResult(); i++) {
        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);
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
