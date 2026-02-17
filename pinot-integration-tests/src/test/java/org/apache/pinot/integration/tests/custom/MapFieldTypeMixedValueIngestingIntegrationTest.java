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
import com.fasterxml.jackson.databind.node.JsonNodeType;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.create;
import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class MapFieldTypeMixedValueIngestingIntegrationTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "MapFieldTypeMixedValueIngestingIntegrationTest";
  private static final String MAP_FIELD_NAME = "tracingContext";
  private static final String TRACE_ID_KEY = "traceId";
  private static final long NUMERIC_TRACE_ID = 9876543210L;
  private static final String STRING_TRACE_ID = "c69b6613-e174-49f1-ac47-4e9ab98e513f";

  @Override
  protected long getCountStarResult() {
    return 2;
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    ComplexFieldSpec tracingContextFieldSpec = new ComplexFieldSpec(MAP_FIELD_NAME, FieldSpec.DataType.MAP, true,
        Map.of(
            ComplexFieldSpec.KEY_FIELD,
            new DimensionFieldSpec(ComplexFieldSpec.KEY_FIELD, FieldSpec.DataType.STRING, true),
            ComplexFieldSpec.VALUE_FIELD,
            new DimensionFieldSpec(ComplexFieldSpec.VALUE_FIELD, FieldSpec.DataType.STRING, true)
        ));
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addField(tracingContextFieldSpec)
        .addDateTimeField(TIMESTAMP_FIELD_NAME, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS", "1:MILLISECONDS")
        .build();
  }

  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    org.apache.avro.Schema mapValueSchema =
        org.apache.avro.Schema.createUnion(Arrays.asList(create(org.apache.avro.Schema.Type.LONG),
            create(org.apache.avro.Schema.Type.STRING)));
    org.apache.avro.Schema mapAvroSchema = org.apache.avro.Schema.createMap(mapValueSchema);
    List<org.apache.avro.Schema.Field> fields =
        Arrays.asList(
            new org.apache.avro.Schema.Field(MAP_FIELD_NAME, mapAvroSchema, null, null),
            new org.apache.avro.Schema.Field(TIMESTAMP_FIELD_NAME, create(org.apache.avro.Schema.Type.LONG), null, null)
        );
    avroSchema.setFields(fields);

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      long tsBase = System.currentTimeMillis();
      Map<String, Object> mixedMapRecord1 = new HashMap<>();
      mixedMapRecord1.put(TRACE_ID_KEY, NUMERIC_TRACE_ID);
      GenericData.Record numericRecord = new GenericData.Record(avroSchema);
      numericRecord.put(MAP_FIELD_NAME, mixedMapRecord1);
      numericRecord.put(TIMESTAMP_FIELD_NAME, tsBase);

      Map<String, Object> mixedMapRecord2 = new HashMap<>();
      mixedMapRecord2.put(TRACE_ID_KEY, STRING_TRACE_ID);
      GenericData.Record stringRecord = new GenericData.Record(avroSchema);
      stringRecord.put(MAP_FIELD_NAME, mixedMapRecord2);
      stringRecord.put(TIMESTAMP_FIELD_NAME, tsBase + 1);

      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      writers.get(0).append(numericRecord);
      writers.get(0).append(stringRecord);
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    return 2;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testNumericMixedMapKeyValuesAsString(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT " + MAP_FIELD_NAME + "['" + TRACE_ID_KEY + "'] FROM " + getTableName()
        + " ORDER BY " + TIMESTAMP_FIELD_NAME;
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0).get(0).getNodeType(), JsonNodeType.STRING);
    assertEquals(rows.get(0).get(0).textValue(), Long.toString(NUMERIC_TRACE_ID));
    assertEquals(rows.get(1).get(0).getNodeType(), JsonNodeType.STRING);
    assertEquals(rows.get(1).get(0).textValue(), STRING_TRACE_ID);

    query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE " + MAP_FIELD_NAME + "['" + TRACE_ID_KEY
        + "'] = '" + STRING_TRACE_ID + "'";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    assertEquals(pinotResponse.get("resultTable").get("rows").get(0).get(0).intValue(), 1);
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }
}
