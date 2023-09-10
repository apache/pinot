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
import java.io.IOException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class MultiStageNullHandlingTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "testTable";
  private static final String INT_COLUMN = "int1";
  private static final String BOOL_COLUMN = "bool1";

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(BOOL_COLUMN, FieldSpec.DataType.BOOLEAN).build();
  }

  @Override
  protected boolean getNullHandlingEnabled() {
    return true;
  }

  @Override
  protected long getCountStarResult() {
    /*
    Uploaded table content:

    row#  int1  bool1
    ----  ====  =====
    1     1     true
    2     2     false
    3     3     null
    4     null  null
     */
    return 4;
  }

  @Override
  public File createAvroFile()
      throws IOException {

    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);

    avroSchema.setFields(ImmutableList.of(new org.apache.avro.Schema.Field(INT_COLUMN,
            org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL)), null, null),
        new org.apache.avro.Schema.Field(BOOL_COLUMN,
            org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN),
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL)), null, null)));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put(INT_COLUMN, 1);
      record.put(BOOL_COLUMN, true);
      fileWriter.append(record);

      record = new GenericData.Record(avroSchema);
      record.put(INT_COLUMN, 2);
      record.put(BOOL_COLUMN, false);
      fileWriter.append(record);

      record = new GenericData.Record(avroSchema);
      record.put(INT_COLUMN, 3);
      record.put(BOOL_COLUMN, null);
      fileWriter.append(record);

      record = new GenericData.Record(avroSchema);
      record.put(INT_COLUMN, null);
      record.put(BOOL_COLUMN, null);
      fileWriter.append(record);
    }

    return avroFile;
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testFilterIsNull(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        "SET enableNullHandling = true; SELECT int1 FROM testTable WHERE bool1 IS NULL ORDER BY int1 NULLS LAST";

    JsonNode jsonNode = postQuery(query);

    assertEquals(getRowSize(jsonNode), 2);
    assertEquals(getRowZeroColumnZero(jsonNode), 3);
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testFilterIsNotNull(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SET enableNullHandling = true; SELECT int1 FROM testTable WHERE bool1 IS NOT NULL";

    JsonNode jsonNode = postQuery(query);

    assertEquals(getRowSize(jsonNode), 2);
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testCount(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SET enableNullHandling = true; SELECT COUNT(int1) FROM testTable";

    JsonNode jsonNode = postQuery(query);

    assertEquals(getRowZeroColumnZero(jsonNode), 3);
  }

  private int getRowSize(JsonNode jsonNode) {
    return jsonNode.get("resultTable").get("rows").size();
  }

  private int getRowZeroColumnZero(JsonNode jsonNode) {
    return Integer.parseInt(jsonNode.get("resultTable").get("rows").get(0).get(0).asText());
  }
}
