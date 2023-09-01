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


/**
 * Integration test for floating point data type (float & double) filter queries.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class FloatingPointDataTypeTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "FloatingPointDataTypeTest";
  private static final int NUM_DOCS = 10;
  private static final String MET_DOUBLE = "metDouble";
  private static final String MET_FLOAT = "metFloat";

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addMetric(MET_DOUBLE, FieldSpec.DataType.DOUBLE)
        .addMetric(MET_FLOAT, FieldSpec.DataType.FLOAT)
        .build();
  }

  @Override
  public File createAvroFile()
      throws IOException {

    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(MET_DOUBLE, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(MET_FLOAT, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT),
            null, null)));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      double doubleValue = 0.0;
      float floatValue = 0.0f;
      for (int i = 0; i < getCountStarResult(); i++) {
        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(MET_DOUBLE, doubleValue);
        record.put(MET_FLOAT, floatValue);
        doubleValue += 0.01;
        floatValue += 0.01f;

        // add avro record to file
        fileWriter.append(record);
      }
    }
    return avroFile;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String[][] filterAndExpectedCount = {
        {MET_DOUBLE + " > 0.05", "4"}, {MET_DOUBLE + " = 0.05", "1"}, {MET_DOUBLE + " < 0.05", "5"},
        {MET_FLOAT + " > 0.05", "4"}
        // FIXME: the result of the following queries is not correct
        //  , {MET_FLOAT + " = 0.05", "1"}, {MET_FLOAT + " < 0.05", "5"}
    };
    for (String[] faec : filterAndExpectedCount) {
      String filter = faec[0];
      long expected = Long.parseLong(faec[1]);
      String query = String.format("SELECT COUNT(*) FROM %s WHERE %s", getTableName(), filter);
      JsonNode jsonNode = postQuery(query);
      assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asLong(), expected);
    }
  }
}
