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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@Test(suiteName = "CustomClusterIntegrationTest")
public class DescribeTableTest extends CustomDataQueryClusterIntegrationTest {

  private static final String TABLE_NAME_3 = "Table3";
  private static final String COL_1 = "col1";
  private static final String COL_2 = "col2";
  private static final String COL_3 = "col3";

  @Test(dataProvider = "useV1QueryEngine")
  public void testDescribeTable(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("DESCRIBE TABLE " + getTableName());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0).get(0).asText(), COL_1);
    assertEquals(rows.get(1).get(0).asText(), COL_2);
    assertEquals(rows.get(2).get(0).asText(), COL_3);
  }
  @Override public String getTableName() {
    return TABLE_NAME_3;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(COL_1, FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension(COL_2, FieldSpec.DataType.INT)
        .addSingleValueDimension(COL_3, FieldSpec.DataType.TIMESTAMP)
        .build();
  }

  @Override
  public long getCountStarResult() {
    return 15L;
  }

  @Override
  public File createAvroFile()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(COL_1,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN),
            null, null),
        new org.apache.avro.Schema.Field(COL_2, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null),
        new org.apache.avro.Schema.Field(COL_3,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
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
              record.put(COL_1, finalI % 4 == 0 || finalI % 4 == 1);
              record.put(COL_2, finalI);
              record.put(COL_3, finalI);
              return record;
            }
        ));
      }
    }
    return avroFile;
  }
}
