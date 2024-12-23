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
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@Test(suiteName = "CustomClusterIntegrationTest")
public class ShowTablesTest extends CustomDataQueryClusterIntegrationTest {

  private static final String TABLE_NAME_1 = "Table1";
  private static final String TABLE_NAME_2 = "Table2";

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    LOGGER.warn("Setting up integration test class: {}", getClass().getSimpleName());
    if (_controllerRequestURLBuilder == null) {
      _controllerRequestURLBuilder =
          ControllerRequestURLBuilder.baseUrl("http://localhost:" + _sharedClusterTestSuite.getControllerPort());
    }
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    // create & upload schema AND table config
    Schema schema = createSchema(TABLE_NAME_1);
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig(TABLE_NAME_1);
    addTableConfig(tableConfig);

    // create & upload segments
    File avroFile = createAvroFile();
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(TABLE_NAME_1, _tarDir);

    schema = createSchema(TABLE_NAME_2);
    addSchema(schema);
    tableConfig = createOfflineTableConfig(TABLE_NAME_2);
    addTableConfig(tableConfig);

    avroFile = createAvroFile();
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(TABLE_NAME_2, _tarDir);

    waitForAllDocsLoaded(60_000);
    LOGGER.warn("Finished setting up integration test class: {}", getClass().getSimpleName());
  }

  @Test(dataProvider = "useV1QueryEngine")
  public void testShowTablesQuery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SHOW TABLES");
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0).get(0).asText(), TABLE_NAME_1);
    assertEquals(rows.get(1).get(0).asText(), TABLE_NAME_2);
  }
  @Override public String getTableName() {
    return TABLE_NAME_1;
  }

  public Schema createSchema(String tableName) {
    return new Schema.SchemaBuilder().setSchemaName(tableName)
        .addSingleValueDimension("col1", FieldSpec.DataType.BOOLEAN)
        .addSingleValueDimension("col2", FieldSpec.DataType.INT)
        .addSingleValueDimension("col3", FieldSpec.DataType.TIMESTAMP)
        .build();
  }

  @Override
  public Schema createSchema() {
    return null;
  }

  public TableConfig createOfflineTableConfig(String tableName) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
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
        new org.apache.avro.Schema.Field("col1",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN),
            null, null),
        new org.apache.avro.Schema.Field("col2", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null),
        new org.apache.avro.Schema.Field("col3",
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
              record.put("col1", finalI % 4 == 0 || finalI % 4 == 1);
              record.put("col2", finalI);
              record.put("col3", finalI);
              return record;
            }
        ));
      }
    }
    return avroFile;
  }
}
