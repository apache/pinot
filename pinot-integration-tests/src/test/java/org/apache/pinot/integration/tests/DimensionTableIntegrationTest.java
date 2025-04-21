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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DimensionTableIntegrationTest extends BaseClusterIntegrationTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(DimensionTableIntegrationTest.class);
  private static final String LONG_COL = "longCol";
  private static final String INT_COL = "intCol";

  @Test
  public void testDelete()
      throws Exception {
    JsonNode node = postQuery("select count(*) from " + getTableName());
    assertNoError(node);
    Assert.assertEquals(node.get("resultTable").get("rows").get(0).get(0).asInt(), getCountStarResult());

    dropOfflineTable(getTableName(), "-1d");

    waitForEVToDisappear(TableNameBuilder.OFFLINE.tableNameWithType(getTableName()));

    Assert.assertNull(
        DimensionTableDataManager.getInstanceByTableName(TableNameBuilder.OFFLINE.tableNameWithType(getTableName())));
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .addSingleValueDimension(LONG_COL, FieldSpec.DataType.LONG)
        .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .setPrimaryKeyColumns(Collections.singletonList(LONG_COL))
        .build();
  }

  List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(LONG_COL, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(INT_COL, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null)));

    ArrayList<File> files = new ArrayList<>();

    for (int fi = 0; fi < 2; fi++) {
      File file = new File(_tempDir, "data" + fi + ".avro");
      try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        writer.create(avroSchema, file);
        for (int i = 0; i < getCountStarResult() / 2; i++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(LONG_COL, i);
          record.put(INT_COL, i);
          writer.append(record);
        }
      }
      files.add(file);
    }
    return files;
  }

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    LOGGER.warn("Setting up integration test class: {}", getClass().getSimpleName());
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    if (_controllerRequestURLBuilder == null) {
      _controllerRequestURLBuilder =
          ControllerRequestURLBuilder.baseUrl("http://localhost:" + getControllerPort());
    }
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    Schema schema = createSchema();
    addSchema(schema);

    List<File> avroFiles = createAvroFiles();
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    waitForAllDocsLoaded(60_000);
    LOGGER.warn("Finished setting up integration test class: {}", getClass().getSimpleName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    LOGGER.warn("Tearing down integration test class: {}", getClass().getSimpleName());
    FileUtils.deleteDirectory(_tempDir);

    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
    LOGGER.warn("Finished tearing down integration test class: {}", getClass().getSimpleName());
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setDimensionTableConfig(new DimensionTableConfig(false, false))
        .setIsDimTable(true)
        .build();
  }
}
