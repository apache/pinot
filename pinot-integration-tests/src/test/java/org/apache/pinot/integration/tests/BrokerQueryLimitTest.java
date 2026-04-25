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
import java.io.File;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class BrokerQueryLimitTest extends SharedRichClusterIntegrationTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BrokerQueryLimitTest.class);
  private static final String LONG_COLUMN = "longCol";
  private static final String SHARED_TABLE_NAME = "broker_query_limit";
  public static final int DEFAULT_LIMIT = 5;
  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @Test
  public void testWhenLimitIsNOTSetExplicitlyThenDefaultLimitIsApplied()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = String.format("SELECT %s FROM %s", LONG_COLUMN, getTableName());

    JsonNode result = postQuery(query).get("resultTable");
    JsonNode columnDataTypesNode = result.get("dataSchema").get("columnDataTypes");
    assertEquals(columnDataTypesNode.get(0).textValue(), "LONG");

    JsonNode rows = result.get("rows");
    assertEquals(rows.size(), DEFAULT_LIMIT);

    for (int rowNum = 0; rowNum < rows.size(); rowNum++) {
      JsonNode row = rows.get(rowNum);
      assertEquals(row.size(), 1);
      assertEquals(row.get(0).asLong(), rowNum);
    }
  }

  @Test
  public void testWhenLimitISSetExplicitlyThenDefaultLimitIsNotApplied()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = String.format("SELECT %s FROM %s limit 20", LONG_COLUMN, getTableName());

    JsonNode result = postQuery(query).get("resultTable");
    JsonNode columnDataTypesNode = result.get("dataSchema").get("columnDataTypes");
    assertEquals(columnDataTypesNode.get(0).textValue(), "LONG");

    JsonNode rows = result.get("rows");
    assertEquals(rows.size(), 20);

    for (int rowNum = 0; rowNum < rows.size(); rowNum++) {
      JsonNode row = rows.get(rowNum);
      assertEquals(row.size(), 1);
      assertEquals(row.get(0).asLong(), rowNum);
    }
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_DEFAULT_QUERY_LIMIT, DEFAULT_LIMIT);
  }

  @Override
  public String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
        .build();
  }

  public File createAvroFile()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(LONG_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null)));

    File avroFile = new File(_classTempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      writer.create(avroSchema, avroFile);
      for (int i = 0; i < getCountStarResult(); i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(LONG_COLUMN, i);
        writer.append(record);
      }
    }
    return avroFile;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    LOGGER.warn("Setting up integration test class: {}", getClass().getSimpleName());
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    cleanTableAndSchema();

    // create & upload schema AND table config
    Schema schema = createSchema();
    addSchema(schema);

    File avroFile = createAvroFile();
    // create offline table
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // create & upload segments
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    waitForAllDocsLoaded(60_000);
    LOGGER.warn("Finished setting up integration test class: {}", getClass().getSimpleName());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    LOGGER.warn("Tearing down integration test class: {}", getClass().getSimpleName());
    Exception exception = null;
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopController);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::cleanTempDirectory);
    if (exception != null) {
      throw exception;
    }
    LOGGER.warn("Finished tearing down integration test class: {}", getClass().getSimpleName());
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData") : _tempDir;
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (_helixResourceManager.getAllTables().contains(offlineTableName) || _helixResourceManager.hasOfflineTable(
        tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }
    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void cleanTempDirectory()
      throws Exception {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
  }
}
