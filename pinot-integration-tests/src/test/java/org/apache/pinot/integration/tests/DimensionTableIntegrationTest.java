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
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DimensionTableIntegrationTest extends SharedRichClusterIntegrationTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(DimensionTableIntegrationTest.class);
  private static final String SHARED_TABLE_NAME = "dimension_table";
  private static final String LONG_COL = "longCol";
  private static final String INT_COL = "intCol";

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @Test
  public void testDelete()
      throws Exception {
    JsonNode node = postQuery("select count(*) from " + getTableName());
    assertNoError(node);
    Assert.assertEquals(node.get("resultTable").get("rows").get(0).get(0).asInt(), getCountStarResult());

    dropOfflineTable(getTableName(), "-1d");

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    waitForTableDataManagerRemoved(offlineTableName);
    waitForEVToDisappear(offlineTableName);

    Assert.assertNull(DimensionTableDataManager.getInstanceByTableName(offlineTableName));
  }

  @Override
  public String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : super.getTableName();
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
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(LONG_COL, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(INT_COL, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null)));

    ArrayList<File> files = new ArrayList<>();

    for (int fi = 0; fi < 2; fi++) {
      File file = new File(_classTempDir, "data" + fi + ".avro");
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
    _classTempDir = getClassTempDir();
    _classSegmentDir = getClassSegmentDir();
    _classTarDir = getClassTarDir();
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    cleanTableAndSchema();

    Schema schema = createSchema();
    addSchema(schema);

    List<File> avroFiles = createAvroFiles();
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _classSegmentDir,
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
    exception = runCleanup(exception, this::stopControllerIfStarted);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::deleteClassTempDir);
    LOGGER.warn("Finished tearing down integration test class: {}", getClass().getSimpleName());
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setDimensionTableConfig(new DimensionTableConfig(false, false))
        .setIsDimTable(true)
        .build();
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData") : _tempDir;
  }

  private File getClassSegmentDir() {
    return isSharedRichClusterEnabled() ? new File(_classTempDir, "segmentDir") : _segmentDir;
  }

  private File getClassTarDir() {
    return isSharedRichClusterEnabled() ? new File(_classTempDir, "tarDir") : _tarDir;
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

  private void stopControllerIfStarted() {
    if (_controllerStarter != null) {
      stopController();
    }
  }

  private void deleteClassTempDir()
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
}
