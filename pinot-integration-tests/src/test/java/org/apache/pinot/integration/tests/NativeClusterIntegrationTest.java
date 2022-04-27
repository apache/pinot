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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class NativeClusterIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String TEXT_COLUMN_NAME = "UniqueCarrier";
  private static final String TIME_COLUMN_NAME = "millisSinceEpoch";
  private static final String TEST_TEXT_COLUMN_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE UniqueCarrier CONTAINS '.*l'";
  private static final String TEST_TEXT_COLUMN_QUERY2 =
      "SELECT COUNT(*) FROM mytable WHERE UniqueCarrier CONTAINS 'a.*'";
  private static final int NUM_BROKERS = 1;

  private String _schemaFileName = DEFAULT_SCHEMA_FILE_NAME;

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  @Override
  protected String getSchemaFileName() {
    return _schemaFileName;
  }

  @Override
  public String getTimeColumnName() {
    return TIME_COLUMN_NAME;
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return Collections.singletonList(TEXT_COLUMN_NAME);
  }

  @Nullable
  @Override
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(getNumBrokers());
    startServers();

    // Create and upload the schema and table config
    Schema schema = new Schema.SchemaBuilder().setSchemaName(DEFAULT_SCHEMA_NAME)
        .addSingleValueDimension(TEXT_COLUMN_NAME, FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments. For exhaustive testing, concurrently upload multiple segments with the same name
    // and validate correctness with parallel push protection enabled.
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    // Create a copy of _tarDir to create multiple segments with the same name.
    File tarDir2 = new File(_tempDir, "tarDir2");
    FileUtils.copyDirectory(_tarDir, tarDir2);

    List<File> tarDirPaths = new ArrayList<>();
    tarDirPaths.add(_tarDir);
    tarDirPaths.add(tarDir2);

    // TODO: Move this block to a separate method.
    try {
      uploadSegments(getTableName(), tarDirPaths, TableType.OFFLINE, true);
    } catch (Exception e) {
      // If enableParallelPushProtection is enabled and the same segment is uploaded concurrently, we could get one
      // of the two exception - 409 conflict of the second call enters ProcessExistingSegment ; segmentZkMetadata
      // creation failure if both calls entered ProcessNewSegment. In/such cases ensure that we upload all the
      // segments again/to ensure that the data is setup correctly.
      assertTrue(e.getMessage().contains("Another segment upload is in progress for segment") || e.getMessage()
          .contains("Failed to create ZK metadata for segment"), e.getMessage());
      uploadSegments(getTableName(), _tarDir);
    }

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected void startServers()
      throws Exception {
    startServer();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testTextSearchCountQuery() {
    runQueryAndVerifyResult();
  }

  private void runQueryAndVerifyResult() {
    Connection connection = getPinotConnection();
    ResultSetGroup pinotResultSetGroup = connection.execute(TEST_TEXT_COLUMN_QUERY);
    org.apache.pinot.client.ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);

    assertEquals(resultTableResultSet.getRowCount(), 1);
    assertEquals(resultTableResultSet.getInt(0), 16731);

    pinotResultSetGroup = connection.execute(TEST_TEXT_COLUMN_QUERY2);
    resultTableResultSet = pinotResultSetGroup.getResultSet(0);

    assertEquals(resultTableResultSet.getRowCount(), 1);
    assertEquals(resultTableResultSet.getInt(0), 11240);
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    Map<String, String> propertiesMap = new HashMap<>();
    propertiesMap.put(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL);

    return Collections.singletonList(
        new FieldConfig(TEXT_COLUMN_NAME, FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TEXT, null,
            propertiesMap));
  }
}
