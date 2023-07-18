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
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MultiStageEngineIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String SCHEMA_FILE_NAME = "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";

  @Override
  protected String getSchemaFileName() {
    return SCHEMA_FILE_NAME;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController(getDefaultControllerConfiguration());
    startBroker();
    startServer();
    setupTenants();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected void setupTenants()
      throws IOException {
  }

  @Override
  protected boolean useMultiStageQueryEngine() {
    return true;
  }

  @Test
  @Override
  public void testHardcodedQueriesMultiStage()
      throws Exception {
    super.testHardcodedQueriesMultiStage();
  }

  @Test
  @Override
  public void testGeneratedQueries()
      throws Exception {
    // test multistage engine, currently we don't support MV columns.
    super.testGeneratedQueries(false, true);
  }

  @Test
  public void testQueryOptions()
      throws Exception {
    String pinotQuery = "SET multistageLeafLimit = 1; SELECT * FROM mytable;";
    String h2Query = "SELECT * FROM mytable limit 1";
    testQueryWithMatchingRowCount(pinotQuery, h2Query);
  }

  @Test
  public void testMultiValueColumnSelectionQuery()
      throws Exception {
    String pinotQuery =
        "SELECT DivAirportIDs, DivAirports FROM mytable WHERE DATE_TIME_CONVERT(DaysSinceEpoch, '1:DAYS:EPOCH', "
            + "'1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd''T''HH:mm:ss.SSS''Z''', '1:DAYS') = '2014-09-05T00:00:00.000Z'";
    String h2Query =
        "SELECT DivAirportIDs[1], DivAirports[1] FROM mytable WHERE DaysSinceEpoch = 16318 LIMIT 10000";
    testQueryWithMatchingRowCount(pinotQuery, h2Query);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
