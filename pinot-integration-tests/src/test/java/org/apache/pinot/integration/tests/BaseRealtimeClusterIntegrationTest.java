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
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that creates a Kafka broker, creates a Pinot cluster that consumes from Kafka and queries Pinot.
 */
public abstract class BaseRealtimeClusterIntegrationTest extends BaseClusterIntegrationTestSet {

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    addTableConfig(tableConfig);

    // Push data into Kafka
    pushAvroIntoKafka(avroFiles);

    // create segments and upload them to controller
    createSegmentsAndUpload(avroFiles, schema, tableConfig);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected void createSegmentsAndUpload(List<File> avroFile, Schema schema, TableConfig tableConfig)
      throws Exception {
    // Do nothing. This is specific to LLC use cases for now.
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, false);
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    List<String> noDictionaryColumns = new ArrayList<>(super.getNoDictionaryColumns());
    // Randomly set time column as no dictionary column.
    if (new Random().nextBoolean()) {
      noDictionaryColumns.add("DaysSinceEpoch");
    }
    return noDictionaryColumns;
  }

  /**
   * In realtime consuming segments, the dictionary is not sorted,
   * and the dictionary based operator should not be used
   *
   * Adding explicit queries to test dictionary based functions,
   * to ensure the right result is computed, wherein dictionary is not read if it is mutable
   * @throws Exception
   */
  @Test
  public void testDictionaryBasedQueries()
      throws Exception {

    // Dictionary columns
    // int
    testDictionaryBasedFunctions("NASDelay");

    // long
    testDictionaryBasedFunctions("AirlineID");

    // double
    testDictionaryBasedFunctions("ArrDelayMinutes");

    // float
    testDictionaryBasedFunctions("DepDelayMinutes");

    // Non Dictionary columns
    // int
    testDictionaryBasedFunctions("ActualElapsedTime");

    // double
    testDictionaryBasedFunctions("DepDelay");

    // float
    testDictionaryBasedFunctions("ArrDelay");
  }

  private void testDictionaryBasedFunctions(String column)
      throws Exception {
    testQuery(String.format("SELECT MIN(%s) FROM %s", column, getTableName()));
    testQuery(String.format("SELECT MAX(%s) FROM %s", column, getTableName()));
    testQuery(String.format("SELECT MIN_MAX_RANGE(%s) FROM %s", column, getTableName()),
        String.format("SELECT MAX(%s)-MIN(%s) FROM %s", column, column, getTableName()));
  }

  @Test
  public void testHardcodedQueries()
      throws Exception {
    super.testHardcodedQueries();
  }

  @Test
  @Override
  public void testQueriesFromQueryFile()
      throws Exception {
    super.testQueriesFromQueryFile();
  }

  @Test
  @Override
  public void testGeneratedQueries()
      throws Exception {
    testGeneratedQueries(true, false);
  }

  @Test
  @Override
  public void testQueryExceptions()
      throws Exception {
    super.testQueryExceptions();
  }

  @Test
  @Override
  public void testInstanceShutdown()
      throws Exception {
    super.testInstanceShutdown();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    waitForTableDataManagerRemoved(TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
