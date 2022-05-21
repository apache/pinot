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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.avro.reflect.Nullable;
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
public class RealtimeClusterIntegrationTest extends ClusterTest {

  protected ClusterIntegrationTestDataAndQuerySet _testDataSet;

  @BeforeClass
  public void setUp()
      throws Exception {
    setUpTestDirectories(this.getClass().getSimpleName());
    TestUtils.ensureDirectoriesExistAndEmpty(getTempDir(), getSegmentDir(), getTarDir());

    if (_testDataSet == null) {
      _testDataSet = new RealtimeClusterIntegrationTestDataSet(this, null);
    }

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    List<File> avroFiles = _testDataSet.unpackAvroData(getTempDir());

    // Create and upload the schema and table config
    Schema schema = _testDataSet.createSchema();
    addSchema(schema);
    TableConfig tableConfig = _testDataSet.createRealtimeTableConfig(avroFiles.get(0));
    addTableConfig(tableConfig);

    // Push data into Kafka
    _testDataSet.pushAvroIntoKafka(avroFiles);

    // create segments and upload them to controller
    createSegmentsAndUpload(avroFiles, schema, tableConfig);

    // Set up the H2 connection
    _testDataSet.setUpH2Connection(avroFiles);

    // Initialize the query generator
    _testDataSet.setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    _testDataSet.waitForAllDocsLoaded(600_000L);
  }

  protected void setTestDataSet(ClusterIntegrationTestDataAndQuerySet dataSet) {
    _testDataSet = dataSet;
  }

  protected void createSegmentsAndUpload(List<File> avroFile, Schema schema, TableConfig tableConfig)
      throws Exception {
    // Do nothing. This is specific to LLC use cases for now.
  }

  @Override
  public void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, false);
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
    _testDataSet.testQuery(String.format("SELECT MIN(%s) FROM %s", column, _testDataSet.getTableName()));
    _testDataSet.testQuery(String.format("SELECT MAX(%s) FROM %s", column, _testDataSet.getTableName()));
    _testDataSet.testQuery(String.format("SELECT MIN_MAX_RANGE(%s) FROM %s", column, _testDataSet.getTableName()),
        String.format("SELECT MAX(%s)-MIN(%s) FROM %s", column, column, _testDataSet.getTableName()));
  }

  @Test
  public void testHardcodedQueries()
      throws Exception {
    _testDataSet.testHardcodedQueries();
  }

  @Test
  public void testQueriesFromQueryFile()
      throws Exception {
    _testDataSet.testQueriesFromQueryFile();
  }

  @Test
  public void testGeneratedQueriesWithMultiValues()
      throws Exception {
    _testDataSet.testGeneratedQueriesWithMultiValues();
  }

  @Test
  public void testQueryExceptions()
      throws Exception {
    _testDataSet.testQueryExceptions();
  }

  @Test
  public void testInstanceShutdown()
      throws Exception {
    _testDataSet.testInstanceShutdown();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(_testDataSet.getTableName());
    _testDataSet.cleanupTestTableDataManager(TableNameBuilder.REALTIME.tableNameWithType(_testDataSet.getTableName()));
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(getTempDir());
  }

  protected static class RealtimeClusterIntegrationTestDataSet extends DefaultIntegrationTestDataSet {

    private String _loadMode;

    public RealtimeClusterIntegrationTestDataSet(ClusterTest clusterTest, @Nullable String loadMode) {
      super(clusterTest);
      _loadMode = loadMode;
    }

    @Nullable
    @Override
    public String getLoadMode() {
      return _loadMode;
    }

    @Override
    public List<String> getNoDictionaryColumns() {
      // Randomly set time column as no dictionary column.
      if (new Random().nextInt(2) == 0) {
        return Arrays.asList("ActualElapsedTime", "ArrDelay", "DepDelay", "CRSDepTime", "DaysSinceEpoch");
      } else {
        return super.getNoDictionaryColumns();
      }
    }
  }
}
