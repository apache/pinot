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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that checks data types for queries with no rows returned.
 */
public class EmptyResponseIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String[] SELECT_STAR_TYPES = new String[]{
      "INT", "INT", "LONG", "INT", "FLOAT", "DOUBLE", "INT", "STRING", "INT", "INT", "INT", "INT", "STRING", "INT",
      "STRING", "INT", "INT", "INT", "INT", "INT", "DOUBLE", "FLOAT", "INT", "STRING", "INT", "STRING", "INT", "INT",
      "INT", "STRING", "STRING", "INT", "STRING", "INT", "INT", "INT", "INT", "INT_ARRAY", "INT", "INT_ARRAY",
      "STRING_ARRAY", "INT", "INT", "FLOAT_ARRAY", "INT", "STRING_ARRAY", "LONG_ARRAY", "INT_ARRAY", "INT_ARRAY",
      "INT", "INT", "STRING", "INT", "INT", "INT", "INT", "INT", "INT", "STRING", "INT", "INT", "INT", "STRING",
      "STRING", "INT", "STRING", "INT", "INT", "STRING_ARRAY", "INT", "STRING", "INT", "INT", "INT", "STRING", "INT",
      "INT", "INT", "INT"
  };
  private static final String SELECT_STAR_QUERY = "SELECT * FROM myTable WHERE %s";
  private static final String SELECT_COLUMN_QUERY = "SELECT AirlineID, ArrTime, ArrTimeBlk FROM myTable WHERE %s";
  private static final String SELECT_TRANSFORM_QUERY = "SELECT AirlineID, ArrTime, ArrTime + 1 FROM myTable WHERE %s";
  private static final String DISTINCT_QUERY = "SELECT DISTINCT AirlineID, ArrTime FROM myTable WHERE %s";
  private static final String AGGREGATE_QUERY = "SELECT avg(ArrTime) FROM myTable WHERE %s";
  private static final String GROUP_BY_QUERY =
      "SELECT AirlineID, avg(ArrTime) FROM myTable WHERE %s GROUP BY AirlineID";
  private static final String SERVER_PRUNE_FILTER = "AirlineID = 0";
  private static final String BROKER_PRUNE_FILTER = "false";
  // A filter that is not compilable with MSE
  private static final String SCHEMA_FALLBACK_FILTER = " AND add(AirTime, AirTime, ArrTime) > 0";
  private static final String QUERY_OPTION_NOT_USE_MSE = "SET useMSEToFillEmptyResponseSchema = false; ";
  private static final String QUERY_OPTION_ENABLE_NULL_HANDLING = "SET enableNullHandling = true; ";

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.Broker.USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA, true);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
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

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Create and upload the segments
    List<File> avroFiles = unpackAvroData(_classTempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
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

  @Test
  public void testSelectStar()
      throws Exception {
    verifyWithAndWithoutMSE(String.format(SELECT_STAR_QUERY, SERVER_PRUNE_FILTER), false, SELECT_STAR_TYPES);
    verifyWithAndWithoutMSE(String.format(SELECT_STAR_QUERY, BROKER_PRUNE_FILTER), true, SELECT_STAR_TYPES);
    verifyWithAndWithoutMSE(String.format(SELECT_STAR_QUERY, SERVER_PRUNE_FILTER + SCHEMA_FALLBACK_FILTER), false,
        SELECT_STAR_TYPES);
    verifyWithAndWithoutMSE(String.format(SELECT_STAR_QUERY, BROKER_PRUNE_FILTER + SCHEMA_FALLBACK_FILTER), true,
        SELECT_STAR_TYPES);
  }

  private void verifyWithAndWithoutMSE(String sql, boolean prunedOnBroker, String... expectedTypes)
      throws Exception {
    for (String query : new String[]{sql, QUERY_OPTION_NOT_USE_MSE + sql}) {
      JsonNode response = postQuery(query);
      verifyResponse(response, prunedOnBroker, expectedTypes);
    }
  }

  private void verifyResponse(JsonNode response, boolean prunedOnBroker, String... expectedTypes) {
    JsonNode resultTable = response.get("resultTable");
    assertTrue(resultTable.get("rows").isEmpty());
    JsonNode columnDataTypes = resultTable.get("dataSchema").get("columnDataTypes");
    assertEquals(columnDataTypes.size(), expectedTypes.length);
    for (int i = 0; i < expectedTypes.length; i++) {
      assertEquals(columnDataTypes.get(i).asText(), expectedTypes[i]);
    }
    assertEquals(response.get("numServersQueried").asInt(), getExpectedNumServersQueried(prunedOnBroker));
  }

  private int getExpectedNumServersQueried(boolean prunedOnBroker) {
    return prunedOnBroker ? 0 : _serverStarters.size();
  }

  @Test
  public void testSelectColumn()
      throws Exception {
    String[] expectedTypes = new String[]{"LONG", "INT", "STRING"};
    verifyWithAndWithoutMSE(String.format(SELECT_COLUMN_QUERY, SERVER_PRUNE_FILTER), false, expectedTypes);
    verifyWithAndWithoutMSE(String.format(SELECT_COLUMN_QUERY, BROKER_PRUNE_FILTER), true, expectedTypes);
    verifyWithAndWithoutMSE(String.format(SELECT_COLUMN_QUERY, SERVER_PRUNE_FILTER + SCHEMA_FALLBACK_FILTER), false,
        expectedTypes);
    verifyWithAndWithoutMSE(String.format(SELECT_COLUMN_QUERY, BROKER_PRUNE_FILTER + SCHEMA_FALLBACK_FILTER), true,
        expectedTypes);
  }

  @Test
  public void testSelectTransform()
      throws Exception {
    verifyTransform(SERVER_PRUNE_FILTER, false);
    verifyTransform(BROKER_PRUNE_FILTER, true);
  }

  /// Transform can only be filled with MSE
  private void verifyTransform(String filter, boolean prunedOnBroker)
      throws Exception {
    String sql = String.format(SELECT_TRANSFORM_QUERY, filter);
    verifyResponse(postQuery(sql), prunedOnBroker, "LONG", "INT", "INT");

    sql = QUERY_OPTION_NOT_USE_MSE + sql;
    verifyResponse(postQuery(sql), prunedOnBroker, "LONG", "INT", "STRING");

    sql = String.format(SELECT_TRANSFORM_QUERY, filter + SCHEMA_FALLBACK_FILTER);
    verifyResponse(postQuery(sql), prunedOnBroker, "LONG", "INT", "STRING");
  }

  @Test
  public void testDistinct()
      throws Exception {
    String[] expectedTypes = new String[]{"LONG", "INT"};
    verifyWithAndWithoutMSE(String.format(DISTINCT_QUERY, SERVER_PRUNE_FILTER), false, expectedTypes);
    verifyWithAndWithoutMSE(String.format(DISTINCT_QUERY, BROKER_PRUNE_FILTER), true, expectedTypes);
    verifyWithAndWithoutMSE(String.format(DISTINCT_QUERY, SERVER_PRUNE_FILTER + SCHEMA_FALLBACK_FILTER), false,
        expectedTypes);
    verifyWithAndWithoutMSE(String.format(DISTINCT_QUERY, BROKER_PRUNE_FILTER + SCHEMA_FALLBACK_FILTER), true,
        expectedTypes);
  }

  @Test
  public void testAggregate()
      throws Exception {
    verifyAggregate(SERVER_PRUNE_FILTER, false);
    verifyAggregate(BROKER_PRUNE_FILTER, true);
  }

  /// Aggregate does not require backfill of data type
  private void verifyAggregate(String filter, boolean prunedOnBroker)
      throws Exception {
    String sql = String.format(AGGREGATE_QUERY, filter);

    JsonNode response = postQuery(sql);
    JsonNode resultTable = response.get("resultTable");
    JsonNode rows = resultTable.get("rows");
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asDouble(), Double.NEGATIVE_INFINITY);
    assertEquals(resultTable.get("dataSchema").get("columnDataTypes").get(0).asText(), "DOUBLE");
    assertEquals(response.get("numServersQueried").asInt(), getExpectedNumServersQueried(prunedOnBroker));

    response = postQuery(QUERY_OPTION_ENABLE_NULL_HANDLING + sql);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 1);
    assertTrue(rows.get(0).get(0).isNull());
    assertEquals(resultTable.get("dataSchema").get("columnDataTypes").get(0).asText(), "DOUBLE");
    assertEquals(response.get("numServersQueried").asInt(), getExpectedNumServersQueried(prunedOnBroker));
  }

  @Test
  public void testGroupBy()
      throws Exception {
    String[] expectedTypes = new String[]{"LONG", "DOUBLE"};
    verifyWithAndWithoutMSE(String.format(GROUP_BY_QUERY, SERVER_PRUNE_FILTER), false, expectedTypes);
    verifyWithAndWithoutMSE(String.format(GROUP_BY_QUERY, BROKER_PRUNE_FILTER), true, expectedTypes);
    verifyWithAndWithoutMSE(String.format(GROUP_BY_QUERY, SERVER_PRUNE_FILTER + SCHEMA_FALLBACK_FILTER), false,
        expectedTypes);
    verifyWithAndWithoutMSE(String.format(GROUP_BY_QUERY, BROKER_PRUNE_FILTER + SCHEMA_FALLBACK_FILTER), true,
        expectedTypes);
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
