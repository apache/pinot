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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.controller.helix.ControllerRequestClient;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.ClusterIntegrationTestUtils.getBrokerQueryApiUrl;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Base class for Row Level Security integration tests.
 * Contains all test logic that is shared between BasicAuth and ZkAuth implementations.
 */
public abstract class RowLevelSecurityIntegrationTestBase extends BaseClusterIntegrationTest {
  public static final String ADMIN_USER = "admin";
  public static final String ADMIN_PASSWORD = "verysecret";
  public static final String AUTH_TOKEN = "Basic YWRtaW46dmVyeXNlY3JldA==";
  public static final Map<String, String> AUTH_HEADER = Map.of("Authorization", AUTH_TOKEN);

  public static final String AUTH_TOKEN_USER = "Basic dXNlcjpzZWNyZXQ=";
  public static final Map<String, String> AUTH_HEADER_USER = Map.of("Authorization", AUTH_TOKEN_USER);

  private static final String AUTH_TOKEN_USER_2 = "Basic dXNlcjI6bm90U29TZWNyZXQ=";
  public static final Map<String, String> AUTH_HEADER_USER_2 = Map.of("Authorization", AUTH_TOKEN_USER_2);

  private static final String DEFAULT_TABLE_NAME_2 = "mytable2";
  private static final String DEFAULT_TABLE_NAME_3 = "mytable3";
  private static final Logger LOGGER = LoggerFactory.getLogger(RowLevelSecurityIntegrationTestBase.class);
  protected List<File> _avroFiles;

  @Override
  protected Connection getPinotConnection() {
    if (_pinotConnection == null) {
      JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
      factory.setHeaders(AUTH_HEADER);

      _pinotConnection =
          ConnectionFactory.fromZookeeper(getZkUrl() + "/" + getHelixClusterName(), factory.buildTransport());
    }
    return _pinotConnection;
  }

  @Override
  public ControllerRequestClient getControllerRequestClient() {
    if (_controllerRequestClient == null) {
      _controllerRequestClient =
          new ControllerRequestClient(_controllerRequestURLBuilder, getHttpClient(), AUTH_HEADER);
    }
    return _controllerRequestClient;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startController();
    startBroker();
    startServer();

    // Allow subclasses to initialize users (for ZkAuth)
    initializeUsers();

    startKafka();
    _avroFiles = unpackAvroData(_tempDir);
    pushAvroIntoKafka(_avroFiles);

    // Set up a table for testing different principals.
    setupTable(DEFAULT_TABLE_NAME);
    setupTable(DEFAULT_TABLE_NAME_2);
    setupTable(DEFAULT_TABLE_NAME_3);

    waitForAllDocsLoaded(600_000L);
  }

  /**
   * Hook for subclasses to initialize users.
   * BasicAuth: no-op (users configured via properties)
   * ZkAuth: create users via REST API
   */
  protected void initializeUsers()
      throws Exception {
    // Default implementation does nothing
  }

  private void setupTable(String tableName)
      throws Exception {

    Schema schema = createSchema();
    schema.setSchemaName(tableName);
    addSchema(schema);

    TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));
    tableConfig.setTableName(tableName);
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("100000");
    addTableConfig(tableConfig);

    waitForDocsLoaded(600_000L, true, tableConfig.getTableName());
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    LOGGER.info("Tearing down...");
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testRowFiltersForSingleStageQuery()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = String.format("select count(*) from %s", DEFAULT_TABLE_NAME);
    String queryWithFiltersForUser1 = "select count(*)from mytable where AirlineID=19805";
    String queryWithFiltersForUser2 =
        "select count(*)from mytable where AirlineID=19805 and DestStateName='California'";

    // compare admin response with that of user
    assertTrue(compareRows(queryBroker(queryWithFiltersForUser1, AUTH_HEADER), queryBroker(query, AUTH_HEADER_USER)));
    assertTrue(compareRows(queryBroker(queryWithFiltersForUser2, AUTH_HEADER), queryBroker(query, AUTH_HEADER_USER_2)));
  }

  @Test
  public void testRowFiltersForSingleTableWithMultiStageQuery()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String query = "select count(*), avg(ActualElapsedTime) from mytable WHERE ActualElapsedTime > "
        + "(select avg(ActualElapsedTime) as avg_profit from mytable)";
    String queryWithFiltersForUser1 = "select count(*), avg(ActualElapsedTime) "
        + "from mytable "
        + "WHERE ActualElapsedTime > ("
        + "    select avg(ActualElapsedTime) as avg_profit "
        + "    from mytable "
        + "    where AirlineID = '19805' "
        + "  ) "
        + "  and AirlineID = '19805'";

    String queryWithFiltersForUser2 = "select count(*), avg(ActualElapsedTime) "
        + "from mytable "
        + "WHERE ActualElapsedTime > ("
        + "    select avg(ActualElapsedTime) as avg_profit "
        + "    from mytable "
        + "    where AirlineID = '19805' "
        + "      and DestStateName = 'California'"
        + "  ) "
        + "  and AirlineID = '19805'"
        + "  and DestStateName = 'California'";

    // compare admin response with that of user
    assertTrue(compareRows(queryBroker(queryWithFiltersForUser1, AUTH_HEADER), queryBroker(query, AUTH_HEADER_USER)));
    assertTrue(compareRows(queryBroker(queryWithFiltersForUser2, AUTH_HEADER), queryBroker(query, AUTH_HEADER_USER_2)));
  }

  @Test
  public void testRowFiltersForTwoTablesWithMultiStageQuery()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String query = "select count(*), avg(ActualElapsedTime) from mytable WHERE ActualElapsedTime > 0.1 * ABS("
        + "(select avg(ActualElapsedTime) as avg_profit from mytable2))";
    String queryWithFiltersForUser1 = "select count(*), avg(ActualElapsedTime) "
        + "from mytable "
        + "WHERE ActualElapsedTime > 0.1 * ABS(("
        + "    select avg(ActualElapsedTime) as avg_profit "
        + "    from mytable2 "
        + "  )) "
        + "  and AirlineID = '19805'";
    String queryWithFiltersForUser2 = "SELECT COUNT(*), AVG(ActualElapsedTime)"
        + "    FROM mytable "
        + "    WHERE ActualElapsedTime > 0.1 * ABS(("
        + "            SELECT AVG(ActualElapsedTime) AS avg_profit"
        + "        FROM mytable2"
        + "        WHERE AirlineID = '20409'"
        + "        AND DestStateName = 'Florida'"
        + "    ))"
        + "    AND DestStateName = 'California'"
        + "    AND AirlineID='19805'";

    // compare admin response with that of user
    assertTrue(compareRows(queryBroker(queryWithFiltersForUser1, AUTH_HEADER), queryBroker(query, AUTH_HEADER_USER)));
    assertTrue(compareRows(queryBroker(queryWithFiltersForUser2, AUTH_HEADER), queryBroker(query, AUTH_HEADER_USER_2)));
  }

  @Test
  public void testRowFiltersForTwoTablesWithComplexExpressions()
      throws Exception {

    // Test for single-stage
    setUseMultiStageQueryEngine(false);

    String singleStageQuery =
        String.format("select AVG(CRSDepTime) as avg_dep_time, count(*) from %s", DEFAULT_TABLE_NAME_3);

    String queryWithFiltersForUser1 =
        "select AVG(CRSDepTime) as avg_dep_time, count(*)  from mytable3 where (AirlineID='20409' OR AirTime>'300') "
            + "AND "
            + "(DestStateName='Florida')";

    assertTrue(compareRows(queryBroker(queryWithFiltersForUser1, AUTH_HEADER),
        queryBroker(singleStageQuery, AUTH_HEADER_USER)));

    // Test for multi-stage
    setUseMultiStageQueryEngine(true);
    String multiStageQuery = "select count(*), avg(ActualElapsedTime) from mytable WHERE ActualElapsedTime > 0.1 * ABS("
        + "(select avg(ActualElapsedTime) as avg_profit from mytable3))";
    String queryWithFiltersForUser2 = "SELECT COUNT(*), AVG(ActualElapsedTime)"
        + "    FROM mytable "
        + "    WHERE ActualElapsedTime > 0.1 * ABS(("
        + "            SELECT AVG(ActualElapsedTime) AS avg_profit"
        + "        FROM mytable3"
        + "        WHERE (AirlineID = '20409'"
        + "               OR DestStateName = 'California')"
        + "        AND DestStateName = 'Florida'"
        + "    ))"
        + "    AND DestStateName = 'California'"
        + "    AND AirlineID='19805'";

    // compare admin response with that of user

    assertTrue(compareRows(queryBroker(queryWithFiltersForUser2, AUTH_HEADER),
        queryBroker(multiStageQuery, AUTH_HEADER_USER_2)));
  }

  private JsonNode queryBroker(String query, Map<String, String> headers)
      throws Exception {
    JsonNode response =
        postQuery(query, getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine()), headers,
            getExtraQueryProperties());
    return response;
  }

  private boolean compareRows(JsonNode adminQueryResponse, JsonNode userQueryResponse) {
    // No filters should get applied for admin response
    assertFalse(adminQueryResponse.get("rlsFiltersApplied").asBoolean());
    // Filters are always applied in case of users
    assertTrue(userQueryResponse.get("rlsFiltersApplied").asBoolean());

    JsonNode responseRow = userQueryResponse.get("resultTable").get("rows").get(0);
    JsonNode expectedRow = adminQueryResponse.get("resultTable").get("rows").get(0);

    // Compare each column
    for (int i = 0; i < responseRow.size(); i++) {
      JsonNode responseValue = responseRow.get(i);
      JsonNode expectedValue = expectedRow.get(i);

      if (responseValue.isNumber() && expectedValue.isNumber()) {
        // For numeric values, use appropriate comparison
        if (responseValue.isIntegralNumber() && expectedValue.isIntegralNumber()) {
          // Integer comparison
          if (responseValue.asLong() != expectedValue.asLong()) {
            return false;
          }
        } else {
          // Floating point comparison with delta
          double delta = Math.abs(responseValue.asDouble() - expectedValue.asDouble());
          if (delta > 0.001) {
            return false;
          }
        }
      }
    }
    return true;
  }
}
