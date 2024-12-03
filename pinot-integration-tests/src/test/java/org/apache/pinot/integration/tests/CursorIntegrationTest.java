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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.controller.cursors.ResponseStoreCleaner;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class CursorIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(CursorIntegrationTest.class);
  private static final int NUM_OFFLINE_SEGMENTS = 8;
  private static final int COUNT_STAR_RESULT = 79003;
  private static final String TEST_QUERY_ONE =
      "SELECT SUM(CAST(CAST(ArrTime AS varchar) AS LONG)) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = "
          + "'DL'";
  private static final String TEST_QUERY_TWO =
      "SELECT CAST(CAST(ArrTime AS varchar) AS LONG) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL' "
          + "ORDER BY ArrTime DESC";
  private static final String TEST_QUERY_THREE =
      "SELECT ArrDelay, CarrierDelay, (ArrDelay - CarrierDelay) AS diff FROM mytable WHERE ArrDelay > CarrierDelay "
          + "ORDER BY diff, ArrDelay, CarrierDelay LIMIT 100000";
  private static final String EMPTY_RESULT_QUERY =
      "SELECT SUM(CAST(CAST(ArrTime AS varchar) AS LONG)) FROM mytable WHERE DaysSinceEpoch <> 16312 AND 1 != 1";

  private static int _resultSize;

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(CommonConstants.CursorConfigs.RESPONSE_STORE_CLEANER_FREQUENCY_PERIOD, "5m");
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_RESPONSE_STORE + ".type", "memory");
  }

  protected long getCountStarResult() {
    return COUNT_STAR_RESULT;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start Zk, Kafka and Pinot
    startZk();
    startController();
    startBroker();
    startServer();

    List<File> avroFiles = getAllAvroFiles();
    List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles, NUM_OFFLINE_SEGMENTS);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    getControllerRequestClient().addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0, _segmentDir,
        _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(100_000L);
  }

  protected String getBrokerGetAllQueryStoresApiUrl(String brokerBaseApiUrl) {
    return brokerBaseApiUrl + "/responseStore";
  }

  protected String getBrokerResponseApiUrl(String brokerBaseApiUrl, String requestId) {
    return getBrokerGetAllQueryStoresApiUrl(brokerBaseApiUrl) + "/" + requestId + "/results";
  }

  protected String getBrokerDeleteQueryStoresApiUrl(String brokerBaseApiUrl, String requestId) {
    return getBrokerGetAllQueryStoresApiUrl(brokerBaseApiUrl) + "/" + requestId;
  }

  protected String getBrokerQueryApiUrl(String brokerBaseApiUrl) {
    return useMultiStageQueryEngine() ? brokerBaseApiUrl + "/query" : brokerBaseApiUrl + "/query/sql";
  }

  protected String getCursorQueryProperties(int numRows) {
    return String.format("?getCursor=true&numRows=%d", numRows);
  }

  protected String getCursorOffset(int offset) {
    return String.format("?offset=%d", offset);
  }

  protected String getCursorOffset(int offset, int numRows) {
    return String.format("?offset=%d&numRows=%d", offset, numRows);
  }

  protected Map<String, String> getHeaders() {
    return Collections.emptyMap();
  }

  /*
   * This test does not use H2 to compare results. Instead, it compares results got from iterating through a
   * cursor AND the complete result set.
   * Right now, it only compares the number of rows and all columns and rows.
   */
  @Override
  protected void testQuery(String pinotQuery, String h2Query)
      throws Exception {
    String queryResourceUrl = getBrokerBaseApiUrl();
    Map<String, String> headers = getHeaders();
    Map<String, String> extraJsonProperties = getExtraQueryProperties();

    // Get Pinot BrokerResponse without cursors
    JsonNode pinotResponse;
    pinotResponse =
        ClusterTest.postQuery(pinotQuery, getBrokerQueryApiUrl(queryResourceUrl), headers, extraJsonProperties);
    if (!pinotResponse.get("exceptions").isEmpty()) {
      throw new RuntimeException("Got Exceptions from Query Response: " + pinotResponse);
    }
    int brokerResponseSize = pinotResponse.get("numRowsResultSet").asInt();

    // Get a list of responses using cursors.
    CursorResponse pinotPagingResponse;
    pinotPagingResponse = JsonUtils.jsonNodeToObject(ClusterTest.postQuery(pinotQuery,
        getBrokerQueryApiUrl(queryResourceUrl) + getCursorQueryProperties(_resultSize), headers,
        getExtraQueryProperties()), CursorResponseNative.class);
    if (!pinotPagingResponse.getExceptions().isEmpty()) {
      throw new RuntimeException("Got Exceptions from Query Response: " + pinotPagingResponse.getExceptions().get(0));
    }
    List<CursorResponse> resultPages = getAllResultPages(queryResourceUrl, headers, pinotPagingResponse, _resultSize);

    int brokerPagingResponseSize = 0;
    for (CursorResponse response : resultPages) {
      brokerPagingResponseSize += response.getNumRows();
    }

    // Compare the number of rows.
    if (brokerResponseSize != brokerPagingResponseSize) {
      throw new RuntimeException(
          "Pinot # of rows from paging API " + brokerPagingResponseSize + " doesn't match # of rows from default API "
              + brokerResponseSize);
    }
  }

  private List<CursorResponse> getAllResultPages(String queryResourceUrl, Map<String, String> headers,
      CursorResponse firstResponse, int numRows)
      throws Exception {
    numRows = numRows == 0 ? CommonConstants.CursorConfigs.DEFAULT_CURSOR_FETCH_ROWS : numRows;

    List<CursorResponse> resultPages = new ArrayList<>();
    resultPages.add(firstResponse);
    int totalRows = firstResponse.getNumRowsResultSet();

    int offset = firstResponse.getNumRows();
    while (offset < totalRows) {
      CursorResponse response = JsonUtils.stringToObject(ClusterTest.sendGetRequest(
          getBrokerResponseApiUrl(queryResourceUrl, firstResponse.getRequestId()) + getCursorOffset(offset, numRows),
          headers), CursorResponseNative.class);
      resultPages.add(response);
      offset += response.getNumRows();
    }
    return resultPages;
  }

  protected Object[][] getPageSizesAndQueryEngine() {
    return new Object[][]{
        {false, 2}, {false, 3}, {false, 10}, {false, 0}, //0 trigger default behaviour
        {true, 2}, {true, 3}, {true, 10}, {true, 0} //0 trigger default behaviour
    };
  }

  @DataProvider(name = "pageSizeAndQueryEngineProvider")
  public Object[][] pageSizeAndQueryEngineProvider() {
    return getPageSizesAndQueryEngine();
  }

  // Test hard coded queries with SSE/MSE AND different cursor response sizes.
  @Test(dataProvider = "pageSizeAndQueryEngineProvider")
  public void testHardcodedQueries(boolean useMultiStageEngine, int pageSize)
      throws Exception {
    _resultSize = pageSize;
    setUseMultiStageQueryEngine(useMultiStageEngine);
    super.testHardcodedQueries();
  }

  @DataProvider(name = "chooseQueryEngine")
  public Object[][] chooseQueryEngine() {
    return new Object[][] {
        {false}, {true}
    };
  }

  // Test a simple cursor workflow.
  @Test(dataProvider = "chooseQueryEngine")
  public void testCursorWorkflow(boolean useMultiStageQueryEngine)
      throws Exception {
    _resultSize = 10000;
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Submit query
    CursorResponse pinotPagingResponse;
    JsonNode jsonNode = ClusterTest.postQuery(TEST_QUERY_THREE,
        getBrokerQueryApiUrl(getBrokerBaseApiUrl()) + getCursorQueryProperties(_resultSize), getHeaders(),
        getExtraQueryProperties());

    pinotPagingResponse = JsonUtils.jsonNodeToObject(jsonNode, CursorResponseNative.class);
    if (!pinotPagingResponse.getExceptions().isEmpty()) {
      throw new RuntimeException("Got Exceptions from Query Response: " + pinotPagingResponse.getExceptions().get(0));
    }
    String requestId = pinotPagingResponse.getRequestId();

    Assert.assertFalse(pinotPagingResponse.getBrokerHost().isEmpty());
    Assert.assertTrue(pinotPagingResponse.getBrokerPort() > 0);
    Assert.assertTrue(pinotPagingResponse.getCursorFetchTimeMs() >= 0);
    Assert.assertTrue(pinotPagingResponse.getCursorResultWriteTimeMs() >= 0);

    int totalRows = pinotPagingResponse.getNumRowsResultSet();
    int offset = pinotPagingResponse.getNumRows();
    while (offset < totalRows) {
      pinotPagingResponse = JsonUtils.stringToObject(ClusterTest.sendGetRequest(
          getBrokerResponseApiUrl(getBrokerBaseApiUrl(), requestId) + getCursorOffset(offset, _resultSize),
          getHeaders()), CursorResponseNative.class);

      Assert.assertFalse(pinotPagingResponse.getBrokerHost().isEmpty());
      Assert.assertTrue(pinotPagingResponse.getBrokerPort() > 0);
      Assert.assertTrue(pinotPagingResponse.getCursorFetchTimeMs() >= 0);
      offset += _resultSize;
    }
    ClusterTest.sendDeleteRequest(getBrokerDeleteQueryStoresApiUrl(getBrokerBaseApiUrl(), requestId), getHeaders());
  }

  @Test
  public void testGetAndDelete()
      throws Exception {
    _resultSize = 100000;
    testQuery(TEST_QUERY_ONE);
    testQuery(TEST_QUERY_TWO);

    List<CursorResponseNative> requestIds = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllQueryStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        new TypeReference<>() {
        });

    Assert.assertEquals(requestIds.size(), 2);

    // Delete the first one
    String deleteRequestId = requestIds.get(0).getRequestId();
    ClusterTest.sendDeleteRequest(getBrokerDeleteQueryStoresApiUrl(getBrokerBaseApiUrl(), deleteRequestId),
        getHeaders());

    requestIds = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllQueryStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        new TypeReference<>() {
        });

    Assert.assertEquals(requestIds.size(), 1);
    Assert.assertNotEquals(requestIds.get(0).getRequestId(), deleteRequestId);
  }

  @Test
  public void testBadGet() {
    try {
      ClusterTest.sendGetRequest(getBrokerResponseApiUrl(getBrokerBaseApiUrl(), "dummy") + getCursorOffset(0),
          getHeaders());
    } catch (IOException e) {
      HttpErrorStatusException h = (HttpErrorStatusException) e.getCause();
      Assert.assertEquals(h.getStatusCode(), 404);
      Assert.assertTrue(h.getMessage().contains("Query results for dummy not found"));
    }
  }

  @Test
  public void testBadDelete() {
    try {
      ClusterTest.sendDeleteRequest(getBrokerDeleteQueryStoresApiUrl(getBrokerBaseApiUrl(), "dummy"), getHeaders());
    } catch (IOException e) {
      HttpErrorStatusException h = (HttpErrorStatusException) e.getCause();
      Assert.assertEquals(h.getStatusCode(), 404);
      Assert.assertTrue(h.getMessage().contains("Query results for dummy not found"));
    }
  }

  @Test
  public void testQueryWithEmptyResult()
      throws Exception {
    JsonNode pinotResponse = ClusterTest.postQuery(EMPTY_RESULT_QUERY,
        getBrokerQueryApiUrl(getBrokerBaseApiUrl()) + getCursorQueryProperties(1000), getHeaders(),
        getExtraQueryProperties());

    // There should be no resultTable.
    Assert.assertNull(pinotResponse.get("resultTable"));
    // Total Rows in result set should be 0.
    Assert.assertEquals(pinotResponse.get("numRowsResultSet").asInt(), 0);
    // Rows in the current response should be 0
    Assert.assertEquals(pinotResponse.get("numRows").asInt(), 0);
    Assert.assertTrue(pinotResponse.get("exceptions").isEmpty());
  }

  @DataProvider(name = "InvalidOffsetQueryProvider")
  public Object[][] invalidOffsetQueryProvider() {
    return new Object[][]{{TEST_QUERY_ONE}, {EMPTY_RESULT_QUERY}};
  }

  @Test(dataProvider = "InvalidOffsetQueryProvider", expectedExceptions = IOException.class,
      expectedExceptionsMessageRegExp = ".*Offset \\d+ should be lesser than totalRecords \\d+.*")
  public void testGetInvalidOffset(String query)
      throws Exception {
    CursorResponse pinotPagingResponse;
    pinotPagingResponse = JsonUtils.jsonNodeToObject(ClusterTest.postQuery(query,
        getBrokerQueryApiUrl(getBrokerBaseApiUrl()) + getCursorQueryProperties(_resultSize), getHeaders(),
        getExtraQueryProperties()), CursorResponseNative.class);
    Assert.assertTrue(pinotPagingResponse.getExceptions().isEmpty());
    ClusterTest.sendGetRequest(
        getBrokerResponseApiUrl(getBrokerBaseApiUrl(), pinotPagingResponse.getRequestId()) + getCursorOffset(
            pinotPagingResponse.getNumRowsResultSet() + 1), getHeaders());
  }

  @Test
  public void testQueryWithRuntimeError()
      throws Exception {
    String queryWithFromMissing = "SELECT * mytable limit 100";
    JsonNode pinotResponse;
    pinotResponse = ClusterTest.postQuery(queryWithFromMissing,
        getBrokerQueryApiUrl(getBrokerBaseApiUrl()) + getCursorQueryProperties(_resultSize), getHeaders(),
        getExtraQueryProperties());
    Assert.assertFalse(pinotResponse.get("exceptions").isEmpty());
    JsonNode exception = pinotResponse.get("exceptions").get(0);
    Assert.assertTrue(exception.get("message").asText().startsWith("QueryValidationError:"));
    Assert.assertEquals(exception.get("errorCode").asInt(), 700);
    Assert.assertTrue(pinotResponse.get("brokerId").asText().startsWith("Broker_"));
  }

  @Test
  public void testResponseStoreCleaner()
      throws Exception {
    List<CursorResponseNative> requestIds = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllQueryStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        new TypeReference<>() {
        });

    int numQueryResults = requestIds.size();

    _resultSize = 100000;
    this.testQuery(TEST_QUERY_ONE);
    // Sleep so that both the queries do not have the same submission time.
    Thread.sleep(50);
    this.testQuery(TEST_QUERY_TWO);

    requestIds = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllQueryStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        new TypeReference<>() {
        });

    int numQueryResultsAfter = requestIds.size();
    Assert.assertEquals(requestIds.size() - numQueryResults, 2);

    CursorResponseNative cursorResponse0 = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerResponseApiUrl(getBrokerBaseApiUrl(), requestIds.get(0).getRequestId()),
            getHeaders()), new TypeReference<>() {
        });

    CursorResponseNative cursorResponse1 = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerResponseApiUrl(getBrokerBaseApiUrl(), requestIds.get(1).getRequestId()),
            getHeaders()), new TypeReference<>() {
        });

    // Get the lower submission time.
    long expirationTime0 = cursorResponse0.getExpirationTimeMs();
    long expirationTime1 = cursorResponse1.getExpirationTimeMs();

    Properties perodicTaskProperties = new Properties();
    perodicTaskProperties.setProperty("requestId", "CursorIntegrationTest");
    perodicTaskProperties.setProperty(ResponseStoreCleaner.CLEAN_AT_TIME,
        Long.toString(Math.min(expirationTime0, expirationTime1)));
    _controllerStarter.getPeriodicTaskScheduler().scheduleNow("ResponseStoreCleaner", perodicTaskProperties);

    // The periodic task is run in an executor thread. Give the thread some time to run the cleaner.
    TestUtils.waitForCondition(aVoid -> {
      try {
        List<CursorResponse> getNumQueryResults = JsonUtils.stringToObject(
            ClusterTest.sendGetRequest(getBrokerGetAllQueryStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
            List.class);
        return getNumQueryResults.size() < numQueryResultsAfter;
      } catch (Exception e) {
        LOGGER.error(e.getMessage());
        return false;
      }
    }, 500L, 100_000L, "Failed to load delete query results", true);
  }
}
