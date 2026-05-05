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
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
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
      "SELECT CAST(CAST(ArrTime AS varchar) AS LONG) FROM mytable WHERE DaysSinceEpoch <> 16312 AND 1 != 1";

  private static int _resultSize;

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
    addSchema(schema);
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

  protected String getBrokerGetAllResponseStoresApiUrl(String brokerBaseApiUrl) {
    return brokerBaseApiUrl + "/responseStore";
  }

  protected String getBrokerResponseApiUrl(String brokerBaseApiUrl, String requestId) {
    return getBrokerGetAllResponseStoresApiUrl(brokerBaseApiUrl) + "/" + requestId + "/results";
  }

  protected String getBrokerDeleteResponseStoresApiUrl(String brokerBaseApiUrl, String requestId) {
    return getBrokerGetAllResponseStoresApiUrl(brokerBaseApiUrl) + "/" + requestId;
  }

  protected String getBrokerDeleteExpiredResponseStoresApiUrl(String brokerBaseApiUrl, long expiredBeforeMs) {
    return getBrokerGetAllResponseStoresApiUrl(brokerBaseApiUrl) + "?expiredBefore=" + expiredBeforeMs;
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

  /**
   * Deletes all cursor responses currently in the broker's response store.
   * Call at the start of tests that need a known clean state.
   */
  protected void deleteAllResponses()
      throws Exception {
    List<CursorResponseNative> responses = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllResponseStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        new TypeReference<>() {
        });
    for (CursorResponseNative response : responses) {
      ClusterTest.sendDeleteRequest(
          getBrokerDeleteResponseStoresApiUrl(getBrokerBaseApiUrl(), response.getRequestId()), getHeaders());
    }
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
    pinotResponse = ClusterTest.postQuery(pinotQuery,
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(queryResourceUrl, useMultiStageQueryEngine()), headers,
        extraJsonProperties);
    if (!pinotResponse.get("exceptions").isEmpty()) {
      throw new RuntimeException("Got Exceptions from Query Response: " + pinotResponse);
    }
    int brokerResponseSize = pinotResponse.get("numRowsResultSet").asInt();

    // Get a list of responses using cursors.
    CursorResponse pinotPagingResponse;
    pinotPagingResponse = JsonUtils.jsonNodeToObject(ClusterTest.postQuery(pinotQuery,
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(queryResourceUrl, useMultiStageQueryEngine())
            + getCursorQueryProperties(_resultSize), headers, getExtraQueryProperties()), CursorResponseNative.class);
    if (!pinotPagingResponse.getExceptions().isEmpty()) {
      throw new RuntimeException("Got Exceptions from Query Response: " + pinotPagingResponse.getExceptions().get(0));
    }
    String requestId = pinotPagingResponse.getRequestId();
    try {
      List<CursorResponse> resultPages = getAllResultPages(queryResourceUrl, headers, pinotPagingResponse, _resultSize);

      int brokerPagingResponseSize = 0;
      for (CursorResponse response : resultPages) {
        brokerPagingResponseSize += response.getNumRows();
      }

      if (brokerResponseSize != brokerPagingResponseSize) {
        throw new RuntimeException(
            "Pinot # of rows from paging API " + brokerPagingResponseSize
                + " doesn't match # of rows from default API " + brokerResponseSize);
      }
    } finally {
      ClusterTest.sendDeleteRequest(getBrokerDeleteResponseStoresApiUrl(queryResourceUrl, requestId), headers);
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

  // Test a simple cursor workflow.
  @Test(dataProvider = "useBothQueryEngines")
  public void testCursorWorkflow(boolean useMultiStageQueryEngine)
      throws Exception {
    _resultSize = 10000;
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Submit query
    CursorResponse pinotPagingResponse;
    JsonNode jsonNode = ClusterTest.postQuery(TEST_QUERY_THREE,
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine())
            + getCursorQueryProperties(_resultSize), getHeaders(), getExtraQueryProperties());

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
    ClusterTest.sendDeleteRequest(getBrokerDeleteResponseStoresApiUrl(getBrokerBaseApiUrl(), requestId), getHeaders());
  }

  @Test
  public void testGetAndDelete()
      throws Exception {
    deleteAllResponses();

    _resultSize = 100000;
    // Create responses directly (don't use testQuery which auto-cleans)
    ClusterTest.postQuery(TEST_QUERY_ONE,
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine())
            + getCursorQueryProperties(_resultSize), getHeaders(), getExtraQueryProperties());
    ClusterTest.postQuery(TEST_QUERY_TWO,
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine())
            + getCursorQueryProperties(_resultSize), getHeaders(), getExtraQueryProperties());

    List<CursorResponseNative> requestIds = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllResponseStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        new TypeReference<>() {
        });

    Assert.assertEquals(requestIds.size(), 2);

    // Delete the first one
    String deleteRequestId = requestIds.get(0).getRequestId();
    ClusterTest.sendDeleteRequest(getBrokerDeleteResponseStoresApiUrl(getBrokerBaseApiUrl(), deleteRequestId),
        getHeaders());

    requestIds = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllResponseStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        new TypeReference<>() {
        });

    Assert.assertEquals(requestIds.size(), 1);
    Assert.assertNotEquals(requestIds.get(0).getRequestId(), deleteRequestId);

    // Clean up the surviving response
    ClusterTest.sendDeleteRequest(
        getBrokerDeleteResponseStoresApiUrl(getBrokerBaseApiUrl(), requestIds.get(0).getRequestId()), getHeaders());
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
      ClusterTest.sendDeleteRequest(getBrokerDeleteResponseStoresApiUrl(getBrokerBaseApiUrl(), "dummy"), getHeaders());
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
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine())
            + getCursorQueryProperties(1000), getHeaders(), getExtraQueryProperties());

    // There should be no resultTable.
    Assert.assertNull(pinotResponse.get("resultTable"));
    // Total Rows in result set should be 0.
    Assert.assertEquals(pinotResponse.get("numRowsResultSet").asInt(), 0);
    // Rows in the current response should be 0
    Assert.assertEquals(pinotResponse.get("numRows").asInt(), 0);
    Assert.assertTrue(pinotResponse.get("exceptions").isEmpty());

    // Clean up the stored response
    String requestId = pinotResponse.get("requestId").asText();
    ClusterTest.sendDeleteRequest(getBrokerDeleteResponseStoresApiUrl(getBrokerBaseApiUrl(), requestId), getHeaders());
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
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine())
            + getCursorQueryProperties(_resultSize), getHeaders(), getExtraQueryProperties()),
        CursorResponseNative.class);
    Assert.assertTrue(pinotPagingResponse.getExceptions().isEmpty());
    try {
      ClusterTest.sendGetRequest(
          getBrokerResponseApiUrl(getBrokerBaseApiUrl(), pinotPagingResponse.getRequestId()) + getCursorOffset(
              pinotPagingResponse.getNumRowsResultSet() + 1), getHeaders());
    } finally {
      ClusterTest.sendDeleteRequest(
          getBrokerDeleteResponseStoresApiUrl(getBrokerBaseApiUrl(), pinotPagingResponse.getRequestId()), getHeaders());
    }
  }

  @Test
  public void testQueryWithRuntimeError()
      throws Exception {
    String queryWithFromMissing = "SELECT * mytable limit 100";
    JsonNode pinotResponse;
    pinotResponse = ClusterTest.postQuery(queryWithFromMissing,
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine())
            + getCursorQueryProperties(_resultSize), getHeaders(), getExtraQueryProperties());
    Assert.assertFalse(pinotResponse.get("exceptions").isEmpty());
    JsonNode exception = pinotResponse.get("exceptions").get(0);
    Assert.assertEquals(exception.get("errorCode").asInt(), QueryErrorCode.QUERY_VALIDATION.getId());
    Assert.assertTrue(pinotResponse.get("brokerId").asText().startsWith("Broker_"));
    // There should be no resultTable.
    Assert.assertNull(pinotResponse.get("resultTable"));
  }

  @Test
  public void testDeleteExpiredResponses()
      throws Exception {
    // Establish clean state so count-based assertions are deterministic
    deleteAllResponses();

    _resultSize = 100000;
    // Post queries directly instead of via testQuery(), which deletes its cursor response after completion.
    ClusterTest.postQuery(TEST_QUERY_ONE,
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine())
            + getCursorQueryProperties(_resultSize), getHeaders(), getExtraQueryProperties());
    // Sleep so that both the queries do not have the same submission time.
    Thread.sleep(50);
    ClusterTest.postQuery(TEST_QUERY_TWO,
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine())
            + getCursorQueryProperties(_resultSize), getHeaders(), getExtraQueryProperties());

    List<CursorResponseNative> responsesAfter = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllResponseStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        new TypeReference<>() {
        });
    Assert.assertEquals(responsesAfter.size(), 2, "Expected exactly 2 responses after clean start");

    CursorResponseNative cursorResponse0 = responsesAfter.get(0);
    CursorResponseNative cursorResponse1 = responsesAfter.get(1);

    long expirationTime0 = cursorResponse0.getExpirationTimeMs();
    long expirationTime1 = cursorResponse1.getExpirationTimeMs();
    Assert.assertNotEquals(expirationTime0, expirationTime1,
        "Expiration timestamps must differ for this test to be deterministic");
    long cutoffMs = Math.min(expirationTime0, expirationTime1);

    // Call the broker's batch delete endpoint
    ClusterTest.sendDeleteRequest(
        getBrokerDeleteExpiredResponseStoresApiUrl(getBrokerBaseApiUrl(), cutoffMs), getHeaders());

    // Verify exactly one of the two responses was deleted
    List<CursorResponseNative> remaining = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllResponseStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        new TypeReference<>() {
        });
    Assert.assertEquals(remaining.size(), 1,
        "Expected exactly one response to be deleted by batch delete");

    // Verify the response with the higher expiration survived
    String survivorId = (expirationTime0 > expirationTime1)
        ? cursorResponse0.getRequestId() : cursorResponse1.getRequestId();
    Assert.assertEquals(remaining.get(0).getRequestId(), survivorId,
        "Response with higher expirationTimeMs should survive batch delete");

    // Clean up the survivor
    ClusterTest.sendDeleteRequest(
        getBrokerDeleteResponseStoresApiUrl(getBrokerBaseApiUrl(), survivorId), getHeaders());
  }

  @Test
  public void testDeleteExpiredResponsesWithoutExpiredBeforeQueryParam()
      throws Exception {
    deleteAllResponses();
    String result = ClusterTest.sendDeleteRequest(
        getBrokerGetAllResponseStoresApiUrl(getBrokerBaseApiUrl()), getHeaders());
    Assert.assertTrue(result.contains("Deleted"), result);
  }
}
