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
package org.apache.pinot.compat.tests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Executes queries in the query file, and compares the results with the ones given in the
 * expected results file
 *
 * TODO:
 *  - If we use current timestamp for realtime tables, we may not be able to use pre-canned queries.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryOp extends BaseOp {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryOp.class);

  private static final String NUM_DOCS_SCANNED = "numDocsScanned";
  private static final String TIME_USED_MS = "timeUsedMs";
  private final String _brokerBaseApiUrl = ClusterDescriptor.CONTROLLER_URL;
  private String _queryFileName;
  private String _expectedResultsFileName;

  public QueryOp() {
    super(OpType.QUERY_OP);
  }

  private static boolean shouldIgnore(String line) {
    String trimmedLine = line.trim();
    return trimmedLine.isEmpty() || trimmedLine.startsWith("#");
  }

  private static String constructResponse(InputStream inputStream) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      StringBuilder responseBuilder = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        responseBuilder.append(line);
      }
      return responseBuilder.toString();
    }
  }

  public String getQueryFileName() {
    return _queryFileName;
  }

  public void setQueryFileName(String queryFileName) {
    _queryFileName = queryFileName;
  }

  public String getExpectedResultsFileName() {
    return _expectedResultsFileName;
  }

  public void setExpectedResultsFileName(String expectedResultsFileName) {
    _expectedResultsFileName = expectedResultsFileName;
  }

  @Override
  boolean runOp() {
    System.out.printf("Verifying queries in %s against results in %s\n", _queryFileName, _expectedResultsFileName);
    try {
      return verifyQueries();
    } catch (Exception e) {
      LOGGER.error("FAILED to verify queries in {}: {}", _queryFileName, e);
      return false;
    }
  }

  boolean verifyQueries() throws Exception {
    BufferedReader expectedResultReader = null;
    boolean testPassed = false;

    try (BufferedReader queryReader = new BufferedReader(
        new InputStreamReader(new FileInputStream(_queryFileName), StandardCharsets.UTF_8))) {
      if (_queryFileName == null) {
        LOGGER.error("Result file is missing!");
        return testPassed;
      } else {
        expectedResultReader = new BufferedReader(
            new InputStreamReader(new FileInputStream(_expectedResultsFileName), StandardCharsets.UTF_8));
      }

      int passed = 0;
      int total = 0;
      int queryLineNum = 0;
      String query;

      while ((query = queryReader.readLine()) != null) {
        queryLineNum++;
        if (shouldIgnore(query)) {
          continue;
        }

        JsonNode expectedJson = null;
        try {
          String expectedResultLine = expectedResultReader.readLine();
          while (shouldIgnore(expectedResultLine)) {
            expectedResultLine = expectedResultReader.readLine();
          }
          expectedJson = JsonUtils.stringToJsonNode(expectedResultLine);
        } catch (Exception e) {
          LOGGER.error("Comparison FAILED: Line: {} Exception caught while getting expected response for query: '{}'",
              queryLineNum, query, e);
        }

        JsonNode actualJson = null;
        if (expectedJson != null) {
          try {
            actualJson = postSqlQuery(query);
          } catch (Exception e) {
            LOGGER.error("Comparison FAILED: Line: {} Exception caught while running query: '{}'", queryLineNum, query,
                e);
          }
        }

        if (expectedJson != null && actualJson != null) {
          try {
            boolean comparisonResult = SqlResultComparator.areEqual(actualJson, expectedJson, query);
            if (comparisonResult) {
              passed++;
              LOGGER.info("Comparison PASSED: Line: {} actual Time: {} ms expected Time: {} ms Docs Scanned: {}",
                  queryLineNum, actualJson.get(TIME_USED_MS), expectedJson.get(TIME_USED_MS),
                  actualJson.get(NUM_DOCS_SCANNED));
              LOGGER.debug("actual Response: {}", actualJson);
              LOGGER.debug("expected Response: {}", expectedJson);
            } else {
              LOGGER.error("Comparison FAILED: Line: {} query: {}", queryLineNum, query);
              LOGGER.info("actual Response: {}", actualJson);
              LOGGER.info("expected Response: {}", expectedJson);
            }
          } catch (Exception e) {
            LOGGER.error(
                "Comparison FAILED: Line: {} Exception caught while comparing query: '{}' actual response: {}, expected response: {}",
                queryLineNum, query, actualJson, expectedJson, e);
          }
        }

        total++;
      }

      LOGGER.info("Total {} out of {} queries passed.", passed, total);
      if (total == passed) {
        testPassed = true;
      }
    } finally {
      if (expectedResultReader != null) {
        expectedResultReader.close();
      }
    }
    return testPassed;
  }

  /**
   * Queries the broker's sql query endpoint (/sql)
   */
  private JsonNode postSqlQuery(String query) throws Exception {
    return postSqlQuery(query, _brokerBaseApiUrl);
  }

  /**
   * Queries the broker's sql query endpoint (/sql)
   */
  private JsonNode postSqlQuery(String query, String brokerBaseApiUrl) throws Exception {
    ObjectNode payload = JsonUtils.newObjectNode();
    payload.put("sql", query);
    payload.put("queryOptions", "groupByMode=sql;responseFormat=sql");

    return JsonUtils.stringToJsonNode(sendPostRequest(brokerBaseApiUrl + "/sql", payload.toString()));
  }

  public String sendPostRequest(String urlString, String payload) throws IOException {
    return sendPostRequest(urlString, payload, Collections.EMPTY_MAP);
  }

  public String sendPostRequest(String urlString, String payload, Map<String, String> headers) throws IOException {
    HttpURLConnection httpConnection = (HttpURLConnection) new URL(urlString).openConnection();
    httpConnection.setRequestMethod("POST");
    if (headers != null) {
      for (String key : headers.keySet()) {
        httpConnection.setRequestProperty(key, headers.get(key));
      }
    }

    if (payload != null && !payload.isEmpty()) {
      httpConnection.setDoOutput(true);
      try (BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(httpConnection.getOutputStream(), StandardCharsets.UTF_8))) {
        writer.write(payload, 0, payload.length());
        writer.flush();
      }
    }
    return constructResponse(httpConnection.getInputStream());
  }
}
