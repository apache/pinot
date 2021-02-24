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
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
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

  private static final String NUM_DOCS_SCANNED_KEY = "numDocsScanned";
  private static final String TIME_USED_MS_KEY = "timeUsedMs";
  private static final String COMMENT_DELIMITER = "#";
  private String _queryFileName;
  private String _expectedResultsFileName;

  public QueryOp() {
    super(OpType.QUERY_OP);
  }

  private boolean shouldIgnore(String line) {
    String trimmedLine = line.trim();
    return trimmedLine.isEmpty() || trimmedLine.startsWith(COMMENT_DELIMITER);
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

  boolean verifyQueries()
      throws Exception {
    boolean testPassed = false;

    try (BufferedReader queryReader = new BufferedReader(
        new InputStreamReader(new FileInputStream(_queryFileName), StandardCharsets.UTF_8));
        BufferedReader expectedResultReader = new BufferedReader(
            new InputStreamReader(new FileInputStream(_expectedResultsFileName), StandardCharsets.UTF_8))) {

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
            actualJson = QueryProcessor.postSqlQuery(query);
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
                  queryLineNum, actualJson.get(TIME_USED_MS_KEY), expectedJson.get(TIME_USED_MS_KEY),
                  actualJson.get(NUM_DOCS_SCANNED_KEY));
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
    }
    return testPassed;
  }
}
