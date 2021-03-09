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
import org.apache.pinot.integration.tests.ClusterTest;
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
  boolean runOp(int generationNumber) {
    System.out.println("Verifying queries in " + _queryFileName + " against results in " + _expectedResultsFileName);
    try {
      return verifyQueries(generationNumber);
    } catch (Exception e) {
      LOGGER.error("FAILED to verify queries in {}: {}", _queryFileName, e);
      return false;
    }
  }

  boolean verifyQueries(int generationNumber)
      throws Exception {
    boolean testPassed = false;

    try (BufferedReader queryReader = new BufferedReader(
        new InputStreamReader(new FileInputStream(_queryFileName), StandardCharsets.UTF_8));
        BufferedReader expectedResultReader = new BufferedReader(
            new InputStreamReader(new FileInputStream(_expectedResultsFileName), StandardCharsets.UTF_8))) {

      int succeededQueryCount = 0;
      int totalQueryCount = 0;
      int queryLineNum = 0;
      String query;

      while ((query = queryReader.readLine()) != null) {
        queryLineNum++;
        if (shouldIgnore(query)) {
          continue;
        }
        query = query.replaceAll(GENERATION_NUMBER_PLACEHOLDER, String.valueOf(generationNumber));
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
            actualJson = ClusterTest.postSqlQuery(query, ClusterDescriptor.BROKER_URL);
          } catch (Exception e) {
            LOGGER.error("Comparison FAILED: Line: {} Exception caught while running query: '{}'", queryLineNum, query,
                e);
          }
        }

        if (expectedJson != null && actualJson != null) {
          try {
            boolean passed = SqlResultComparator.areEqual(actualJson, expectedJson, query);
            if (passed) {
              succeededQueryCount++;
              LOGGER.debug("Comparison PASSED: Line: {}, query: '{}', actual response: {}, expected response: {}",
                  queryLineNum, query, actualJson, expectedJson);
            } else {
              LOGGER.error("Comparison FAILED: Line: {}, query: '{}', actual response: {}, expected response: {}",
                  queryLineNum, query, actualJson, expectedJson);
            }
          } catch (Exception e) {
            LOGGER.error(
                "Comparison FAILED: Line: {} Exception caught while comparing query: '{}' actual response: {}, expected response: {}",
                queryLineNum, query, actualJson, expectedJson, e);
          }
        }
        totalQueryCount++;
      }

      LOGGER.info("Total {} out of {} queries passed.", succeededQueryCount, totalQueryCount);
      if (succeededQueryCount == totalQueryCount) {
        testPassed = true;
      }
    }
    return testPassed;
  }
}
