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
package org.apache.pinot.compat;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.SqlResultComparator;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.utils.ExplainPlanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Executes queries in the query file and verifies either result rows or an expected error-message substring.
/// Exactly one of `expectedResultsFileName` and `expectedErrorMessageContains` must be configured.
///
/// TODO:
///  - If we use current timestamp for realtime tables, we may not be able to use pre-canned queries.
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryOp extends BaseOp {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryOp.class);

  private static final String COMMENT_DELIMITER = "#";
  private String _queryFileName;
  @Nullable
  private String _expectedResultsFileName;
  @Nullable
  private String _expectedErrorMessageContains;
  private boolean _useMultiStageQueryEngine = false;
  private boolean _enableNullHandling = false;
  private int _maxAttempts = 1;
  private long _retryDelayMs = 1000L;

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

  /// Returns the expected-results file, or {@code null} when expected-error mode is configured.
  @Nullable
  public String getExpectedResultsFileName() {
    return _expectedResultsFileName;
  }

  /// Configures result-comparison mode. Pass {@code null} to clear it before configuring expected-error mode.
  public void setExpectedResultsFileName(@Nullable String expectedResultsFileName) {
    _expectedResultsFileName = expectedResultsFileName;
  }

  /// Returns the required error-message substring, or {@code null} when result-comparison mode is configured.
  @Nullable
  public String getExpectedErrorMessageContains() {
    return _expectedErrorMessageContains;
  }

  /// Configures expected-error mode. Pass {@code null} to clear it before configuring result-comparison mode.
  public void setExpectedErrorMessageContains(@Nullable String expectedErrorMessageContains) {
    _expectedErrorMessageContains = expectedErrorMessageContains;
  }

  public boolean getUseMultiStageQueryEngine() {
    return _useMultiStageQueryEngine;
  }

  public void setUseMultiStageQueryEngine(boolean useMultiStageQueryEngine) {
    _useMultiStageQueryEngine = useMultiStageQueryEngine;
  }

  public boolean isNullHandlingEnabled() {
    return _enableNullHandling;
  }

  public void setNullHandlingEnabled(boolean nullHandlingEnabled) {
    _enableNullHandling = nullHandlingEnabled;
  }

  public int getMaxAttempts() {
    return _maxAttempts;
  }

  public void setMaxAttempts(int maxAttempts) {
    _maxAttempts = maxAttempts;
  }

  public long getRetryDelayMs() {
    return _retryDelayMs;
  }

  public void setRetryDelayMs(long retryDelayMs) {
    _retryDelayMs = retryDelayMs;
  }

  @Override
  boolean runOp(int generationNumber) {
    if (!hasValidExpectedOutcomeConfiguration()) {
      LOGGER.error(
          "Exactly one of expectedResultsFileName or expectedErrorMessageContains must be configured for queries in {}",
          _queryFileName);
      return false;
    }
    if (_maxAttempts < 1 || _retryDelayMs < 0) {
      LOGGER.error("maxAttempts must be positive and retryDelayMs must be non-negative for queries in {}",
          _queryFileName);
      return false;
    }
    if (_expectedErrorMessageContains != null) {
      LOGGER.info("Verifying queries in {} fail with an error containing '{}'", _queryFileName,
          _expectedErrorMessageContains);
    } else {
      LOGGER.info("Verifying queries in {} against results in {}", _queryFileName, _expectedResultsFileName);
    }
    try {
      for (int i = 1; i <= generationNumber; i++) {
        if (!verifyQueriesWithRetry(i)) {
          return false;
        }
      }
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted while retrying queries in {}", _queryFileName);
      return false;
    } catch (Exception e) {
      LOGGER.error("FAILED to verify queries in {}: {}", _queryFileName, e);
      return false;
    }
  }

  private boolean verifyQueriesWithRetry(int generationNumber)
      throws Exception {
    for (int attempt = 1; attempt <= _maxAttempts; attempt++) {
      if (verifyQueries(generationNumber)) {
        return true;
      }
      if (attempt < _maxAttempts) {
        LOGGER.info("Retrying queries in {} after failed attempt {} of {}", _queryFileName, attempt, _maxAttempts);
        Thread.sleep(_retryDelayMs);
      }
    }
    return false;
  }

  boolean hasValidExpectedOutcomeConfiguration() {
    if (_expectedResultsFileName != null && _expectedErrorMessageContains != null) {
      return false;
    }
    boolean hasExpectedResults = _expectedResultsFileName != null && !_expectedResultsFileName.isBlank();
    boolean hasExpectedError =
        _expectedErrorMessageContains != null && !_expectedErrorMessageContains.isBlank();
    return hasExpectedResults != hasExpectedError;
  }

  boolean verifyQueries(int generationNumber)
      throws Exception {
    boolean testPassed = false;

    try (BufferedReader queryReader = new BufferedReader(
        new InputStreamReader(new FileInputStream(getAbsoluteFileName(_queryFileName)), StandardCharsets.UTF_8));
        BufferedReader expectedResultReader = _expectedErrorMessageContains != null ? null
            : new BufferedReader(new InputStreamReader(
                new FileInputStream(getAbsoluteFileName(_expectedResultsFileName)), StandardCharsets.UTF_8))) {

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
        if (_expectedErrorMessageContains == null) {
          try {
            String expectedResultLine = readNextExpectedResult(expectedResultReader, queryLineNum);
            expectedJson = JsonUtils.stringToJsonNode(expectedResultLine);
          } catch (Exception e) {
            LOGGER.error("Comparison FAILED: Line: {} Exception caught while getting expected response for query: '{}'",
                queryLineNum, query, e);
          }
        }

        JsonNode actualJson = null;
        if (_expectedErrorMessageContains != null || expectedJson != null) {
          try {
            actualJson = _useMultiStageQueryEngine
                ? Utils.postMultiStageSqlQuery(query, ClusterDescriptor.getInstance().getBrokerUrl(),
                    _enableNullHandling)
                : Utils.postSqlQuery(query, ClusterDescriptor.getInstance().getBrokerUrl(), _enableNullHandling);
          } catch (Exception e) {
            LOGGER.error("Comparison FAILED: Line: {} Exception caught while running query: '{}', explain plan: {}",
                queryLineNum, query, getExplainPlan(query), e);
          }
        }

        if (actualJson != null && (_expectedErrorMessageContains != null || expectedJson != null)) {
          try {
            boolean passed = matchesExpectedResponse(actualJson, expectedJson, _expectedErrorMessageContains,
                _useMultiStageQueryEngine, query);
            if (passed) {
              succeededQueryCount++;
              if (_expectedErrorMessageContains != null) {
                LOGGER.debug("Comparison PASSED: Line: {}, query: '{}', actual response contains expected error: '{}'",
                    queryLineNum, query, _expectedErrorMessageContains);
              } else {
                LOGGER.debug("Comparison PASSED: Line: {}, query: '{}', actual response: {}, expected response: {}",
                    queryLineNum, query, actualJson, expectedJson);
              }
            } else if (_expectedErrorMessageContains != null) {
              LOGGER.error(
                  "Comparison FAILED: Line: {}, query: '{}', actual response: {}, expected an exception containing: "
                      + "'{}'",
                  queryLineNum, query, actualJson, _expectedErrorMessageContains);
            } else {
              LOGGER.error(
                  "Comparison FAILED: Line: {}, query: '{}', actual response: {}, expected response: {}, explain "
                      + "plan: {}",
                  queryLineNum, query, actualJson, expectedJson, getExplainPlan(query));
            }
          } catch (Exception e) {
            LOGGER.error(
                "Comparison FAILED: Line: {} Exception caught while comparing query: '{}' actual response: {}, "
                    + "expected response: {}, explain plan: {}", queryLineNum, query, actualJson, expectedJson,
                getExplainPlan(query), e);
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

  static String readNextExpectedResult(BufferedReader expectedResultReader, int queryLineNum)
      throws IOException {
    String expectedResultLine;
    while ((expectedResultLine = expectedResultReader.readLine()) != null) {
      if (!expectedResultLine.trim().isEmpty() && !expectedResultLine.trim().startsWith(COMMENT_DELIMITER)) {
        return expectedResultLine;
      }
    }
    throw new IOException("Expected results file ended before query at line " + queryLineNum);
  }

  static boolean matchesExpectedResponse(JsonNode actual, @Nullable JsonNode expected,
      @Nullable String expectedErrorMessageContains, boolean useMultiStageQueryEngine, String query)
      throws IOException {
    if (expectedErrorMessageContains != null) {
      return hasExpectedError(actual, expectedErrorMessageContains);
    }
    return useMultiStageQueryEngine
        ? SqlResultComparator.areMultiStageQueriesEqual(actual, expected, query)
        : SqlResultComparator.areEqual(actual, expected, query);
  }

  /// Returns whether a response has no result rows and contains only errors matching the required substring.
  static boolean hasExpectedError(@Nullable JsonNode response, @Nullable String expectedErrorMessageContains) {
    if (response == null || expectedErrorMessageContains == null || expectedErrorMessageContains.isEmpty()) {
      return false;
    }
    JsonNode resultRows = response.path("resultTable").path("rows");
    if ((resultRows.isArray() && !resultRows.isEmpty()) || response.path("numRowsResultSet").asInt() > 0) {
      return false;
    }
    JsonNode exceptions = response.path("exceptions");
    if (exceptions.isArray()) {
      if (exceptions.isEmpty()) {
        return false;
      }
      for (JsonNode exception : exceptions) {
        if (!hasExpectedErrorMessage(exception, expectedErrorMessageContains)) {
          return false;
        }
      }
      return true;
    }
    return hasExpectedErrorMessage(exceptions, expectedErrorMessageContains);
  }

  private static boolean hasExpectedErrorMessage(JsonNode exception, String expectedErrorMessageContains) {
    JsonNode message = exception.path("message");
    return message.isTextual() && message.asText().contains(expectedErrorMessageContains);
  }

  private String getExplainPlan(String query) {
    try {
      if (!_useMultiStageQueryEngine) {
        JsonNode explainPlanResponse =
            Utils.postSqlQuery("explain plan for " + query, ClusterDescriptor.getInstance().getBrokerUrl(),
                _enableNullHandling);
        return ExplainPlanUtils.formatExplainPlan(explainPlanResponse);
      } else {
        JsonNode explainPlanResponse =
            Utils.postMultiStageSqlQuery("explain plan for " + query,
                ClusterDescriptor.getInstance().getBrokerUrl(), _enableNullHandling);
        return ExplainPlanUtils.formatMultiStageExplainPlan(explainPlanResponse);
      }
    } catch (Throwable error) {
      return error.getMessage();
    }
  }
}
