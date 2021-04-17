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
package org.apache.pinot.tools.query.comparison;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.scan.query.GroupByOperator;
import org.apache.pinot.tools.scan.query.QueryResponse;
import org.apache.pinot.tools.scan.query.ScanBasedQueryProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryComparison {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryComparison.class);
  private static final double EPSILON = 0.00001;

  private static final String SELECTION_RESULTS = "selectionResults";
  private static final String AGGREGATION_RESULTS = "aggregationResults";
  private static final String NUM_DOCS_SCANNED = "numDocsScanned";
  private static final String FUNCTION = "function";
  private static final String VALUE = "value";
  private static final String COLUMNS = "columns";
  private static final String RESULTS = "results";
  private static final String GROUP_BY_COLUMNS = "groupByColumns";
  private static final String GROUP_BY_RESULT = "groupByResult";
  private static final String GROUP = "group";
  private static final String TIME_USED_MS = "timeUsedMs";
  private static final String EXCEPTIONS = "exceptions";
  private static boolean _compareNumDocs = true;

  private File _segmentsDir;
  private File _queryFile;
  private File _resultFile;

  private final QueryComparisonConfig _config;
  private ClusterStarter _clusterStarter;

  public QueryComparison(QueryComparisonConfig config) {
    _config = config;
    _queryFile = new File(config.getQueryFile());

    if (!_queryFile.exists() || !_queryFile.isFile()) {
      LOGGER.error("Invalid query file: {}", _queryFile.getName());
      return;
    }

    String segmentDir = config.getSegmentsDir();
    String results = config.getResultFile();

    if (segmentDir == null && results == null) {
      LOGGER.error("Neither segments directory nor expected results file specified");
      return;
    }

    _segmentsDir = (segmentDir != null) ? new File(segmentDir) : null;
    _resultFile = (results != null) ? new File(results) : null;

    if (_segmentsDir != null && (!_segmentsDir.exists() || !_segmentsDir.isDirectory())) {
      LOGGER.error("Invalid segments directory: {}", _segmentsDir.getName());
      return;
    }

    if (_config.getPerfMode() && (_config.getPerfUrl() == null)) {
      LOGGER.error("Must specify perf url in perf mode");
      return;
    }
  }

  private void run() throws Exception {
    startCluster();

    // For function mode, compare response with the expected response.
    if (_config.getFunctionMode()) {
      runFunctionMode();
    }

    // For perf mode, count the client side response time.
    if (_config.getPerfMode()) {
      runPerfMode();
    }
  }

  private void runFunctionMode() throws Exception {
    BufferedReader resultReader = null;
    ScanBasedQueryProcessor scanBasedQueryProcessor = null;

    try (BufferedReader queryReader =
        new BufferedReader(new InputStreamReader(new FileInputStream(_queryFile), "UTF8"))) {
      if (_resultFile == null) {
        scanBasedQueryProcessor = new ScanBasedQueryProcessor(_segmentsDir.getAbsolutePath());
      } else {
        resultReader = new BufferedReader(new InputStreamReader(new FileInputStream(_resultFile), "UTF8"));
      }

      int passed = 0;
      int total = 0;
      String query;
      while ((query = queryReader.readLine()) != null) {
        if (query.isEmpty() || query.startsWith("#")) {
          continue;
        }

        JsonNode expectedJson = null;
        try {
          if (resultReader != null) {
            expectedJson = JsonUtils.stringToJsonNode(resultReader.readLine());
          } else {
            QueryResponse expectedResponse = scanBasedQueryProcessor.processQuery(query);
            expectedJson = JsonUtils.objectToJsonNode(expectedResponse);
          }
        } catch (Exception e) {
          LOGGER.error("Comparison FAILED: Id: {} Exception caught while getting expected response for query: '{}'",
              total, query, e);
        }

        JsonNode actualJson = null;
        if (expectedJson != null) {
          try {
            actualJson = JsonUtils.stringToJsonNode(_clusterStarter.query(query));
          } catch (Exception e) {
            LOGGER.error("Comparison FAILED: Id: {} Exception caught while running query: '{}'", total, query, e);
          }
        }

        if (expectedJson != null && actualJson != null) {
          try {
            if (compare(actualJson, expectedJson)) {
              passed++;
              LOGGER.info("Comparison PASSED: Id: {} actual Time: {} ms expected Time: {} ms Docs Scanned: {}", total,
                  actualJson.get(TIME_USED_MS), expectedJson.get(TIME_USED_MS), actualJson.get(NUM_DOCS_SCANNED));
              LOGGER.debug("actual Response: {}", actualJson);
              LOGGER.debug("expected Response: {}", expectedJson);
            } else {
              LOGGER.error("Comparison FAILED: Id: {} query: {}", query);
              LOGGER.info("actual Response: {}", actualJson);
              LOGGER.info("expected Response: {}", expectedJson);
            }
          } catch (Exception e) {
            LOGGER.error(
                "Comparison FAILED: Id: {} Exception caught while comparing query: '{}' actual response: {}, expected response: {}",
                total, query, actualJson, expectedJson, e);
          }
        }

        total++;
      }

      LOGGER.info("Total {} out of {} queries passed.", passed, total);
    } finally {
      if (resultReader != null) {
        resultReader.close();
      }
    }
  }

  private void runPerfMode() throws Exception {
    try (BufferedReader queryReader =
        new BufferedReader(new InputStreamReader(new FileInputStream(_queryFile), "UTF8"))) {
      String query;
      while ((query = queryReader.readLine()) != null) {
        if (query.isEmpty() || query.startsWith("#")) {
          continue;
        }

        int clientTime = _clusterStarter.perfQuery(query);
        LOGGER.info("Client side response time: {} ms", clientTime);
      }
    }
  }

  private void startCluster() throws Exception {
    _clusterStarter = new ClusterStarter(_config);

    if (_config.getStartCluster()) {
      LOGGER.info("Bringing up Pinot Cluster");
      _clusterStarter.start();
      LOGGER.info("Pinot Cluster is now up");
    } else {
      LOGGER.info("Skipping cluster setup as specified in the config file.");
    }
  }

  public enum ComparisonStatus {
    PASSED,
    EMPTY,
    FAILED
  }

  public static ComparisonStatus compareWithEmpty(JsonNode actualJson, JsonNode expectedJson) {
    if (actualJson.get(EXCEPTIONS).size() != 0) {
      return ComparisonStatus.FAILED;
    }

    // If no records found, nothing to compare.
    if ((actualJson.get(NUM_DOCS_SCANNED).asLong() == 0) && expectedJson.get(NUM_DOCS_SCANNED).asLong() == 0) {
      LOGGER.info("Empty results, nothing to compare.");
      return ComparisonStatus.EMPTY;
    }

    if (!compareSelection(actualJson, expectedJson)) {
      return ComparisonStatus.FAILED;
    }

    if (!compareAggregation(actualJson, expectedJson)) {
      return ComparisonStatus.FAILED;
    }

    return ComparisonStatus.PASSED;
  }

  public static boolean compare(JsonNode actualJson, JsonNode expectedJson) {
    ComparisonStatus comparisonStatus = compareWithEmpty(actualJson, expectedJson);
    return !comparisonStatus.equals(ComparisonStatus.FAILED);
  }

  /**
   * Some clients (eg Star Tree) may have different num docs scanned, but produce the same result.
   * This compare method will ignore comparing number of documents scanned.
   * @param actualJson
   * @param expectedJson
   * @param compareNumDocs
   * @return
   */
  public static boolean compare(JsonNode actualJson, JsonNode expectedJson, boolean compareNumDocs) {
    _compareNumDocs = compareNumDocs;
    return compare(actualJson, expectedJson);
  }

  public static void setCompareNumDocs(boolean compareNumDocs) {
    _compareNumDocs = compareNumDocs;
  }

  public static boolean compareAggregation(JsonNode actualJson, JsonNode expectedJson) {
    if (!actualJson.has(AGGREGATION_RESULTS) && !expectedJson.has(AGGREGATION_RESULTS)) {
      return true;
    }
    JsonNode actualAggregation = actualJson.get(AGGREGATION_RESULTS);
    if (actualAggregation.size() == 0) {
      return !expectedJson.has(AGGREGATION_RESULTS);
    }

    if (_compareNumDocs && !compareNumDocsScanned(actualJson, expectedJson)) {
      return false;
    }

    if (actualAggregation.get(0).has(GROUP_BY_RESULT)) {
      return compareAggregationGroupBy(actualJson, expectedJson);
    }

    JsonNode expectedAggregation = expectedJson.get(AGGREGATION_RESULTS);
    return compareAggregationArrays(actualAggregation, expectedAggregation);
  }

  private static boolean compareAggregationArrays(JsonNode actualAggregation, JsonNode expectedAggregation) {
    Map<String, Double> map = new HashMap<>();

    for (int i = 0; i < expectedAggregation.size(); ++i) {
      JsonNode object = expectedAggregation.get(i);
      map.put(object.get(FUNCTION).asText().toLowerCase(), object.get(VALUE).asDouble());
    }

    for (int i = 0; i < actualAggregation.size(); ++i) {
      JsonNode object = actualAggregation.get(i);
      String function = object.get(FUNCTION).asText().toLowerCase();
      String valueString = object.get(VALUE).asText();

      if (!isNumeric(valueString)) {
        LOGGER.warn("Found non-numeric value for aggregation ignoring Function: {} Value: {}", function, valueString);
        continue;
      }

      Double value = Double.valueOf(valueString);

      if (!map.containsKey(function)) {
        LOGGER.error("expected Response does not contain function {}", function);
        return false;
      }

      Double expectedValue = map.get(function);
      if (!fuzzyEqual(value, expectedValue)) {
        LOGGER.error("Aggregation value mismatch for function {}, {}, {}", function, value, expectedValue);
        return false;
      }
    }

    return true;
  }

  private static boolean compareAggregationGroupBy(JsonNode actualJson, JsonNode expectedJson) {
    JsonNode actualGroupByResults = actualJson.get(AGGREGATION_RESULTS);
    JsonNode expectedGroupByResults = expectedJson.get(AGGREGATION_RESULTS);

    int numActualGroupBy = actualGroupByResults.size();
    int numExpectedGroupBy = expectedGroupByResults.size();

    // Build map based on function (function_column name) to match individual entries.
    Map<String, Integer> functionMap = new HashMap<>();
    for (int i = 0; i < numExpectedGroupBy; ++i) {
      JsonNode expectedAggr = expectedGroupByResults.get(i);
      String expectedFunction = expectedAggr.get(FUNCTION).asText().toLowerCase();
      functionMap.put(expectedFunction, i);
    }

    for (int i = 0; i < numActualGroupBy; ++i) {
      JsonNode actualAggr = actualGroupByResults.get(i);
      String actualFunction = actualAggr.get(FUNCTION).asText().toLowerCase();

      if (!functionMap.containsKey(actualFunction)) {
        LOGGER.error("Missing group by function in expected response: {}", actualFunction);
        return false;
      }

      JsonNode expectedAggr = expectedGroupByResults.get(functionMap.get(actualFunction));
      if (!compareGroupByColumns(actualAggr, expectedAggr)) {
        return false;
      }

      if (!compareAggregationValues(actualAggr, expectedAggr)) {
        return false;
      }
    }
    return true;
  }

  private static List<Object> jsonArrayToList(JsonNode jsonArray) {
    List<Object> list = new ArrayList<>();
    for (int i = 0; i < jsonArray.size(); ++i) {
      list.add(jsonArray.get(i));
    }
    return list;
  }

  private static boolean compareAggregationValues(JsonNode actualAggr, JsonNode expectedAggr) {
    JsonNode actualResult = actualAggr.get(GROUP_BY_RESULT);
    JsonNode expectedResult = expectedAggr.get(GROUP_BY_RESULT);

    Map<GroupByOperator, Double> expectedMap = new HashMap<>();
    for (int i = 0; i < expectedResult.size(); ++i) {
      List<Object> group = jsonArrayToList(expectedResult.get(i).get(GROUP));
      GroupByOperator groupByOperator = new GroupByOperator(group);
      expectedMap.put(groupByOperator, expectedResult.get(i).get(VALUE).asDouble());
    }

    for (int i = 0; i < actualResult.size(); ++i) {
      List<Object> group = jsonArrayToList(actualResult.get(i).get(GROUP));
      GroupByOperator groupByOperator = new GroupByOperator(group);

      double actualValue = actualResult.get(i).get(VALUE).asDouble();
      if (!expectedMap.containsKey(groupByOperator)) {
        LOGGER.error("Missing group by value for group: {}", group);
        return false;
      }

      double expectedValue = expectedMap.get(groupByOperator);
      if (!fuzzyEqual(actualValue, expectedValue)) {
        LOGGER.error("Aggregation group by value mis-match: actual: {}, expected: {}", actualValue, expectedValue);
        return false;
      }
    }

    return true;
  }

  private static boolean compareGroupByColumns(JsonNode actualAggr, JsonNode expectedAggr) {
    JsonNode actualCols = actualAggr.get(GROUP_BY_COLUMNS);
    JsonNode expectedCols = expectedAggr.get(GROUP_BY_COLUMNS);

    if (!compareLists(actualCols, expectedCols, null)) {
      return false;
    }
    return true;
  }

  private static boolean compareSelection(JsonNode actualJson, JsonNode expectedJson) {
    if (!actualJson.has(SELECTION_RESULTS) && !expectedJson.has(SELECTION_RESULTS)) {
      return true;
    }

    /* We cannot compare numDocsScanned in selection because when we just return part of the selection result (has a
       low limit), this number can change over time. */

    JsonNode actualSelection = actualJson.get(SELECTION_RESULTS);
    JsonNode expectedSelection = expectedJson.get(SELECTION_RESULTS);
    Map<Integer, Integer> expectedToActualColMap = new HashMap<>(actualSelection.get(COLUMNS).size());

    return compareLists(actualSelection.get(COLUMNS), expectedSelection.get(COLUMNS), expectedToActualColMap)
        && compareSelectionRows(actualSelection.get(RESULTS), expectedSelection.get(RESULTS), expectedToActualColMap);
  }

  private static boolean compareSelectionRows(JsonNode actualRows, JsonNode expectedRows,
      Map<Integer, Integer> expectedToActualColMap) {
    final int numActualRows = actualRows.size();
    final int numExpectedRows = expectedRows.size();
    if (numActualRows > numExpectedRows) {
      LOGGER.error("In selection, number of actual rows: {} more than expected rows: {}", numActualRows,
          numExpectedRows);
      return false;
    }
    Map<String, Integer> expectedRowMap = new HashMap<>(numExpectedRows);
    for (int i = 0; i < numExpectedRows; i++) {
      String serialized = serializeRow(expectedRows.get(i), expectedToActualColMap);
      Integer count = expectedRowMap.get(serialized);
      if (count == null) {
        expectedRowMap.put(serialized, 1);
      } else {
        expectedRowMap.put(serialized, count + 1);
      }
    }

    for (int i = 0; i < numActualRows; i++) {
      String serialized = serializeRow(actualRows.get(i), null);
      Integer count = expectedRowMap.get(serialized);
      if (count == null || count == 0) {
        LOGGER.error("Cannot find match for row {} in actual result", i);
        return false;
      }
      expectedRowMap.put(serialized, count - 1);
    }
    return true;
  }

  private static String serializeRow(JsonNode row, Map<Integer, Integer> expectedToActualColMap) {
    StringBuilder sb = new StringBuilder();
    final int numCols = row.size();
    sb.append(numCols).append('_');
    for (int i = 0; i < numCols; i++) {
      String toAppend;
      if (expectedToActualColMap == null) {
        toAppend = row.get(i).asText();
      } else {
        toAppend = row.get(expectedToActualColMap.get(i)).asText();
      }
      // For number value, uniform the format and do fuzzy comparison
      try {
        double numValue = Double.parseDouble(toAppend);
        sb.append((int) (numValue * 100)).append('_');
      } catch (NumberFormatException e) {
        sb.append(toAppend).append('_');
      }
    }
    return sb.toString();
  }

  private static boolean isNumeric(String str) {
    if (str == null) {
      return false;
    }

    try {
      Double.parseDouble(str);
    } catch (NumberFormatException nfe) {
      return false;
    }
    return true;
  }

  private static boolean compareLists(JsonNode actualList, JsonNode expectedList,
      Map<Integer, Integer> expectedToActualColMap) {
    int actualSize = actualList.size();
    int expectedSize = expectedList.size();

    if (actualSize != expectedSize) {
      LOGGER.error("Number of columns mis-match: actual: {} expected: {}", actualSize, expectedSize);
      return false;
    }

    if (expectedToActualColMap == null) {
      for (int i = 0; i < expectedList.size(); ++i) {
        String actualColumn = actualList.get(i).asText();
        String expectedColumn = expectedList.get(i).asText();

        if (!actualColumn.equals(expectedColumn)) {
          LOGGER.error("Column name mis-match: actual: {} expected: {}", actualColumn, expectedColumn);
          return false;
        }
      }
    } else {
      for (int i = 0; i < expectedList.size(); i++) {
        boolean found = false;
        final String expectedColumn = expectedList.get(i).asText();
        for (int j = 0; j < actualList.size(); j++) {
          if (expectedColumn.equals(actualList.get(j).asText())) {
            expectedToActualColMap.put(i, j);
            found = true;
            break;
          }
        }
        if (!found) {
          LOGGER.error("Column name " + expectedColumn + " not found in actual");
          return false;
        }
      }
    }

    return true;
  }

  private static boolean compareNumDocsScanned(JsonNode actualJson, JsonNode expectedJson) {
    long actualDocs = actualJson.get(NUM_DOCS_SCANNED).asLong();
    long expectedDocs = expectedJson.get(NUM_DOCS_SCANNED).asLong();

    if (actualDocs != expectedDocs) {
      LOGGER.error("Mis-match in number of docs scanned: actual: {} expected: {}", actualDocs, expectedDocs);
      return false;
    }

    return true;
  }

  private static boolean fuzzyEqual(double d1, double d2) {
    if (d1 == d2) {
      return true;
    }

    if (Math.abs(d1 - d2) < EPSILON) {
      return true;
    }

    // For really large numbers, use relative error.
    if (d1 != 0 && ((Math.abs(d1 - d2)) / Math.abs(d1)) < EPSILON) {
      return true;
    }

    return false;
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      LOGGER.error("Incorrect number of arguments.");
      LOGGER.info("Usage: <exec> <config-file>");
      System.exit(1);
    }

    try {
      QueryComparisonConfig config = new QueryComparisonConfig(new File(args[0]));
      QueryComparison queryComparison = new QueryComparison(config);
      queryComparison.run();
    } catch (Exception e) {
      LOGGER.error("Exception caught, aborting query comparison: ", e);
    }

    System.exit(0);
  }
}
