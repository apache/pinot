/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.tools.query.comparison;

import com.linkedin.pinot.tools.scan.query.GroupByOperator;
import com.linkedin.pinot.tools.scan.query.QueryResponse;
import com.linkedin.pinot.tools.scan.query.ScanBasedQueryProcessor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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

  private void run()
      throws Exception {
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

  private void runFunctionMode()
      throws Exception {
    String query;
    ScanBasedQueryProcessor scanBasedQueryProcessor = null;
    BufferedReader resultReader = null;
    BufferedReader queryReader = new BufferedReader(new FileReader(_queryFile));

    if (_resultFile == null) {
      scanBasedQueryProcessor = new ScanBasedQueryProcessor(_segmentsDir.getAbsolutePath());
    } else {
      resultReader = new BufferedReader(new FileReader(_resultFile));
    }

    int passed = 0;
    int total = 0;
    while ((query = queryReader.readLine()) != null) {
      if (query.isEmpty() || query.startsWith("#")) {
        continue;
      }

      try {
        JSONObject expectedJson;

        if (resultReader != null) {
          expectedJson = new JSONObject(resultReader.readLine());
        } else {
          QueryResponse expectedResponse = scanBasedQueryProcessor.processQuery(query);
          expectedJson = new JSONObject(new ObjectMapper().writeValueAsString(expectedResponse));
        }

        String actualResponse = _clusterStarter.query(query);
        JSONObject actualJson = new JSONObject(actualResponse);

        if (compare(actualJson, expectedJson)) {
          ++passed;
          LOGGER.info(
              "Comparison PASSED: Id: {} actual Time: {} ms expected Time: {} ms Docs Scanned: {}",
              total, actualJson.get(TIME_USED_MS), expectedJson.get(TIME_USED_MS),
              actualJson.get(NUM_DOCS_SCANNED));
          LOGGER.debug("actual Response: {}", actualJson);
          LOGGER.debug("expected Response: {}", expectedJson);
        } else {
          LOGGER.error("Comparison FAILED: {}", query);
          LOGGER.info("actual Response: {}", actualJson);
          LOGGER.info("expected Response: {}", expectedJson);
        }
      } catch (Exception e) {
        LOGGER.error("Exception caught while processing query: '{}'", query, e);
      } finally {
        ++total;
      }
    }

    LOGGER.info("Total {} out of {} queries passed.", passed, total);
  }

  private void runPerfMode()
      throws Exception {
    String query;
    BufferedReader queryReader = new BufferedReader(new FileReader(_queryFile));

    while ((query = queryReader.readLine()) != null) {
      if (query.isEmpty() || query.startsWith("#")) {
        continue;
      }

      int clientTime = _clusterStarter.perfQuery(query);
      LOGGER.info("Client side response time: {} ms", clientTime);
    }
  }

  private void startCluster()
      throws Exception {
    _clusterStarter = new ClusterStarter(_config);

    if (_config.getStartCluster()) {
      LOGGER.info("Bringing up Pinot Cluster");
      _clusterStarter.start();
      LOGGER.info("Pinot Cluster is now up");
    } else {
      LOGGER.info("Skipping cluster setup as specified in the config file.");
    }
  }

  public static boolean compare(JSONObject actualJson, JSONObject expectedJson)
      throws JSONException {
    // If no records found, nothing to compare.
    if ((actualJson.getInt(NUM_DOCS_SCANNED) == 0) && expectedJson.getInt(NUM_DOCS_SCANNED) == 0) {
      LOGGER.info("Empty results, nothing to compare.");
      return true;
    }

    if (!compareSelection(actualJson, expectedJson)) {
      return false;
    }

    if (!compareAggregation(actualJson, expectedJson)) {
      return false;
    }

    return true;
  }

  /**
   * Some clients (eg Star Tree) may have different num docs scanned, but produce the same result.
   * This compare method will ignore comparing number of documents scanned.
   * @param actualJson
   * @param expectedJson
   * @param compareNumDocs
   * @return
   */
  public static boolean compare(JSONObject actualJson, JSONObject expectedJson, boolean compareNumDocs)
      throws JSONException {
    _compareNumDocs = compareNumDocs;
    return compare(actualJson, expectedJson);
  }

  public static boolean compareAggregation(JSONObject actualJson, JSONObject expectedJson)
      throws JSONException {
    if ((actualJson.getJSONArray(AGGREGATION_RESULTS).length() == 0) && !expectedJson.has(AGGREGATION_RESULTS)) {
      return true;
    }

    if (_compareNumDocs && !compareNumDocsScanned(actualJson, expectedJson)) {
      return false;
    }

    JSONArray actualAggregation = actualJson.getJSONArray(AGGREGATION_RESULTS);
    if (actualAggregation.length() == 0) {
      return true;
    }

    if (actualAggregation.getJSONObject(0).has(GROUP_BY_RESULT)) {
      return compareAggregationGroupBy(actualJson, expectedJson);
    }

    JSONArray expectedAggregation = expectedJson.getJSONArray(AGGREGATION_RESULTS);
    return compareAggregationArrays(actualAggregation, expectedAggregation);
  }

  private static boolean compareAggregationArrays(JSONArray actualAggregation, JSONArray expectedAggregation)
      throws JSONException {
    Map<String, Double> map = new HashMap<>();

    for (int i = 0; i < expectedAggregation.length(); ++i) {
      JSONObject object = expectedAggregation.getJSONObject(i);
      map.put(object.getString(FUNCTION).toLowerCase(), Double.valueOf(object.getString(VALUE)));
    }

    for (int i = 0; i < actualAggregation.length(); ++i) {
      JSONObject object = actualAggregation.getJSONObject(i);
      String function = object.getString(FUNCTION).toLowerCase();
      String valueString = object.getString(VALUE);

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

  private static boolean compareAggregationGroupBy(JSONObject actualJson, JSONObject expectedJson)
      throws JSONException {
    JSONArray actualGroupByResults = actualJson.getJSONArray(AGGREGATION_RESULTS);
    JSONArray expectedGroupByResults = expectedJson.getJSONArray(AGGREGATION_RESULTS);

    int numActualGroupBy = actualGroupByResults.length();
    int numExpectedGroupBy = expectedGroupByResults.length();

    // Build map based on function (function_column name) to match individual entries.
    Map<String, Integer> functionMap = new HashMap<>();
    for (int i = 0; i < numExpectedGroupBy; ++i) {
      JSONObject expectedAggr = expectedGroupByResults.getJSONObject(i);
      String expectedFunction = expectedAggr.getString(FUNCTION).toLowerCase();
      functionMap.put(expectedFunction, i);
    }

    for (int i = 0; i < numActualGroupBy; ++i) {
      JSONObject actualAggr = actualGroupByResults.getJSONObject(i);
      String actualFunction = actualAggr.getString(FUNCTION).toLowerCase();

      if (!functionMap.containsKey(actualFunction)) {
        LOGGER.error("Missing group by function in expected response: {}", actualFunction);
        return false;
      }

      JSONObject expectedAggr = expectedGroupByResults.getJSONObject(functionMap.get(actualFunction));
      if (!compareGroupByColumns(actualAggr, expectedAggr)) {
        return false;
      }

      if (!compareAggregationValues(actualAggr, expectedAggr)) {
        return false;
      }
    }
    return true;
  }

  private static List<Object> jsonArrayToList(JSONArray jsonArray)
      throws JSONException {
    List<Object> list = new ArrayList<>();
    for (int i = 0; i < jsonArray.length(); ++i) {
      list.add(jsonArray.get(i));
    }
    return list;
  }

  private static boolean compareAggregationValues(JSONObject actualAggr, JSONObject expectedAggr)
      throws JSONException {
    JSONArray actualResult = actualAggr.getJSONArray(GROUP_BY_RESULT);
    JSONArray expectedResult = expectedAggr.getJSONArray(GROUP_BY_RESULT);

    Map<GroupByOperator, Double> expectedMap = new HashMap<>();
    for (int i = 0; i < expectedResult.length(); ++i) {
      List<Object> group = jsonArrayToList(expectedResult.getJSONObject(i).getJSONArray(GROUP));
      GroupByOperator groupByOperator = new GroupByOperator(group);
      expectedMap.put(groupByOperator, expectedResult.getJSONObject(i).getDouble(VALUE));
    }

    for (int i = 0; i < actualResult.length(); ++i) {
      List<Object> group = jsonArrayToList(actualResult.getJSONObject(i).getJSONArray(GROUP));
      GroupByOperator groupByOperator = new GroupByOperator(group);

      double actualValue = actualResult.getJSONObject(i).getDouble(VALUE);
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

  private static boolean compareGroupByColumns(JSONObject actualAggr, JSONObject expectedAggr)
      throws JSONException {
    JSONArray actualCols = actualAggr.getJSONArray(GROUP_BY_COLUMNS);
    JSONArray expectedCols = expectedAggr.getJSONArray(GROUP_BY_COLUMNS);

    if (!compareLists(actualCols, expectedCols)) {
      return false;
    }
    return true;
  }

  private static boolean compareSelection(JSONObject actualJson, JSONObject expectedJson)
      throws JSONException {
    if (!actualJson.has(SELECTION_RESULTS) && !expectedJson.has(SELECTION_RESULTS)) {
      return true;
    }

    JSONObject actualSelection = actualJson.getJSONObject(SELECTION_RESULTS);
    JSONObject expectedSelection = expectedJson.getJSONObject(SELECTION_RESULTS);

    if (!compareLists(actualSelection.getJSONArray(COLUMNS), expectedSelection.getJSONArray(COLUMNS))) {
      return false;
    }

    if (!compareSelectionRows(actualSelection.getJSONArray(RESULTS), expectedSelection.getJSONArray(RESULTS))) {
      return false;
    }
    return true;
  }

  private static boolean compareSelectionRows(JSONArray actualRows, JSONArray expectedRows)
      throws JSONException {
    int numActualRows = actualRows.length();

    for (int i = 0; i < numActualRows; ++i) {
      JSONArray actualRow = actualRows.getJSONArray(i);
      JSONArray expectedRow = expectedRows.getJSONArray(i);

      int numActualCols = actualRow.length();
      int numExpectedCols = expectedRow.length();

      if (numActualCols != numActualCols) {
        LOGGER.error("Number of columns mis-match: actual: {} expected: {}", numActualCols, numExpectedCols);
        return false;
      }

      for (int j = 0; j < numActualCols; ++j) {
        String actualVal = actualRow.getString(j);
        String expectedVal = expectedRow.getString(j);

        if (!compareAsNumber(actualVal, expectedVal) && !compareAsString(actualVal, expectedVal)) {
          LOGGER.error("Value mis-match: actual {} expected: {}", actualVal, expectedVal);
          return false;
        }
      }
    }

    return true;
  }

  private static boolean compareAsString(String actualString, String expectedString) {
    return (actualString.equals(expectedString));
  }

  private static boolean isNumeric(String str) {
    if (str == null) {
      return false;
    }

    try {
      double doubleValue = Double.parseDouble(str);
    } catch (NumberFormatException nfe) {
      return false;
    }
    return true;
  }

  private static boolean compareAsNumber(String actualString, String expectedString) {
    try {
      double actualVal = Double.parseDouble(actualString);
      double expectedVal = Double.parseDouble(expectedString);
      return (fuzzyEqual(actualVal, expectedVal));
    } catch (NumberFormatException nfe) {
      return true;
    }
  }

  private static boolean compareLists(JSONArray actualList, JSONArray expectedList)
      throws JSONException {
    int actualSize = actualList.length();
    int expectedSize = expectedList.length();

    if (actualSize != expectedSize) {
      LOGGER.error("Number of columns mismatch: actual: {} expected: {}", actualSize, expectedSize);
      return false;
    }

    for (int i = 0; i < expectedList.length(); ++i) {
      String actualColumn = actualList.getString(i);
      String expectedColumn = expectedList.getString(i);

      if (!actualColumn.equals(expectedColumn)) {
        LOGGER.error("Column name mis-match: actual: {} expected: {}", actualColumn, expectedColumn);
        return false;
      }
    }

    return true;
  }

  private static boolean compareNumDocsScanned(JSONObject actualJson, JSONObject expectedJson)
      throws JSONException {
    int actualDocs = actualJson.getInt(NUM_DOCS_SCANNED);
    int expectedDocs = expectedJson.getInt(NUM_DOCS_SCANNED);

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
    if (d1 > 0 && ((Math.abs(d1 - d2)) / d1) < EPSILON) {
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
