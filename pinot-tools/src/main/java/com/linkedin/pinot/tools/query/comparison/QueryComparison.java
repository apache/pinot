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
      LOGGER.error("Neither segments directory nor reference results file specified");
      return;
    }

    _segmentsDir = (segmentDir != null) ? new File(segmentDir) : null;
    _resultFile = (results != null) ? new File(results) : null;

    if (_segmentsDir != null && (!_segmentsDir.exists() || !_segmentsDir.isDirectory())) {
      LOGGER.error("Invalid segments directory: {}", _segmentsDir.getName());
      return;
    }
  }

  private void run()
      throws Exception {
    startCluster();

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
        JSONObject scanJson;

        if (resultReader != null) {
          scanJson = new JSONObject(resultReader.readLine());
        } else {
          QueryResponse scanResponse = scanBasedQueryProcessor.processQuery(query);
          scanJson = new JSONObject(new ObjectMapper().writeValueAsString(scanResponse));
        }

        String clusterResponse = _clusterStarter.query(query);
        JSONObject clusterJson = new JSONObject(clusterResponse);

        if (compare(clusterJson, scanJson)) {
          ++passed;
          LOGGER.info("Comparison PASSED: Id: {} Cluster Time: {} ms Scan Time: {} ms Docs Scanned: {}", total,
              clusterJson.get(TIME_USED_MS), scanJson.get(TIME_USED_MS), clusterJson.get(NUM_DOCS_SCANNED));
          LOGGER.debug("Cluster Result: {}", clusterJson);
          LOGGER.debug("Scan Result: {}", scanJson);
        } else {
          LOGGER.error("Comparison FAILED: {}", query);
          LOGGER.info("Cluster Response: {}", clusterJson);
          LOGGER.info("Scan Response: {}", scanJson);
        }
      } catch (Exception e) {
        LOGGER.error("Exception caught while processing query: '{}'", query, e);
      } finally {
        ++total;
      }
    }

    LOGGER.info("Total {} out of {} queries passed.", passed, total);
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

  public static boolean compare(JSONObject clusterJson, JSONObject scanJson)
      throws JSONException {
    // If no records found, nothing to compare.
    if ((clusterJson.getInt(NUM_DOCS_SCANNED) == 0) && scanJson.getInt(NUM_DOCS_SCANNED) == 0) {
      return true;
    }

    if (!compareSelection(clusterJson, scanJson)) {
      return false;
    }

    if (!compareAggregation(clusterJson, scanJson)) {
      return false;
    }

    return true;
  }

  public static boolean compareAggregation(JSONObject clusterJson, JSONObject scanJson)
      throws JSONException {
    if ((clusterJson.getJSONArray(AGGREGATION_RESULTS).length() == 0) && !scanJson.has(AGGREGATION_RESULTS)) {
      return true;
    }

    if (!compareNumDocsScanned(clusterJson, scanJson)) {
      return false;
    }

    JSONArray clusterAggregation = clusterJson.getJSONArray(AGGREGATION_RESULTS);
    if (clusterAggregation.length() == 0) {
      return true;
    }

    if (clusterAggregation.getJSONObject(0).has(GROUP_BY_RESULT)) {
      return compareAggregationGroupBy(clusterJson, scanJson);
    }

    JSONArray scanAggregation = scanJson.getJSONArray(AGGREGATION_RESULTS);
    return compareAggregationArrays(clusterAggregation, scanAggregation);
  }

  private static boolean compareAggregationArrays(JSONArray clusterAggregation, JSONArray scanAggregation)
      throws JSONException {
    Map<String, Double> map = new HashMap<>();

    for (int i = 0; i < scanAggregation.length(); ++i) {
      JSONObject object = scanAggregation.getJSONObject(i);
      map.put(object.getString(FUNCTION).toLowerCase(), Double.valueOf(object.getString(VALUE)));
    }

    for (int i = 0; i < clusterAggregation.length(); ++i) {
      JSONObject object = clusterAggregation.getJSONObject(i);
      String function = object.getString(FUNCTION).toLowerCase();
      String valueString = object.getString(VALUE);

      if (!isNumeric(valueString)) {
        LOGGER.warn("Found non-numeric value for aggregation ignoring Function: {} Value: {}", function, valueString);
        continue;
      }

      Double value = Double.valueOf(valueString);

      if (!map.containsKey(function)) {
        LOGGER.error("Scan result does not contain function {}", function);
        return false;
      }

      Double scanValue = map.get(function);
      if (!fuzzyEqual(value, scanValue)) {
        LOGGER.error("Aggregation value mismatch for function {}, {}, {}", function, value, scanValue);
        return false;
      }
    }

    return true;
  }

  private static boolean compareAggregationGroupBy(JSONObject clusterJson, JSONObject scanJson)
      throws JSONException {
    JSONArray clusterGroupByResults = clusterJson.getJSONArray(AGGREGATION_RESULTS);
    JSONArray scanGroupByResults = scanJson.getJSONArray(AGGREGATION_RESULTS);

    int numClusterGroupBy = clusterGroupByResults.length();
    int numScanGroupBy = scanGroupByResults.length();

    // Build map based on function (function_column name) to match individual entries.
    Map<String, Integer> functionMap = new HashMap<>();
    for (int i = 0; i < numScanGroupBy; ++i) {
      JSONObject scanAggr = scanGroupByResults.getJSONObject(i);
      String scanFunction = scanAggr.getString(FUNCTION).toLowerCase();
      functionMap.put(scanFunction, i);
    }

    for (int i = 0; i < numClusterGroupBy; ++i) {
      JSONObject clusterAggr = clusterGroupByResults.getJSONObject(i);
      String clusterFunction = clusterAggr.getString(FUNCTION).toLowerCase();

      if (!functionMap.containsKey(clusterFunction)) {
        LOGGER.error("Missing group by function in scan: {}", clusterFunction);
        return false;
      }

      JSONObject scanAggr = scanGroupByResults.getJSONObject(functionMap.get(clusterFunction));
      if (!compareGroupByColumns(clusterAggr, scanAggr)) {
        return false;
      }

      if (!compareAggregationValues(clusterAggr, scanAggr)) {
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

  private static boolean compareAggregationValues(JSONObject clusterAggr, JSONObject scanAggr)
      throws JSONException {
    JSONArray clusterResult = clusterAggr.getJSONArray(GROUP_BY_RESULT);
    JSONArray scanResult = scanAggr.getJSONArray(GROUP_BY_RESULT);

    Map<GroupByOperator, Double> scanMap = new HashMap<>();
    for (int i = 0; i < scanResult.length(); ++i) {
      List<Object> group = jsonArrayToList(scanResult.getJSONObject(i).getJSONArray(GROUP));
      GroupByOperator groupByOperator = new GroupByOperator(group);
      scanMap.put(groupByOperator, scanResult.getJSONObject(i).getDouble(VALUE));
    }

    for (int i = 0; i < clusterResult.length(); ++i) {
      List<Object> group = jsonArrayToList(clusterResult.getJSONObject(i).getJSONArray(GROUP));
      GroupByOperator groupByOperator = new GroupByOperator(group);

      double clusterValue = clusterResult.getJSONObject(i).getDouble(VALUE);
      if (!scanMap.containsKey(groupByOperator)) {
        LOGGER.error("Missing group by value for group: {}", group);
        return false;
      }

      double scanValue = scanMap.get(groupByOperator);
      if (!fuzzyEqual(clusterValue, scanValue)) {
        LOGGER.error("Aggregation group by value mis-match: Cluster: {}, Scan: {}", clusterValue, scanValue);
        return false;
      }
    }

    return true;
  }

  private static boolean compareGroupByColumns(JSONObject clusterAggr, JSONObject scanAggr)
      throws JSONException {
    JSONArray clusterCols = clusterAggr.getJSONArray(GROUP_BY_COLUMNS);
    JSONArray scanCols = scanAggr.getJSONArray(GROUP_BY_COLUMNS);

    if (!compareLists(clusterCols, scanCols)) {
      return false;
    }
    return true;
  }

  private static boolean compareSelection(JSONObject clusterJson, JSONObject scanJson)
      throws JSONException {
    if (!clusterJson.has(SELECTION_RESULTS) && !scanJson.has(SELECTION_RESULTS)) {
      return true;
    }

    JSONObject clusterSelection = clusterJson.getJSONObject(SELECTION_RESULTS);
    JSONObject scanSelection = scanJson.getJSONObject(SELECTION_RESULTS);

    if (!compareLists(clusterSelection.getJSONArray(COLUMNS), scanSelection.getJSONArray(COLUMNS))) {
      return false;
    }

    if (!compareSelectionRows(clusterSelection.getJSONArray(RESULTS), scanSelection.getJSONArray(RESULTS))) {
      return false;
    }
    return true;
  }

  private static boolean compareSelectionRows(JSONArray clusterRows, JSONArray scanRows)
      throws JSONException {
    int numClusterRows = clusterRows.length();
    int numScanRows = scanRows.length();

    for (int i = 0; i < numClusterRows; ++i) {
      JSONArray clusterRow = clusterRows.getJSONArray(i);
      JSONArray scanRow = scanRows.getJSONArray(i);

      int numClusterCols = clusterRow.length();
      int numScanCols = scanRow.length();

      if (numClusterCols != numClusterCols) {
        LOGGER.error("Number of columns mis-match: Cluster: {} Scan: {}", numClusterCols, numScanCols);
        return false;
      }

      for (int j = 0; j < numClusterCols; ++j) {
        String clusterVal = clusterRow.getString(j);
        String scanVal = scanRow.getString(j);

        if (!compareAsNumber(clusterVal, scanVal) && !compareAsString(clusterVal, scanVal)) {
          LOGGER.error("Value mis-match: Cluster {} Scan: {}", clusterVal, scanVal);
          return false;
        }
      }
    }

    return true;
  }

  private static boolean compareAsString(String clusterString, String scanString) {
    return (clusterString.equals(scanString));
  }

  private static boolean isNumeric(String str) {
    if (str == null) {
      return false;
    }

    try {
      double d = Double.parseDouble(str);
    } catch (NumberFormatException nfe) {
      return false;
    }
    return true;
  }

  private static boolean compareAsNumber(String clusterString, String scanString) {
    try {
      double clusterVal = Double.parseDouble(clusterString);
      double scanVal = Double.parseDouble(scanString);
      return (fuzzyEqual(clusterVal, scanVal));
    } catch (NumberFormatException nfe) {
      return true;
    }
  }

  private static boolean compareLists(JSONArray clusterList, JSONArray scanList)
      throws JSONException {
    int clusterSize = clusterList.length();
    int scanSize = scanList.length();

    if (clusterSize != scanSize) {
      LOGGER.error("Number of columns mismatch: Cluster: {} Scan: {}", clusterSize, scanSize);
      return false;
    }

    for (int i = 0; i < scanList.length(); ++i) {
      String clusterColumn = clusterList.getString(i);
      String scanColumn = scanList.getString(i);

      if (!clusterColumn.equals(scanColumn)) {
        LOGGER.error("Column name mis-match: Cluster: {} Scan: {}", clusterColumn, scanColumn);
        return false;
      }
    }

    return true;
  }

  private static boolean compareNumDocsScanned(JSONObject clusterJson, JSONObject scanJson)
      throws JSONException {
    int clusterDocs = clusterJson.getInt(NUM_DOCS_SCANNED);
    int scanDocs = scanJson.getInt(NUM_DOCS_SCANNED);

    if (clusterDocs != scanDocs) {
      LOGGER.error("Mis-match in number of docs scanned: Cluster: {} Scan: {}", clusterDocs, scanDocs);
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
