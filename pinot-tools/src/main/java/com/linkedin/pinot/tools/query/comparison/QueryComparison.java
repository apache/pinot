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
        JSONObject refJson;

        if (resultReader != null) {
          refJson = new JSONObject(resultReader.readLine());
        } else {
          QueryResponse refResponse = scanBasedQueryProcessor.processQuery(query);
          refJson = new JSONObject(new ObjectMapper().writeValueAsString(refResponse));
        }

        String flowResponse = _clusterStarter.query(query);
        JSONObject flowJson = new JSONObject(flowResponse);

        if (compare(flowJson, refJson)) {
          ++passed;
          LOGGER.info("Comparison PASSED: Id: {} Flow Time: {} ms Ref Time: {} ms Docs Scanned: {}", total,
              flowJson.get(TIME_USED_MS), refJson.get(TIME_USED_MS), flowJson.get(NUM_DOCS_SCANNED));
          LOGGER.debug("Flow Response: {}", flowJson);
          LOGGER.debug("Ref Response: {}", refJson);
        } else {
          LOGGER.error("Comparison FAILED: {}", query);
          LOGGER.info("Flow Response: {}", flowJson);
          LOGGER.info("Ref Response: {}", refJson);
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

  public static boolean compare(JSONObject flowJson, JSONObject refJson)
      throws JSONException {
    // If no records found, nothing to compare.
    if ((flowJson.getInt(NUM_DOCS_SCANNED) == 0) && refJson.getInt(NUM_DOCS_SCANNED) == 0) {
      LOGGER.info("Empty results, nothing to compare.");
      return true;
    }

    if (!compareSelection(flowJson, refJson)) {
      return false;
    }

    if (!compareAggregation(flowJson, refJson)) {
      return false;
    }

    return true;
  }

  public static boolean compareAggregation(JSONObject flowJson, JSONObject refJson)
      throws JSONException {
    if ((flowJson.getJSONArray(AGGREGATION_RESULTS).length() == 0) && !refJson.has(AGGREGATION_RESULTS)) {
      return true;
    }

    if (_compareNumDocs && !compareNumDocsScanned(flowJson, refJson)) {
      return false;
    }

    JSONArray flowAggregation = flowJson.getJSONArray(AGGREGATION_RESULTS);
    if (flowAggregation.length() == 0) {
      return true;
    }

    if (flowAggregation.getJSONObject(0).has(GROUP_BY_RESULT)) {
      return compareAggregationGroupBy(flowJson, refJson);
    }

    JSONArray refAggregation = refJson.getJSONArray(AGGREGATION_RESULTS);
    return compareAggregationArrays(flowAggregation, refAggregation);
  }

  private static boolean compareAggregationArrays(JSONArray flowAggregation, JSONArray refAggregation)
      throws JSONException {
    Map<String, Double> map = new HashMap<>();

    for (int i = 0; i < refAggregation.length(); ++i) {
      JSONObject object = refAggregation.getJSONObject(i);
      map.put(object.getString(FUNCTION).toLowerCase(), Double.valueOf(object.getString(VALUE)));
    }

    for (int i = 0; i < flowAggregation.length(); ++i) {
      JSONObject object = flowAggregation.getJSONObject(i);
      String function = object.getString(FUNCTION).toLowerCase();
      String valueString = object.getString(VALUE);

      if (!isNumeric(valueString)) {
        LOGGER.warn("Found non-numeric value for aggregation ignoring Function: {} Value: {}", function, valueString);
        continue;
      }

      Double value = Double.valueOf(valueString);

      if (!map.containsKey(function)) {
        LOGGER.error("Ref Response does not contain function {}", function);
        return false;
      }

      Double refValue = map.get(function);
      if (!fuzzyEqual(value, refValue)) {
        LOGGER.error("Aggregation value mismatch for function {}, {}, {}", function, value, refValue);
        return false;
      }
    }

    return true;
  }

  private static boolean compareAggregationGroupBy(JSONObject flowJson, JSONObject refJson)
      throws JSONException {
    JSONArray flowGroupByResults = flowJson.getJSONArray(AGGREGATION_RESULTS);
    JSONArray refGroupByResults = refJson.getJSONArray(AGGREGATION_RESULTS);

    int numFlowGroupBy = flowGroupByResults.length();
    int numRefGroupBy = refGroupByResults.length();

    // Build map based on function (function_column name) to match individual entries.
    Map<String, Integer> functionMap = new HashMap<>();
    for (int i = 0; i < numRefGroupBy; ++i) {
      JSONObject refAggr = refGroupByResults.getJSONObject(i);
      String refFunction = refAggr.getString(FUNCTION).toLowerCase();
      functionMap.put(refFunction, i);
    }

    for (int i = 0; i < numFlowGroupBy; ++i) {
      JSONObject flowAggr = flowGroupByResults.getJSONObject(i);
      String flowFunction = flowAggr.getString(FUNCTION).toLowerCase();

      if (!functionMap.containsKey(flowFunction)) {
        LOGGER.error("Missing group by function in ref response: {}", flowFunction);
        return false;
      }

      JSONObject refAggr = refGroupByResults.getJSONObject(functionMap.get(flowFunction));
      if (!compareGroupByColumns(flowAggr, refAggr)) {
        return false;
      }

      if (!compareAggregationValues(flowAggr, refAggr)) {
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

  private static boolean compareAggregationValues(JSONObject flowAggr, JSONObject refAggr)
      throws JSONException {
    JSONArray flowResult = flowAggr.getJSONArray(GROUP_BY_RESULT);
    JSONArray refResult = refAggr.getJSONArray(GROUP_BY_RESULT);

    Map<GroupByOperator, Double> refMap = new HashMap<>();
    for (int i = 0; i < refResult.length(); ++i) {
      List<Object> group = jsonArrayToList(refResult.getJSONObject(i).getJSONArray(GROUP));
      GroupByOperator groupByOperator = new GroupByOperator(group);
      refMap.put(groupByOperator, refResult.getJSONObject(i).getDouble(VALUE));
    }

    for (int i = 0; i < flowResult.length(); ++i) {
      List<Object> group = jsonArrayToList(flowResult.getJSONObject(i).getJSONArray(GROUP));
      GroupByOperator groupByOperator = new GroupByOperator(group);

      double flowValue = flowResult.getJSONObject(i).getDouble(VALUE);
      if (!refMap.containsKey(groupByOperator)) {
        LOGGER.error("Missing group by value for group: {}", group);
        return false;
      }

      double refValue = refMap.get(groupByOperator);
      if (!fuzzyEqual(flowValue, refValue)) {
        LOGGER.error("Aggregation group by value mis-match: Flow: {}, Ref: {}", flowValue, refValue);
        return false;
      }
    }

    return true;
  }

  private static boolean compareGroupByColumns(JSONObject flowAggr, JSONObject refAggr)
      throws JSONException {
    JSONArray flowCols = flowAggr.getJSONArray(GROUP_BY_COLUMNS);
    JSONArray refCols = refAggr.getJSONArray(GROUP_BY_COLUMNS);

    if (!compareLists(flowCols, refCols)) {
      return false;
    }
    return true;
  }

  private static boolean compareSelection(JSONObject flowJson, JSONObject refJson)
      throws JSONException {
    if (!flowJson.has(SELECTION_RESULTS) && !refJson.has(SELECTION_RESULTS)) {
      return true;
    }

    JSONObject flowSelection = flowJson.getJSONObject(SELECTION_RESULTS);
    JSONObject refSelection = refJson.getJSONObject(SELECTION_RESULTS);

    if (!compareLists(flowSelection.getJSONArray(COLUMNS), refSelection.getJSONArray(COLUMNS))) {
      return false;
    }

    if (!compareSelectionRows(flowSelection.getJSONArray(RESULTS), refSelection.getJSONArray(RESULTS))) {
      return false;
    }
    return true;
  }

  private static boolean compareSelectionRows(JSONArray flowRows, JSONArray refRows)
      throws JSONException {
    int numFlowRows = flowRows.length();
    int numRefRows = refRows.length();

    for (int i = 0; i < numFlowRows; ++i) {
      JSONArray flowRow = flowRows.getJSONArray(i);
      JSONArray refRow = refRows.getJSONArray(i);

      int numFlowCols = flowRow.length();
      int numRefCols = refRow.length();

      if (numFlowCols != numFlowCols) {
        LOGGER.error("Number of columns mis-match: Flow: {} Ref: {}", numFlowCols, numRefCols);
        return false;
      }

      for (int j = 0; j < numFlowCols; ++j) {
        String flowVal = flowRow.getString(j);
        String refVal = refRow.getString(j);

        if (!compareAsNumber(flowVal, refVal) && !compareAsString(flowVal, refVal)) {
          LOGGER.error("Value mis-match: Flow {} Ref: {}", flowVal, refVal);
          return false;
        }
      }
    }

    return true;
  }

  private static boolean compareAsString(String flowString, String refString) {
    return (flowString.equals(refString));
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

  private static boolean compareAsNumber(String flowString, String refString) {
    try {
      double flowVal = Double.parseDouble(flowString);
      double refVal = Double.parseDouble(refString);
      return (fuzzyEqual(flowVal, refVal));
    } catch (NumberFormatException nfe) {
      return true;
    }
  }

  private static boolean compareLists(JSONArray flowList, JSONArray refList)
      throws JSONException {
    int flowSize = flowList.length();
    int refSize = refList.length();

    if (flowSize != refSize) {
      LOGGER.error("Number of columns mismatch: Flow: {} Ref: {}", flowSize, refSize);
      return false;
    }

    for (int i = 0; i < refList.length(); ++i) {
      String flowColumn = flowList.getString(i);
      String refColumn = refList.getString(i);

      if (!flowColumn.equals(refColumn)) {
        LOGGER.error("Column name mis-match: Flow: {} Ref: {}", flowColumn, refColumn);
        return false;
      }
    }

    return true;
  }

  private static boolean compareNumDocsScanned(JSONObject flowJson, JSONObject refJson)
      throws JSONException {
    int flowDocs = flowJson.getInt(NUM_DOCS_SCANNED);
    int refDocs = refJson.getInt(NUM_DOCS_SCANNED);

    if (flowDocs != refDocs) {
      LOGGER.error("Mis-match in number of docs scanned: Flow: {} Ref: {}", flowDocs, refDocs);
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

  public static void setCompareNumDocs(boolean value) {
    _compareNumDocs = value;
  }
}
