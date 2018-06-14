/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.utils.ClientSSLContextGenerator;
import com.linkedin.pinot.tools.scan.query.QueryResponse;
import com.linkedin.pinot.tools.scan.query.ScanBasedQueryProcessor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLContext;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SSLQueryComparison {
  private static final Logger LOGGER = LoggerFactory.getLogger(SSLQueryComparison.class);

  private static final double EPSILON = 0.00001;

  private static final String SELECTION_RESULTS = "selectionResults";
  private static final String AGGREGATION_RESULTS = "aggregationResults";
  private static final String METADATA = "metadata";
  private static final String ELEMENTS = "elements";
  private static final String NUM_DOCS_SCANNED = "numDocsScanned";
  private static final String FUNCTION = "function";
  private static final String VALUE = "value";
  private static final String COLUMN_NAMES = "columnNames";
  private static final String RESULTS = "results";
  private static final String COLUMNS = "columns";
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

  public SSLQueryComparison(QueryComparisonConfig config) {
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

  private void runFunctionMode() throws Exception {
    BufferedReader resultReader = null;
    ScanBasedQueryProcessor scanBasedQueryProcessor = null;
    LOGGER.info("Running function mode...");

    try (
        BufferedReader queryReader = new BufferedReader(new InputStreamReader(new FileInputStream(_queryFile), "UTF8"))
    ) {
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

        JSONObject expectedJson = null;
        try {
          if (resultReader != null) {
            expectedJson = new JSONObject(resultReader.readLine());
          } else {
            QueryResponse expectedResponse = scanBasedQueryProcessor.processQuery(query);
            expectedJson = new JSONObject(new ObjectMapper().writeValueAsString(expectedResponse));
          }
        } catch (Exception e) {
          LOGGER.error("Comparison FAILED: Id: {} Exception caught while getting expected response for query: '{}'",
              total, query, e);
        }

        JSONObject actualJson = null;
        if (expectedJson != null) {
          try {
            actualJson = new JSONObject(_clusterStarter.query(query));
          } catch (Exception e) {
            LOGGER.error("Comparison FAILED: Id: {} Exception caught while running query: '{}'", total, query, e);
          }
        }

        if (expectedJson != null && actualJson != null) {
          try {
            if (compare(actualJson, expectedJson)) {
              passed++;
              LOGGER.info("Comparison PASSED: Id: {} actual Time: {} ms expected Time: {} ms Docs Scanned: {}", total,
                  actualJson.getJSONObject(METADATA).get(TIME_USED_MS), expectedJson.getJSONObject(METADATA).get(TIME_USED_MS), actualJson.getJSONObject(METADATA).get(NUM_DOCS_SCANNED));
              LOGGER.debug("actual Response: {}", actualJson);
              LOGGER.debug("expected Response: {}", expectedJson);
            } else {
              LOGGER.error("Comparison FAILED: Id: {} query: {}", query);
              LOGGER.info("actual Response: {}", actualJson);
              LOGGER.info("expected Response: {}", expectedJson);
            }
          } catch (Exception e) {
            LOGGER.error("Comparison FAILED: Id: {} Exception caught while comparing query: '{}' actual response: {}, expected response: {}",
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
    try (
        BufferedReader queryReader = new BufferedReader(new InputStreamReader(new FileInputStream(_queryFile), "UTF8"))
    ) {
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

  public static boolean compare(JSONObject actualJson, JSONObject expectedJson)
      throws JSONException {
    QueryComparison.ComparisonStatus comparisonStatus = compareWithEmpty(actualJson, expectedJson);
    return !comparisonStatus.equals(QueryComparison.ComparisonStatus.FAILED);
  }

  public static QueryComparison.ComparisonStatus compareWithEmpty(JSONObject actualJson, JSONObject expectedJson)
      throws JSONException {
//    if (actualJson.getJSONArray(EXCEPTIONS).length() != 0) {
//      return QueryComparison.ComparisonStatus.FAILED;
//    }

    // If no records found, nothing to compare.
    if (((actualJson.getJSONObject(METADATA).getInt(NUM_DOCS_SCANNED) == 0) && (expectedJson.getJSONObject(METADATA).getInt(NUM_DOCS_SCANNED) == 0)) ||
        (!actualJson.has(ELEMENTS) && !expectedJson.has(ELEMENTS)) ||
        (actualJson.getJSONArray(ELEMENTS).length() == 0 && expectedJson.getJSONArray(ELEMENTS).length() == 0)) {
      LOGGER.info("Empty results, nothing to compare.");
      return QueryComparison.ComparisonStatus.EMPTY;
    }

    if (actualJson.getJSONArray(ELEMENTS).length() != expectedJson.getJSONArray(ELEMENTS).length()) {
      return QueryComparison.ComparisonStatus.FAILED;
    }

    JSONArray expectedElements = expectedJson.getJSONArray(ELEMENTS);
    JSONArray actualElements = actualJson.getJSONArray(ELEMENTS);
    Map<String, Double> map = new HashMap<>();
    for (int i = 0; i < expectedElements.length(); i++) {
      JSONObject expectedElement = expectedElements.getJSONObject(i);
      JSONObject actualElement = actualElements.getJSONObject(i);

      // Compare column names.
      JSONArray expectedColumnNames = expectedElement.getJSONArray(COLUMN_NAMES);
      JSONArray actualColumnNames = actualElement.getJSONArray(COLUMN_NAMES);
      Map<Integer, Integer> expectedToActualColMap = new HashMap<>(actualColumnNames.length());
      if (!compareColumnNames(expectedColumnNames, actualColumnNames, expectedToActualColMap)) {
        return QueryComparison.ComparisonStatus.FAILED;
      }

      JSONArray expectedResults = expectedElement.getJSONArray(RESULTS);
      JSONArray actualResults = actualElement.getJSONArray(RESULTS);
      if (!compareResults(expectedResults, actualResults, expectedToActualColMap)) {
        return QueryComparison.ComparisonStatus.FAILED;
      }
    }

    return QueryComparison.ComparisonStatus.PASSED;
  }

  private void startCluster() throws Exception {
    SSLContext sslContext = new ClientSSLContextGenerator(_config).generate();
    _clusterStarter = new ClusterStarter(_config, sslContext);

    if (_config.getStartCluster()) {
      LOGGER.info("Bringing up Pinot Cluster");
      _clusterStarter.start();
      LOGGER.info("Pinot Cluster is now up");
    } else {
      LOGGER.info("Skipping cluster setup as specified in the config file.");
    }
  }

  private static boolean compareColumnNames(JSONArray expectedColumnNames, JSONArray actualColumnNames, Map<Integer, Integer> expectedToActualColMap)
      throws JSONException {
    if (expectedColumnNames == null && actualColumnNames == null) {
      return true;
    }
    if (expectedColumnNames == null || actualColumnNames == null || expectedColumnNames.length() != actualColumnNames.length()) {
      return false;
    }
    for (int i = 0; i < expectedColumnNames.length(); i++) {
      boolean found = false;
      final String expectedColumn = expectedColumnNames.getString(i);
      for (int j = 0; j < actualColumnNames.length(); j++) {
        if (expectedColumn.equals(actualColumnNames.getString(j))) {
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
    return true;
  }

  private static boolean compareResults(JSONArray expectedResults, JSONArray actualResults, Map<Integer, Integer> expectedToActualColMap)
      throws JSONException {
    if (expectedResults == null && actualResults == null) {
      return true;
    }
    if (expectedResults == null || actualResults == null || expectedResults.length() != actualResults.length()) {
      return false;
    }
    Map<String, Integer> countMap = new HashMap<>();
    Map<String, Set<List<String>>> listMap = new HashMap<>();
    for (int i = 0; i < expectedResults.length(); i++) {
      JSONArray expectedResult = expectedResults.getJSONArray(i);
      if (expectedResult == null) {
        continue;
      }
      StringBuilder stringBuilder = new StringBuilder();
      List<String> list = new ArrayList<>();
      for (int j = 0; j < expectedResult.length(); j++) {
        String toAppend;
        if (expectedToActualColMap == null) {
          toAppend = expectedResult.getString(j);
        } else {
          toAppend = expectedResult.getString(expectedToActualColMap.get(j));
        }

        // For number value, uniform the format and do fuzzy comparison
        try {
          double numValue = Double.parseDouble(toAppend);
          stringBuilder.append((long) (numValue * 100)).append("|");
          list.add(Long.toString((long) (numValue * 100)));

        } catch (NumberFormatException e) {
          stringBuilder.append(toAppend).append("|");
          list.add(toAppend);
        }
      }
      String key = stringBuilder.toString();
      Integer count = countMap.get(key);
      Set<List<String>> set;
      if (count != null) {
        countMap.put(key, count + 1);
        set = listMap.get(key);
        set.add(list);
        listMap.put(key, set);
      } else {
        countMap.put(key, 1);
        set = new HashSet<>();
        set.add(list);
        listMap.put(key, set);
      }
    }

    for (int i = 0; i < actualResults.length(); i++) {
      JSONArray actualResult = actualResults.getJSONArray(i);
      if (actualResult == null) {
        continue;
      }
      StringBuilder stringBuilder = new StringBuilder();
      List<String> actualList = new ArrayList<>();
      for (int j = 0; j < actualResult.length(); j++) {
        String toAppend = actualResult.getString(j);
        // For number value, uniform the format and do fuzzy comparison
        try {
          double numValue = Double.parseDouble(toAppend);
          stringBuilder.append((long) (numValue * 100)).append("|");
          actualList.add(Long.toString((long) (numValue * 100)));
        } catch (NumberFormatException e) {
          stringBuilder.append(toAppend).append("|");
          actualList.add(toAppend);
        }

      }
      String key = stringBuilder.toString();
      Integer count = countMap.get(key);
      if (count == null || count == 0) {
        LOGGER.error("Cannot find match for row {} in actual result", i);
        return false;
      }
      countMap.put(key, count - 1);

      Set<List<String>> set = listMap.get(key);
      if (set == null || set.size() == 0) {
        LOGGER.error("expected Response does not contain result {}", key);
        return false;
      }

      List<String> target = null;
      boolean found = false;
      for (List<String> list : set) {
        int numMatch = 0;
        for (int j = 0; j < list.size(); j++) {
          String valueString = list.get(j);
          if (!isNumeric(valueString) || !isNumeric(actualList.get(j))) {
            if (valueString.equals(actualList.get(j))){
              numMatch++;
            }
          } else {
            Double value = Double.valueOf(valueString);
            Double expectedValue = Double.valueOf(actualList.get(j));
            if (fuzzyEqual(value, expectedValue)) {
              numMatch++;
            }
          }
        }
        if (numMatch == list.size()) {
          target = list;
          found = true;
          break;
        }
      }
      set.remove(target);
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

  public static void main(String[] args) {
    if (args.length != 1) {
      LOGGER.error("Incorrect number of arguments.");
      LOGGER.info("Usage: <exec> <config-file>");
      System.exit(1);
    }

    try {
      QueryComparisonConfig config = new QueryComparisonConfig(new File(args[0]));
      SSLQueryComparison sslQueryComparison = new SSLQueryComparison(config);
      sslQueryComparison.run();
    } catch (Exception e) {
      LOGGER.error("Exception caught, aborting query comparison: ", e);
    }

    System.exit(0);
  }
}
