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

package com.linkedin.pinot.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarTreeBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(StarTreeBenchmark.class);

  @Option(name = "-table", required = false, usage = "pinot table name")
  String tableName;

  @Option(name = "-dimensions", required = true, usage = "dimension name(s) in the table", handler = StringArrayOptionHandler.class)
  List<String> dimensions;

  @Option(name = "-metrics", required = true, usage = "metric name(s) in the table", handler = StringArrayOptionHandler.class)
  List<String> metrics;

  @Option(name = "-timeColumn", required = false, usage = "time column name in the table")
  String timeColumn;

  @Option(name = "-brokerHostPort", required = false, usage = "brokerHostPort in the format host:port")
  private String brokerHostPort;

  private static final int MAX_QUERIES = 1000;
  /**
   * MIN number of records that should match the query 
   */
  private static final int MIN_THRESHOLD_COUNT = 10000;

  /**
   * Threshold at which it will stop drilling down further
   */
  private static final int MIN_THRESHOLD_TO_DRILL_DOWN_FURTHER = 100000;

  private StarTreeBenchmark() {

  }

  public StarTreeBenchmark(List<String> dimensions, List<String> metrics, String timeColumn, String tableName, String brokerHostPort) {
    super();
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.timeColumn = timeColumn;
    this.tableName = tableName;
    this.brokerHostPort = brokerHostPort;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<String> dimensions) {
    this.dimensions = dimensions;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<String> metrics) {
    this.metrics = metrics;
  }

  public String getTimeColumn() {
    return timeColumn;
  }

  public void setTimeColumn(String timeColumn) {
    this.timeColumn = timeColumn;
  }

  public String getBrokerHostPort() {
    return brokerHostPort;
  }

  public void setBrokerHostPort(String brokerHostPort) {
    this.brokerHostPort = brokerHostPort;
  }

  public void run() throws Exception {
    Random random = new Random();
    Queue<Map<String, String>> queue = new LinkedList<>();
    queue.add(new HashMap<String, String>());
    List<Map<String, String>> validCombinations = new ArrayList<>();
    while (!queue.isEmpty() && validCombinations.size() < MAX_QUERIES) {
      Map<String, String> parentMap = queue.poll();
      for (String dimension : dimensions) {
        if (parentMap.containsKey(dimension)) {
          continue;
        }
        StringBuilder sql = new StringBuilder("Select sum(__COUNT) from " + tableName);
        String delim = " WHERE ";
        for (Entry<String, String> entry : parentMap.entrySet()) {
          sql.append(delim).append(entry.getKey()).append("=").append("'").append(entry.getValue()).append("'");
          delim = " AND ";
        }
        sql.append(" group by " + dimension + " top 100000");
        // LOG.info(sql);
        JSONObject response = postQuery(sql.toString(), true);
        // LOG.info(response);
        JSONArray array = response.getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult");
        for (int i = 0; i < array.length(); i++) {
          JSONObject jsonObject = array.getJSONObject(i);
          String dimValue = jsonObject.getJSONArray("group").getString(0);
          double value = jsonObject.getDouble("value");

          if (value > MIN_THRESHOLD_COUNT) {

            Map<String, String> childMap = new HashMap<>(parentMap);
            childMap.put(dimension, dimValue);
            validCombinations.add(childMap);
            LOG.info("Adding " + childMap + ":" + value);
            // don't drill down further if the parent count is not big enough
            if (value > MIN_THRESHOLD_TO_DRILL_DOWN_FURTHER) {
              queue.add(childMap);
            }
          }
        }
      }
      LOG.info("Valid Combinations:" + validCombinations.size() + " queue size:" + queue.size());
    }
    Set<String> queries = new HashSet<>();
    Set<String> whereClauseSet = new HashSet<>();

    for (Map<String, String> whereClauseMap : validCombinations) {
      // GENERATE where clause
      StringBuilder whereClause = new StringBuilder(" WHERE ");
      String delim = "";
      for (String dimension : whereClauseMap.keySet()) {
        String dimValue = whereClauseMap.get(dimension);
        whereClause.append(delim).append(dimension).append("=").append("'").append(dimValue).append("'");
        delim = " AND ";
      }
      // Aggregation function
      int numMetricsToSelect = random.nextInt(metrics.size()) + 1;
      Set<String> selectedMetrics = new HashSet<>();
      while (selectedMetrics.size() < numMetricsToSelect) {
        String metric = metrics.get(random.nextInt(metrics.size()));
        selectedMetrics.add(metric);
      }
      StringBuilder selection = new StringBuilder();
      delim = "";
      for (String metric : selectedMetrics) {
        selection.append(delim).append("SUM(").append(metric).append(")");
        delim = ", ";
      }

      // Generate the sql
      StringBuilder sqlBuilder = new StringBuilder("SELECT ");
      sqlBuilder.append(selection).append(" FROM ").append(tableName).append(whereClause);

      // LOG.info(queryId + ": " + sqlBuilder);
      queries.add(sqlBuilder.toString());
      whereClauseSet.add(whereClause.toString());
    }
    DescriptiveStatistics starTreeTimeStats = new DescriptiveStatistics();
    DescriptiveStatistics rawPinotTimeStats = new DescriptiveStatistics();
    DescriptiveStatistics starTreeDocScannedStats = new DescriptiveStatistics();
    DescriptiveStatistics rawPinotDocScannedStats = new DescriptiveStatistics();

    try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File("/tmp/queries." + System.currentTimeMillis())))) {
      for (String query : queries) {
        // QUERY
        String queryString = "query:" + query;
        LOG.info(queryString);
        bufferedWriter.append(queryString);

        // STAR TREE
        JSONObject starTree = postQuery(query, true);
        int starTreeDocsScanned = starTree.getInt("numDocsScanned");
        int starTreeTime = starTree.getInt("timeUsedMs");
        String starTreeOutput = String.format("starTreeOutput: time=%s docsScanned=%s", starTreeTime, starTreeDocsScanned);
        LOG.info(starTreeOutput);
        bufferedWriter.append(starTreeOutput);
        bufferedWriter.append('\n');

        // RAW PINOT
        JSONObject rawPinot = postQuery(query, false);
        int rawPinotDocsScanned = rawPinot.getInt("numDocsScanned");
        int rawPinotTime = rawPinot.getInt("timeUsedMs");
        String rawPinotOutput = String.format("rawPinotOutput: time=%s, docsScanned=%s", rawPinotTime, rawPinotDocsScanned);
        LOG.info(rawPinotOutput);
        LOG.info("*********************");
        bufferedWriter.append(rawPinotOutput);
        bufferedWriter.append('\n');
        bufferedWriter.append('\n');
        bufferedWriter.flush();

        // update stats
        starTreeTimeStats.addValue(starTreeTime);
        starTreeDocScannedStats.addValue(starTreeDocsScanned);

        rawPinotTimeStats.addValue(rawPinotTime);
        rawPinotDocScannedStats.addValue(rawPinotDocsScanned);
      }
      LOG.info("starTreeTimeStats: " + starTreeTimeStats);
      LOG.info("rawPinotTimeStats: " + rawPinotTimeStats);

      LOG.info("starTreeDocScannedStats: " + starTreeDocScannedStats);
      LOG.info("rawPinotDocScannedStats: " + rawPinotDocScannedStats);
    }
  }

  public JSONObject postQuery(String query, boolean useStarTree) throws Exception {
    final JSONObject json = new JSONObject();
    json.put("pql", query);
    json.put("debugOptions", "useStarTree=" + useStarTree);
    final long start = System.currentTimeMillis();
    final URLConnection conn = new URL(String.format("http://%s", brokerHostPort) + "/query").openConnection();
    conn.setDoOutput(true);
    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));
    final String reqStr = json.toString();

    writer.write(reqStr, 0, reqStr.length());
    writer.flush();
    final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

    final StringBuilder sb = new StringBuilder();
    String line = null;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    final long stop = System.currentTimeMillis();

    final String res = sb.toString();
    final JSONObject ret = new JSONObject(res);
    ret.put("totalTime", (stop - start));
    return ret;
  }

  /**
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    StarTreeBenchmark benchmark = new StarTreeBenchmark();
    CmdLineParser parser = new CmdLineParser(benchmark);
    parser.parseArgument(args);
    benchmark.run();
  }
}
