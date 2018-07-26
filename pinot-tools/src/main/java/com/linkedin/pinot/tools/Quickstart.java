/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.tools.admin.command.QuickstartRunner;
import java.io.File;
import java.net.URL;
import java.util.Enumeration;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Quickstart {
  private Quickstart() {
  }

  private static final String TAB = "\t\t";
  private static final String NEW_LINE = "\n";

  public enum Color {
    RESET("\u001B[0m"),
    GREEN("\u001B[32m"),
    YELLOW("\u001B[33m"),
    CYAN("\u001B[36m");

    private String _code;

    Color(String code) {
      _code = code;
    }
  }

  public static void printStatus(Color color, String message) {
    System.out.println(color._code + message + Color.RESET._code);
  }

  public static String prettyPrintResponse(JSONObject response)
      throws JSONException {
    StringBuilder responseBuilder = new StringBuilder();

    // Selection query
    if (response.has("selectionResults")) {
      JSONArray columns = response.getJSONObject("selectionResults").getJSONArray("columns");
      int numColumns = columns.length();
      for (int i = 0; i < numColumns; i++) {
        responseBuilder.append(columns.getString(i)).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      JSONArray rows = response.getJSONObject("selectionResults").getJSONArray("results");
      int numRows = rows.length();
      for (int i = 0; i < numRows; i++) {
        JSONArray row = rows.getJSONArray(i);
        for (int j = 0; j < numColumns; j++) {
          responseBuilder.append(row.getString(j)).append(TAB);
        }
        responseBuilder.append(NEW_LINE);
      }
      return responseBuilder.toString();
    }

    // Aggregation only query
    if (!response.getJSONArray("aggregationResults").getJSONObject(0).has("groupByResult")) {
      JSONArray aggregationResults = response.getJSONArray("aggregationResults");
      int numAggregations = aggregationResults.length();
      for (int i = 0; i < numAggregations; i++) {
        responseBuilder.append(aggregationResults.getJSONObject(i).getString("function")).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      for (int i = 0; i < numAggregations; i++) {
        responseBuilder.append(aggregationResults.getJSONObject(i).getString("value")).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      return responseBuilder.toString();
    }

    // Aggregation group-by query
    JSONArray groupByResults = response.getJSONArray("aggregationResults");
    int numGroupBys = groupByResults.length();
    for (int i = 0; i < numGroupBys; i++) {
      JSONObject groupByResult = groupByResults.getJSONObject(i);
      responseBuilder.append(groupByResult.getString("function")).append(TAB);
      JSONArray columns = groupByResult.getJSONArray("groupByColumns");
      int numColumns = columns.length();
      for (int j = 0; j < numColumns; j++) {
        responseBuilder.append(columns.getString(j)).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      JSONArray rows = groupByResult.getJSONArray("groupByResult");
      int numRows = rows.length();
      for (int j = 0; j < numRows; j++) {
        JSONObject row = rows.getJSONObject(j);
        responseBuilder.append(row.getString("value")).append(TAB);
        JSONArray columnValues = row.getJSONArray("group");
        for (int k = 0; k < numColumns; k++) {
          responseBuilder.append(columnValues.getString(k)).append(TAB);
        }
        responseBuilder.append(NEW_LINE);
      }
    }
    return responseBuilder.toString();
  }

  public static void logOnlyErrors() {
    Logger root = Logger.getRootLogger();
    root.setLevel(Level.ERROR);
    Enumeration allLoggers = root.getLoggerRepository().getCurrentCategories();
    while (allLoggers.hasMoreElements()) {
      Category tmpLogger = (Category) allLoggers.nextElement();
      tmpLogger.setLevel(Level.ERROR);
    }
  }

  public void execute()
      throws Exception {
    final File quickStartDataDir = new File("quickStartData" + System.currentTimeMillis());

    if (!quickStartDataDir.exists()) {
      Preconditions.checkState(quickStartDataDir.mkdirs());
    }

    File schemaFile = new File(quickStartDataDir, "baseballStats_schema.json");
    File dataFile = new File(quickStartDataDir, "baseballStats_data.csv");
    File tableConfigFile = new File(quickStartDataDir, "baseballStats_offline_table_config.json");

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource("sample_data/baseballStats_schema.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource = classLoader.getResource("sample_data/baseballStats_data.csv");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, dataFile);
    resource = classLoader.getResource("sample_data/baseballStats_offline_table_config.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    File tempDir = new File("/tmp", String.valueOf(System.currentTimeMillis()));
    Preconditions.checkState(tempDir.mkdirs());
    QuickstartTableRequest request =
        new QuickstartTableRequest("baseballStats", schemaFile, tableConfigFile, quickStartDataDir, FileFormat.CSV);
    final QuickstartRunner runner = new QuickstartRunner(Lists.newArrayList(request), 1, 1, 1, tempDir);

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, broker and server *****");
    runner.startAll();
    printStatus(Color.CYAN, "***** Adding baseballStats schema *****");
    runner.addSchema();
    printStatus(Color.CYAN, "***** Adding baseballStats table *****");
    runner.addTable();
    printStatus(Color.CYAN, "***** Building index segment for baseballStats *****");
    runner.buildSegment();
    printStatus(Color.CYAN, "***** Pushing segment to the controller *****");
    runner.pushSegment();
    printStatus(Color.CYAN, "***** Waiting for 5 seconds for the server to fetch the assigned segment *****");
    Thread.sleep(5000);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          printStatus(Color.GREEN, "***** Shutting down offline quick start *****");
          runner.stop();
          FileUtils.deleteDirectory(quickStartDataDir);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    printStatus(Color.YELLOW, "***** Offline quickstart setup complete *****");

    String q1 = "select count(*) from baseballStats limit 0";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 = "select sum('runs') from baseballStats group by playerName top 5 limit 0";
    printStatus(Color.YELLOW, "Top 5 run scorers of all time ");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 = "select sum('runs') from baseballStats where yearID=2000 group by playerName top 5 limit 0";
    printStatus(Color.YELLOW, "Top 5 run scorers of the year 2000");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");

    String q4 = "select sum('runs') from baseballStats where yearID>=2000 group by playerName limit 0";
    printStatus(Color.YELLOW, "Top 10 run scorers after 2000");
    printStatus(Color.CYAN, "Query : " + q4);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Color.GREEN, "***************************************************");

    String q5 = "select playerName,runs,homeRuns from baseballStats order by yearID limit 10";
    printStatus(Color.YELLOW, "Print playerName,runs,homeRuns for 10 records from the table and order them by yearID");
    printStatus(Color.CYAN, "Query : " + q5);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q5)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "You can always go to http://localhost:9000/query/ to play around in the query console");
  }

  public static void main(String[] args)
      throws Exception {
    logOnlyErrors();
    new Quickstart().execute();
  }
}
