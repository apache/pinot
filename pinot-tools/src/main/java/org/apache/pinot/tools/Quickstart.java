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
package org.apache.pinot.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class Quickstart extends QuickStartBase {
  private static final String TAB = "\t\t";
  private static final String NEW_LINE = "\n";


  public enum Color {
    RESET("\u001B[0m"), GREEN("\u001B[32m"), YELLOW("\u001B[33m"), CYAN("\u001B[36m");

    private String _code;

    Color(String code) {
      _code = code;
    }
  }

  public String getBootstrapDataDir() {
    return "examples/batch/baseballStats";
  }

  public int getNumMinions() {
    return 0;
  }

  public String getAuthToken() {
    return null;
  }

  public Map<String, Object> getConfigOverrides() {
    return null;
  }

  /**
   * Set query options which will be appended to end of the query e.g. option(timeoutMs=60000)
   */
  public String getQueryOptions() {
    return null;
  }

  public static void printStatus(Color color, String message) {
    System.out.println(color._code + message + Color.RESET._code);
  }

  public static String prettyPrintResponse(JsonNode response) {
    StringBuilder responseBuilder = new StringBuilder();

    // Sql Results
    if (response.has("resultTable")) {
      JsonNode columns = response.get("resultTable").get("dataSchema").get("columnNames");
      int numColumns = columns.size();
      for (int i = 0; i < numColumns; i++) {
        responseBuilder.append(columns.get(i).asText()).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      JsonNode rows = response.get("resultTable").get("rows");
      for (int i = 0; i < rows.size(); i++) {
        JsonNode row = rows.get(i);
        for (int j = 0; j < numColumns; j++) {
          responseBuilder.append(row.get(j).asText()).append(TAB);
        }
        responseBuilder.append(NEW_LINE);
      }
      return responseBuilder.toString();
    }

    // Selection query
    if (response.has("selectionResults")) {
      JsonNode columns = response.get("selectionResults").get("columns");
      int numColumns = columns.size();
      for (int i = 0; i < numColumns; i++) {
        responseBuilder.append(columns.get(i).asText()).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      JsonNode rows = response.get("selectionResults").get("results");
      int numRows = rows.size();
      for (int i = 0; i < numRows; i++) {
        JsonNode row = rows.get(i);
        for (int j = 0; j < numColumns; j++) {
          responseBuilder.append(row.get(j).asText()).append(TAB);
        }
        responseBuilder.append(NEW_LINE);
      }
      return responseBuilder.toString();
    }

    // Aggregation only query
    if (!response.get("aggregationResults").get(0).has("groupByResult")) {
      JsonNode aggregationResults = response.get("aggregationResults");
      int numAggregations = aggregationResults.size();
      for (int i = 0; i < numAggregations; i++) {
        responseBuilder.append(aggregationResults.get(i).get("function").asText()).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      for (int i = 0; i < numAggregations; i++) {
        responseBuilder.append(aggregationResults.get(i).get("value").asText()).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      return responseBuilder.toString();
    }

    // Aggregation group-by query
    JsonNode groupByResults = response.get("aggregationResults");
    int numGroupBys = groupByResults.size();
    for (int i = 0; i < numGroupBys; i++) {
      JsonNode groupByResult = groupByResults.get(i);
      responseBuilder.append(groupByResult.get("function").asText()).append(TAB);
      JsonNode columns = groupByResult.get("groupByColumns");
      int numColumns = columns.size();
      for (int j = 0; j < numColumns; j++) {
        responseBuilder.append(columns.get(j).asText()).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      JsonNode rows = groupByResult.get("groupByResult");
      int numRows = rows.size();
      for (int j = 0; j < numRows; j++) {
        JsonNode row = rows.get(j);
        responseBuilder.append(row.get("value").asText()).append(TAB);
        JsonNode columnValues = row.get("group");
        for (int k = 0; k < numColumns; k++) {
          responseBuilder.append(columnValues.get(k).asText()).append(TAB);
        }
        responseBuilder.append(NEW_LINE);
      }
    }
    return responseBuilder.toString();
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(_tmpDir, String.valueOf(System.currentTimeMillis()));
    File baseDir = new File(quickstartTmpDir, "baseballStats");
    File dataDir = new File(baseDir, "rawdata");
    Preconditions.checkState(dataDir.mkdirs());

    File schemaFile = new File(baseDir, "baseballStats_schema.json");
    File tableConfigFile = new File(baseDir, "baseballStats_offline_table_config.json");
    File ingestionJobSpecFile = new File(baseDir, "ingestionJobSpec.yaml");
    File dataFile = new File(dataDir, "baseballStats_data.csv");

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource(getBootstrapDataDir() + "/baseballStats_schema.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource = classLoader.getResource(getBootstrapDataDir() + "/rawdata/baseballStats_data.csv");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, dataFile);
    resource = classLoader.getResource(getBootstrapDataDir() + "/ingestionJobSpec.yaml");
    if (resource != null) {
      FileUtils.copyURLToFile(resource, ingestionJobSpecFile);
    }
    resource = classLoader.getResource(getBootstrapDataDir() + "/baseballStats_offline_table_config.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    QuickstartTableRequest request = new QuickstartTableRequest(baseDir.getAbsolutePath());
    QuickstartRunner runner =
        new QuickstartRunner(Lists.newArrayList(request), 1, 1, 1, getNumMinions(), dataDir, true, getAuthToken(),
            getConfigOverrides(), getQueryOptions());

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, broker and server *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down offline quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    printStatus(Color.CYAN, "***** Bootstrap baseballStats table *****");
    runner.bootstrapTable();

    printStatus(Color.CYAN, "***** Waiting for 5 seconds for the server to fetch the assigned segment *****");
    Thread.sleep(5000);

    printStatus(Color.YELLOW, "***** Offline quickstart setup complete *****");

    String q1 = "select count(*) from baseballStats limit 1";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 = "select playerName, sum(runs) from baseballStats group by playerName order by sum(runs) desc limit 5";
    printStatus(Color.YELLOW, "Top 5 run scorers of all time ");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 =
        "select playerName, sum(runs) from baseballStats where yearID=2000 group by playerName order by sum(runs) desc limit 5";
    printStatus(Color.YELLOW, "Top 5 run scorers of the year 2000");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");

    String q4 =
        "select playerName, sum(runs) from baseballStats where yearID>=2000 group by playerName order by sum(runs) desc limit 10";
    printStatus(Color.YELLOW, "Top 10 run scorers after 2000");
    printStatus(Color.CYAN, "Query : " + q4);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Color.GREEN, "***************************************************");

    String q5 = "select playerName, runs, homeRuns from baseballStats order by yearID limit 10";
    printStatus(Color.YELLOW, "Print playerName,runs,homeRuns for 10 records from the table and order them by yearID");
    printStatus(Color.CYAN, "Query : " + q5);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q5)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "BATCH"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
