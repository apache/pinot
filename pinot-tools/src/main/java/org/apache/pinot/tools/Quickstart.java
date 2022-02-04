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
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class Quickstart extends QuickStartBase {
  @Override
  public List<String> types() {
    return Arrays.asList("OFFLINE", "BATCH");
  }

  private static final String TAB = "\t\t";
  private static final String NEW_LINE = "\n";
  private static final String DEFAULT_BOOTSTRAP_DIRECTORY = "examples/batch/baseballStats";

  public enum Color {
    RESET("\u001B[0m"), GREEN("\u001B[32m"), YELLOW("\u001B[33m"), CYAN("\u001B[36m");

    private final String _code;

    public String getCode() {
      return _code;
    }

    Color(String code) {
      _code = code;
    }
  }

  public int getNumMinions() {
    return 0;
  }

  public String getAuthToken() {
    return null;
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
    }
    return responseBuilder.toString();
  }

  public void execute()
      throws Exception {
    String tableName = getTableName(DEFAULT_BOOTSTRAP_DIRECTORY);
    File quickstartTmpDir = new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File baseDir = new File(quickstartTmpDir, tableName);
    File dataDir = new File(baseDir, "rawdata");
    Preconditions.checkState(dataDir.mkdirs());

    if (useDefaultBootstrapTableDir()) {
      copyResourceTableToTmpDirectory(getBootstrapDataDir(DEFAULT_BOOTSTRAP_DIRECTORY), tableName, baseDir, dataDir);
    } else {
      copyFilesystemTableToTmpDirectory(getBootstrapDataDir(DEFAULT_BOOTSTRAP_DIRECTORY), tableName, baseDir);
    }

    QuickstartTableRequest request = new QuickstartTableRequest(baseDir.getAbsolutePath());
    QuickstartRunner runner =
        new QuickstartRunner(Lists.newArrayList(request), 1, 1, 1,
            getNumMinions(), dataDir, true, getAuthToken(),
            getConfigOverrides(), null, true);

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
    printStatus(Color.CYAN, "***** Bootstrap " + tableName + " table *****");
    runner.bootstrapTable();

    waitForBootstrapToComplete(runner);

    printStatus(Color.YELLOW, "***** Offline quickstart setup complete *****");

    if (useDefaultBootstrapTableDir()) {
      // Quickstart is using the default baseballStats sample table, so run sample queries.
      runSampleQueries(runner);
    }

    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }

  private static void copyResourceTableToTmpDirectory(String sourcePath, String tableName, File baseDir, File dataDir)
      throws IOException {

    File schemaFile = new File(baseDir, tableName + "_schema.json");
    File tableConfigFile = new File(baseDir, tableName + "_offline_table_config.json");
    File ingestionJobSpecFile = new File(baseDir, "ingestionJobSpec.yaml");
    File dataFile = new File(dataDir, tableName + "_data.csv");

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource(sourcePath + File.separator + tableName + "_schema.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource =
        classLoader.getResource(sourcePath + File.separator + "rawdata" + File.separator + tableName + "_data.csv");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, dataFile);
    resource = classLoader.getResource(sourcePath + File.separator + "ingestionJobSpec.yaml");
    if (resource != null) {
      FileUtils.copyURLToFile(resource, ingestionJobSpecFile);
    }
    resource = classLoader.getResource(sourcePath + File.separator + tableName + "_offline_table_config.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);
  }

  private static void copyFilesystemTableToTmpDirectory(String sourcePath, String tableName, File baseDir)
      throws IOException {
    File fileDb = new File(sourcePath);

    if (!fileDb.exists() || !fileDb.isDirectory()) {
      throw new RuntimeException("Directory " + fileDb.getAbsolutePath() + " not found.");
    }

    File schemaFile = new File(fileDb, tableName + "_schema.json");
    if (!schemaFile.exists()) {
      throw new RuntimeException("Schema file " + schemaFile.getAbsolutePath() + " not found.");
    }

    File tableFile = new File(fileDb, tableName + "_offline_table_config.json");
    if (!tableFile.exists()) {
      throw new RuntimeException("Table table " + tableFile.getAbsolutePath() + " not found.");
    }

    File data = new File(fileDb, "rawdata" + File.separator + tableName + "_data.csv");
    if (!data.exists()) {
      throw new RuntimeException(("Data file " + data.getAbsolutePath() + " not found. "));
    }

    FileUtils.copyDirectory(fileDb, baseDir);
  }

  private static void runSampleQueries(QuickstartRunner runner)
      throws Exception {
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
        "select playerName, sum(runs) from baseballStats where yearID=2000 group by playerName order by sum(runs) "
            + "desc limit 5";
    printStatus(Color.YELLOW, "Top 5 run scorers of the year 2000");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");

    String q4 =
        "select playerName, sum(runs) from baseballStats where yearID>=2000 group by playerName order by sum(runs) "
            + "desc limit 10";
    printStatus(Color.YELLOW, "Top 10 run scorers after 2000");
    printStatus(Color.CYAN, "Query : " + q4);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Color.GREEN, "***************************************************");

    String q5 = "select playerName, runs, homeRuns from baseballStats order by yearID limit 10";
    printStatus(Color.YELLOW, "Print playerName,runs,homeRuns for 10 records from the table and order them by yearID");
    printStatus(Color.CYAN, "Query : " + q5);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q5)));
    printStatus(Color.GREEN, "***************************************************");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "BATCH"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
