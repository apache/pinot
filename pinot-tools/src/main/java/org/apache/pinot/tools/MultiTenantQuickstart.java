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
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class MultiTenantQuickstart extends QuickStartBase {
  @Override
  public List<String> types() {
    return Arrays.asList("MULTI-TENANT");
  }

  private static final String TAB = "\t\t";
  private static final String NEW_LINE = "\n";
  private static final String[] DEFAULT_BOOTSTRAP_DIRECTORY =
      new String[]{"examples/multi-tenant/batch/baseballStats", "examples/multi-tenant/batch/airlineStats"};

  public AuthProvider getAuthProvider() {
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
    List<QuickstartTableRequest> tableRequests = new ArrayList<>();
    File quickstartTmpDir = new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    for (String bootstrapDirectory : DEFAULT_BOOTSTRAP_DIRECTORY) {
      String tableName = getTableName(bootstrapDirectory);
      File baseDir = new File(quickstartTmpDir, tableName);

      if (useDefaultBootstrapTableDir()) {
        copyResourceTableToTmpDirectory(getBootstrapDataDir(bootstrapDirectory), tableName, baseDir);
      } else {
        copyFilesystemTableToTmpDirectory(getBootstrapDataDir(bootstrapDirectory), tableName, baseDir);
      }
      tableRequests.add(new QuickstartTableRequest(baseDir.getAbsolutePath()));
    }
    File pinotDeploymentDir = new File(quickstartTmpDir, "pinotDeployment");
    pinotDeploymentDir.mkdirs();
    QuickstartRunner runner =
        new QuickstartRunner(tableRequests, 1, 2, 2, 2, pinotDeploymentDir, false,
            getAuthProvider(), getConfigOverrides(), null, true);

    printStatus(Quickstart.Color.CYAN, "***** Starting Zookeeper, controller, broker and server *****");
    runner.startAll();
    waitForBootstrapToComplete(runner);

    runner.createBrokerTenantWith(1, "brokerOne");
    runner.createBrokerTenantWith(1, "brokerTwo");
    runner.createServerTenantWith(1, 0, "serverOne");
    runner.createServerTenantWith(1, 0, "serverTwo");
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Quickstart.Color.GREEN, "***** Shutting down offline quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    printStatus(Quickstart.Color.CYAN, "***** Bootstrap tables *****");
    runner.bootstrapTable();

    waitForBootstrapToComplete(runner);

    printStatus(Quickstart.Color.YELLOW, "***** Offline quickstart setup complete *****");

    if (useDefaultBootstrapTableDir()) {
      // Quickstart is using the default baseballStats sample table, so run sample queries.
      runSampleQueries(runner);
    }

    printStatus(Quickstart.Color.GREEN,
        "You can always go to http://localhost:9000 to play around in the query console");
  }

  private static void copyResourceTableToTmpDirectory(String sourcePath, String tableName, File baseDir)
      throws IOException {

    File schemaFile = new File(baseDir, tableName + "_schema.json");
    File tableConfigFile = new File(baseDir, tableName + "_offline_table_config.json");

    ClassLoader classLoader = MultiTenantQuickstart.class.getClassLoader();
    URL resource = classLoader.getResource(sourcePath + File.separator + tableName + "_schema.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource = classLoader.getResource(sourcePath + File.separator + tableName + "_offline_table_config.json");
    Preconditions.checkNotNull(resource);
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
    printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table");
    printStatus(Quickstart.Color.CYAN, "Query : " + q1);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q2 = "select playerName, sum(runs) from baseballStats group by playerName order by sum(runs) desc limit 5";
    printStatus(Quickstart.Color.YELLOW, "Top 5 run scorers of all time ");
    printStatus(Quickstart.Color.CYAN, "Query : " + q2);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q3 =
        "select playerName, sum(runs) from baseballStats where yearID=2000 group by playerName order by sum(runs) "
            + "desc limit 5";
    printStatus(Quickstart.Color.YELLOW, "Top 5 run scorers of the year 2000");
    printStatus(Quickstart.Color.CYAN, "Query : " + q3);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q4 =
        "select playerName, sum(runs) from baseballStats where yearID>=2000 group by playerName order by sum(runs) "
            + "desc limit 10";
    printStatus(Quickstart.Color.YELLOW, "Top 10 run scorers after 2000");
    printStatus(Quickstart.Color.CYAN, "Query : " + q4);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q5 = "select playerName, runs, homeRuns from baseballStats order by yearID limit 10";
    printStatus(Quickstart.Color.YELLOW,
        "Print playerName,runs,homeRuns for 10 records from the table and order them by yearID");
    printStatus(Quickstart.Color.CYAN, "Query : " + q5);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q5)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q6 = "select count(*) from airlineStats limit 1";
    printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table");
    printStatus(Quickstart.Color.CYAN, "Query : " + q6);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q6)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q7 =
        "select AirlineID, sum(Cancelled) from airlineStats group by AirlineID order by sum(Cancelled) desc limit 5";
    printStatus(Quickstart.Color.YELLOW, "Top 5 airlines in cancellation ");
    printStatus(Quickstart.Color.CYAN, "Query : " + q7);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q7)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q8 =
        "select AirlineID, Year, sum(Flights) from airlineStats where Year > 2010 group by AirlineID, Year order by "
            + "sum(Flights) desc limit 5";
    printStatus(Quickstart.Color.YELLOW, "Top 5 airlines in number of flights after 2010");
    printStatus(Quickstart.Color.CYAN, "Query : " + q8);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q8)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q9 =
        "select OriginCityName, max(Flights) from airlineStats group by OriginCityName order by max(Flights) desc "
            + "limit 5";
    printStatus(Quickstart.Color.YELLOW, "Top 5 cities for number of flights");
    printStatus(Quickstart.Color.CYAN, "Query : " + q9);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q9)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q10 = "select AirlineID, OriginCityName, DestCityName, Year from airlineStats order by Year limit 5";
    printStatus(Quickstart.Color.YELLOW,
        "Print AirlineID, OriginCityName, DestCityName, Year for 5 records ordered by Year");
    printStatus(Quickstart.Color.CYAN, "Query : " + q10);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q10)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");
  }

  protected Map<String, Object> getConfigOverrides() {
    Map<String, Object> configOverrides = new HashMap<>(super.getConfigOverrides());
    configOverrides.put(CommonConstants.Broker.CONFIG_OF_ROUTE_REQUESTS_TO_OTHER_TENANTS, true);
    return configOverrides;
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "MULTI-TENANT"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
