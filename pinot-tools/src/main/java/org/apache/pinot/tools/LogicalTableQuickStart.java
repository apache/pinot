package org.apache.pinot.tools;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.AddLogicalTableCommand;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class LogicalTableQuickStart extends QuickStartBase {
  @Override
  public List<String> types() {
    return Arrays.asList("LOGICAL", "LOGICAL-ROLLOUT");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "LOGICAL"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  private void createLogicalTable(String resourcePath) throws Exception {
    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource(resourcePath);
    assert resource != null;
    File logicalTableConfigFile = new File(resource.getPath());
    AddLogicalTableCommand addLogicalTableCommand = new AddLogicalTableCommand();
    addLogicalTableCommand.setLogicalTableConfigFile(logicalTableConfigFile.getAbsolutePath())
        .setControllerHost("localhost")
        .setControllerPort(String.valueOf(9000))
        .setExecute(true);
    if (!addLogicalTableCommand.execute()) {
      throw new RuntimeException("Failed to create logical table with config file - " + logicalTableConfigFile);
    }
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    List<String> tableNames = List.of("airlineStats", "v2.airlineStats", "v3.airlineStats");
    String q1 = "select count(*) from %s limit 1";
    printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table");
    for (String tableName : tableNames) {
      String query = String.format(q1, tableName);
      printStatus(Quickstart.Color.CYAN, "Query : " + query);
      printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(query)));
      printStatus(Quickstart.Color.GREEN, "----");
    }

    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q2 =
        "select AirlineID, sum(Cancelled) from %s group by AirlineID order by sum(Cancelled) desc limit 5";
    printStatus(Quickstart.Color.YELLOW, "Top 5 airlines in cancellation ");
    for (String tableName : tableNames) {
      String query = String.format(q2, tableName);
      printStatus(Quickstart.Color.CYAN, "Query : " + query);
      printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(query)));
      printStatus(Quickstart.Color.GREEN, "----");
    }
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q3 =
        "select AirlineID, Year, sum(Flights) from %s where Year > 2010 group by AirlineID, Year order by "
            + "sum(Flights) desc limit 5";
    printStatus(Quickstart.Color.YELLOW, "Top 5 airlines in number of flights after 2010");
    for (String tableName : tableNames) {
      String query = String.format(q3, tableName);
      printStatus(Quickstart.Color.CYAN, "Query : " + query);
      printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(query)));
      printStatus(Quickstart.Color.GREEN, "----");
    }
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q4 =
        "select OriginCityName, max(Flights) from %s group by OriginCityName order by max(Flights) desc "
            + "limit 5";
    printStatus(Quickstart.Color.YELLOW, "Top 5 cities for number of flights");
    for (String tableName : tableNames) {
      String query = String.format(q4, tableName);
      printStatus(Quickstart.Color.CYAN, "Query : " + query);
      printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(query)));
      printStatus(Quickstart.Color.GREEN, "----");
    }
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q5 = "select AirlineID, OriginCityName, DestCityName, Year from %s order by Year limit 5";
    printStatus(Quickstart.Color.YELLOW, "Print AirlineID, OriginCityName, DestCityName, Year for 5 records ordered by Year");
    for (String tableName : tableNames) {
      String query = String.format(q5, tableName);
      printStatus(Quickstart.Color.CYAN, "Query : " + query);
      printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(query)));
      printStatus(Quickstart.Color.GREEN, "----");
    }
    printStatus(Quickstart.Color.GREEN, "***************************************************");
  }

  @Override
  protected List<QuickstartTableRequest> bootstrapOfflineTableDirectories(File quickstartTmpDir)
      throws IOException {
    String tableName = "airlineStats_0525";
    String directory = "examples/batch/airlineStats_0525";

    File baseDir = new File(quickstartTmpDir, tableName);
    File dataDir = new File(baseDir, "rawdata");
    Preconditions.checkState(dataDir.mkdirs());
    copyResourceTableToTmpDirectory(directory, tableName, baseDir, dataDir, false);
    return List.of(new QuickstartTableRequest(baseDir.getAbsolutePath(), getValidationTypesToSkip()));
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir =
        _setCustomDataDir ? _dataDir : new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File quickstartRunnerDir = new File(quickstartTmpDir, "quickstart");
    Preconditions.checkState(quickstartRunnerDir.mkdirs());
    List<QuickstartTableRequest> quickstartTableRequests = new ArrayList<>();
    quickstartTableRequests.addAll(bootstrapStreamTableDirectories(quickstartTmpDir));
    quickstartTableRequests.addAll(bootstrapOfflineTableDirectories(quickstartTmpDir));

    final QuickstartRunner runner =
        new QuickstartRunner(quickstartTableRequests, 1, 1, 1, 1, quickstartRunnerDir, getConfigOverrides());

    startKafka();
    startAllDataStreams(_kafkaStarter, quickstartTmpDir);

    printStatus(Quickstart.Color.CYAN, "***** Starting Zookeeper, controller, broker, server and minion *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Quickstart.Color.GREEN, "***** Shutting down realtime quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    printStatus(Quickstart.Color.CYAN, "***** Bootstrap all tables *****");
    runner.bootstrapTable();

    printStatus(Quickstart.Color.CYAN, "***** Waiting for 5 seconds for a few events to get populated *****");
    Thread.sleep(5000);

    printStatus(Quickstart.Color.CYAN, "***** Creating logical tables *****");
    createLogicalTable("examples/logical/v2_airlineStats_default_airlineStats.json");
    createLogicalTable("examples/logical/v3_airlineStats_realtime_offline.json");

    printStatus(Quickstart.Color.YELLOW, "***** Realtime quickstart setup complete *****");

    runSampleQueries(runner);

    printStatus(Quickstart.Color.GREEN,
        String.format("You can always go to http://localhost:%d to play around in the query console",
            QuickstartRunner.DEFAULT_CONTROLLER_PORT));
  }
}
