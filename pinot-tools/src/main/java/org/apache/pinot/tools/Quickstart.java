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

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.utils.SampleQueries;


/// The basic Batch/Offline Quickstart.
///
/// This quickstart bootstraps every batch example table and demonstrates the batch-side features that used to have
/// a dedicated quickstart each: the multi-stage engine, JSON indexes, complex type handling, timestamp indexes,
/// lookup joins and vector similarity. The types those quickstarts used to own are kept as deprecated aliases.
public class Quickstart extends QuickStartBase {
  protected static final Map<String, String> OPTIONS_TO_USE_MSE = Map.of("queryOptions",
      CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE + "=true");

  private static final List<String> DEPRECATED_TYPES =
      List.of("MULTI_STAGE", "JOIN", "TIMESTAMP", "OFFLINE_JSON_INDEX", "OFFLINE-JSON-INDEX", "BATCH_JSON_INDEX",
          "BATCH-JSON-INDEX", "OFFLINE_COMPLEX_TYPE", "OFFLINE-COMPLEX-TYPE", "BATCH_COMPLEX_TYPE",
          "BATCH-COMPLEX-TYPE");

  @Override
  public List<String> types() {
    List<String> types = new ArrayList<>(List.of("BATCH", "OFFLINE"));
    types.addAll(DEPRECATED_TYPES);
    return types;
  }

  @Override
  public List<String> deprecatedTypes() {
    return DEPRECATED_TYPES;
  }

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

  public AuthProvider getAuthProvider() {
    return null;
  }

  @Override
  protected Map<String, Object> getConfigOverrides() {
    Map<String, Object> configOverrides = new HashMap<>(super.getConfigOverrides());
    configOverrides.put(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    return configOverrides;
  }

  @Override
  protected Map<String, String> getClusterConfigOverrides() {
    Map<String, String> clusterConfigOverrides = new HashMap<>(super.getClusterConfigOverrides());
    clusterConfigOverrides.put(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS, "50");
    return clusterConfigOverrides;
  }

  protected Map<String, Object> getIndividualBrokerConfigOverridesFunction(int brokerId) {
    return Map.of();
  }

  protected Map<String, Object> getIndividualServerConfigOverridesFunction(int serverId) {
    return Map.of();
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir =
        _setCustomDataDir ? _dataDir : new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File quickstartRunnerDir = new File(quickstartTmpDir, "quickstart");
    Preconditions.checkState(quickstartRunnerDir.mkdirs());
    List<QuickstartTableRequest> quickstartTableRequests = bootstrapOfflineTableDirectories(quickstartTmpDir);

    QuickstartRunner runner =
        new QuickstartRunner(quickstartTableRequests, 1, 1, getNumQuickstartRunnerServers(), 1, quickstartRunnerDir,
            true, getAuthProvider(),
            getConfigOverrides(), _zkExternalAddress, true, getClusterConfigOverrides(),
            this::getIndividualBrokerConfigOverridesFunction, this::getIndividualServerConfigOverridesFunction);

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, broker, server and minion *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    if (!CollectionUtils.isEmpty(quickstartTableRequests)) {
      printStatus(Color.CYAN, "***** Bootstrap tables *****");
      runner.bootstrapTable();
      waitForBootstrapToComplete(runner);
    }

    printStatus(Color.YELLOW, "***** Quick start setup complete *****");

    if (useDefaultBootstrapTableDir() && !CollectionUtils.isEmpty(quickstartTableRequests)) {
      // Quickstart is using the default baseballStats sample table, so run sample queries.
      runSampleQueries(runner);
    }

    printStatus(Color.GREEN,
        String.format("You can always go to http://localhost:%d to play around in the query console",
            QuickstartRunner.DEFAULT_CONTROLLER_PORT));
  }

  /// Three servers so that the multi-stage sample queries below actually exercise a cross-server exchange, which is
  /// what the merged MULTI_STAGE and JOIN quickstarts used to set up.
  ///
  /// This is the default for every quickstart extending this class, not just `-type BATCH`. Subclasses that do not
  /// need distributed execution can override it; [TPCHQuickStart] for instance runs on a single server.
  protected int getNumQuickstartRunnerServers() {
    return 3;
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    runBaseballQueries(runner);
    runMultiStageEngineQueries(runner);
    runStarSchemaBenchmarkQueries(runner);
    runLookupJoinQueries(runner);
    runJsonIndexQueries(runner);
    runComplexTypeQueries(runner);
    runTimestampIndexQueries(runner);
    if (hasTables(runner, "fineFoodReviews")) {
      runVectorQueryExamples(runner);
    }
  }

  private void runBaseballQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "baseballStats")) {
      return;
    }
    printStatus(Color.YELLOW, "***** Baseball stats *****");
    runAndPrintQuery(runner, "Total number of documents in the table", "select count(*) from baseballStats limit 1");
    runAndPrintQuery(runner, "Top 5 run scorers of all time ", SampleQueries.BASEBALL_SUM_RUNS_Q1);
    runAndPrintQuery(runner, "Top 5 run scorers of the year 2000", SampleQueries.BASEBALL_SUM_RUNS_Q2);
    runAndPrintQuery(runner, "Top 10 run scorers after 2000", SampleQueries.BASEBALL_SUM_RUNS_Q3);
    runAndPrintQuery(runner, "Print playerName,runs,homeRuns for 10 records from the table and order them by yearID",
        SampleQueries.BASEBALL_ORDER_BY_YEAR);
  }

  private void runMultiStageEngineQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "baseballStats", "dimBaseballTeams")) {
      return;
    }
    printStatus(Color.YELLOW, "***** Multi-stage engine *****");
    runAndPrintQuery(runner, "Total number of documents in the table", SampleQueries.COUNT_BASEBALL_STATS,
        OPTIONS_TO_USE_MSE);
    runAndPrintQuery(runner, "Correlate the same player(s) with more than 160-run some year(s) and with less than "
        + "2-run some other year(s)", SampleQueries.BASEBALL_STATS_SELF_JOIN, OPTIONS_TO_USE_MSE);
    runAndPrintQuery(runner, "Baseball Stats with joined team names", SampleQueries.BASEBALL_JOIN_DIM_BASEBALL_TEAMS,
        OPTIONS_TO_USE_MSE);
  }

  private void runStarSchemaBenchmarkQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "lineorder", "customer", "dates")) {
      return;
    }
    printStatus(Color.YELLOW, "***** Star schema benchmark, on the multi-stage engine *****");
    runAndPrintQuery(runner, "Total number of line orders", "select count(*) from lineorder",
        OPTIONS_TO_USE_MSE);
    runAndPrintQuery(runner, "Revenue by year for line orders with a small discount and quantity",
        "select d.D_YEAR, sum(lo.LO_REVENUE) as revenue from lineorder lo join dates d on lo.LO_ORDERDATE = "
            + "d.D_DATEKEY where lo.LO_DISCOUNT between 1 and 3 and lo.LO_QUANTITY < 25 group by d.D_YEAR "
            + "order by d.D_YEAR limit 10", OPTIONS_TO_USE_MSE);
    runAndPrintQuery(runner, "Top 10 customer nations by revenue",
        "select c.C_NATION, sum(lo.LO_REVENUE) as revenue from lineorder lo join customer c on lo.LO_CUSTKEY = "
            + "c.C_CUSTKEY group by c.C_NATION order by revenue desc limit 10", OPTIONS_TO_USE_MSE);
  }

  private void runLookupJoinQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "baseballStats", "dimBaseballTeams")) {
      return;
    }
    printStatus(Color.YELLOW, "***** Lookup join *****");
    runAndPrintQuery(runner, "Baseball Teams", "select count(*) from dimBaseballTeams limit 1");
    runAndPrintQuery(runner, "Baseball Stats with joined team names",
        "select playerName, teamID, lookup('dimBaseballTeams', 'teamName', 'teamID', teamID) from baseballStats "
            + "limit 10");
  }

  private void runJsonIndexQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "githubEvents")) {
      return;
    }
    printStatus(Color.YELLOW, "***** JSON index *****");
    runAndPrintQuery(runner, "Most contributed repos by 'LombiqBot'",
        "select json_extract_scalar(repo, '$.name', 'STRING'), count(*) from githubEvents where json_match(actor, "
            + "'\"$.login\"=''LombiqBot''') group by 1 order by 2 desc limit 10");
  }

  private void runComplexTypeQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "githubComplexTypeEvents")) {
      return;
    }
    printStatus(Color.YELLOW, "***** Complex type handling *****");
    runAndPrintQuery(runner, "Flattened commit authors",
        "select id, \"payload.commits.author.name\", \"payload.commits.author.email\" from githubComplexTypeEvents "
            + "limit 10");
  }

  private void runTimestampIndexQueries(QuickstartRunner runner)
      throws Exception {
    if (!hasTables(runner, "airlineStats")) {
      return;
    }
    printStatus(Color.YELLOW, "***** Timestamp index *****");
    runAndPrintQuery(runner, "Pick one row with timestamp and different granularity using generated column name ",
        "select ts, $ts$DAY, $ts$WEEK, $ts$MONTH from airlineStats limit 1");
    runAndPrintQuery(runner, "Pick one row with timestamp and different granularity using dateTrunc function",
        "select ts, dateTrunc('DAY', ts), dateTrunc('WEEK', ts), dateTrunc('MONTH', ts) from airlineStats limit 1");
    runAndPrintQuery(runner, "Count events in week basis ",
        "select count(*), toTimestamp(dateTrunc('WEEK', ts)) as tsWeek from airlineStats GROUP BY tsWeek limit 1");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "BATCH"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
