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
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


/**
 * The basic Batch/Offline Quickstart.
 */
public class Quickstart extends QuickStartBase {
  @Override
  public List<String> types() {
    return Arrays.asList("OFFLINE", "BATCH");
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

  public void execute()
      throws Exception {
    File quickstartTmpDir =
        _setCustomDataDir ? _dataDir : new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File quickstartRunnerDir = new File(quickstartTmpDir, "quickstart");
    Preconditions.checkState(quickstartRunnerDir.mkdirs());
    List<QuickstartTableRequest> quickstartTableRequests = bootstrapOfflineTableDirectories(quickstartTmpDir);

    QuickstartRunner runner =
        new QuickstartRunner(quickstartTableRequests, 1, 1, getNumQuickstartRunnerServers(), 1, quickstartRunnerDir,
            true, getAuthProvider(), getConfigOverrides(), _zkExternalAddress, true);

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, broker, server and minion *****");
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

    if (!CollectionUtils.isEmpty(quickstartTableRequests)) {
      printStatus(Color.CYAN, "***** Bootstrap tables *****");
      runner.bootstrapTable();
      waitForBootstrapToComplete(runner);
    }

    printStatus(Color.YELLOW, "***** Offline quickstart setup complete *****");

    if (useDefaultBootstrapTableDir() && !CollectionUtils.isEmpty(quickstartTableRequests)) {
      // Quickstart is using the default baseballStats sample table, so run sample queries.
      runSampleQueries(runner);
    }

    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }

  protected int getNumQuickstartRunnerServers() {
    return 1;
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
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

    runVectorQueryExamples(runner);
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "BATCH"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
