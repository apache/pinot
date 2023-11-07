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
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class HybridQuickstart extends Quickstart {
  @Override
  public List<String> types() {
    return Collections.singletonList("HYBRID");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "HYBRID"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  public Map<String, Object> getConfigOverrides() {
    Map<String, Object> overrides = new HashMap<>(super.getConfigOverrides());
    overrides.put("pinot.server.grpc.enable", "true");
    overrides.put("pinot.server.grpc.port", "8090");
    return overrides;
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    String q1 = "select count(*) from airlineStats limit 1";
    printStatus(Color.YELLOW, "Total number of documents in the table");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 =
        "select AirlineID, sum(Cancelled) from airlineStats group by AirlineID order by sum(Cancelled) desc limit 5";
    printStatus(Color.YELLOW, "Top 5 airlines in cancellation ");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 =
        "select AirlineID, Year, sum(Flights) from airlineStats where Year > 2010 group by AirlineID, Year order by "
            + "sum(Flights) desc limit 5";
    printStatus(Color.YELLOW, "Top 5 airlines in number of flights after 2010");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");

    String q4 =
        "select OriginCityName, max(Flights) from airlineStats group by OriginCityName order by max(Flights) desc "
            + "limit 5";
    printStatus(Color.YELLOW, "Top 5 cities for number of flights");
    printStatus(Color.CYAN, "Query : " + q4);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4)));
    printStatus(Color.GREEN, "***************************************************");

    String q5 = "select AirlineID, OriginCityName, DestCityName, Year from airlineStats order by Year limit 5";
    printStatus(Color.YELLOW, "Print AirlineID, OriginCityName, DestCityName, Year for 5 records ordered by Year");
    printStatus(Color.CYAN, "Query : " + q5);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q5)));
    printStatus(Color.GREEN, "***************************************************");
  }

  @Override
  protected String[] getDefaultBatchTableDirectories() {
    return new String[]{"examples/batch/airlineStats"};
  }

  @Override
  protected Map<String, String> getDefaultStreamTableDirectories() {
    return ImmutableMap.of("airlineStats", "examples/stream/airlineStats");
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir =
        _setCustomDataDir ? _dataDir : new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File quickstartRunnerDir = new File(quickstartTmpDir, "quickstart");
    Preconditions.checkState(quickstartRunnerDir.mkdirs());
    Set<QuickstartTableRequest> quickstartTableRequests = new HashSet<>();
    quickstartTableRequests.addAll(bootstrapOfflineTableDirectories(quickstartTmpDir));
    quickstartTableRequests.addAll(bootstrapStreamTableDirectories(quickstartTmpDir));
    final QuickstartRunner runner =
        new QuickstartRunner(new ArrayList<>(quickstartTableRequests), 1, 1, 1, 1, quickstartRunnerDir,
            getConfigOverrides());

    startKafka();
    startAllDataStreams(_kafkaStarter, quickstartTmpDir);

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, broker, server and minion *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down realtime quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    printStatus(Color.CYAN, "***** Bootstrap all tables *****");
    runner.bootstrapTable();

    printStatus(Color.CYAN, "***** Waiting for 5 seconds for a few events to get populated *****");
    Thread.sleep(5000);

    printStatus(Color.YELLOW, "***** Realtime quickstart setup complete *****");
    runSampleQueries(runner);

    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }
}
