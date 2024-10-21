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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfiguration;
import org.apache.pinot.tsdb.spi.series.SimpleTimeSeriesBuilderFactory;


public class TimeSeriesEngineQuickStart extends Quickstart {
  private static final String[] TIME_SERIES_TABLE_DIRECTORIES = new String[]{
      "examples/batch/airlineStats",
      "examples/batch/baseballStats",
      "examples/batch/billing",
      "examples/batch/dimBaseballTeams",
      "examples/batch/githubEvents",
      "examples/batch/githubComplexTypeEvents",
      "examples/batch/ssb/customer",
      "examples/batch/ssb/dates",
      "examples/batch/ssb/lineorder",
      "examples/batch/ssb/part",
      "examples/batch/ssb/supplier",
      "examples/batch/starbucksStores",
      "examples/batch/fineFoodReviews",
  };

  @Override
  public String[] getDefaultBatchTableDirectories() {
    return TIME_SERIES_TABLE_DIRECTORIES;
  }

  @Override
  public List<String> types() {
    return Collections.singletonList("TIME_SERIES");
  }

  @Override
  protected Map<String, Object> getConfigOverrides() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(PinotTimeSeriesConfiguration.getEnabledLanguagesConfigKey(), "m3ql");
    configs.put(PinotTimeSeriesConfiguration.getLogicalPlannerConfigKey("m3ql"),
        "org.apache.pinot.tsdb.m3ql.M3TimeSeriesPlanner");
    configs.put(PinotTimeSeriesConfiguration.getSeriesBuilderFactoryConfigKey("m3ql"),
        SimpleTimeSeriesBuilderFactory.class.getName());
    return configs;
  }

  @Override
  public void execute()
      throws Exception {
    File quickstartTmpDir =
        _setCustomDataDir ? _dataDir : new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File quickstartRunnerDir = new File(quickstartTmpDir, "quickstart");
    Preconditions.checkState(quickstartRunnerDir.mkdirs());
    List<QuickstartTableRequest> quickstartTableRequests = bootstrapStreamTableDirectories(quickstartTmpDir);
    final QuickstartRunner runner =
        new QuickstartRunner(quickstartTableRequests, 1, 1, 1, 1, quickstartRunnerDir, getConfigOverrides());

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
