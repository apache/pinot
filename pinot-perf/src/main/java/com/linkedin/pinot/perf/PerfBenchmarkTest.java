/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.perf;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;


public class PerfBenchmarkTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PerfBenchmarkTest.class);

  private static void setupCluster(String dataDir, String offlineTableName) throws Exception {
    LOGGER.info("Setting up cluster");
    PerfBenchmarkRunner.startComponents(true, true, true, false);
    PerfBenchmarkRunner.startServerWithPreLoadedSegments(dataDir, Lists.newArrayList(offlineTableName), new ArrayList<String>());
  }

  private static void runQueries(String queryFile) throws Exception {
    System.out.println("Running queries....");
    PerfBenchmarkDriverConf conf = new PerfBenchmarkDriverConf();
    conf.setStartBroker(false);
    conf.setStartController(false);
    conf.setStartServer(false);
    conf.setStartZookeeper(false);
    conf.setUploadIndexes(false);
    conf.setRunQueries(true);
    conf.setConfigureResources(false);

    QueryRunner.singleThreadedQueryRunner(conf, queryFile);
    LOGGER.info("Running queries completed.");
  }

  private static void execute(String dataDir, String offlineTableName, String queryFile) throws Exception {
    setupCluster(dataDir, offlineTableName);
    runQueries(queryFile);
  }

  public static void main(String[] args) {
    if (args.length != 3) {
      LOGGER.error("Incorrect number of arguments.");
      LOGGER.info("PerfBenchmarkTest <DataDir> <offlineTableName> <PathToQueryFile>");
      return;
    }

    try {
      PerfBenchmarkTest.execute(args[0], args[1], args[2]);
      LOGGER.info("Benchmark test completed");
      System.exit(0);
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }
}
