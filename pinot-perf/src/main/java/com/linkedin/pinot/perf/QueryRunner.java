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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Simple code to run queries against a server
 * USAGE: QueryRunner confFile query numberOfTimesToRun
 */
public class QueryRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRunner.class);

  public static void multiThreadedQueryRunner(PerfBenchmarkDriverConf conf, String queryFile)
      throws Exception {
    final PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);

    final List<String> queries = IOUtils.readLines(new FileInputStream(new File(queryFile)));
    final Random random = new Random();
    final int targetQps = 10;
    final int numClients = 3;
    final int sleepMillis = 1000 / (numClients * targetQps);
    ArrayList<Callable<Void>> tasks = new ArrayList<Callable<Void>>();

    for (int i = 0; i < numClients; i++) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          while (true) {
            String query = queries.get(random.nextInt(queries.size()));
            JSONObject response = driver.postQuery(query);
            Thread.sleep(sleepMillis);
          }
        }

      };
      tasks.add(callable);
    }
    List<Future<Void>> invokeAll = Executors.newFixedThreadPool(numClients).invokeAll(tasks);
    for (int i = 0; i < numClients; i++) {
      Future<Void> future = invokeAll.get(i);
      future.get();
    }

  }

  public static void singleThreadedQueryRunner(PerfBenchmarkDriverConf conf, String queryFile)
      throws Exception {
    File file = new File(queryFile);
    FileReader fileReader = new FileReader(file);
    BufferedReader bufferedReader = new BufferedReader(fileReader);
    String query;

    int numQueries = 0;
    long totalQueryTime = 0;
    long totalClientTime = 0;

    while ((query = bufferedReader.readLine()) != null) {
      JSONObject response = runSingleQuery(conf, query, 1);

      totalQueryTime += response.getLong("timeUsedMs");
      totalClientTime += response.getLong("totalTime");

      if ((numQueries > 0) && (numQueries % 1000) == 0) {
        LOGGER.info(
            "Processed  {} queries, Total Query time: {} ms Total Client side time : {} ms.",
            numQueries, totalQueryTime, totalClientTime);
      }

      ++numQueries;
    }
    LOGGER.info("Processed  {} queries, Total Query time: {} Total Client side time : {}.",
        numQueries, totalQueryTime, totalClientTime);
    fileReader.close();
  }

  public static JSONObject runSingleQuery(PerfBenchmarkDriverConf conf, String query, int numRuns)
      throws Exception {
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    JSONObject response = null;

    for (int i = 0; i < numRuns; i++) {
      response = driver.postQuery(query);
    }
    return response;
  }

  /*
   * USAGE: QueryRunner <queryFile> <brokerHost(optional. default=localhost)> <brokerPort (optional.
   * default=8099)> <mode(optional.(single-threaded(optional)|multi-threaded))>)
   */

  public static void main(String[] args) throws Exception {

    String queryFile = null;
    if (args.length >= 1) {
      queryFile = args[0];
    } else {
      System.out.println(
          "QueryRunner <queryFile> <brokerHost(optional. default=localhost)> <brokerPort (optional. default=8099)> <mode(optional.(single-threaded(optional)|multi-threaded))>)");
      System.exit(1);
    }

    String brokerHost = null;
    String brokerPort = null;
    if (args.length >= 3) {
      brokerHost = args[1];
      brokerPort = args[2];
    }
    boolean multiThreaded = false;
    if (args.length >= 4) {
      if ("multi-threaded".equals(args[3])) {
        multiThreaded = true;
      }
    }
    PerfBenchmarkDriverConf conf = new PerfBenchmarkDriverConf();
    // since its only to run queries, we should ensure no services get started
    if (brokerHost != null) {
      conf.setBrokerHost(brokerHost);
      conf.setBrokerPort(Integer.parseInt(brokerPort));
    }
    conf.setStartBroker(false);
    conf.setStartController(false);
    conf.setStartServer(false);
    conf.setStartZookeeper(false);
    conf.setUploadIndexes(false);
    conf.setRunQueries(true);
    conf.setConfigureResources(false);
    if (multiThreaded) {
      multiThreadedQueryRunner(conf, queryFile);
    } else {
      singleThreadedQueryRunner(conf, queryFile);
    }

  }
}
