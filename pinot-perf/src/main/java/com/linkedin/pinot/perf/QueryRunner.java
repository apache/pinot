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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple code to run queries against a server
 * USAGE: QueryRunner QueryFile ({BrokerHost} {BrokerPort} <single-threaded/multi-threaded>)
 */
public class QueryRunner {
  private QueryRunner() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRunner.class);
  private static final int MILLIS_PER_SECOND = 1000;

  @SuppressWarnings("InfiniteLoopStatement")
  public static void multiThreadedQueryRunner(PerfBenchmarkDriverConf conf, String queryFile)
      throws Exception {
    final long randomSeed = 123456789L;
    final int numClients = 10;
    final int reportIntervalMillis = 3000;

    final List<String> queries;
    try (FileInputStream input = new FileInputStream(new File(queryFile))) {
      queries = IOUtils.readLines(input);
    }

    final int queryNum = queries.size();
    final PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    final Random random = new Random(randomSeed);
    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicLong totalResponseTime = new AtomicLong(0L);
    final ExecutorService executorService = Executors.newFixedThreadPool(numClients);

    for (int i = 0; i < numClients; i++) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          while (true) {
            String query = queries.get(random.nextInt(queryNum));
            long start = System.currentTimeMillis();
            try {
              driver.postQuery(query);
              counter.getAndIncrement();
              totalResponseTime.getAndAdd(System.currentTimeMillis() - start);
            } catch (Exception e) {
              LOGGER.error("Caught exception while running query: {}", query, e);
              return;
            }
          }
        }
      });
    }

    long startTime = System.currentTimeMillis();
    while (true) {
      Thread.sleep(reportIntervalMillis);
      double timePassedSeconds = ((double) (System.currentTimeMillis() - startTime)) / MILLIS_PER_SECOND;
      int count = counter.get();
      double avgResponseTime = ((double) totalResponseTime.get()) / count;
      LOGGER.info("Time Passed: {}s, Query Executed: {}, QPS: {}, Avg Response Time: {}ms", timePassedSeconds, count,
          count / timePassedSeconds, avgResponseTime);
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

