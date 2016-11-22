/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.tools.perf.PerfBenchmarkDriver;
import com.linkedin.pinot.tools.perf.PerfBenchmarkDriverConf;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.json.JSONObject;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryRunner {
  @Option(name = "-queryFile", required = true, usage = "query file path")
  private String _queryFile;
  @Option(name = "-mode", required = true, usage = "query runner mode (singleThread|multiThreads|targetQPS)")
  private String _mode;
  @Option(name = "-numThreads", required = false,
      usage = "number of threads sending queries for multiThread mode and targetQPS mode")
  private int _numThreads;
  @Option(name = "-startQPS", required = false, usage = "start QPS for targetQPS mode")
  private double _startQPS;
  @Option(name = "-deltaQPS", required = false, usage = "delta QPS for targetQPS mode")
  private double _deltaQPS;
  @Option(name = "-brokerHost", required = false, usage = "broker host name (default: localhost)")
  private String _brokerHost = "localhost";
  @Option(name = "-brokerPort", required = false, usage = "broker port number (default: 8099)")
  private String _brokerPort = "8099";
  @Option(name = "-help", required = false, help = true, aliases = { "-h" }, usage = "print this message")
  private boolean _help;

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRunner.class);
  private static final int MILLIS_PER_SECOND = 1000;

  /**
   * Use single thread to run queries as fast as possible.
   *
   * Use a single thread to send queries back to back and log statistic information periodically.
   *
   * @param conf perf benchmark driver config.
   * @param queryFile query file.
   * @throws Exception
   */
  public static void singleThreadedQueryRunner(PerfBenchmarkDriverConf conf, String queryFile)
      throws Exception {
    final PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);

    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFile))) {
      int numQueries = 0;
      int totalServerTime = 0;
      int totalBrokerTime = 0;
      int totalClientTime = 0;

      String query;
      DescriptiveStatistics stats = new DescriptiveStatistics();
      while ((query = bufferedReader.readLine()) != null) {
        long startTime = System.currentTimeMillis();
        JSONObject response = driver.postQuery(query);
        numQueries++;
        long clientTime = System.currentTimeMillis() - startTime;
        totalClientTime += clientTime;
        totalServerTime += response.getLong("timeUsedMs");
        long brokerTime = response.getLong("totalTime");
        totalBrokerTime += brokerTime;
        stats.addValue(clientTime);

        if (numQueries % 1000 == 0) {
          LOGGER.info(
              "Processed {} Queries, Total Server Time: {}ms, Total Broker Time: {}ms, Total Client Time : {}ms.",
              numQueries, totalServerTime, totalBrokerTime, totalClientTime);

          if (numQueries % 10000 == 0) {
            printStats(stats);
          }
        }
      }

      LOGGER.info("Processed {} Queries, Total Server Time: {}ms, Total Broker Time: {}ms, Total Client Time : {}ms.",
          numQueries, totalServerTime, totalBrokerTime, totalClientTime);
      printStats(stats);
    }
  }

  private static void printStats(DescriptiveStatistics stats) {
    LOGGER.info(stats.toString());
    LOGGER.info("10th percentile: {}ms", stats.getPercentile(10.0));
    LOGGER.info("25th percentile: {}ms", stats.getPercentile(25.0));
    LOGGER.info("50th percentile: {}ms", stats.getPercentile(50.0));
    LOGGER.info("90th percentile: {}ms", stats.getPercentile(90.0));
    LOGGER.info("95th percentile: {}ms", stats.getPercentile(95.0));
    LOGGER.info("99th percentile: {}ms", stats.getPercentile(99.0));
    LOGGER.info("99.9th percentile: {}ms", stats.getPercentile(99.9));
  }

  /**
   * Use multiple threads to run queries as fast as possible.
   *
   * Start {numThreads} worker threads to send queries (blocking call) back to back, and use the main thread to collect
   * the statistic information and log them periodically.
   *
   * @param conf perf benchmark driver config.
   * @param queryFile query file.
   * @param numThreads number of threads sending queries.
   * @throws Exception
   */
  @SuppressWarnings("InfiniteLoopStatement")
  public static void multiThreadedsQueryRunner(PerfBenchmarkDriverConf conf, String queryFile, final int numThreads)
      throws Exception {
    final long randomSeed = 123456789L;
    final Random random = new Random(randomSeed);
    final int reportIntervalMillis = 3000;

    final List<String> queries;
    try (FileInputStream input = new FileInputStream(new File(queryFile))) {
      queries = IOUtils.readLines(input);
    }

    final int numQueries = queries.size();
    final PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicLong totalResponseTime = new AtomicLong(0L);
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    final DescriptiveStatistics stats = new DescriptiveStatistics();
    final CountDownLatch latch = new CountDownLatch(numThreads);

    for (int i = 0; i < numThreads; i++) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          for (int j = 0; j < numQueries; j++) {
            String query = queries.get(random.nextInt(numQueries));
            long startTime = System.currentTimeMillis();
            try {
              driver.postQuery(query);
              long clientTime = System.currentTimeMillis() - startTime;
              synchronized (stats) {
                stats.addValue(clientTime);
              }

              counter.getAndIncrement();
              totalResponseTime.getAndAdd(clientTime);
            } catch (Exception e) {
              LOGGER.error("Caught exception while running query: {}", query, e);
              return;
            }
          }
          latch.countDown();
        }
      });
    }

    executorService.shutdown();

    int iter = 0;
    long startTime = System.currentTimeMillis();
    while (latch.getCount() > 0) {
      Thread.sleep(reportIntervalMillis);
      double timePassedSeconds = ((double) (System.currentTimeMillis() - startTime)) / MILLIS_PER_SECOND;
      int count = counter.get();
      double avgResponseTime = ((double) totalResponseTime.get()) / count;
      LOGGER.info("Time Passed: {}s, Query Executed: {}, QPS: {}, Avg Response Time: {}ms", timePassedSeconds, count,
          count / timePassedSeconds, avgResponseTime);

      iter++;
      if (iter % 10 == 0) {
        printStats(stats);
      }
    }

    printStats(stats);
  }

  /**
   * Use multiple threads to run query at an increasing target QPS.
   *
   * Use a concurrent linked queue to buffer the queries to be sent. Use the main thread to insert queries into the
   * queue at the target QPS, and start {numThreads} worker threads to fetch queries from the queue and send them.
   * We start with the start QPS, and keep adding delta QPS to the start QPS during the test. The main thread is
   * responsible for collecting the statistic information and log them periodically.
   *
   * @param conf perf benchmark driver config.
   * @param queryFile query file.
   * @param numThreads number of threads sending queries.
   * @param startQPS start QPS
   * @param deltaQPS delta QPS
   * @throws Exception
   */
  @SuppressWarnings("InfiniteLoopStatement")
  public static void targetQPSQueryRunner(PerfBenchmarkDriverConf conf, String queryFile, int numThreads,
      double startQPS, double deltaQPS)
      throws Exception {
    final long randomSeed = 123456789L;
    final Random random = new Random(randomSeed);
    final int timePerTargetQPSMillis = 60000;
    final int queueLengthThreshold = Math.max(20, (int) startQPS);

    final List<String> queries;
    try (FileInputStream input = new FileInputStream(new File(queryFile))) {
      queries = IOUtils.readLines(input);
    }
    final int numQueries = queries.size();

    final PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicLong totalResponseTime = new AtomicLong(0L);
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    final ConcurrentLinkedQueue<String> queryQueue = new ConcurrentLinkedQueue<>();
    double currentQPS = startQPS;
    int intervalMillis = (int) (MILLIS_PER_SECOND / currentQPS);

    for (int i = 0; i < numThreads; i++) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          while (true) {
            String query = queryQueue.poll();
            if (query == null) {
              try {
                Thread.sleep(1);
                continue;
              } catch (InterruptedException e) {
                LOGGER.error("Interrupted.", e);
                return;
              }
            }
            long startTime = System.currentTimeMillis();
            try {
              driver.postQuery(query);
              counter.getAndIncrement();
              totalResponseTime.getAndAdd(System.currentTimeMillis() - startTime);
            } catch (Exception e) {
              LOGGER.error("Caught exception while running query: {}", query, e);
              return;
            }
          }
        }
      });
    }

    LOGGER.info("Start with QPS: {}, delta QPS: {}", startQPS, deltaQPS);
    while (true) {
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime <= timePerTargetQPSMillis) {
        if (queryQueue.size() > queueLengthThreshold) {
          executorService.shutdownNow();
          throw new RuntimeException("Cannot achieve target QPS of: " + currentQPS);
        }
        queryQueue.add(queries.get(random.nextInt(numQueries)));
        Thread.sleep(intervalMillis);
      }
      double timePassedSeconds = ((double) (System.currentTimeMillis() - startTime)) / MILLIS_PER_SECOND;
      int count = counter.getAndSet(0);
      double avgResponseTime = ((double) totalResponseTime.getAndSet(0)) / count;
      LOGGER.info("Target QPS: {}, Interval: {}ms, Actual QPS: {}, Avg Response Time: {}ms", currentQPS, intervalMillis,
          count / timePassedSeconds, avgResponseTime);

      // Find a new interval
      int newIntervalMillis;
      do {
        currentQPS += deltaQPS;
        newIntervalMillis = (int) (MILLIS_PER_SECOND / currentQPS);
      } while (newIntervalMillis == intervalMillis);
      intervalMillis = newIntervalMillis;
    }
  }

  private static void printUsage() {
    System.out.println("Usage: QueryRunner");
    for (Field field : QueryRunner.class.getDeclaredFields()) {
      if (field.isAnnotationPresent(Option.class)) {
        Option option = field.getAnnotation(Option.class);
        System.out.println(String.format("\t%-15s: %s (required=%s)", option.name(), option.usage(), option.required()));
      }
    }
  }

  public static void main(String[] args)
      throws Exception {
    QueryRunner queryRunner = new QueryRunner();
    CmdLineParser parser = new CmdLineParser(queryRunner);
    parser.parseArgument(args);

    if (queryRunner._help) {
      printUsage();
      return;
    }

    PerfBenchmarkDriverConf conf = new PerfBenchmarkDriverConf();
    conf.setBrokerHost(queryRunner._brokerHost);
    conf.setBrokerPort(Integer.parseInt(queryRunner._brokerPort));
    conf.setRunQueries(true);
    conf.setStartZookeeper(false);
    conf.setStartController(false);
    conf.setStartBroker(false);
    conf.setStartServer(false);
    conf.setUploadIndexes(false);
    conf.setConfigureResources(false);

    long start = System.currentTimeMillis();
    switch (queryRunner._mode) {
      case "singleThread":
        singleThreadedQueryRunner(conf, queryRunner._queryFile);
        break;
      case "multiThreads":
        if (queryRunner._numThreads <= 0) {
          System.out.println("For multiThreads mode, need to specify a positive numThreads");
          printUsage();
          return;
        }
        multiThreadedsQueryRunner(conf, queryRunner._queryFile, queryRunner._numThreads);
        break;
      case "targetQPS":
        if (queryRunner._numThreads <= 0) {
          System.out.println("For targetQPS mode, need to specify a positive numThreads");
          printUsage();
          return;
        }
        if (queryRunner._startQPS <= 0) {
          System.out.println("For targetQPS mode, need to specify a positive startQPS");
          printUsage();
          return;
        }
        if (queryRunner._deltaQPS <= 0) {
          System.out.println("For targetQPS mode, need to specify a positive deltaQPS");
          printUsage();
          return;
        }
        targetQPSQueryRunner(conf, queryRunner._queryFile, queryRunner._numThreads, queryRunner._startQPS,
            queryRunner._deltaQPS);
        break;
      default:
        System.out.println("Invalid mode: " + queryRunner._mode);
        printUsage();
        break;
    }

    System.out.println("Overall time: " + (System.currentTimeMillis() - start) + "ms");
  }
}

