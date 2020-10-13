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
package org.apache.pinot.tools.perf;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("FieldCanBeLocal")
public class QueryRunner extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRunner.class);
  private static final int MILLIS_PER_SECOND = 1000;
  private static final String CLIENT_TIME_STATISTICS = "CLIENT TIME STATISTICS";

  @Option(name = "-mode", required = true, metaVar = "<String>", usage = "Mode of query runner (singleThread|multiThreads|targetQPS|increasingQPS).")
  private String _mode;
  @Option(name = "-queryFile", required = true, metaVar = "<String>", usage = "Path to query file.")
  private String _queryFile;
  @Option(name = "-queryMode", required = false, metaVar = "<String>", usage = "Mode of query generator (list|sample).")
  private String _queryMode = QueryMode.LIST.toString();
  @Option(name = "-queryCount", required = false, metaVar = "<int>", usage = "Number of queries to run (default 0 = all).")
  private int _queryCount = 0;
  @Option(name = "-numTimesToRunQueries", required = false, metaVar = "<int>", usage = "Number of times to run all queries in the query file, 0 means infinite times (default 1).")
  private int _numTimesToRunQueries = 1;
  @Option(name = "-reportIntervalMs", required = false, metaVar = "<int>", usage = "Interval in milliseconds to report simple statistics (default 3000).")
  private int _reportIntervalMs = 3000;
  @Option(name = "-numIntervalsToReportAndClearStatistics", required = false, metaVar = "<int>", usage = "Number of report intervals to report detailed statistics and clear them, 0 means never (default 10).")
  private int _numIntervalsToReportAndClearStatistics = 10;
  @Option(name = "-numThreads", required = false, metaVar = "<int>", usage =
      "Number of threads sending queries for multiThreads, targetQPS and increasingQPS mode (default 5). "
          + "This can be used to simulate multiple clients sending queries concurrently.")
  private int _numThreads = 5;
  @Option(name = "-startQPS", required = false, metaVar = "<int>", usage = "Start QPS for targetQPS and increasingQPS mode")
  private double _startQPS;
  @Option(name = "-deltaQPS", required = false, metaVar = "<int>", usage = "Delta QPS for increasingQPS mode.")
  private double _deltaQPS;
  @Option(name = "-numIntervalsToIncreaseQPS", required = false, metaVar = "<int>", usage = "Number of report intervals to increase QPS for increasingQPS mode (default 10).")
  private int _numIntervalsToIncreaseQPS = 10;
  @Option(name = "-brokerHost", required = false, metaVar = "<String>", usage = "Broker host name (default localhost).")
  private String _brokerHost = "localhost";
  @Option(name = "-brokerPort", required = false, metaVar = "<int>", usage = "Broker port number (default 8099).")
  private int _brokerPort = 8099;
  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help;

  private enum QueryMode {
    LIST,
    SAMPLE
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public String description() {
    return "Run queries from a query file in singleThread, multiThreads, targetQPS or increasingQPS mode. E.g.\n"
        + "  QueryRunner -mode singleThread -queryFile <queryFile> -numTimesToRunQueries 0 -numIntervalsToReportAndClearStatistics 5\n"
        + "  QueryRunner -mode multiThreads -queryFile <queryFile> -numThreads 10 -reportIntervalMs 1000\n"
        + "  QueryRunner -mode targetQPS -queryFile <queryFile> -startQPS 50\n"
        + "  QueryRunner -mode increasingQPS -queryFile <queryFile> -startQPS 50 -deltaQPS 10 -numIntervalsToIncreaseQPS 20\n";
  }

  @Override
  public boolean execute()
      throws Exception {
    if (!new File(_queryFile).isFile()) {
      LOGGER.error("Argument queryFile: {} is not a valid file.", _queryFile);
      printUsage();
      return false;
    }
    if (_numTimesToRunQueries < 0) {
      LOGGER.error("Argument numTimesToRunQueries should be a non-negative number.");
      printUsage();
      return false;
    }
    if (_reportIntervalMs <= 0) {
      LOGGER.error("Argument reportIntervalMs should be a positive number.");
      printUsage();
      return false;
    }
    if (_numIntervalsToReportAndClearStatistics < 0) {
      LOGGER.error("Argument numIntervalsToReportAndClearStatistics should be a non-negative number.");
      printUsage();
      return false;
    }

    LOGGER.info("Start query runner targeting broker: {}:{}", _brokerHost, _brokerPort);
    PerfBenchmarkDriverConf conf = new PerfBenchmarkDriverConf();
    conf.setBrokerHost(_brokerHost);
    conf.setBrokerPort(_brokerPort);
    conf.setRunQueries(true);
    conf.setStartZookeeper(false);
    conf.setStartController(false);
    conf.setStartBroker(false);
    conf.setStartServer(false);

    Stream<String> queries = makeQueries(
            IOUtils.readLines(new FileInputStream(_queryFile)),
            QueryMode.valueOf(_queryMode.toUpperCase()),
            _queryCount);

    switch (_mode) {
      case "singleThread":
        LOGGER.info("MODE singleThread with queryFile: {}, numTimesToRunQueries: {}, reportIntervalMs: {}, "
                + "numIntervalsToReportAndClearStatistics: {}", _queryFile, _numTimesToRunQueries, _reportIntervalMs,
            _numIntervalsToReportAndClearStatistics);
        singleThreadedQueryRunner(conf, queries, _numTimesToRunQueries, _reportIntervalMs,
            _numIntervalsToReportAndClearStatistics);
        break;
      case "multiThreads":
        if (_numThreads <= 0) {
          LOGGER.error("For multiThreads mode, argument numThreads should be a positive number.");
          printUsage();
          break;
        }
        LOGGER.info("MODE multiThreads with queryFile: {}, numTimesToRunQueries: {}, numThreads: {}, "
                + "reportIntervalMs: {}, numIntervalsToReportAndClearStatistics: {}", _queryFile, _numTimesToRunQueries,
            _numThreads, _reportIntervalMs, _numIntervalsToReportAndClearStatistics);
        multiThreadedQueryRunner(conf, queries, _numTimesToRunQueries, _numThreads, _reportIntervalMs,
            _numIntervalsToReportAndClearStatistics);
        break;
      case "targetQPS":
        if (_numThreads <= 0) {
          LOGGER.error("For targetQPS mode, argument numThreads should be a positive number.");
          printUsage();
          break;
        }
        if (_startQPS <= 0 || _startQPS > 1000.0) {
          LOGGER.error("For targetQPS mode, argument startQPS should be a positive number that less or equal to 1000.");
          printUsage();
          break;
        }
        LOGGER.info("MODE targetQPS with queryFile: {}, numTimesToRunQueries: {}, numThreads: {}, startQPS: {}, "
                + "reportIntervalMs: {}, numIntervalsToReportAndClearStatistics: {}", _queryFile, _numTimesToRunQueries,
            _numThreads, _startQPS, _reportIntervalMs, _numIntervalsToReportAndClearStatistics);
        targetQPSQueryRunner(conf, queries, _numTimesToRunQueries, _numThreads, _startQPS, _reportIntervalMs,
            _numIntervalsToReportAndClearStatistics);
        break;
      case "increasingQPS":
        if (_numThreads <= 0) {
          LOGGER.error("For increasingQPS mode, argument numThreads should be a positive number.");
          printUsage();
          break;
        }
        if (_startQPS <= 0 || _startQPS > 1000.0) {
          LOGGER.error(
              "For increasingQPS mode, argument startQPS should be a positive number that less or equal to 1000.");
          printUsage();
          break;
        }
        if (_deltaQPS <= 0) {
          LOGGER.error("For increasingQPS mode, argument deltaQPS should be a positive number.");
          printUsage();
          break;
        }
        if (_numIntervalsToIncreaseQPS <= 0) {
          LOGGER.error("For increasingQPS mode, argument numIntervalsToIncreaseQPS should be a positive number.");
          printUsage();
          break;
        }
        LOGGER.info("MODE increasingQPS with queryFile: {}, numTimesToRunQueries: {}, numThreads: {}, startQPS: {}, "
                + "deltaQPS: {}, reportIntervalMs: {}, numIntervalsToReportAndClearStatistics: {}, "
                + "numIntervalsToIncreaseQPS: {}", _queryFile, _numTimesToRunQueries, _numThreads, _startQPS, _deltaQPS,
            _reportIntervalMs, _numIntervalsToReportAndClearStatistics, _numIntervalsToIncreaseQPS);
        increasingQPSQueryRunner(conf, queries, _numTimesToRunQueries, _numThreads, _startQPS, _deltaQPS,
            _reportIntervalMs, _numIntervalsToReportAndClearStatistics, _numIntervalsToIncreaseQPS);
        break;
      default:
        LOGGER.error("Invalid mode: {}", _mode);
        printUsage();
        break;
    }
    return true;
  }

  /**
   * Use single thread to run queries as fast as possible.
   * <p>Use a single thread to send queries back to back and log statistic information periodically.
   * <p>Queries are picked sequentially from the query file.
   * <p>Query runner will stop when all queries in the query file has been executed number of times configured.
   *
   * @param conf perf benchmark driver config.
   * @param queries query stream.
   * @param numTimesToRunQueries number of times to run all queries in the query file, 0 means infinite times.
   * @param reportIntervalMs report interval in milliseconds.
   * @param numIntervalsToReportAndClearStatistics number of report intervals to report detailed statistics and clear
   *                                               them, 0 means never.
   * @throws Exception
   */
  public static void singleThreadedQueryRunner(PerfBenchmarkDriverConf conf, Stream<String> queries, int numTimesToRunQueries,
      int reportIntervalMs, int numIntervalsToReportAndClearStatistics)
      throws Exception {
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    int numQueriesExecuted = 0;
    long totalBrokerTime = 0L;
    long totalClientTime = 0L;
    List<Statistics> statisticsList = Collections.singletonList(new Statistics(CLIENT_TIME_STATISTICS));

    long startTime = System.currentTimeMillis();
    long reportStartTime = startTime;
    int numReportIntervals = 0;
    int numTimesExecuted = 0;
    while (numTimesToRunQueries == 0 || numTimesExecuted < numTimesToRunQueries) {
      Iterator<String> itQuery = queries.iterator();
      while (itQuery.hasNext()) {
        String query = itQuery.next();

        JsonNode response = driver.postQuery(query);
        numQueriesExecuted++;
        long brokerTime = response.get("timeUsedMs").asLong();
        totalBrokerTime += brokerTime;
        long clientTime = response.get("totalTime").asLong();
        totalClientTime += clientTime;
        statisticsList.get(0).addValue(clientTime);

        long currentTime = System.currentTimeMillis();
        if (currentTime - reportStartTime >= reportIntervalMs) {
          long timePassed = currentTime - startTime;
          LOGGER.info("Time Passed: {}ms, Queries Executed: {}, Average QPS: {}, Average Broker Time: {}ms, "
                  + "Average Client Time: {}ms.", timePassed, numQueriesExecuted,
              numQueriesExecuted / ((double) timePassed / MILLIS_PER_SECOND),
              totalBrokerTime / (double) numQueriesExecuted, totalClientTime / (double) numQueriesExecuted);
          reportStartTime = currentTime;
          numReportIntervals++;

          if ((numIntervalsToReportAndClearStatistics != 0) && (numReportIntervals
              == numIntervalsToReportAndClearStatistics)) {
            numReportIntervals = 0;
            startTime = currentTime;
            numQueriesExecuted = 0;
            totalBrokerTime = 0L;
            totalClientTime = 0L;
            for (Statistics statistics : statisticsList) {
              statistics.report();
              statistics.clear();
            }
          }
        }
      }
      numTimesExecuted++;
    }

    long timePassed = System.currentTimeMillis() - startTime;
    LOGGER.info("--------------------------------------------------------------------------------");
    LOGGER.info("FINAL REPORT:");
    LOGGER.info("Time Passed: {}ms, Queries Executed: {}, Average QPS: {}, Average Broker Time: {}ms, "
            + "Average Client Time: {}ms.", timePassed, numQueriesExecuted,
        numQueriesExecuted / ((double) timePassed / MILLIS_PER_SECOND), totalBrokerTime / (double) numQueriesExecuted,
        totalClientTime / (double) numQueriesExecuted);
    for (Statistics statistics : statisticsList) {
      statistics.report();
    }
  }

  /**
   * Use multiple threads to run queries as fast as possible.
   * <p>Use a concurrent linked queue to buffer the queries to be sent. Use the main thread to insert queries into the
   * queue whenever the queue length is low, and start <code>numThreads</code> worker threads to fetch queries from the
   * queue and send them.
   * <p>The main thread is responsible for collecting and logging the statistic information periodically.
   * <p>Queries are picked sequentially from the query file.
   * <p>Query runner will stop when all queries in the query file has been executed number of times configured.
   *
   * @param conf perf benchmark driver config.
   * @param queries query stream.
   * @param numTimesToRunQueries number of times to run all queries in the query file, 0 means infinite times.
   * @param numThreads number of threads sending queries.
   * @param reportIntervalMs report interval in milliseconds.
   * @param numIntervalsToReportAndClearStatistics number of report intervals to report detailed statistics and clear
   *                                               them, 0 means never.
   * @throws Exception
   */
  public static void multiThreadedQueryRunner(PerfBenchmarkDriverConf conf, Stream<String> queries, int numTimesToRunQueries,
      int numThreads, int reportIntervalMs, int numIntervalsToReportAndClearStatistics)
      throws Exception {
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    ConcurrentLinkedQueue<String> queryQueue = new ConcurrentLinkedQueue<>();
    AtomicInteger numQueriesExecuted = new AtomicInteger(0);
    AtomicLong totalBrokerTime = new AtomicLong(0L);
    AtomicLong totalClientTime = new AtomicLong(0L);
    List<Statistics> statisticsList = Collections.singletonList(new Statistics(CLIENT_TIME_STATISTICS));

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executorService
          .submit(new Worker(driver, queryQueue, numQueriesExecuted, totalBrokerTime, totalClientTime, statisticsList));
    }
    executorService.shutdown();

    long startTime = System.currentTimeMillis();
    long reportStartTime = startTime;
    int numReportIntervals = 0;
    int numTimesExecuted = 0;
    while (numTimesToRunQueries == 0 || numTimesExecuted < numTimesToRunQueries) {
      if (executorService.isTerminated()) {
        LOGGER.error("All threads got exception and already dead.");
        return;
      }

      Iterator<String> itQuery = queries.iterator();
      while (itQuery.hasNext()) {
        String query = itQuery.next();

        queryQueue.add(query);

        // Keep 20 queries inside the query queue.
        while (queryQueue.size() == 20) {
          Thread.sleep(1);

          long currentTime = System.currentTimeMillis();
          if (currentTime - reportStartTime >= reportIntervalMs) {
            long timePassed = currentTime - startTime;
            int numQueriesExecutedInt = numQueriesExecuted.get();
            LOGGER.info("Time Passed: {}ms, Queries Executed: {}, Average QPS: {}, Average Broker Time: {}ms, "
                    + "Average Client Time: {}ms.", timePassed, numQueriesExecutedInt,
                numQueriesExecutedInt / ((double) timePassed / MILLIS_PER_SECOND),
                totalBrokerTime.get() / (double) numQueriesExecutedInt,
                totalClientTime.get() / (double) numQueriesExecutedInt);
            reportStartTime = currentTime;
            numReportIntervals++;

            if ((numIntervalsToReportAndClearStatistics != 0) && (numReportIntervals
                == numIntervalsToReportAndClearStatistics)) {
              numReportIntervals = 0;
              startTime = currentTime;
              reportAndClearStatistics(numQueriesExecuted, totalBrokerTime, totalClientTime, statisticsList);
            }
          }
        }
      }
      numTimesExecuted++;
    }

    // Wait for all queries getting executed.
    while (queryQueue.size() != 0) {
      Thread.sleep(1);
    }
    executorService.shutdownNow();
    while (!executorService.isTerminated()) {
      Thread.sleep(1);
    }

    long timePassed = System.currentTimeMillis() - startTime;
    int numQueriesExecutedInt = numQueriesExecuted.get();
    LOGGER.info("--------------------------------------------------------------------------------");
    LOGGER.info("FINAL REPORT:");
    LOGGER.info("Time Passed: {}ms, Queries Executed: {}, Average QPS: {}, Average Broker Time: {}ms, "
            + "Average Client Time: {}ms.", timePassed, numQueriesExecutedInt,
        numQueriesExecutedInt / ((double) timePassed / MILLIS_PER_SECOND),
        totalBrokerTime.get() / (double) numQueriesExecutedInt, totalClientTime.get() / (double) numQueriesExecutedInt);
    for (Statistics statistics : statisticsList) {
      statistics.report();
    }
  }

  /**
   * Use multiple threads to run query at a target QPS.
   * <p>Use a concurrent linked queue to buffer the queries to be sent. Use the main thread to insert queries into the
   * queue at the target QPS, and start <code>numThreads</code> worker threads to fetch queries from the queue and send
   * them.
   * <p>The main thread is responsible for collecting and logging the statistic information periodically.
   * <p>Queries are picked sequentially from the query file.
   * <p>Query runner will stop when all queries in the query file has been executed number of times configured.
   *
   * @param conf perf benchmark driver config.
   * @param queries query stream.
   * @param numTimesToRunQueries number of times to run all queries in the query file, 0 means infinite times.
   * @param numThreads number of threads sending queries.
   * @param startQPS start QPS (target QPS).
   * @param reportIntervalMs report interval in milliseconds.
   * @param numIntervalsToReportAndClearStatistics number of report intervals to report detailed statistics and clear
   *                                               them, 0 means never.
   * @throws Exception
   */
  public static void targetQPSQueryRunner(PerfBenchmarkDriverConf conf, Stream<String> queries, int numTimesToRunQueries,
      int numThreads, double startQPS, int reportIntervalMs, int numIntervalsToReportAndClearStatistics)
      throws Exception {
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    ConcurrentLinkedQueue<String> queryQueue = new ConcurrentLinkedQueue<>();
    AtomicInteger numQueriesExecuted = new AtomicInteger(0);
    AtomicLong totalBrokerTime = new AtomicLong(0L);
    AtomicLong totalClientTime = new AtomicLong(0L);
    List<Statistics> statisticsList = Collections.singletonList(new Statistics(CLIENT_TIME_STATISTICS));

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executorService
          .submit(new Worker(driver, queryQueue, numQueriesExecuted, totalBrokerTime, totalClientTime, statisticsList));
    }
    executorService.shutdown();

    int queryIntervalMs = (int) (MILLIS_PER_SECOND / startQPS);
    long startTime = System.currentTimeMillis();
    long reportStartTime = startTime;
    int numReportIntervals = 0;
    int numTimesExecuted = 0;
    while (numTimesToRunQueries == 0 || numTimesExecuted < numTimesToRunQueries) {
      if (executorService.isTerminated()) {
        LOGGER.error("All threads got exception and already dead.");
        return;
      }

      Iterator<String> itQuery = queries.iterator();
      while (itQuery.hasNext()) {
        String query = itQuery.next();

        queryQueue.add(query);
        Thread.sleep(queryIntervalMs);

        long currentTime = System.currentTimeMillis();
        if (currentTime - reportStartTime >= reportIntervalMs) {
          long timePassed = currentTime - startTime;
          int numQueriesExecutedInt = numQueriesExecuted.get();
          LOGGER.info("Target QPS: {}, Time Passed: {}ms, Queries Executed: {}, Average QPS: {}, "
                  + "Average Broker Time: {}ms, Average Client Time: {}ms, Queries Queued: {}.", startQPS, timePassed,
              numQueriesExecutedInt, numQueriesExecutedInt / ((double) timePassed / MILLIS_PER_SECOND),
              totalBrokerTime.get() / (double) numQueriesExecutedInt,
              totalClientTime.get() / (double) numQueriesExecutedInt, queryQueue.size());
          reportStartTime = currentTime;
          numReportIntervals++;

          if ((numIntervalsToReportAndClearStatistics != 0) && (numReportIntervals
              == numIntervalsToReportAndClearStatistics)) {
            numReportIntervals = 0;
            startTime = currentTime;
            reportAndClearStatistics(numQueriesExecuted, totalBrokerTime, totalClientTime, statisticsList);
          }
        }
      }
      numTimesExecuted++;
    }

    // Wait for all queries getting executed.
    while (queryQueue.size() != 0) {
      Thread.sleep(1);
    }
    executorService.shutdownNow();
    while (!executorService.isTerminated()) {
      Thread.sleep(1);
    }

    long timePassed = System.currentTimeMillis() - startTime;
    int numQueriesExecutedInt = numQueriesExecuted.get();
    LOGGER.info("--------------------------------------------------------------------------------");
    LOGGER.info("FINAL REPORT:");
    LOGGER.info("Target QPS: {}, Time Passed: {}ms, Queries Executed: {}, Average QPS: {}, "
            + "Average Broker Time: {}ms, Average Client Time: {}ms.", startQPS, timePassed, numQueriesExecutedInt,
        numQueriesExecutedInt / ((double) timePassed / MILLIS_PER_SECOND),
        totalBrokerTime.get() / (double) numQueriesExecutedInt, totalClientTime.get() / (double) numQueriesExecutedInt);
    for (Statistics statistics : statisticsList) {
      statistics.report();
    }
  }

  /**
   * Use multiple threads to run query at an increasing target QPS.
   * <p>Use a concurrent linked queue to buffer the queries to be sent. Use the main thread to insert queries into the
   * queue at the target QPS, and start <code>numThreads</code> worker threads to fetch queries from the queue and send
   * them.
   * <p>We start with the start QPS, and keep adding delta QPS to the start QPS during the test.
   * <p>The main thread is responsible for collecting and logging the statistic information periodically.
   * <p>Queries are picked sequentially from the query file.
   * <p>Query runner will stop when all queries in the query file has been executed number of times configured.
   *
   * @param conf perf benchmark driver config.
   * @param queries query stream.
   * @param numTimesToRunQueries number of times to run all queries in the query file, 0 means infinite times.
   * @param numThreads number of threads sending queries.
   * @param startQPS start QPS.
   * @param deltaQPS delta QPS.
   * @param reportIntervalMs report interval in milliseconds.
   * @param numIntervalsToReportAndClearStatistics number of report intervals to report detailed statistics and clear
   *                                               them, 0 means never.
   * @param numIntervalsToIncreaseQPS number of intervals to increase QPS.
   * @throws Exception
   */

  public static void increasingQPSQueryRunner(PerfBenchmarkDriverConf conf, Stream<String> queries, int numTimesToRunQueries,
      int numThreads, double startQPS, double deltaQPS, int reportIntervalMs,
      int numIntervalsToReportAndClearStatistics, int numIntervalsToIncreaseQPS)
      throws Exception {
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    ConcurrentLinkedQueue<String> queryQueue = new ConcurrentLinkedQueue<>();
    AtomicInteger numQueriesExecuted = new AtomicInteger(0);
    AtomicLong totalBrokerTime = new AtomicLong(0L);
    AtomicLong totalClientTime = new AtomicLong(0L);
    List<Statistics> statisticsList = Collections.singletonList(new Statistics(CLIENT_TIME_STATISTICS));

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executorService
          .submit(new Worker(driver, queryQueue, numQueriesExecuted, totalBrokerTime, totalClientTime, statisticsList));
    }
    executorService.shutdown();

    long startTime = System.currentTimeMillis();
    long reportStartTime = startTime;
    int numReportIntervals = 0;
    int numTimesExecuted = 0;
    double currentQPS = startQPS;
    int queryIntervalMs = (int) (MILLIS_PER_SECOND / currentQPS);
    while (numTimesToRunQueries == 0 || numTimesExecuted < numTimesToRunQueries) {
      if (executorService.isTerminated()) {
        LOGGER.error("All threads got exception and already dead.");
        return;
      }

      Iterator<String> itQuery = queries.iterator();
      while (itQuery.hasNext()) {
        String query = itQuery.next();

        queryQueue.add(query);
        Thread.sleep(queryIntervalMs);

        long currentTime = System.currentTimeMillis();
        if (currentTime - reportStartTime >= reportIntervalMs) {
          long timePassed = currentTime - startTime;
          reportStartTime = currentTime;
          numReportIntervals++;

          if (numReportIntervals == numIntervalsToIncreaseQPS) {
            // Try to find the next interval.
            double newQPS = currentQPS + deltaQPS;
            int newQueryIntervalMs;
            // Skip the target QPS with the same interval as the previous one.
            while ((newQueryIntervalMs = (int) (MILLIS_PER_SECOND / newQPS)) == queryIntervalMs) {
              newQPS += deltaQPS;
            }
            if (newQueryIntervalMs == 0) {
              LOGGER.warn("Due to sleep granularity of millisecond, cannot further increase QPS.");
            } else {
              // Find the next interval.
              LOGGER.info("--------------------------------------------------------------------------------");
              LOGGER.info("REPORT FOR TARGET QPS: {}", currentQPS);
              int numQueriesExecutedInt = numQueriesExecuted.get();
              LOGGER.info("Current Target QPS: {}, Time Passed: {}ms, Queries Executed: {}, Average QPS: {}, "
                      + "Average Broker Time: {}ms, Average Client Time: {}ms, Queries Queued: {}.", currentQPS, timePassed,
                  numQueriesExecutedInt, numQueriesExecutedInt / ((double) timePassed / MILLIS_PER_SECOND),
                  totalBrokerTime.get() / (double) numQueriesExecutedInt,
                  totalClientTime.get() / (double) numQueriesExecutedInt, queryQueue.size());
              numReportIntervals = 0;
              startTime = currentTime;
              reportAndClearStatistics(numQueriesExecuted, totalBrokerTime, totalClientTime, statisticsList);

              currentQPS = newQPS;
              queryIntervalMs = newQueryIntervalMs;
              LOGGER
                  .info("Increase target QPS to: {}, the following statistics are for the new target QPS.", currentQPS);
            }
          } else {
            int numQueriesExecutedInt = numQueriesExecuted.get();
            LOGGER.info("Current Target QPS: {}, Time Passed: {}ms, Queries Executed: {}, Average QPS: {}, "
                    + "Average Broker Time: {}ms, Average Client Time: {}ms, Queries Queued: {}.", currentQPS, timePassed,
                numQueriesExecutedInt, numQueriesExecutedInt / ((double) timePassed / MILLIS_PER_SECOND),
                totalBrokerTime.get() / (double) numQueriesExecutedInt,
                totalClientTime.get() / (double) numQueriesExecutedInt, queryQueue.size());

            if ((numIntervalsToReportAndClearStatistics != 0) && (
                numReportIntervals % numIntervalsToReportAndClearStatistics == 0)) {
              startTime = currentTime;
              reportAndClearStatistics(numQueriesExecuted, totalBrokerTime, totalClientTime, statisticsList);
            }
          }
        }
      }
      numTimesExecuted++;
    }

    // Wait for all queries getting executed.
    while (queryQueue.size() != 0) {
      Thread.sleep(1);
    }
    executorService.shutdownNow();
    while (!executorService.isTerminated()) {
      Thread.sleep(1);
    }

    long timePassed = System.currentTimeMillis() - startTime;
    int numQueriesExecutedInt = numQueriesExecuted.get();
    LOGGER.info("--------------------------------------------------------------------------------");
    LOGGER.info("FINAL REPORT:");
    LOGGER.info("Current Target QPS: {}, Time Passed: {}ms, Queries Executed: {}, Average QPS: {}, "
            + "Average Broker Time: {}ms, Average Client Time: {}ms.", currentQPS, timePassed, numQueriesExecutedInt,
        numQueriesExecutedInt / ((double) timePassed / MILLIS_PER_SECOND),
        totalBrokerTime.get() / (double) numQueriesExecutedInt, totalClientTime.get() / (double) numQueriesExecutedInt);
    for (Statistics statistics : statisticsList) {
      statistics.report();
    }
  }

  private static Stream<String> makeQueries(List<String> inputs, QueryMode queryMode, int queryCount) {
    queryCount = queryCount > 0 ? queryCount : inputs.size();

    switch (queryMode) {
      case LIST:
        return inputs.stream().limit(queryCount);

      case SAMPLE:
        Random r = new Random(inputs.hashCode()); // anything deterministic will do
        return r.ints(queryCount, 0, inputs.size()).boxed().map(inputs::get);

      default:
        throw new IllegalArgumentException(String.format("Unsupported queryMode '%s", queryMode));
    }
  }

  private static void reportAndClearStatistics(AtomicInteger numQueriesExecuted, AtomicLong totalBrokerTime,
      AtomicLong totalClientTime, List<Statistics> statisticsList) {
    numQueriesExecuted.set(0);
    totalBrokerTime.set(0L);
    totalClientTime.set(0L);
    for (Statistics statistics : statisticsList) {
      statistics.report();
      statistics.clear();
    }
  }

  private static void executeQueryInMultiThreads(PerfBenchmarkDriver driver, String query,
      AtomicInteger numQueriesExecuted, AtomicLong totalBrokerTime, AtomicLong totalClientTime,
      List<Statistics> statisticsList)
      throws Exception {
    JsonNode response = driver.postQuery(query);
    numQueriesExecuted.getAndIncrement();
    long brokerTime = response.get("timeUsedMs").asLong();
    totalBrokerTime.getAndAdd(brokerTime);
    long clientTime = response.get("totalTime").asLong();
    totalClientTime.getAndAdd(clientTime);
    statisticsList.get(0).addValue(clientTime);
  }

  private static class Worker implements Runnable {
    private final PerfBenchmarkDriver _driver;
    private final ConcurrentLinkedQueue<String> _queryQueue;
    private final AtomicInteger _numQueriesExecuted;
    private final AtomicLong _totalBrokerTime;
    private final AtomicLong _totalClientTime;
    private final List<Statistics> _statisticsList;

    private Worker(PerfBenchmarkDriver driver, ConcurrentLinkedQueue<String> queryQueue,
        AtomicInteger numQueriesExecuted, AtomicLong totalBrokerTime, AtomicLong totalClientTime,
        List<Statistics> statisticsList) {
      _driver = driver;
      _queryQueue = queryQueue;
      _numQueriesExecuted = numQueriesExecuted;
      _totalBrokerTime = totalBrokerTime;
      _totalClientTime = totalClientTime;
      _statisticsList = statisticsList;
    }

    @Override
    public void run() {
      while (true) {
        String query = _queryQueue.poll();
        if (query == null) {
          try {
            Thread.sleep(1);
            continue;
          } catch (InterruptedException e) {
            return;
          }
        }
        try {
          executeQueryInMultiThreads(_driver, query, _numQueriesExecuted, _totalBrokerTime, _totalClientTime,
              _statisticsList);
        } catch (Exception e) {
          LOGGER.error("Caught exception while running query: {}", query, e);
          return;
        }
      }
    }
  }

  @ThreadSafe
  private static class Statistics {
    private final DescriptiveStatistics _statistics = new DescriptiveStatistics();
    private final String _name;

    public Statistics(String name) {
      _name = name;
    }

    public void addValue(double value) {
      synchronized (_statistics) {
        _statistics.addValue(value);
      }
    }

    public void report() {
      synchronized (_statistics) {
        LOGGER.info("--------------------------------------------------------------------------------");
        LOGGER.info("{}:", _name);
        LOGGER.info(_statistics.toString());
        LOGGER.info("10th percentile: {}", _statistics.getPercentile(10.0));
        LOGGER.info("25th percentile: {}", _statistics.getPercentile(25.0));
        LOGGER.info("50th percentile: {}", _statistics.getPercentile(50.0));
        LOGGER.info("90th percentile: {}", _statistics.getPercentile(90.0));
        LOGGER.info("95th percentile: {}", _statistics.getPercentile(95.0));
        LOGGER.info("99th percentile: {}", _statistics.getPercentile(99.0));
        LOGGER.info("99.9th percentile: {}", _statistics.getPercentile(99.9));
        LOGGER.info("--------------------------------------------------------------------------------");
      }
    }

    public void clear() {
      synchronized (_statistics) {
        _statistics.clear();
      }
    }
  }

  public static void main(String[] args)
      throws Exception {
    QueryRunner queryRunner = new QueryRunner();
    CmdLineParser parser = new CmdLineParser(queryRunner);
    parser.parseArgument(args);

    if (queryRunner._help) {
      queryRunner.printUsage();
    } else {
      queryRunner.execute();
    }
  }
}

