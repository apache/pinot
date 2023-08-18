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
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@SuppressWarnings("FieldCanBeLocal")
@CommandLine.Command(name = "QueryRunner", description = "Run queries from a query file in singleThread, "
                                                         + "multiThreads, targetQPS or increasingQPS mode.",
    mixinStandardHelpOptions = true)
public class QueryRunner extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRunner.class);
  private static final int MILLIS_PER_SECOND = 1000;
  private static final long NANO_DELTA = (long) 5E5;
  private static final String CLIENT_TIME_STATISTICS = "CLIENT TIME STATISTICS";

  @CommandLine.Option(names = {"-mode"}, required = true, description = "Mode of query runner "
      + "(singleThread|multiThreads|targetQPS|increasingQPS).")
  private String _mode;
  @CommandLine.Option(names = {"-queryFile"}, required = true, description = "Path to query file.")
  private String _queryFile;
  @CommandLine.Option(names = {"-queryMode"}, required = false, description = "Mode of query generator "
      + "(full|resample).")
  private String _queryMode = QueryMode.FULL.toString();
  @CommandLine.Option(names = {"-queryCount"}, required = false, description = "Number of queries to run (default 0 ="
      + " all).")
  private int _queryCount = 0;
  @CommandLine.Option(names = {"-numTimesToRunQueries"}, required = false, description = "Number of times to run all "
      + "queries in the query file, 0 means infinite times (default 1).")
  private int _numTimesToRunQueries = 1;
  @CommandLine.Option(names = {"-reportIntervalMs"}, required = false, description = "Interval in milliseconds to "
      + "report simple statistics (default 3000).")
  private int _reportIntervalMs = 3000;
  @CommandLine.Option(names = {"-numIntervalsToReportAndClearStatistics"}, required = false, description =
      "Number of report intervals to report detailed statistics and clear them," + " 0 means never (default 10).")
  private int _numIntervalsToReportAndClearStatistics = 10;
  @CommandLine.Option(names = {"-numThreads"}, required = false, description =
      "Number of threads sending queries for multiThreads, targetQPS and increasingQPS mode (default 5). "
          + "This can be used to simulate multiple clients sending queries concurrently.")
  private int _numThreads = 5;
  @CommandLine.Option(names = {"-startQPS"}, required = false, description = "Start QPS for targetQPS and "
      + "increasingQPS mode")
  private double _startQPS;
  @CommandLine.Option(names = {"-deltaQPS"}, required = false, description = "Delta QPS for increasingQPS mode.")
  private double _deltaQPS;
  @CommandLine.Option(names = {"-numIntervalsToIncreaseQPS"}, required = false, description = "Number of report "
      + "intervals to increase QPS for increasingQPS mode (default 10).")
  private int _numIntervalsToIncreaseQPS = 10;
  @CommandLine.Option(names = {"-brokerHost"}, required = false, description = "Broker host name (default localhost).")
  private String _brokerHost = "localhost";
  @CommandLine.Option(names = {"-brokerPort"}, required = false, description = "Broker port number (default 8099).")
  private int _brokerPort = 8099;
  @CommandLine.Option(names = {"-brokerURL"}, required = false, description = "Broker URL (no default, uses "
      + "brokerHost:brokerPort by default.")
  private String _brokerURL;
  @CommandLine.Option(names = {"-queueDepth"}, required = false, description = "Queue size limit for multi-threaded "
      + "execution (default 64).")
  private int _queueDepth = 64;
  @CommandLine.Option(names = {"-timeout"}, required = false, description = "Timeout in milliseconds for completing "
      + "all queries (default: unlimited).")
  private long _timeout = 0;
  @CommandLine.Option(names = {"-verbose"}, required = false, description = "Enable verbose query logging (default: "
      + "false).")
  private boolean _verbose = false;
  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true, description = "Print "
      + "this message.")
  private boolean _help;

  private enum QueryMode {
    FULL, RESAMPLE
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public boolean execute()
      throws Exception {
    if (!new File(_queryFile).isFile()) {
      LOGGER.error("Argument queryFile: {} is not a valid file.", _queryFile);
      getDescription();
      return false;
    }
    if (_numTimesToRunQueries < 0) {
      LOGGER.error("Argument numTimesToRunQueries should be a non-negative number.");
      getDescription();
      return false;
    }
    if (_reportIntervalMs <= 0) {
      LOGGER.error("Argument reportIntervalMs should be a positive number.");
      getDescription();
      return false;
    }
    if (_numIntervalsToReportAndClearStatistics < 0) {
      LOGGER.error("Argument numIntervalsToReportAndClearStatistics should be a non-negative number.");
      getDescription();
      return false;
    }
    if (_queueDepth <= 0) {
      LOGGER.error("Argument queueDepth should be a positive number.");
      getDescription();
      return false;
    }

    LOGGER.info("Start query runner targeting broker: {}:{}", _brokerHost, _brokerPort);
    PerfBenchmarkDriverConf conf = new PerfBenchmarkDriverConf();
    conf.setBrokerHost(_brokerHost);
    conf.setBrokerPort(_brokerPort);
    conf.setBrokerURL(_brokerURL);
    conf.setRunQueries(true);
    conf.setStartZookeeper(false);
    conf.setStartController(false);
    conf.setStartBroker(false);
    conf.setStartServer(false);
    conf.setVerbose(_verbose);

    List<String> queries =
        makeQueries(IOUtils.readLines(new FileInputStream(_queryFile)), QueryMode.valueOf(_queryMode.toUpperCase()),
            _queryCount);

    switch (_mode) {
      case "singleThread":
        LOGGER.info("MODE singleThread with queryFile: {}, numTimesToRunQueries: {}, reportIntervalMs: {}, "
                + "numIntervalsToReportAndClearStatistics: {}, timeout: {}", _queryFile, _numTimesToRunQueries,
            _reportIntervalMs, _numIntervalsToReportAndClearStatistics, _timeout);
        singleThreadedQueryRunner(conf, queries, _numTimesToRunQueries, _reportIntervalMs,
            _numIntervalsToReportAndClearStatistics, _timeout);
        break;
      case "multiThreads":
        if (_numThreads <= 0) {
          LOGGER.error("For multiThreads mode, argument numThreads should be a positive number.");
          getDescription();
          break;
        }
        LOGGER.info("MODE multiThreads with queryFile: {}, numTimesToRunQueries: {}, numThreads: {}, "
                + "reportIntervalMs: {}, numIntervalsToReportAndClearStatistics: {}, queueDepth: {}, timeout: {}",
            _queryFile, _numTimesToRunQueries, _numThreads, _reportIntervalMs, _numIntervalsToReportAndClearStatistics,
            _queueDepth, _timeout);
        multiThreadedQueryRunner(conf, queries, _numTimesToRunQueries, _numThreads, _queueDepth, _reportIntervalMs,
            _numIntervalsToReportAndClearStatistics, _timeout);
        break;
      case "targetQPS":
        if (_numThreads <= 0) {
          LOGGER.error("For targetQPS mode, argument numThreads should be a positive number.");
          getDescription();
          break;
        }
        if (_startQPS <= 0 || _startQPS > 1000000.0) {
          LOGGER.error(
              "For targetQPS mode, argument startQPS should be a positive number that less or equal to 1000000.");
          getDescription();
          break;
        }
        LOGGER.info("MODE targetQPS with queryFile: {}, numTimesToRunQueries: {}, numThreads: {}, startQPS: {}, "
                    + "reportIntervalMs: {}, numIntervalsToReportAndClearStatistics: {}, queueDepth: {}, timeout: {}",
            _queryFile, _numTimesToRunQueries, _numThreads, _startQPS, _reportIntervalMs,
            _numIntervalsToReportAndClearStatistics, _queueDepth, _timeout);
        targetQPSQueryRunner(conf, queries, _numTimesToRunQueries, _numThreads, _queueDepth, _startQPS,
            _reportIntervalMs, _numIntervalsToReportAndClearStatistics, _timeout);
        break;
      case "increasingQPS":
        if (_numThreads <= 0) {
          LOGGER.error("For increasingQPS mode, argument numThreads should be a positive number.");
          getDescription();
          break;
        }
        if (_startQPS <= 0 || _startQPS > 1000000.0) {
          LOGGER.error(
              "For increasingQPS mode, argument startQPS should be a positive number that less or equal to 1000000.");
          getDescription();
          break;
        }
        if (_deltaQPS <= 0) {
          LOGGER.error("For increasingQPS mode, argument deltaQPS should be a positive number.");
          getDescription();
          break;
        }
        if (_numIntervalsToIncreaseQPS <= 0) {
          LOGGER.error("For increasingQPS mode, argument numIntervalsToIncreaseQPS should be a positive number.");
          getDescription();
          break;
        }
        LOGGER.info("MODE increasingQPS with queryFile: {}, numTimesToRunQueries: {}, numThreads: {}, startQPS: {}, "
                + "deltaQPS: {}, reportIntervalMs: {}, numIntervalsToReportAndClearStatistics: {}, "
                + "numIntervalsToIncreaseQPS: {}, queueDepth: {}, timeout: {}", _queryFile, _numTimesToRunQueries,
            _numThreads, _startQPS, _deltaQPS, _reportIntervalMs, _numIntervalsToReportAndClearStatistics,
            _numIntervalsToIncreaseQPS, _queueDepth, _timeout);
        increasingQPSQueryRunner(conf, queries, _numTimesToRunQueries, _numThreads, _queueDepth, _startQPS, _deltaQPS,
            _reportIntervalMs, _numIntervalsToReportAndClearStatistics, _numIntervalsToIncreaseQPS, _timeout);
        break;
      default:
        LOGGER.error("Invalid mode: {}", _mode);
        getDescription();
        break;
    }
    return true;
  }

  public static QuerySummary singleThreadedQueryRunner(PerfBenchmarkDriverConf conf, List<String> queries,
      int numTimesToRunQueries, int reportIntervalMs, int numIntervalsToReportAndClearStatistics, long timeout)
      throws Exception {
    return singleThreadedQueryRunner(conf, queries, numTimesToRunQueries, reportIntervalMs,
        numIntervalsToReportAndClearStatistics, timeout, Collections.emptyMap());
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
   * @param timeout timeout in milliseconds for completing all queries.
   * @param headers for the query request, e.g. to carry security token.
   *
   * @return QuerySummary containing final report of query stats
   * @throws Exception
   */
  public static QuerySummary singleThreadedQueryRunner(PerfBenchmarkDriverConf conf, List<String> queries,
      int numTimesToRunQueries, int reportIntervalMs, int numIntervalsToReportAndClearStatistics, long timeout,
      Map<String, String> headers)
      throws Exception {
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    int numQueriesExecuted = 0;
    int numExceptions = 0;
    long totalBrokerTime = 0L;
    long totalClientTime = 0L;
    List<Statistics> statisticsList = Collections.singletonList(new Statistics(CLIENT_TIME_STATISTICS));

    final long startTimeAbsolute = System.currentTimeMillis();
    boolean timeoutReached = false;
    long startTime = System.currentTimeMillis();
    long reportStartTime = startTime;
    int numReportIntervals = 0;
    int numTimesExecuted = 0;

    while (!timeoutReached && (numTimesToRunQueries == 0 || numTimesExecuted < numTimesToRunQueries)) {
      for (String query : queries) {
        if (timeout > 0 && System.currentTimeMillis() - startTimeAbsolute > timeout) {
          LOGGER.info("Timeout of {} sec reached. Aborting", timeout);
          timeoutReached = true;
          break;
        }

        JsonNode response = driver.postQuery(query, headers);
        numQueriesExecuted++;
        long brokerTime = response.get("timeUsedMs").asLong();
        totalBrokerTime += brokerTime;
        long clientTime = response.get("totalTime").asLong();
        totalClientTime += clientTime;
        boolean hasException = !response.get("exceptions").isEmpty();
        numExceptions += hasException ? 1 : 0;
        statisticsList.get(0).addValue(clientTime);

        long currentTime = System.currentTimeMillis();
        if (currentTime - reportStartTime >= reportIntervalMs) {
          long timePassed = currentTime - startTime;
          LOGGER.info("Time Passed: {}ms, Queries Executed: {}, Exceptions: {}, Average QPS: {}, " + "Average "
                      + "Broker Time: {}ms, Average Client Time: {}ms.", timePassed, numQueriesExecuted, numExceptions,
              numQueriesExecuted / ((double) timePassed / MILLIS_PER_SECOND),
              totalBrokerTime / (double) numQueriesExecuted, totalClientTime / (double) numQueriesExecuted);
          reportStartTime = currentTime;
          numReportIntervals++;

          if ((numIntervalsToReportAndClearStatistics != 0) && (numReportIntervals
                                                                == numIntervalsToReportAndClearStatistics)) {
            numReportIntervals = 0;
            startTime = currentTime;
            numQueriesExecuted = 0;
            numExceptions = 0;
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

    QuerySummary querySummary =
        new QuerySummary(timePassed, numQueriesExecuted, numExceptions, totalBrokerTime, totalClientTime,
            statisticsList);
    LOGGER.info("--------------------------------------------------------------------------------");
    LOGGER.info("FINAL REPORT:");
    LOGGER.info(querySummary.toString());
    for (Statistics statistics : statisticsList) {
      statistics.report();
    }

    return querySummary;
  }

  public static QuerySummary multiThreadedQueryRunner(PerfBenchmarkDriverConf conf, List<String> queries,
      int numTimesToRunQueries, int numThreads, int queueDepth, int reportIntervalMs,
      int numIntervalsToReportAndClearStatistics, long timeout)
      throws Exception {
    return multiThreadedQueryRunner(conf, queries, numTimesToRunQueries, numThreads, queueDepth, reportIntervalMs,
        numIntervalsToReportAndClearStatistics, timeout, Collections.emptyMap());
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
   * @param queueDepth queue size limit for query generator
   * @param reportIntervalMs report interval in milliseconds.
   * @param numIntervalsToReportAndClearStatistics number of report intervals to report detailed statistics and clear
   *                                               them, 0 means never.
   * @param timeout timeout in milliseconds for completing all queries
   * @param headers for the query request, e.g. to carry security token.
   *
   * @return QuerySummary containing final report of query stats
   * @throws Exception
   */
  public static QuerySummary multiThreadedQueryRunner(PerfBenchmarkDriverConf conf, List<String> queries,
      int numTimesToRunQueries, int numThreads, int queueDepth, int reportIntervalMs,
      int numIntervalsToReportAndClearStatistics, long timeout, Map<String, String> headers)
      throws Exception {
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    Queue<String> queryQueue = new LinkedBlockingDeque<>(queueDepth);
    AtomicInteger numQueriesExecuted = new AtomicInteger(0);
    AtomicInteger numExceptions = new AtomicInteger(0);
    AtomicLong totalBrokerTime = new AtomicLong(0L);
    AtomicLong totalClientTime = new AtomicLong(0L);
    List<Statistics> statisticsList = Collections.singletonList(new Statistics(CLIENT_TIME_STATISTICS));

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          new Worker(driver, queryQueue, numQueriesExecuted, totalBrokerTime, totalClientTime, numExceptions,
              statisticsList, headers));
    }
    executorService.shutdown();

    final long startTimeAbsolute = System.currentTimeMillis();
    long startTime = System.currentTimeMillis();
    long reportStartTime = startTime;
    int numReportIntervals = 0;
    int numTimesExecuted = 0;
    boolean timeoutReached = false;
    while (!timeoutReached && (numTimesToRunQueries == 0 || numTimesExecuted < numTimesToRunQueries)) {
      if (executorService.isTerminated()) {
        throw new IllegalThreadStateException("All threads got exception and already dead.");
      }

      for (String query : queries) {
        if (timeout > 0 && System.currentTimeMillis() - startTimeAbsolute > timeout) {
          LOGGER.info("Timeout of {} sec reached. Aborting", timeout);
          timeoutReached = true;
          break;
        }
        while (!queryQueue.offer(query)) {
          Thread.sleep(1);
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime - reportStartTime >= reportIntervalMs) {
          long timePassed = currentTime - startTime;
          int numQueriesExecutedInt = numQueriesExecuted.get();
          LOGGER.info("Time Passed: {}ms, Queries Executed: {}, Exceptions: {}, Average QPS: {}, " + "Average "
                      + "Broker Time: {}ms, Average Client Time: {}ms.", timePassed, numQueriesExecutedInt,
              numExceptions.get(), numQueriesExecutedInt / ((double) timePassed / MILLIS_PER_SECOND),
              totalBrokerTime.get() / (double) numQueriesExecutedInt,
              totalClientTime.get() / (double) numQueriesExecutedInt);
          reportStartTime = currentTime;
          numReportIntervals++;

          if ((numIntervalsToReportAndClearStatistics != 0) && (numReportIntervals
                                                                == numIntervalsToReportAndClearStatistics)) {
            numReportIntervals = 0;
            startTime = currentTime;
            reportAndClearStatistics(numQueriesExecuted, numExceptions, totalBrokerTime, totalClientTime,
                statisticsList);
          }
        }
      }
      numTimesExecuted++;
    }

    // Wait for all queries getting executed.
    while (!queryQueue.isEmpty()) {
      Thread.sleep(1);
    }
    executorService.shutdownNow();
    while (!executorService.isTerminated()) {
      Thread.sleep(1);
    }

    long timePassed = System.currentTimeMillis() - startTime;
    QuerySummary querySummary =
        new QuerySummary(timePassed, numQueriesExecuted.get(), numExceptions.get(), totalBrokerTime.get(),
            totalClientTime.get(), statisticsList);
    LOGGER.info("--------------------------------------------------------------------------------");
    LOGGER.info("FINAL REPORT:");
    LOGGER.info(querySummary.toString());
    for (Statistics statistics : statisticsList) {
      statistics.report();
    }

    return querySummary;
  }

  public static QuerySummary targetQPSQueryRunner(PerfBenchmarkDriverConf conf, List<String> queries,
      int numTimesToRunQueries, int numThreads, int queueDepth, double startQPS, int reportIntervalMs,
      int numIntervalsToReportAndClearStatistics, long timeout)
      throws Exception {
    return targetQPSQueryRunner(conf, queries, numTimesToRunQueries, numThreads, queueDepth, startQPS, reportIntervalMs,
        numIntervalsToReportAndClearStatistics, timeout, Collections.emptyMap());
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
   * @param queueDepth queue size limit for query generator
   * @param startQPS start QPS (target QPS).
   * @param reportIntervalMs report interval in milliseconds.
   * @param numIntervalsToReportAndClearStatistics number of report intervals to report detailed statistics and clear
   *                                               them, 0 means never.
   * @param timeout timeout in milliseconds for completing all queries
   * @param headers for the query request, e.g. to carry security token.
   *
   * @return QuerySummary containing final report of query stats
   * @throws Exception
   */
  public static QuerySummary targetQPSQueryRunner(PerfBenchmarkDriverConf conf, List<String> queries,
      int numTimesToRunQueries, int numThreads, int queueDepth, double startQPS, int reportIntervalMs,
      int numIntervalsToReportAndClearStatistics, long timeout, Map<String, String> headers)
      throws Exception {
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    Queue<String> queryQueue = new LinkedBlockingDeque<>(queueDepth);
    AtomicInteger numQueriesExecuted = new AtomicInteger(0);
    AtomicInteger numExceptions = new AtomicInteger(0);
    AtomicLong totalBrokerTime = new AtomicLong(0L);
    AtomicLong totalClientTime = new AtomicLong(0L);
    List<Statistics> statisticsList = Collections.singletonList(new Statistics(CLIENT_TIME_STATISTICS));

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          new Worker(driver, queryQueue, numQueriesExecuted, totalBrokerTime, totalClientTime, numExceptions,
              statisticsList, headers));
    }
    executorService.shutdown();

    final long startTimeAbsolute = System.currentTimeMillis();
    final int queryIntervalNanos = (int) (1E9 / startQPS);
    long startTime = System.currentTimeMillis();
    long reportStartTime = startTime;
    int numReportIntervals = 0;
    int numTimesExecuted = 0;
    boolean timeoutReached = false;
    while (!timeoutReached && (numTimesToRunQueries == 0 || numTimesExecuted < numTimesToRunQueries)) {
      if (executorService.isTerminated()) {
        throw new IllegalThreadStateException("All threads got exception and already dead.");
      }

      long nextQueryNanos = System.nanoTime();
      for (String query : queries) {
        if (timeout > 0 && System.currentTimeMillis() - startTimeAbsolute > timeout) {
          LOGGER.info("Timeout of {} sec reached. Aborting", timeout);
          timeoutReached = true;
          break;
        }
        long nanoTime = System.nanoTime();
        while (nextQueryNanos > nanoTime - NANO_DELTA) {
          Thread.sleep(Math.max((int) ((nextQueryNanos - nanoTime) / 1E6), 1));
          nanoTime = System.nanoTime();
        }

        while (!queryQueue.offer(query)) {
          Thread.sleep(1);
        }

        nextQueryNanos += queryIntervalNanos;

        long currentTime = System.currentTimeMillis();
        if (currentTime - reportStartTime >= reportIntervalMs) {
          long timePassed = currentTime - startTime;
          int numQueriesExecutedInt = numQueriesExecuted.get();
          LOGGER.info("Target QPS: {}, Time Passed: {}ms, Queries Executed: {}, Exceptions: {}, Average QPS: {}, "
                  + "Average Broker Time: {}ms, Average Client Time: {}ms, Queries Queued: {}.", startQPS, timePassed,
              numQueriesExecutedInt, numExceptions.get(),
              numQueriesExecutedInt / ((double) timePassed / MILLIS_PER_SECOND),
              totalBrokerTime.get() / (double) numQueriesExecutedInt,
              totalClientTime.get() / (double) numQueriesExecutedInt, queryQueue.size());
          reportStartTime = currentTime;
          numReportIntervals++;

          if ((numIntervalsToReportAndClearStatistics != 0) && (numReportIntervals
              == numIntervalsToReportAndClearStatistics)) {
            numReportIntervals = 0;
            startTime = currentTime;
            reportAndClearStatistics(numQueriesExecuted, numExceptions, totalBrokerTime, totalClientTime,
                statisticsList);
          }
        }
      }
      numTimesExecuted++;
    }

    // Wait for all queries getting executed.
    while (!queryQueue.isEmpty()) {
      Thread.sleep(1);
    }
    executorService.shutdownNow();
    while (!executorService.isTerminated()) {
      Thread.sleep(1);
    }

    long timePassed = System.currentTimeMillis() - startTime;
    QuerySummary querySummary =
        new QuerySummary(timePassed, numQueriesExecuted.get(), numExceptions.get(), totalBrokerTime.get(),
            totalClientTime.get(), statisticsList);
    LOGGER.info("--------------------------------------------------------------------------------");
    LOGGER.info("FINAL REPORT:");
    LOGGER.info("Target QPS: {}", startQPS);
    LOGGER.info(querySummary.toString());
    for (Statistics statistics : statisticsList) {
      statistics.report();
    }

    return querySummary;
  }

  public static QuerySummary increasingQPSQueryRunner(PerfBenchmarkDriverConf conf, List<String> queries,
      int numTimesToRunQueries, int numThreads, int queueDepth, double startQPS, double deltaQPS, int reportIntervalMs,
      int numIntervalsToReportAndClearStatistics, int numIntervalsToIncreaseQPS, long timeout)
      throws Exception {
    return increasingQPSQueryRunner(conf, queries, numTimesToRunQueries, numThreads, queueDepth, startQPS, deltaQPS,
        reportIntervalMs, numIntervalsToReportAndClearStatistics, numIntervalsToIncreaseQPS, timeout,
        Collections.emptyMap());
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
   * @param queueDepth queue size limit for query generator
   * @param startQPS start QPS.
   * @param deltaQPS delta QPS.
   * @param reportIntervalMs report interval in milliseconds.
   * @param numIntervalsToReportAndClearStatistics number of report intervals to report detailed statistics and clear
   *                                               them, 0 means never.
   * @param timeout timeout in milliseconds for completing all queries.
   * @param numIntervalsToIncreaseQPS number of intervals to increase QPS.
   * @param headers for the query request, e.g. to carry security token.
   *
   * @return QuerySummary containing final report of query stats
   * @throws Exception
   */
  public static QuerySummary increasingQPSQueryRunner(PerfBenchmarkDriverConf conf, List<String> queries,
      int numTimesToRunQueries, int numThreads, int queueDepth, double startQPS, double deltaQPS, int reportIntervalMs,
      int numIntervalsToReportAndClearStatistics, int numIntervalsToIncreaseQPS, long timeout,
      Map<String, String> headers)
      throws Exception {
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    Queue<String> queryQueue = new LinkedBlockingDeque<>(queueDepth);
    AtomicInteger numQueriesExecuted = new AtomicInteger(0);
    AtomicInteger numExceptions = new AtomicInteger(0);
    AtomicLong totalBrokerTime = new AtomicLong(0L);
    AtomicLong totalClientTime = new AtomicLong(0L);
    List<Statistics> statisticsList = Collections.singletonList(new Statistics(CLIENT_TIME_STATISTICS));

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          new Worker(driver, queryQueue, numQueriesExecuted, totalBrokerTime, totalClientTime, numExceptions,
              statisticsList, headers));
    }
    executorService.shutdown();

    final long startTimeAbsolute = System.currentTimeMillis();
    long startTime = System.currentTimeMillis();
    long reportStartTime = startTime;
    int numReportIntervals = 0;
    int numTimesExecuted = 0;
    double currentQPS = startQPS;
    long queryIntervalNanos = (long) (1E9 / currentQPS);
    boolean timeoutReached = false;
    while (!timeoutReached && (numTimesToRunQueries == 0 || numTimesExecuted < numTimesToRunQueries)) {
      if (executorService.isTerminated()) {
        throw new IllegalThreadStateException("All threads got exception and already dead.");
      }

      long nextQueryNanos = System.nanoTime();
      for (String query : queries) {
        if (timeout > 0 && System.currentTimeMillis() - startTimeAbsolute > timeout) {
          LOGGER.info("Timeout of {} sec reached. Aborting", timeout);
          timeoutReached = true;
          break;
        }
        long nanoTime = System.nanoTime();
        while (nextQueryNanos > nanoTime - NANO_DELTA) {
          Thread.sleep(Math.max((int) ((nextQueryNanos - nanoTime) / 1E6), 1));
          nanoTime = System.nanoTime();
        }

        while (!queryQueue.offer(query)) {
          Thread.sleep(1);
        }

        nextQueryNanos += queryIntervalNanos;

        long currentTime = System.currentTimeMillis();
        if (currentTime - reportStartTime >= reportIntervalMs) {
          long timePassed = currentTime - startTime;
          reportStartTime = currentTime;
          numReportIntervals++;

          if (numReportIntervals == numIntervalsToIncreaseQPS) {
            // Find the next interval.
            LOGGER.info("--------------------------------------------------------------------------------");
            LOGGER.info("REPORT FOR TARGET QPS: {}", currentQPS);
            int numQueriesExecutedInt = numQueriesExecuted.get();
            LOGGER.info("Current Target QPS: {}, Time Passed: {}ms, Queries Executed: {}, Exceptions: {}, "
                    + "Average QPS: {}, Average Broker Time: {}ms, Average Client Time: {}ms, Queries Queued: {}.",
                currentQPS, timePassed, numQueriesExecutedInt, numExceptions.get(),
                numQueriesExecutedInt / ((double) timePassed / MILLIS_PER_SECOND),
                totalBrokerTime.get() / (double) numQueriesExecutedInt,
                totalClientTime.get() / (double) numQueriesExecutedInt, queryQueue.size());
            numReportIntervals = 0;
            startTime = currentTime;
            reportAndClearStatistics(numQueriesExecuted, numExceptions, totalBrokerTime, totalClientTime,
                statisticsList);

            currentQPS += deltaQPS;
            queryIntervalNanos = (long) (1E9 / currentQPS);
            LOGGER.info("Increase target QPS to: {}, the following statistics are for the new target QPS.", currentQPS);
          } else {
            int numQueriesExecutedInt = numQueriesExecuted.get();
            LOGGER.info("Current Target QPS: {}, Time Passed: {}ms, Queries Executed: {}, Average QPS: {}, "
                    + "Average Broker Time: {}ms, Average Client Time: {}ms, Queries Queued: {}.", currentQPS,
                timePassed,
                numQueriesExecutedInt, numQueriesExecutedInt / ((double) timePassed / MILLIS_PER_SECOND),
                totalBrokerTime.get() / (double) numQueriesExecutedInt,
                totalClientTime.get() / (double) numQueriesExecutedInt, queryQueue.size());

            if ((numIntervalsToReportAndClearStatistics != 0) && (
                numReportIntervals % numIntervalsToReportAndClearStatistics == 0)) {
              startTime = currentTime;
              reportAndClearStatistics(numQueriesExecuted, numExceptions, totalBrokerTime, totalClientTime,
                  statisticsList);
            }
          }
        }
      }
      numTimesExecuted++;
    }

    // Wait for all queries getting executed.
    while (!queryQueue.isEmpty()) {
      Thread.sleep(1);
    }
    executorService.shutdownNow();
    while (!executorService.isTerminated()) {
      Thread.sleep(1);
    }

    long timePassed = System.currentTimeMillis() - startTime;
    QuerySummary querySummary =
        new QuerySummary(timePassed, numQueriesExecuted.get(), numExceptions.get(), totalBrokerTime.get(),
            totalClientTime.get(), statisticsList);
    LOGGER.info("--------------------------------------------------------------------------------");
    LOGGER.info("FINAL REPORT:");
    LOGGER.info("Current Target QPS: {}", currentQPS);
    LOGGER.info(querySummary.toString());
    for (Statistics statistics : statisticsList) {
      statistics.report();
    }

    return querySummary;
  }

  private static List<String> makeQueries(List<String> queries, QueryMode queryMode, int queryCount) {
    int numQueries = queries.size();
    switch (queryMode) {
      case FULL:
        if (queryCount > 0 && queryCount < numQueries) {
          return queries.subList(0, queryCount);
        } else {
          return queries;
        }
      case RESAMPLE:
        Preconditions.checkArgument(queryCount > 0, "Query count must be positive for RESAMPLE mode");
        Random random = new Random(0); // anything deterministic will do
        List<String> resampledQueries = new ArrayList<>(queryCount);
        for (int i = 0; i < queryCount; i++) {
          resampledQueries.add(queries.get(random.nextInt(numQueries)));
        }
        return resampledQueries;
      default:
        throw new IllegalArgumentException(String.format("Unsupported queryMode '%s", queryMode));
    }
  }

  private static void reportAndClearStatistics(AtomicInteger numQueriesExecuted, AtomicInteger numExceptions,
      AtomicLong totalBrokerTime, AtomicLong totalClientTime, List<Statistics> statisticsList) {
    numQueriesExecuted.set(0);
    numExceptions.set(0);
    totalBrokerTime.set(0L);
    totalClientTime.set(0L);
    for (Statistics statistics : statisticsList) {
      statistics.report();
      statistics.clear();
    }
  }

  private static void executeQueryInMultiThreads(PerfBenchmarkDriver driver, String query,
      AtomicInteger numQueriesExecuted, AtomicLong totalBrokerTime, AtomicLong totalClientTime,
      AtomicInteger numExceptions, List<Statistics> statisticsList, Map<String, String> headers)
      throws Exception {
    JsonNode response = driver.postQuery(query, headers);
    numQueriesExecuted.getAndIncrement();
    long brokerTime = response.get("timeUsedMs").asLong();
    totalBrokerTime.getAndAdd(brokerTime);
    long clientTime = response.get("totalTime").asLong();
    totalClientTime.getAndAdd(clientTime);
    boolean hasException = !response.get("exceptions").isEmpty();
    numExceptions.getAndAdd(hasException ? 1 : 0);

    statisticsList.get(0).addValue(clientTime);
  }

  private static class Worker implements Runnable {
    private final PerfBenchmarkDriver _driver;
    private final Queue<String> _queryQueue;
    private final AtomicInteger _numQueriesExecuted;
    private final AtomicLong _totalBrokerTime;
    private final AtomicLong _totalClientTime;
    private final AtomicInteger _numExceptions;
    private final List<Statistics> _statisticsList;
    private final Map<String, String> _headers;

    private Worker(PerfBenchmarkDriver driver, Queue<String> queryQueue, AtomicInteger numQueriesExecuted,
        AtomicLong totalBrokerTime, AtomicLong totalClientTime, AtomicInteger numExceptions,
        List<Statistics> statisticsList, Map<String, String> headers) {
      _driver = driver;
      _queryQueue = queryQueue;
      _numQueriesExecuted = numQueriesExecuted;
      _totalBrokerTime = totalBrokerTime;
      _totalClientTime = totalClientTime;
      _numExceptions = numExceptions;
      _statisticsList = statisticsList;
      _headers = headers;
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
              _numExceptions, _statisticsList, _headers);
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

    public double getPercentile(double p) {
      return _statistics.getPercentile(p);
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

  public static class QuerySummary {
    private final long _timePassed;
    private final int _numQueriesExecuted;
    private final int _numExceptions;
    private final double _avgQps;
    private final double _avgBrokerTime;
    private final double _avgClientTime;
    private final List<Statistics> _statisticsList;

    private QuerySummary(long timePassed, int numQueriesExecuted, int numExceptions, long totalBrokerTime,
        long totalClientTime, List<Statistics> statisticsList) {
      _timePassed = timePassed;
      _numQueriesExecuted = numQueriesExecuted;
      _numExceptions = numExceptions;
      _avgQps = numQueriesExecuted / ((double) timePassed / MILLIS_PER_SECOND);
      _avgBrokerTime = totalBrokerTime / (double) numQueriesExecuted;
      _avgClientTime = totalClientTime / (double) numQueriesExecuted;
      _statisticsList = statisticsList;
    }

    public long getTimePassed() {
      return _timePassed;
    }

    public int getNumQueriesExecuted() {
      return _numQueriesExecuted;
    }

    public int getNumExceptions() {
      return _numExceptions;
    }

    public double getAvgQps() {
      return _avgQps;
    }

    public double getAvgBrokerTime() {
      return _avgBrokerTime;
    }

    public double getAvgClientTime() {
      return _avgClientTime;
    }

    public double getPercentile(double p) {
      if (_statisticsList == null || _statisticsList.size() == 0) {
        return 0.0;
      }

      // the last run's statistics is used;
      return _statisticsList.get(_statisticsList.size() - 1).getPercentile(p);
    }


    public List<Statistics> getStatisticsList() {
      return _statisticsList;
    }

    @Override
    public String toString() {
      return String.format("Time Passed: %sms\nQueries Executed: %s\nExceptions: %s\n"
              + "Average QPS: %s\nAverage Broker Time: %sms\nAverage Client Time: %sms", _timePassed,
          _numQueriesExecuted,
          _numExceptions, _avgQps, _avgBrokerTime, _avgClientTime);
    }
  }

  public static void main(String[] args)
      throws Exception {
    QueryRunner queryRunner = new QueryRunner();
    CommandLine commandLine = new CommandLine(queryRunner);
    commandLine.parseArgs(args);
    if (queryRunner._help) {
      queryRunner.getDescription();
    } else {
      queryRunner.execute();
    }
  }
}
