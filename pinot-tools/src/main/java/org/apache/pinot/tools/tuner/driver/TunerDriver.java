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
package org.apache.pinot.tools.tuner.driver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.QuerySrc;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.apache.pinot.tools.tuner.strategy.AbstractAccumulator;
import org.apache.pinot.tools.tuner.strategy.Strategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *TunerDriver is an executable interface, has three pluggable modules:
 *   MetaData Manager: a manger for MetaManager, which is an interface to access segment metadata.
 *   QuerySrc: an iterator interface over input source, has a pluggable AbstractQueryParser, who parses each item in input source, and returns AbstractQueryStats, a wrapper of relevant fields input.
 *   Strategy, which has four user defined functions operating on a map of Map<Long, Map<String, Map<String, AbstractMergerObj>>>:
 *                                                                             |		       |				    |				     |
 *                                                                        ThreadID	   TableName	  ColumnName		  Abstract object of stats for a column
 *       Filter: A function to filter AbstractQueryStats, by table name, number of entries scanned in filters, number of entries scanned post filter, etc. The relevant AbstractQueryStats will be feed to Accumulator.
 *       Accumulator: A function to process AbstractQueryStats and MetaManager; then accumulate stats to corresponding AbstractMergerObj entry.
 *       Merger: A function to merge two AbstractMergerObj entries having the same TableName/ColumnName from different threads.
 *       Reporter: A function to postprocess and print(email) out the final results of a table.
 */
public class TunerDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(TunerDriver.class);
  public static final int NO_CONCURRENCY = 0;

  private QuerySrc _querySrc = null;
  private MetaManager _metaManager = null;
  private Strategy _strategy = null;
  private int _threadPoolSize = 0;

  /**
   * Set the number of threads used in action
   * @param threadPoolSize The number of threads used in action
   * @return this
   */
  public TunerDriver setThreadPoolSize(int threadPoolSize) {
    _threadPoolSize = threadPoolSize;
    return this;
  }

  /**
   * Set the query source, e.g. LogFileSrcImpl
   * @param querySrc E.g. LogFileSrcImpl
   * @return this
   */
  public TunerDriver setQuerySrc(QuerySrc querySrc) {
    _querySrc = querySrc;
    return this;
  }

  /**
   * Set the metaManager for caching and query cardinality e.g. MetaManager
   * @param metaManager E.g. MetaManager
   * @return this
   */
  public TunerDriver setMetaManager(MetaManager metaManager) {
    _metaManager = metaManager;
    return this;
  }

  /**
   * Set the strategy for the recommendation, e.g. FrequencyImpl, OLSAnalysisImpl, ParserBasedImpl
   * @param strategy E.g. ParserBasedImpl
   * @return this
   */
  public TunerDriver setStrategy(Strategy strategy) {
    _strategy = strategy;
    return this;
  }

  private Map<Long, Map<String, Map<String, AbstractAccumulator>>> _threadAccumulator = null;
  private Map<String, Map<String, AbstractAccumulator>> _mergedResults;

  /**
   * Execute strategy
   */
  public void execute() {
    // Accumulate all the query results to _threadAccumulator:/threadID/table/column/AbstractMergerObj
    _threadAccumulator = new HashMap<>();
    LOGGER.info("Setting up executor for accumulation: {} threads", this._threadPoolSize);
    ThreadPoolExecutor accumulateExecutor = null;
    // setup threadpool, NO_CONCURRENCY for debugging
    if (_threadPoolSize != NO_CONCURRENCY) {
      accumulateExecutor = new ThreadPoolExecutor(this._threadPoolSize, this._threadPoolSize, 365, TimeUnit.DAYS,
          new LinkedBlockingQueue<>(Integer.MAX_VALUE), new ThreadPoolExecutor.CallerRunsPolicy());
    }
    while (_querySrc.hasNext()) {
      AbstractQueryStats abstractQueryStats = _querySrc.next();
      if (abstractQueryStats != null && _strategy.filter(abstractQueryStats)) {
        LOGGER.debug("Master thread {} submitting: {}", Thread.currentThread().getId(), abstractQueryStats.toString());
        if (_threadPoolSize != NO_CONCURRENCY) {
          accumulateExecutor.execute(() -> {
            long threadID = Thread.currentThread().getId();
            LOGGER.debug("Thread {} accumulating: {}", threadID, abstractQueryStats.toString());
            _threadAccumulator.putIfAbsent(threadID, new HashMap<>());
            _strategy.accumulate(abstractQueryStats, _metaManager, _threadAccumulator.get(threadID));
          });
        } else {
          long threadID = Thread.currentThread().getId();
          LOGGER.debug("Thread {} accumulating: {}", threadID, abstractQueryStats.toString());
          _threadAccumulator.putIfAbsent(threadID, new HashMap<>());
          _strategy.accumulate(abstractQueryStats, _metaManager, _threadAccumulator.get(threadID));
        }
      }
    }
    if (_threadPoolSize != NO_CONCURRENCY) {
      accumulateExecutor.shutdown();
      LOGGER.info("All queries queued for accumulation");
      try {
        accumulateExecutor.awaitTermination(365, TimeUnit.DAYS);
      } catch (InterruptedException e) {
        LOGGER.error(e.toString());
      }
      LOGGER.info("All accumulation done");
    }

    // Merge corresponding entries
    LOGGER.info("Setting up mergedResults for merging");
    _mergedResults = new HashMap<>();
    for (Map.Entry<Long, Map<String, Map<String, AbstractAccumulator>>> threadEntry : _threadAccumulator.entrySet()) {
      for (String tableNameWithoutType : threadEntry.getValue().keySet()) {
        _mergedResults.putIfAbsent(tableNameWithoutType, new HashMap<>());
      }
    }
    LOGGER.info("tableNames: {}", _mergedResults.keySet().toString());

    LOGGER.info("Setting up executor for merging: {} threads", this._threadPoolSize);
    ThreadPoolExecutor mergeExecutor = null;
    if (_threadPoolSize != NO_CONCURRENCY) {
      mergeExecutor = new ThreadPoolExecutor(this._threadPoolSize, this._threadPoolSize, 365, TimeUnit.DAYS,
          new LinkedBlockingQueue<>(Integer.MAX_VALUE), new ThreadPoolExecutor.CallerRunsPolicy());
    }
    for (String tableNameWithoutType : _mergedResults.keySet()) {
      if (_threadPoolSize != NO_CONCURRENCY) {
        mergeExecutor.execute(() -> {
          LOGGER.debug("Thread {} working on table {}", Thread.currentThread().getId(), tableNameWithoutType);
          _threadAccumulator.forEach(
              (threadID, threadAccumulator) -> threadAccumulator.getOrDefault(tableNameWithoutType, new HashMap<>())
                  .forEach((colName, mergerObj) -> {
                    try {
                      _mergedResults.get(tableNameWithoutType).putIfAbsent(colName, mergerObj.getClass().newInstance());
                    } catch (Exception e) {
                      LOGGER.error("Instantiation Exception in Merger!");
                      LOGGER.error(e.toString());
                    }
                    _strategy.merge(_mergedResults.get(tableNameWithoutType).get(colName), mergerObj);
                  }));
        });
      } else {
        _threadAccumulator.forEach(
            (threadID, threadAccumulator) -> threadAccumulator.getOrDefault(tableNameWithoutType, new HashMap<>())
                .forEach((colName, mergerObj) -> {
                  try {
                    _mergedResults.get(tableNameWithoutType).putIfAbsent(colName, mergerObj.getClass().newInstance());
                  } catch (Exception e) {
                    LOGGER.error("Instantiation Exception in Merger!");
                    LOGGER.error(e.toString());
                  }
                  _strategy.merge(_mergedResults.get(tableNameWithoutType).get(colName), mergerObj);
                }));
      }
    }
    if (_threadPoolSize != NO_CONCURRENCY) {
      LOGGER.info("All tables waiting for merge");
      mergeExecutor.shutdown();
      try {
        mergeExecutor.awaitTermination(365, TimeUnit.DAYS);
      } catch (InterruptedException e) {
        LOGGER.error(e.toString());
      }
      LOGGER.info("All merge done");
    }

    //Report
    _strategy.report(_mergedResults);
  }
}
