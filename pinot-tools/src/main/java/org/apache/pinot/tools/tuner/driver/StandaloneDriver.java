package org.apache.pinot.tools.tuner.driver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.BasicQueryStats;
import org.apache.pinot.tools.tuner.query.src.QuerySrc;
import org.apache.pinot.tools.tuner.strategy.BasicMergerObj;
import org.apache.pinot.tools.tuner.strategy.BasicStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Local concurrent executor.
 */
public abstract class StandaloneDriver extends TunerDriver {
  protected static final Logger LOGGER = LoggerFactory.getLogger(StandaloneDriver.class);
  public static final int NO_CONCURRENCY = 0;
  private int _threadPoolSize = 0;

  /**
   * set the number of threads used in action
   * @param threadPoolSize
   * @return
   */
  protected StandaloneDriver setThreadPoolSize(int threadPoolSize) {
    _threadPoolSize = threadPoolSize;
    return this;
  }

  /**
   * Set the query source, e.g. LogFileSrcImpl
   * @param querySrc e.g. LogFileSrcImpl
   * @return
   */
  public StandaloneDriver setQuerySrc(QuerySrc querySrc) {
    _querySrc = querySrc;
    return this;
  }

  /**
   * set the metaManager for caching and query cardinality e.g. MetaManager
   * @param metaManager e.g. MetaManager
   * @return
   */
  public StandaloneDriver setMetaManager(MetaManager metaManager) {
    _metaManager = metaManager;
    return this;
  }

  /**
   * set the strategy for the recommendation, e.g. FrequencyImpl, OLSAnalysisImpl, ParserBasedImpl
   * @param strategy
   * @return
   */
  public StandaloneDriver setStrategy(BasicStrategy strategy) {
    _strategy = strategy;
    return this;
  }

  private Map<Long, Map<String, Map<String, BasicMergerObj>>> _threadAccumulator = null;
  private Map<String, Map<String, BasicMergerObj>> _mergedResults;

  /**
   * Execute strategy
   */
  @Override
  public void excute() {
    // Accumulate all the query results to _threadAccumulator:/threadID/table/column/BasicMergerObj
    _threadAccumulator = new HashMap<>();
    LOGGER.info("Setting up executor for accumulation: {} threads", this._threadPoolSize);
    ThreadPoolExecutor accumulateExecutor = null;
    // setup threadpool, NO_CONCURRENCY for debugging
    if (_threadPoolSize != NO_CONCURRENCY) {
      accumulateExecutor = new ThreadPoolExecutor(this._threadPoolSize, this._threadPoolSize, 365, TimeUnit.DAYS,
          new LinkedBlockingQueue<>(Integer.MAX_VALUE), new ThreadPoolExecutor.CallerRunsPolicy());
    }
    while (_querySrc.hasNext()) {
      BasicQueryStats basicQueryStats = _querySrc.next();
      if (basicQueryStats != null && _strategy.filter(basicQueryStats)) {
        LOGGER.debug("Master thread {} submitting: {}", Thread.currentThread().getId(), basicQueryStats.toString());
        if (_threadPoolSize != NO_CONCURRENCY) {
          accumulateExecutor.execute(() -> {
            long threadID = Thread.currentThread().getId();
            LOGGER.debug("Thread {} accumulating: {}", threadID, basicQueryStats.toString());
            _threadAccumulator.putIfAbsent(threadID, new HashMap<>());
            _strategy.accumulator(basicQueryStats, _metaManager, _threadAccumulator.get(threadID));
          });
        } else {
          long threadID = Thread.currentThread().getId();
          LOGGER.debug("Thread {} accumulating: {}", threadID, basicQueryStats.toString());
          _threadAccumulator.putIfAbsent(threadID, new HashMap<>());
          _strategy.accumulator(basicQueryStats, _metaManager, _threadAccumulator.get(threadID));
        }
      }
    }
    if (_threadPoolSize != NO_CONCURRENCY) {
      accumulateExecutor.shutdown();
      LOGGER.info("All queries waiting for accumulation");
      try {
        accumulateExecutor.awaitTermination(365, TimeUnit.DAYS);
      } catch (InterruptedException e) {
        LOGGER.error(e.toString());
      }
      LOGGER.info("All accumulation done");
    }

    // Merge corresponding entries
    LOGGER.info("Setting up _mergedResults for merging");
    _mergedResults = new HashMap<>();
    for (Map.Entry<Long, Map<String, Map<String, BasicMergerObj>>> threadEntry : _threadAccumulator.entrySet()) {
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
                    _strategy.merger(_mergedResults.get(tableNameWithoutType).get(colName), mergerObj);
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
                  _strategy.merger(_mergedResults.get(tableNameWithoutType).get(colName), mergerObj);
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
    for (Map.Entry<String, Map<String, BasicMergerObj>> tableStat : _mergedResults.entrySet()) {
      _strategy.reporter(tableStat.getKey(), tableStat.getValue());
    }
  }
}
