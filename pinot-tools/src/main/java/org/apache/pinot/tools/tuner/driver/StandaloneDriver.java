package org.apache.pinot.tools.tuner.driver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.BasicQueryStats;
import org.apache.pinot.tools.tuner.query.src.QuerySrc;
import org.apache.pinot.tools.tuner.strategy.BasicStrategy;
import org.apache.pinot.tools.tuner.strategy.MergerObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * Local concurrent driver
 */
public abstract class StandaloneDriver extends TunerDriver {
  protected static final Logger LOGGER = LoggerFactory.getLogger(StandaloneDriver.class);

  private int _coreSize = 0;

  protected StandaloneDriver setCoreSize(int coreSize) {
    _coreSize = coreSize;
    return this;
  }

  public StandaloneDriver setQuerySrc(QuerySrc querySrc) {
    _querySrc = querySrc;
    return this;
  }

  public StandaloneDriver setMetaManager(MetaManager metaManager) {
    _metaManager = metaManager;
    return this;
  }

  public StandaloneDriver setStrategy(BasicStrategy strategy) {
    _strategy = strategy;
    return this;
  }


  private Map<Long, Map<String, Map<String, MergerObj>>> _threadAccumulator = null;
  private Map<String, Map<String, MergerObj>> _mergedResults;

  /*
   * Execute strategy
   */
  @Override
  public void excute() {
    /*
     * Accumulate all the queries to threadAccumulator:/threadID/table/column
     */
    _threadAccumulator = new HashMap<>();
    LOGGER.debug("Setting up executor for accumulation: {} threads",this._coreSize);
    ThreadPoolExecutor accumulateExecutor = new ThreadPoolExecutor(this._coreSize, this._coreSize, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(Integer.MAX_VALUE), new ThreadPoolExecutor.CallerRunsPolicy());

    while (_querySrc.hasNext()) {
      BasicQueryStats basicQueryStats = _querySrc.next();
      if (basicQueryStats!=null && _strategy.filter(basicQueryStats)) {
        LOGGER.debug("Master thread {} submitting: {}",Thread.currentThread().getId(),basicQueryStats.toString());
        accumulateExecutor.execute(() -> {
          long threadID = Thread.currentThread().getId();
          LOGGER.debug("Thread {} accumulating: {}", threadID, basicQueryStats.toString());
          _threadAccumulator.putIfAbsent(threadID, new HashMap<>());
          _strategy.accumulator(basicQueryStats, _metaManager, _threadAccumulator.get(threadID));
        });
      }
    }
    accumulateExecutor.shutdown();
    LOGGER.debug("All queries waiting for accumulation");
    try {
      accumulateExecutor.awaitTermination(24, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      LOGGER.error(e.toString());
    }
    LOGGER.debug("All accumulation done");

    /*
     * Merge corresponding entries
     */
    LOGGER.debug("Setting up _mergedResults for merging");
    _mergedResults = new HashMap<>();
    for (Map.Entry<Long, Map<String, Map<String, MergerObj>>> threadEntry : _threadAccumulator.entrySet()) {
      for (String tableNameWithType : threadEntry.getValue().keySet()) {
        _mergedResults.putIfAbsent(tableNameWithType, new HashMap<>());
      }
    }
    LOGGER.debug("tableNames: {}", _mergedResults.keySet().toString());

    LOGGER.debug("Setting up executor for merging: {} threads",this._coreSize);
    ThreadPoolExecutor mergeExecutor = new ThreadPoolExecutor(this._coreSize, this._coreSize, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(Integer.MAX_VALUE), new ThreadPoolExecutor.CallerRunsPolicy());
    for (String tableNameWithType : _mergedResults.keySet()) {
      mergeExecutor.execute(() -> {
        LOGGER.debug("Thread {} working on table {}",Thread.currentThread().getId(), tableNameWithType);
        for (Map.Entry<Long, Map<String, Map<String, MergerObj>>> tableEntries : _threadAccumulator.entrySet()) {
          for (Map.Entry<String, MergerObj> columnEntry : tableEntries.getValue()
              .getOrDefault(tableNameWithType, new HashMap<>()).entrySet()) {
            try {
              _mergedResults.get(tableNameWithType)
                  .putIfAbsent(columnEntry.getKey(), columnEntry.getValue().getClass().newInstance());
            } catch (Exception e) {
              LOGGER.error("Instantiation Exception in Merger!");
              LOGGER.error(e.toString());
            }
            _strategy.merger(_mergedResults.get(tableNameWithType).get(columnEntry.getKey()), columnEntry.getValue());
          }
        }
      });
    }
    LOGGER.debug("All tables waiting for merge");
    mergeExecutor.shutdown();
    try {
      mergeExecutor.awaitTermination(24, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      LOGGER.error(e.toString());
    }
    LOGGER.debug("All merge done");
    /*
     * Report
     */
    for (Map.Entry<String, Map<String, MergerObj>> tableStat: _mergedResults.entrySet()){
      _strategy.reporter(tableStat.getKey(),tableStat.getValue());
    }
  }
}
