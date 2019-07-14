package org.apache.pinot.tools.tuner.driver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.tools.tuner.query.src.BasicQueryStats;
import org.apache.pinot.tools.tuner.strategy.ColumnStatsObj;


/*
 * Local concurrent driver
 */
public abstract class StandaloneDriver extends TunerDriver {

  int _coreSize = 0;
  private Map<Long, Map<String, Map<String, ColumnStatsObj>>> _threadAccumulator=null;
  private Map<String, Map<String, ColumnStatsObj>> _mergedResults;

  public StandaloneDriver setCoreSize(int coreSize) {
    _coreSize = coreSize;
    return this;
  }

  /*
   * Execute strategy
   */
  @Override
  public void excute() {
    /*
     * Accumulate all the queries to threadAccumulator:/threadID/table/column
     */
    _threadAccumulator = new HashMap<>();
    ThreadPoolExecutor accumulateExecutor = new ThreadPoolExecutor(this._coreSize, this._coreSize, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(Integer.MAX_VALUE), new ThreadPoolExecutor.CallerRunsPolicy());

    while (_querySrc.hasNext()) {
      BasicQueryStats basicQueryStats = _querySrc.next();
      if (_strategy.filter(basicQueryStats)) {
        accumulateExecutor.execute(() -> {
          long threadID = Thread.currentThread().getId();
          _threadAccumulator.putIfAbsent(threadID, new HashMap<>());
          _strategy.accumulator(basicQueryStats, _metaManager, _threadAccumulator.get(threadID));
        });
      }
    }
    accumulateExecutor.shutdown();
    try {
      accumulateExecutor.awaitTermination(24, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      LOGGER.error(e.toString());
    }

    /*
     * Merge corresponding entries
     */
    _mergedResults=new HashMap<>();
    for(Map.Entry<Long,Map<String, Map<String, ColumnStatsObj>>> threadEntry: _threadAccumulator.entrySet()){
      for (String tableNameWithType: threadEntry.getValue().keySet()){
        _mergedResults.putIfAbsent(tableNameWithType,new HashMap<>());
      }
    }
    ThreadPoolExecutor mergeExecutor = new ThreadPoolExecutor(this._coreSize, this._coreSize, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(Integer.MAX_VALUE), new ThreadPoolExecutor.CallerRunsPolicy());
    for(String tableNameWithType: _mergedResults.keySet()){
      mergeExecutor.execute(() -> {
        for (Map.Entry<Long,Map<String, Map<String, ColumnStatsObj>>> tableEntries: _threadAccumulator.entrySet()){
          for(Map.Entry<String, ColumnStatsObj> columnEntry: tableEntries.getValue().getOrDefault(tableNameWithType, new HashMap<>()).entrySet()){
            try {
              _mergedResults.get(tableNameWithType).putIfAbsent(columnEntry.getKey(), columnEntry.getValue().getClass().newInstance());
            }
            catch (Exception e){
              LOGGER.error("Instantiation Exception in Merger!");
              LOGGER.error(e.toString());
            }
            _strategy.merger(_mergedResults.get(tableNameWithType).get(columnEntry.getKey()), columnEntry.getValue());
          }
        }
      });
    }
    mergeExecutor.shutdown();
    try {
      mergeExecutor.awaitTermination(24, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      LOGGER.error(e.toString());
    }

    /*
     * Report
     */
  }
}
