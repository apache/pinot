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
    Map<Long, Map<String, Map<String, ColumnStatsObj>>> threadAccumulator=new HashMap<>();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        this._coreSize,
        this._coreSize,
        60,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(Integer.MAX_VALUE),
        new ThreadPoolExecutor.CallerRunsPolicy());

    executor.setKeepAliveTime(60, TimeUnit.SECONDS);

    while(this._querySrc.hasNext()){
      BasicQueryStats basicQueryStats=this._querySrc.next();
      if(this._strategy.filter(basicQueryStats)) {
        executor.execute(() -> {
          long threadID = Thread.currentThread().getId();
          this._strategy.accumulator(basicQueryStats, this._metaManager, threadAccumulator.getOrDefault(threadID, new HashMap<>()));
        });
      }
    }
    executor.shutdown();
    try{
      executor.awaitTermination(24, TimeUnit.HOURS);
    }
    catch (InterruptedException e){
      LOGGER.error(e.getMessage());
    }

    /*
     * Merge corresponding entries
     */

  }
}
