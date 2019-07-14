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

  @Override
  public void excute() {
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
      executor.execute(() -> {
        long threadID=Thread.currentThread().getId();
        if(!threadAccumulator.containsKey(threadID)){
          threadAccumulator.put(threadID,new HashMap<>());
        }
        this._strategy.accumulator(basicQueryStats, this._metaManager, threadAccumulator.get(threadID));
      });
    }
    executor.shutdown();
    try{
      executor.awaitTermination(24, TimeUnit.HOURS);
    }
    catch (InterruptedException e){
      LOGGER.error(e.getMessage());
    }

  }
}
