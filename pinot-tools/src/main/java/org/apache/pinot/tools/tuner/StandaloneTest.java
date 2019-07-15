package org.apache.pinot.tools.tuner;

import org.apache.pinot.tools.tuner.driver.StandaloneDriver;
import org.apache.pinot.tools.tuner.meta.manager.JsonFileMetaManagerImpl;
import org.apache.pinot.tools.tuner.query.src.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.query.src.IndexSuggestQueryStatsImpl;
import org.apache.pinot.tools.tuner.query.src.LogFileSrcImpl;
import org.apache.pinot.tools.tuner.strategy.ParserBasedImpl;


public class StandaloneTest extends StandaloneDriver {
  public static void main(String[] args) {
    StandaloneTest standaloneTest=(StandaloneTest)new StandaloneTest()
        .setCoreSize(1)
        .setStrategy(new ParserBasedImpl.Builder()._algorithmOrder(1)._numEntriesScannedThreshold(0).build())
        .setQuerySrc(new LogFileSrcImpl.Builder()._parser(new BrokerLogParserImpl())._path("/Users/jiaguo/Downloads/pinot-broker.log.2019-07-09").build())
        .setMetaManager(new JsonFileMetaManagerImpl.Builder()._path("/Users/jiaguo/Downloads/col_meta")._use_existing_index(JsonFileMetaManagerImpl.USE_EXISTING_INDEX).build());
    standaloneTest.excute();
  }
}
