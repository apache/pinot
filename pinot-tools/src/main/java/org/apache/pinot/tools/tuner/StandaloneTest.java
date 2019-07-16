package org.apache.pinot.tools.tuner;

import org.apache.pinot.tools.tuner.driver.StandaloneDriver;
import org.apache.pinot.tools.tuner.meta.manager.JsonFileMetaManagerImpl;
import org.apache.pinot.tools.tuner.query.src.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.query.src.LogFileSrcImpl;
import org.apache.pinot.tools.tuner.strategy.ParserBasedImpl;


public class StandaloneTest extends StandaloneDriver {
  public static void main(String[] args) {
    StandaloneTest standaloneTest=(StandaloneTest)new StandaloneTest()
        .setCoreSize(3)
        .setStrategy(new ParserBasedImpl.Builder()._algorithmOrder(ParserBasedImpl.FIRST_ORDER)._numEntriesScannedThreshold(ParserBasedImpl.NO_IN_FILTER_THRESHOLD).build())
        .setQuerySrc(new LogFileSrcImpl.Builder()._parser(new BrokerLogParserImpl())._path("/Users/jiaguo/scin_v2_additive.broker.log").build())
        .setMetaManager(new JsonFileMetaManagerImpl.Builder()._path("/Users/jiaguo/Workspace/pinot-tuna-script/data/meta/scin_v2_additive/col_meta")._use_existing_index(JsonFileMetaManagerImpl.USE_EXISTING_INDEX).build());
    standaloneTest.excute();
  }
}
