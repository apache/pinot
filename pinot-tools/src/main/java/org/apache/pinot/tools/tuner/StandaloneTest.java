package org.apache.pinot.tools.tuner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.tools.tuner.driver.StandaloneDriver;
import org.apache.pinot.tools.tuner.meta.manager.JsonFileMetaManagerImpl;
import org.apache.pinot.tools.tuner.query.src.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.query.src.LogFileSrcImpl;
import org.apache.pinot.tools.tuner.query.src.ServerLogParserImpl;
import org.apache.pinot.tools.tuner.strategy.FrequencyImpl;
import org.apache.pinot.tools.tuner.strategy.OLSAnalysisImpl;
import org.apache.pinot.tools.tuner.strategy.ParserBasedImpl;


public class StandaloneTest extends StandaloneDriver {
  public static void main(String[] args) {
    StandaloneDriver parserBased=new StandaloneTest()
        .setThreadPoolSize(3)
        .setStrategy(new ParserBasedImpl.Builder()._algorithmOrder(ParserBasedImpl.FIRST_ORDER)._numEntriesScannedThreshold(ParserBasedImpl.NO_IN_FILTER_THRESHOLD).build())
        .setQuerySrc(new LogFileSrcImpl.Builder()._standaloneLog(true)._parser(new BrokerLogParserImpl())._path("/Users/jiaguo/Downloads/2factorfit_f/bidSuggestion_OFFLINE/bidSuggestion_noindex.log").build())
        .setMetaManager(new JsonFileMetaManagerImpl.Builder()._path("/Users/jiaguo/Workspace/pinot-tuna-script/data/meta/prodAll/col_meta")._use_existing_index(JsonFileMetaManagerImpl.DONT_USE_EXISTING_INDEX).build());
    parserBased.excute();

//    StandaloneDriver freqBased=new StandaloneTest()
//        .setThreadPoolSize(3)
//        .setStrategy(new FrequencyImpl.Builder()._numEntriesScannedThreshold(ParserBasedImpl.NO_IN_FILTER_THRESHOLD).build())
//        .setQuerySrc(new LogFileSrcImpl.Builder()._parser(new BrokerLogParserImpl())._path("/Users/jiaguo/scin_v2_additive.broker.log").build())
//        .setMetaManager(new JsonFileMetaManagerImpl.Builder()._path("/Users/jiaguo/Workspace/pinot-tuna-script/data/meta/scin_v2_additive/col_meta")._use_existing_index(JsonFileMetaManagerImpl.USE_EXISTING_INDEX).build());
//    freqBased.excute();

//    StandaloneDriver fitModel=new StandaloneTest()
//        .setThreadPoolSize(3)
//        .setStrategy(new OLSAnalysisImpl.Builder()._numEntriesScannedThreshold(ParserBasedImpl.NO_IN_FILTER_THRESHOLD).build())
//        .setQuerySrc(new LogFileSrcImpl.Builder()._standaloneLog(true)._parser(new ServerLogParserImpl())._path("/Users/jiaguo/Downloads/2factorfit/bidSuggestion_OFFLINE/server.bidSuggestion_noindex.log").build())
//        .setMetaManager(new JsonFileMetaManagerImpl.Builder()._path("/Users/jiaguo/Workspace/pinot-tuna-script/data/meta/prodAll/col_meta.json")._use_existing_index(JsonFileMetaManagerImpl.USE_EXISTING_INDEX).build());
//    fitModel.excute();
  }
}