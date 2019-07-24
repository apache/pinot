package org.apache.pinot.tools.tuner;

import org.apache.pinot.tools.tuner.driver.TunerDriver;
import org.apache.pinot.tools.tuner.meta.manager.JsonFileMetaManagerImpl;
import org.apache.pinot.tools.tuner.query.src.parser.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.query.src.LogFileSrcImpl;
import org.apache.pinot.tools.tuner.strategy.ParserBasedImpl;


public class TunerTest extends TunerDriver {
  public static void main(String[] args) {
    TunerDriver parserBased = new TunerTest().setThreadPoolSize(3).setStrategy(
        new ParserBasedImpl.Builder().setAlgorithmOrder(1)
            .setNumEntriesScannedThreshold(ParserBasedImpl.NO_IN_FILTER_THRESHOLD).build()).setQuerySrc(
        new LogFileSrcImpl.Builder().setValidLineBeginnerRegex(LogFileSrcImpl.REGEX_VALID_LINE_STANDALONE)
            .setParser(new BrokerLogParserImpl())
            .setPath("/Users/jiaguo/Downloads/2factorfit_f/bidSuggestion_OFFLINE/bidSuggestion_noindex.log").build())
        .setMetaManager(new JsonFileMetaManagerImpl.Builder()
            .setPath("/Users/jiaguo/Workspace/pinot-tuna-script/data/meta/prodAll/col_meta")
            .useExistingIndex(JsonFileMetaManagerImpl.DONT_USE_EXISTING_INDEX).build());
    parserBased.execute();

//    TunerDriver freqBased=new TunerTest()
//        .setThreadPoolSize(3)
//        .setStrategy(new FrequencyImpl.Builder()._numEntriesScannedThreshold(ParserBasedImpl.NO_IN_FILTER_THRESHOLD).build())
//        .setQuerySrc(new LogFileSrcImpl.Builder()._parser(new BrokerLogParserImpl()).setPath("/Users/jiaguo/scin_v2_additive.broker.log").build())
//        .setMetaManager(new JsonFileMetaManagerImpl.Builder().setPath("/Users/jiaguo/Workspace/pinot-tuna-script/data/meta/scin_v2_additive/col_meta").useExistingIndex(JsonFileMetaManagerImpl.USE_EXISTING_INDEX).build());
//    freqBased.execute();

//    TunerDriver fitModel=new TunerTest()
//        .setThreadPoolSize(3)
//        .setStrategy(new OLSAnalysisImpl.Builder()._numEntriesScannedThreshold(ParserBasedImpl.NO_IN_FILTER_THRESHOLD).build())
//        .setQuerySrc(new LogFileSrcImpl.Builder()._standaloneLog(true)._parser(new ServerLogParserImpl()).setPath("/Users/jiaguo/Downloads/2factorfit/bidSuggestion_OFFLINE/server.bidSuggestion_noindex.log").build())
//        .setMetaManager(new JsonFileMetaManagerImpl.Builder().setPath("/Users/jiaguo/Workspace/pinot-tuna-script/data/meta/prodAll/col_meta.json").useExistingIndex(JsonFileMetaManagerImpl.USE_EXISTING_INDEX).build());
//    fitModel.execute();
  }
}