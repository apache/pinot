package org.apache.pinot.tools.tuner;

import com.google.gson.Gson;
import java.math.BigInteger;
import java.util.HashMap;
import org.apache.pinot.tools.tuner.driver.TunerDriver;
import org.apache.pinot.tools.tuner.meta.manager.JsonFileMetaManagerImpl;
import org.apache.pinot.tools.tuner.query.src.parser.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.query.src.LogQuerySrcImpl;
import org.apache.pinot.tools.tuner.strategy.ParserBasedImpl;


public class TunerTest extends TunerDriver {
  public static void main(String[] args) {

//    TunerDriver parserBased = new TunerTest().setThreadPoolSize(3).setStrategy(
//        new ParserBasedImpl.Builder().setAlgorithmOrder(1)
//            .setNumEntriesScannedThreshold(ParserBasedImpl.NO_IN_FILTER_THRESHOLD).build()).setQuerySrc(
//        new LogQuerySrcImpl.Builder().setValidLineBeginnerRegex(LogQuerySrcImpl.REGEX_VALID_LINE_STANDALONE)
//            .setParser(new BrokerLogParserImpl()).setPath(
//            "/Users/jiaguo/Workspace/pinot-tuna-script/data/logs/logs_2019-06-28/lor1-app11412.prod.linkedin.com/logs/pinot-broker.log.2019-06-26")
//            .build())
//        .setMetaManager(new JsonFileMetaManagerImpl.Builder()
//            .setPath("/Users/jiaguo/Workspace/pinot-tuna-script/data/meta/prodAll/col_meta")
//            .setUseExistingIndex(JsonFileMetaManagerImpl.DONT_USE_EXISTING_INDEX).build());
//    parserBased.execute();

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