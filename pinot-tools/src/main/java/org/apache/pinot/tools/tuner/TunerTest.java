package org.apache.pinot.tools.tuner;

import java.util.HashSet;
import org.apache.pinot.tools.tuner.driver.TunerDriver;
import org.apache.pinot.tools.tuner.meta.manager.JsonFileMetaManagerImpl;
import org.apache.pinot.tools.tuner.meta.manager.collector.AccumulateStats;
import org.apache.pinot.tools.tuner.meta.manager.collector.CompressedFilePathIter;
import org.apache.pinot.tools.tuner.query.src.LogQuerySrcImpl;
import org.apache.pinot.tools.tuner.query.src.parser.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.strategy.ParserBasedImpl;


public class TunerTest extends TunerDriver {
  public static void main(String[] args) {
//    HashSet<String> filter = new HashSet<>();
//    filter.add("scin_v2_additive");
//    TunerDriver metaFetch = new TunerTest()
//        .setThreadPoolSize(3)
//        .setStrategy(
//            new AccumulateStats.Builder()
//                .setTableNamesWithoutType(filter)
//                .setOutputDir("/Users/jiaguo/tmp3")
//                .build())
//        .setQuerySrc(
//            new CompressedFilePathIter.Builder()
//                .set_directory("/Users/jiaguo/Workspace/pinot-tuna-script/data/segments")
//                .build())
//        .setMetaManager(null);
//    metaFetch.execute();

    TunerDriver parserBased = new TunerTest().setThreadPoolSize(3).setStrategy(
        new ParserBasedImpl.Builder().setAlgorithmOrder(ParserBasedImpl.FIRST_ORDER)
            .setNumEntriesScannedThreshold(ParserBasedImpl.NO_IN_FILTER_THRESHOLD)
            .setNumProcessedThreshold(ParserBasedImpl.NO_PROCESSED_THRESH).build()).setQuerySrc(
        new LogQuerySrcImpl.Builder().setValidLineBeginnerRegex(LogQuerySrcImpl.REGEX_VALID_LINE_TIME)
            .setParser(new BrokerLogParserImpl()).setPath("/Users/jiaguo/finalTestData/broker.scin_v2_additive.log")
            .build()).setMetaManager(new JsonFileMetaManagerImpl.Builder()
        .setPath("/Users/jiaguo/Workspace/pinot-tuna-script/data/meta/scin_v2_additive/col_meta")
        .setUseExistingIndex(JsonFileMetaManagerImpl.DONT_USE_EXISTING_INDEX).build());
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