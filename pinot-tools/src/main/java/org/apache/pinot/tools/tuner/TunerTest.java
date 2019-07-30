package org.apache.pinot.tools.tuner;

import org.apache.pinot.tools.tuner.driver.TunerDriver;
import org.apache.pinot.tools.tuner.meta.manager.JsonFileMetaManagerImpl;
import org.apache.pinot.tools.tuner.query.src.LogQuerySrcImpl;
import org.apache.pinot.tools.tuner.query.src.parser.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.strategy.ParserBasedImpl;


/**
 * TODO: This is test and will be deleted
 */
public class TunerTest extends TunerDriver {
  public static void main(String[] args) {
//    TunerDriver metaFetch = new TunerTest().setThreadPoolSize(3)
//        .setTuningStrategy(new AccumulateStats.Builder()
//            .setTableNamesWithoutType(new HashSet<String>() {{
//              add("scin_v2_additive");
//            }})
//            .setOutputDir("/Users/jiaguo/tmp3")
//            .build())
//        .setQuerySrc(new CompressedFilePathIter.Builder()
//            .set_directory("/Users/jiaguo/Workspace/pinot-tuna-script/data/segments")
//            .build())
//        .setMetaManager(null);
//    metaFetch.execute();

    TunerDriver parserBased = new TunerTest().setThreadPoolSize(3)
        .setTuningStrategy(new ParserBasedImpl.Builder().setAlgorithmOrder(ParserBasedImpl.FIRST_ORDER)
            .setNumEntriesScannedThreshold(ParserBasedImpl.DEFAULT_NUM_ENTRIES_IN_FILTER_THRESHOLD)
            .setNumQueriesThreshold(ParserBasedImpl.DEFAULT_NUM_QUERIES_THRESHOLD)
            .build())
        .setQuerySrc(new LogQuerySrcImpl.Builder().setValidLinePrefixRegex(LogQuerySrcImpl.REGEX_VALID_LINE_TIME)
            .setParser(new BrokerLogParserImpl())
            .setPath("/Users/jiaguo/finalTestData/broker.audienceCount.log")
            .build())
        .setMetaManager(
            new JsonFileMetaManagerImpl.Builder().setPath("/Users/jiaguo/finalTestData/meta/prodMetaV2/metadata.json")
                .setUseExistingIndex(JsonFileMetaManagerImpl.DONT_USE_EXISTING_INDEX)
                .build());
    parserBased.execute();

//    TunerDriver freqBased=new TunerTest()
//        .setThreadPoolSize(3)
//        .setStrategy(new FrequencyImpl.Builder()._numEntriesScannedThreshold(ParserBasedImpl.NO_IN_FILTER_THRESHOLD).build())
//        .setQuerySrc(new LogFileSrcImpl.Builder()._parser(new BrokerLogParserImpl()).setPath("/Users/jiaguo/scin_v2_additive.broker.log").build())
//        .setMetaManager(new JsonFileMetaManagerImpl.Builder().setPath("/Users/jiaguo/Workspace/pinot-tuna-script/data/meta/scin_v2_additive/col_meta").useExistingIndex(JsonFileMetaManagerImpl.USE_EXISTING_INDEX).build());
//    freqBased.execute();

//    TunerDriver fitModel = new TunerTest().setThreadPoolSize(3).setStrategy(new OLSAnalysisImpl.Builder().build())
//        .setQuerySrc(new LogQuerySrcImpl.Builder().setValidLinePrefixRegex(LogQuerySrcImpl.REGEX_VALID_LINE_TIME)
//            .setParser(new BrokerLogParserImpl()).setPath("/Users/jiaguo/broker/pinot-broker.log.2019-07-25")
//            .build());
//    fitModel.execute();
  }
}