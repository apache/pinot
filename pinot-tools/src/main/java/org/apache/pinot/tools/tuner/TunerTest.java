/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.tuner;

import java.io.FileNotFoundException;
import org.apache.pinot.tools.tuner.driver.TunerDriver;
import org.apache.pinot.tools.tuner.meta.manager.JsonFileMetaManagerImpl;
import org.apache.pinot.tools.tuner.query.src.LogInputIteratorImpl;
import org.apache.pinot.tools.tuner.query.src.parser.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.strategy.ParserBasedImpl;


/**
 * TODO: This is test and will be deleted
 */
public class TunerTest extends TunerDriver {
  public static void main(String[] args) throws FileNotFoundException {
//    TunerDriver metaFetch = new TunerTest().setThreadPoolSize(3)
//        .setTuningStrategy(new SegmentMetadataCollector.Builder()
//            .setTableNamesWithoutType(new HashSet<String>() {{
//              add("scin_v2_additive");
//            }})
//            .setOutputDir("/Users/jiaguo/tmp3")
//            .build())
//        .setInputIterator(new CompressedFilePathIter.Builder()
//            .set_directory("/Users/jiaguo/Workspace/pinot-tuna-script/data/segments")
//            .build())
//        .setMetaManager(null);
//    metaFetch.execute();

//    TunerDriver parserBased = new TunerTest().setThreadPoolSize(3)
//        .setTuningStrategy(new ParserBasedImpl.Builder().setAlgorithmOrder(ParserBasedImpl.THIRD_ORDER)
//            .setNumEntriesScannedThreshold(ParserBasedImpl.DEFAULT_NUM_ENTRIES_IN_FILTER_THRESHOLD)
//            .setNumQueriesThreshold(ParserBasedImpl.DEFAULT_NUM_QUERIES_THRESHOLD)
//            .build())
//        .setInputIterator(new LogInputIteratorImpl.Builder().setValidLinePrefixRegex(LogInputIteratorImpl.REGEX_VALID_LINE_TIME)
//            .setParser(new BrokerLogParserImpl()).setPath("/Users/jiaguo/finalTestData/broker.audienceCount.log")
//            .build())
//        .setMetaManager(
//            new JsonFileMetaManagerImpl.Builder().setPath("/Users/jiaguo/finalTestData/meta/prodMetaV2/metadata.json")
//                .setUseExistingIndex(JsonFileMetaManagerImpl.DONT_USE_EXISTING_INDEX)
//                .build());
//    parserBased.execute();

    TunerDriver parserBased = new TunerDriver().setThreadPoolSize(Runtime.getRuntime().availableProcessors() - 1)
        .setTuningStrategy(new ParserBasedImpl.Builder().setTableNamesWithoutType(null)
            .setNumQueriesThreshold(0).setAlgorithmOrder(ParserBasedImpl.FIRST_ORDER)
            .setNumEntriesScannedThreshold(0)
            .build()).setInputIterator(new LogInputIteratorImpl.Builder().setParser(new BrokerLogParserImpl())
            .setPath("/Users/jiaguo/finalTestData/originalBrokerLogs/suForecasting/broker.offsiteForecasting.log")
            .build())
        .setMetaManager(new JsonFileMetaManagerImpl.Builder().setUseExistingIndex(
            JsonFileMetaManagerImpl.USE_EXISTING_INDEX) //Delete after demo
            .setPath("/Users/jiaguo/finalTestData/meta/prodMetaV2/metadata.json").build());
    parserBased.execute();
////
//    TunerDriver freqBased=new TunerTest()
//        .setThreadPoolSize(3)
//        .setStrategy(new FrequencyImpl.Builder()._numEntriesScannedThreshold(ParserBasedImpl.NO_IN_FILTER_THRESHOLD).build())
//        .setInputIterator(new LogFileSrcImpl.Builder()._parser(new BrokerLogParserImpl()).setPath("/Users/jiaguo/scin_v2_additive.broker.log").build())
//        .setMetaManager(new JsonFileMetaManagerImpl.Builder().setPath("/Users/jiaguo/Workspace/pinot-tuna-script/data/meta/scin_v2_additive/col_meta").useExistingIndex(JsonFileMetaManagerImpl.USE_EXISTING_INDEX).build());
//    freqBased.execute();

//    TunerDriver fitModel = new TunerTest().setThreadPoolSize(3).setTuningStrategy(new QuantileAnalysisImpl.Builder().build())
//        .setInputIterator(new LogInputIteratorImpl.Builder().setValidLinePrefixRegex(LogInputIteratorImpl.REGEX_VALID_LINE_TIME)
//            .setParser(new BrokerLogParserImpl()).setPath("/Users/jiaguo/finalTestData/originalBrokerLogs/suForecasting/broker.suForecasting.log")
//            .build());
//    fitModel.execute();
  }
}