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

import java.util.Arrays;
import java.util.HashSet;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.tuner.driver.TunerDriver;
import org.apache.pinot.tools.tuner.meta.manager.JsonFileMetaManagerImpl;
import org.apache.pinot.tools.tuner.query.src.LogQuerySrcImpl;
import org.apache.pinot.tools.tuner.query.src.parser.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.strategy.FrequencyImpl;
import org.apache.pinot.tools.tuner.strategy.ParserBasedImpl;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IndexTunerCommand extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexTunerCommand.class);

  private static final long DEFAULT_NUM_ENTRIES_SCANNED_THRESHOLD = 0;
  private static final long DEFAULT_NUM_QUERIES_TO_GIVE_RECOMMENDATION = 0;
  public static final int DEFAULT_SELECTIVITY_THRESHOLD = 1;

  private static final String INVERTED_INDEX = "inverted";
  private static final String SORTED_INDEX = "sorted";
  private static final String STRATEGY_PARSER_BASED = "parser";

  @Option(name = "-metaData", required = true, metaVar = "<String>", usage = "Path to packed metadata file (json), CollectSegmentMetadata can be used to create this.")
  private String _metaData;

  @Option(name = "-brokerLog", required = true, metaVar = "<String>", usage = "Path to broker log file.")
  private String _brokerLog;

  @Option(name = "-indexType", required = true, metaVar = "<inverted/sorted>", usage = "Select target index.")
  private String _indexType;

  @Option(name = "-strategy", required = true, metaVar = "<freq/parser>", usage = "Select tuning strategy.")
  private String _strategy;

  @Option(name = "-selectivityThreshold", required = false, metaVar = "<long>", usage = "Selectivity threshold (>1), default to 1, ")
  private int _selectivityThreshold = DEFAULT_SELECTIVITY_THRESHOLD;

  @Option(name = "-entriesScannedThreshold", required = false, metaVar = "<long>", usage = "Log lines with numEntriesScannedInFilter below this threshold will be excluded.")
  private long _numEntriesScannedThreshold = DEFAULT_NUM_ENTRIES_SCANNED_THRESHOLD;

  @Option(name = "-numQueriesThreshold", required = false, metaVar = "<long>", usage = "Tables with log lines scanned threshold will be excluded.")
  private long _numQueriesThreshold = DEFAULT_NUM_QUERIES_TO_GIVE_RECOMMENDATION;

  @Option(name = "-tables", required = false, usage = "Comma separated list of table names to work on without type (unset run on all tables)")
  private String _tableNamesWithoutType = null;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h",
      "--help"}, usage = "Print this message.")
  private boolean _help;

  @Override
  public boolean execute() {
    HashSet<String> tableNamesWithoutType = new HashSet<>();
    if (_tableNamesWithoutType != null && !_tableNamesWithoutType.trim().equals("")) {
      tableNamesWithoutType.addAll(Arrays.asList(_tableNamesWithoutType.split(",")));
    }
    String tableNamesWithoutTypeStr;
    if (tableNamesWithoutType.isEmpty()) {
      tableNamesWithoutTypeStr = "All tables";
    } else {
      tableNamesWithoutTypeStr = tableNamesWithoutType.toString();
    }
    LOGGER.info("Index: {}\nstrategy: {}\nmetadata file: {}\nbroker log: {}\ntables{}\n", _indexType, _strategy,
        _metaData, _brokerLog,
        tableNamesWithoutTypeStr);

    if (_selectivityThreshold < 1) {
      _selectivityThreshold = 1;
    }

    if (_strategy.equals(STRATEGY_PARSER_BASED)) {
      if (_indexType.equals(INVERTED_INDEX)) {
        TunerDriver parserBased = new TunerDriver().setThreadPoolSize(Runtime.getRuntime().availableProcessors() - 1)
            .setTuningStrategy(new ParserBasedImpl.Builder().setTableNamesWithoutType(tableNamesWithoutType)
                .setNumQueriesThreshold(_numQueriesThreshold)
                .setAlgorithmOrder(ParserBasedImpl.FIRST_ORDER)
                .setNumEntriesScannedThreshold(_numEntriesScannedThreshold)
                .setSelectivityThreshold(_selectivityThreshold)
                .build())
            .setQuerySrc(new LogQuerySrcImpl.Builder().setParser(new BrokerLogParserImpl()).setPath(_brokerLog).build())
            .setMetaManager(new JsonFileMetaManagerImpl.Builder().setPath(_metaData).build());
        parserBased.execute();
      } else if (_indexType.equals(SORTED_INDEX)) {
        TunerDriver parserBased = new TunerDriver().setThreadPoolSize(Runtime.getRuntime().availableProcessors() - 1)
            .setTuningStrategy(new ParserBasedImpl.Builder().setTableNamesWithoutType(tableNamesWithoutType)
                .setNumQueriesThreshold(_numQueriesThreshold)
                .setAlgorithmOrder(ParserBasedImpl.THIRD_ORDER)
                .setNumEntriesScannedThreshold(_numEntriesScannedThreshold)
                .setSelectivityThreshold(_selectivityThreshold)
                .build())
            .setQuerySrc(new LogQuerySrcImpl.Builder().setParser(new BrokerLogParserImpl()).setPath(_brokerLog).build())
            .setMetaManager(new JsonFileMetaManagerImpl.Builder().setPath(_metaData).build());
        parserBased.execute();
      } else {
        return false;
      }
    } else {
      if (_indexType == SORTED_INDEX) {
        LOGGER.error("Simple frequency strategy is for inverted index only!");
        return false;
      }
      TunerDriver freqBased = new TunerTest().setThreadPoolSize(3)
          .setTuningStrategy(new FrequencyImpl.Builder().setNumQueriesThreshold(_numQueriesThreshold)
              .setNumEntriesScannedThreshold(_numEntriesScannedThreshold)
              .setTableNamesWithoutType(tableNamesWithoutType).setCardinalityThreshold(_selectivityThreshold)
              .build())
          .setQuerySrc(new LogQuerySrcImpl.Builder().setParser(new BrokerLogParserImpl()).setPath(_brokerLog).build())
          .setMetaManager(new JsonFileMetaManagerImpl.Builder().setPath(_metaData).build());
      freqBased.execute();
    }
    return true;
  }

  @Override
  public String description() {
    return "Give optimization boundary analysis and indexing recommendation to specific tables, based on packed segment metadata (containing the weighted sum of cardinality, number of documents, number of entries, etc.) and broker logs (containing time of execution, numEntriesScannedInFilter, numEntriesScannedPostFilter, query text).";
  }

  @Override
  public String getName() {
    return "IndexTunerCommand";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }
}