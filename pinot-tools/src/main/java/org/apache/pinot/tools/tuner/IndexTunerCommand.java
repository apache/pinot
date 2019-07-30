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
import org.apache.pinot.tools.tuner.strategy.ParserBasedImpl;
import org.kohsuke.args4j.Option;


public class IndexTunerCommand extends AbstractBaseCommand implements Command {
  private static final long DEFAULT_NUM_ENTRIES_SCANNED_THRESHOLD = 0;
  private static final long DEFAULT_NUM_QUERIES_TO_GIVE_RECOMMENDATION = 0;

  private static final String INVERTED_INDEX = "inverted";
  private static final String SORTED_INDEX = "sorted";

  @Option(name = "-metaData", required = true, metaVar = "<String>", usage = "Path to packed metadata file (json).")
  private String _metaData;

  @Option(name = "-brokerLog", required = true, metaVar = "<String>", usage = "Path to broker log file.")
  private String _brokerLog;

  @Option(name = "-strategy", required = true, metaVar = "<inverted/sorted>", usage = "Select execution strategy.")
  private String _strategy;

  @Option(name = "-entriesScannedThreshold", required = false, metaVar = "<long>", usage = "Log lines with numEntriesScannedInFilter below this threshold will be excluded.")
  private long _numEntriesScannedThreshold = DEFAULT_NUM_ENTRIES_SCANNED_THRESHOLD;

  @Option(name = "-queriesToReport", required = false, metaVar = "<long>", usage = "Tables with log lines scanned threshold will be excluded.")
  private long _numQueriesToGiveRecommendation = DEFAULT_NUM_QUERIES_TO_GIVE_RECOMMENDATION;

  @Option(name = "-tables", required = false, usage = "Comma separated list of table names to work on without type (unset run on all tables)")
  private String _tableNamesWithoutType = null;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help;

  @Override
  public boolean execute() {
    HashSet<String> tableNamesWithoutType = new HashSet<>();
    if (_tableNamesWithoutType != null && !_tableNamesWithoutType.trim().equals("")) {
      tableNamesWithoutType.addAll(Arrays.asList(_tableNamesWithoutType.split(",")));
    }

    if (_strategy.equals(INVERTED_INDEX)) {
      TunerDriver parserBased = new TunerDriver().setThreadPoolSize(Runtime.getRuntime().availableProcessors() - 1)
          .setStrategy(new ParserBasedImpl.Builder()
              .setTableNamesWithoutType(tableNamesWithoutType)
              .setNumProcessedThreshold(_numQueriesToGiveRecommendation)
              .setAlgorithmOrder(ParserBasedImpl.FIRST_ORDER)
              .setNumEntriesScannedThreshold(_numEntriesScannedThreshold)
              .build())
          .setQuerySrc(new LogQuerySrcImpl.Builder()
              .setParser(new BrokerLogParserImpl())
              .setPath(_brokerLog)
              .build())
          .setMetaManager(new JsonFileMetaManagerImpl.Builder()
              .setPath(_metaData)
              .build());
      parserBased.execute();
    } else if (_strategy.equals(SORTED_INDEX)) {
      TunerDriver parserBased = new TunerDriver().setThreadPoolSize(Runtime.getRuntime().availableProcessors() - 1)
          .setStrategy(new ParserBasedImpl.Builder()
              .setTableNamesWithoutType(tableNamesWithoutType)
              .setNumProcessedThreshold(_numQueriesToGiveRecommendation)
              .setAlgorithmOrder(ParserBasedImpl.SECOND_ORDER)
              .setNumEntriesScannedThreshold(_numEntriesScannedThreshold)
              .build())
          .setQuerySrc(new LogQuerySrcImpl.Builder()
              .setParser(new BrokerLogParserImpl())
              .setPath(_brokerLog)
              .build())
          .setMetaManager(new JsonFileMetaManagerImpl.Builder()
              .setPath(_metaData)
              .build());
      parserBased.execute();
    } else {
      return false;
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