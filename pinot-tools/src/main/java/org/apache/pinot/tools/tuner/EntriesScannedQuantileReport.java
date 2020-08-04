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
import org.apache.pinot.tools.tuner.query.src.LogInputIteratorImpl;
import org.apache.pinot.tools.tuner.query.src.parser.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.strategy.QuantileAnalysisImpl;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A command to scan through broker log (containing time of execution, numEntriesScannedInFilter, numEntriesScannedPostFilter) and give percentile of numEntriesScannedInFilter
 */
public class EntriesScannedQuantileReport extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(EntriesScannedQuantileReport.class);

  @Option(name = "-log", required = true, metaVar = "<String>", usage = "Path to broker log file.")
  private String _brokerLog;

  @Option(name = "-tables", required = false, usage = "Comma separated list of table names to work on without type (unset run on all tables)")
  private String _tableNamesWithoutType = null;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h",
      "--help"}, usage = "Print this message.")
  private boolean _help;

  @Override
  public boolean execute() throws Exception {
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
    LOGGER.info("\nTables{}\n", tableNamesWithoutTypeStr);


    TunerDriver fitModel = new TunerDriver().setThreadPoolSize(Runtime.getRuntime().availableProcessors() - 1)
        .setTuningStrategy(new QuantileAnalysisImpl.Builder().setTableNamesWithoutType(tableNamesWithoutType).build())
        .setInputIterator(new LogInputIteratorImpl.Builder()
            .setParser(new BrokerLogParserImpl())
            .setPath(_brokerLog)
            .build());
    fitModel.execute();
    return true;
  }

  @Override
  public String description() {
    return "Scan  through broker log (containing time of execution, numEntriesScannedInFilter, numEntriesScannedPostFilter) and give percentile of numEntriesScannedInFilter";
  }

  @Override
  public String getName() {
    return "EntriesScannedQuantileReport";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }
}
