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
import java.util.Arrays;
import java.util.HashSet;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.tuner.driver.TunerDriver;
import org.apache.pinot.tools.tuner.meta.manager.collector.AccumulateStats;
import org.apache.pinot.tools.tuner.meta.manager.collector.CompressedFilePathIter;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CollectSegmentMetadata extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectSegmentMetadata.class);

  @Option(name = "-out", required = true, metaVar = "<String>", usage = "An empty directory to work on, for tmp files and output metadata.json file，must have r/w access")
  private String _workDir;

  @Option(name = "-segments", required = true, metaVar = "<String>", usage = "The directory, which contains tableNamesWithoutType/{tarred segments}")
  private String _segmentsDir;

  @Option(name = "-tables", required = false, usage = "Comma separated list of table names to work on without type (unset to run on all tables)")
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

    try {
      TunerDriver metaFetch = new TunerDriver().setThreadPoolSize(Runtime.getRuntime().availableProcessors() - 1)
          .setTuningStrategy(new AccumulateStats.Builder().setTableNamesWithoutType(tableNamesWithoutType)
              .setOutputDir(_workDir)
              .build())
          .setQuerySrc(new CompressedFilePathIter.Builder().setDirectory(_segmentsDir).build())
          .setMetaManager(null);
      metaFetch.execute();
    } catch (FileNotFoundException e) {
      LOGGER.error("Invalid tarred segments dir: {}", _segmentsDir);
      return false;
    }
    return true;
  }

  @Override
  public String description() {
    return "A tool to extract and pack metadata and index info to a json file, from tarred segments.";
  }

  @Override
  public String getName() {
    return "CollectSegmentMetadata";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }
}
