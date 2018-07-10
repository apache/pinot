/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.perf;

import com.linkedin.pinot.tools.perf.PerfBenchmarkDriver;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>PerfBenchmarkRunner</code> class is a tool to start Pinot cluster with optional preloaded segments.
 */
public class PerfBenchmarkRunner {
  private PerfBenchmarkRunner() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PerfBenchmarkRunner.class);

  /**
   * Start Pinot server with pre-loaded segments.
   *
   * @param dataDir data directory.
   * @param tableNames list of table names to be loaded.
   * @param invertedIndexColumns list of inverted index columns.
   * @throws Exception
   */
  public static void startServerWithPreLoadedSegments(String dataDir, List<String> tableNames,
      List<String> invertedIndexColumns)
      throws Exception {
    LOGGER.info("Starting server and uploading segments.");
    PerfBenchmarkDriver driver = PerfBenchmarkDriver.startComponents(false, false, false, true, dataDir);
    for (String tableName : tableNames) {
      com.linkedin.pinot.tools.perf.PerfBenchmarkRunner.loadTable(driver, dataDir, tableName, invertedIndexColumns);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length > 0) {
      if (args[0].equalsIgnoreCase("startAllButServer") || args[0].equalsIgnoreCase("startAll")) {
        PerfBenchmarkDriver.startComponents(true, true, true, false, null);
      }

      if (args[0].equalsIgnoreCase("startServerWithPreLoadedSegments") || args[0].equalsIgnoreCase("startAll")) {
        String tableNames = args[1];
        String dataDir = args[2];
        List<String> invertedIndexColumns = null;
        if (args.length == 4) {
          invertedIndexColumns = Arrays.asList(args[3].split(","));
        }
        startServerWithPreLoadedSegments(dataDir, Arrays.asList(tableNames.split(",")), invertedIndexColumns);
      }
    } else {
      LOGGER.error("Expected one of [startAll|startAllButServer|StartServerWithPreLoadedSegments]");
    }
  }
}
