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
package org.apache.pinot.tools.perf;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@SuppressWarnings("FieldCanBeLocal")
@CommandLine.Command(name = "PerfBanchmarkRunner", description = "Start Pinot cluster with optional preloaded "
                                                                 + "segments.", mixinStandardHelpOptions = true)
public class PerfBenchmarkRunner extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(PerfBenchmarkRunner.class);

  @CommandLine.Option(names = {"-mode"}, required = true,
      description = "Mode of the PerfBenchmarkRunner (startAll|startAllButServer|startServerWithPreLoadedSegments).")
  private String _mode;

  @CommandLine.Option(names = {"-dataDir"}, required = false,
      description = "Path to directory containing un-tarred segments.")
  private String _dataDir;

  @CommandLine.Option(names = {"-tempDir"}, required = false,
      description = "Path to temporary directory to start the cluster")
  private String _tempDir = "/tmp/";

  @CommandLine.Option(names = {"-loadMode"}, required = false, description = "Load mode of the segments (HEAP|MMAP).")
  private String _loadMode = "HEAP";

  @CommandLine.Option(names = {"-segmentFormatVersion"}, required = false,
      description = "Segment format version to be loaded (v1|v3).")
  private String _segmentFormatVersion;

  @CommandLine.Option(names = {"-batchLoad"}, required = false, description = "Batch load multiple tables.")
  private boolean _isBatchLoad;

  @CommandLine.Option(names = {"-numThreads"}, required = false,
      description = "Number of threads for batch load (default 10).")
  private int _numThreads = 10;

  @CommandLine.Option(names = {"-timeoutInSeconds"}, required = false,
      description = "Timeout in seconds for batch load (default 60).")
  private int _timeoutInSeconds = 60;

  @CommandLine.Option(names = {"-tableNames"}, required = false,
      description = "Comma separated table names with types to be loaded (non-batch load).")
  private String _tableNames;

  @CommandLine.Option(names = {"-invertedIndexColumns"}, required = false,
      description = "Comma separated inverted index columns to be created (non-batch load).")
  private String _invertedIndexColumns;

  @CommandLine.Option(names = {"-bloomFilterColumns"}, required = false,
      description = "Comma separated bloom filter columns to be created (non-batch load).")
  private String _bloomFilterColumns;

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public boolean execute()
      throws Exception {
    if (_mode.equalsIgnoreCase("startAll") || _mode.equalsIgnoreCase("startAllButServer")) {
      startAllButServer();
    }
    if (_mode.equalsIgnoreCase("startAll") || _mode.equalsIgnoreCase("startServerWithPreLoadedSegments")) {
      startServerWithPreLoadedSegments();
    }
    return true;
  }

  public void startAllButServer()
      throws Exception {
    PerfBenchmarkDriverConf perfBenchmarkDriverConf = new PerfBenchmarkDriverConf();
    perfBenchmarkDriverConf.setStartServer(false);
    PerfBenchmarkDriver driver =
        new PerfBenchmarkDriver(perfBenchmarkDriverConf, _tempDir, _loadMode, _segmentFormatVersion, false);
    driver.run();
  }

  private void startServerWithPreLoadedSegments()
      throws Exception {
    PerfBenchmarkDriverConf perfBenchmarkDriverConf = new PerfBenchmarkDriverConf();
    perfBenchmarkDriverConf.setStartZookeeper(false);
    perfBenchmarkDriverConf.setStartController(false);
    perfBenchmarkDriverConf.setStartBroker(false);
    perfBenchmarkDriverConf.setServerInstanceDataDir(_dataDir);
    final PerfBenchmarkDriver driver =
        new PerfBenchmarkDriver(perfBenchmarkDriverConf, _tempDir, _loadMode, _segmentFormatVersion, false);
    driver.run();

    if (_isBatchLoad) {
      String[] tableNames = new File(_dataDir).list();
      Preconditions.checkNotNull(tableNames);
      ExecutorService executorService = Executors.newFixedThreadPool(_numThreads);
      for (final String tableName : tableNames) {
        executorService.submit(new Runnable() {
          @Override
          public void run() {
            try {
              loadTable(driver, _dataDir, tableName, null, null);
            } catch (Exception e) {
              LOGGER.error("Caught exception while loading table: {}", tableName, e);
            }
          }
        });
      }
      executorService.shutdown();
      executorService.awaitTermination(_timeoutInSeconds, TimeUnit.SECONDS);
    } else {
      List<String> invertedIndexColumns = null;
      if (_invertedIndexColumns != null) {
        invertedIndexColumns = Arrays.asList(_invertedIndexColumns.split(","));
      }
      List<String> bloomFilterColumns = null;
      if (_bloomFilterColumns != null) {
        bloomFilterColumns = Arrays.asList(_bloomFilterColumns.split(","));
      }
      for (String tableName : _tableNames.split(",")) {
        loadTable(driver, _dataDir, tableName, invertedIndexColumns, bloomFilterColumns);
      }
    }
  }

  public static void loadTable(PerfBenchmarkDriver driver, String dataDir, String tableName,
      List<String> invertedIndexColumns, List<String> bloomFilterColumns)
      throws Exception {
    boolean tableConfigured = false;
    // Skip BaseTableDataManager._resourceTmpDir
    File[] segments = new File(dataDir, tableName).listFiles((dir, name) -> !name.equals("tmp"));
    Preconditions.checkNotNull(segments);
    for (File segment : segments) {
      if (segment.isDirectory()) {
        SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segment);
        if (!tableConfigured) {
          driver.configureTable(tableName, invertedIndexColumns, bloomFilterColumns);
          tableConfigured = true;
        }
        driver.addSegment(tableName, segmentMetadata);
      }
    }
  }

  /**
   * Main method for the class.
   *
   * @param args arguments for the perf benchmark runner.
   * @throws Exception
   */
  public static void main(String[] args)
      throws Exception {
    PerfBenchmarkRunner perfBenchmarkRunner = new PerfBenchmarkRunner();
    CommandLine commandLine = new CommandLine(perfBenchmarkRunner);
    CommandLine.ParseResult result = commandLine.parseArgs(args);
    if (result.isUsageHelpRequested() || result.matchedArgs().size() == 0) {
      commandLine.usage(System.out);
      return;
    }
    commandLine.execute();
  }
}
