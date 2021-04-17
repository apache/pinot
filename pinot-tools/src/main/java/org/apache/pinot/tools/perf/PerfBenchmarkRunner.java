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
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("FieldCanBeLocal")
public class PerfBenchmarkRunner extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(PerfBenchmarkRunner.class);

  @Option(name = "-mode", required = true, metaVar = "<String>",
      usage = "Mode of the PerfBenchmarkRunner (startAll|startAllButServer|startServerWithPreLoadedSegments).")
  private String _mode;

  @Option(name = "-dataDir", required = false, metaVar = "<String>",
      usage = "Path to directory containing un-tarred segments.")
  private String _dataDir;

  @Option(name = "-tempDir", required = false, metaVar = "<String>",
      usage = "Path to temporary directory to start the cluster")
  private String _tempDir = "/tmp/";

  @Option(name = "-loadMode", required = false, metaVar = "<String>", usage = "Load mode of the segments (HEAP|MMAP).")
  private String _loadMode = "HEAP";

  @Option(name = "-segmentFormatVersion", required = false, metaVar = "<String>",
      usage = "Segment format version to be loaded (v1|v3).")
  private String _segmentFormatVersion;

  @Option(name = "-batchLoad", required = false, metaVar = "<boolean>", usage = "Batch load multiple tables.")
  private boolean _isBatchLoad;

  @Option(name = "-numThreads", required = false, metaVar = "<int>",
      usage = "Number of threads for batch load (default 10).")
  private int _numThreads = 10;

  @Option(name = "-timeoutInSeconds", required = false, metaVar = "<int>",
      usage = "Timeout in seconds for batch load (default 60).")
  private int _timeoutInSeconds = 60;

  @Option(name = "-tableNames", required = false, metaVar = "<String>",
      usage = "Comma separated table names with types to be loaded (non-batch load).")
  private String _tableNames;

  @Option(name = "-invertedIndexColumns", required = false, metaVar = "<String>",
      usage = "Comma separated inverted index columns to be created (non-batch load).")
  private String _invertedIndexColumns;

  @Option(name = "-bloomFilterColumns", required = false, metaVar = "<String>",
      usage = "Comma separated bloom filter columns to be created (non-batch load).")
  private String _bloomFilterColumns;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public String description() {
    return "Start Pinot cluster with optional preloaded segments.";
  }

  @Override
  public boolean execute() throws Exception {
    if (_mode.equalsIgnoreCase("startAll") || _mode.equalsIgnoreCase("startAllButServer")) {
      startAllButServer();
    }
    if (_mode.equalsIgnoreCase("startAll") || _mode.equalsIgnoreCase("startServerWithPreLoadedSegments")) {
      startServerWithPreLoadedSegments();
    }
    return true;
  }

  public void startAllButServer() throws Exception {
    PerfBenchmarkDriverConf perfBenchmarkDriverConf = new PerfBenchmarkDriverConf();
    perfBenchmarkDriverConf.setStartServer(false);
    PerfBenchmarkDriver driver =
        new PerfBenchmarkDriver(perfBenchmarkDriverConf, _tempDir, _loadMode, _segmentFormatVersion, false);
    driver.run();
  }

  private void startServerWithPreLoadedSegments() throws Exception {
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
      List<String> invertedIndexColumns, List<String> bloomFilterColumns) throws Exception {
    boolean tableConfigured = false;
    File[] segments = new File(dataDir, tableName).listFiles();
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
  public static void main(String[] args) throws Exception {
    PerfBenchmarkRunner perfBenchmarkRunner = new PerfBenchmarkRunner();
    CmdLineParser parser = new CmdLineParser(perfBenchmarkRunner);
    parser.parseArgument(args);

    if (perfBenchmarkRunner._help) {
      perfBenchmarkRunner.printUsage();
    } else {
      perfBenchmarkRunner.execute();
    }
  }
}
