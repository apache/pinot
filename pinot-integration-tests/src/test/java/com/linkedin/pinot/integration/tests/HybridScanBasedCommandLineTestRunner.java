/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.data.Schema;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.TestListenerAdapter;
import org.testng.TestNG;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * A command line runner to invoke the HybridClusterScanComparisonIntegrationTest via command-line.
 * The arguments expected are as follows:
 *    tableName: The name of the table in the queries. This is substituted with an internal table name
 *    schemaFile : This is the full path of the file that has the pinot schema.
 *    segQueryDirPath: Full path of a directory in which we expect to see the following (we don't pay attention to other stuff)
 *        A directory called avro-files under which all the avro files must reside, with their names indicating the date ranges
 *        A file called queries.txt that should have all the queries that we execute against this dataset.
 *    timeColumnName : Name of the time column in the schema (e.g. "daysSinceEpoch")
 *    timeColType :  Type of the time col (e.g. "DAYS")
 *    invIndexCols: A list of comma-separated column-names for inverted index.
 *    sortedCol: The name of the sorted column to be used for bulding realtime segments
 *
 * The command can be invoked as follows:
 *    CLASSPATH_PREFIX=pinot-integration-tests/target/pinot-integration-tests-*-tests.jar pinot-integration-tests/target/pinot-integration-tests-pkg/bin/pinot-hybrid-cluster-test.sh args...
 */
// TODO: clean up this test
public class HybridScanBasedCommandLineTestRunner {

  public static void usage() {
    System.err.println("Usage: pinot-hybrid-cluster.sh [--llc] [--record] tableName schemaFilePath segQueryDirPath invIndexCols sortedCol");
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      usage();
    }
    int expectedArgsLen = 5;
    int ix = 0;
    // Parse optional arguments first.
    while (args[ix].startsWith("-")) {
        if (args[ix].equals("--record")) {
          CustomHybridClusterScanComparisonIntegrationTest._recordScanResponses = true;
        } else if (args[ix].equals("--llc")) {
          CustomHybridClusterScanComparisonIntegrationTest._useLlc = true;
        } else {
          usage();
        }
      ix++;
      expectedArgsLen++;
    }

    if (args.length != expectedArgsLen) {
      usage();
    }

    final String tableName = args[ix++];
    final String schemaFilePath = args[ix++];
    final String segQueryDirPath = args[ix++]; // we expect a dir called 'avro-files' and files called 'queries.txt' and 'scan-responses.txt' in here
    final String invIndexCols = args[ix++];
    final String sortedCol = args[ix++];

    CustomHybridClusterScanComparisonIntegrationTest.setParams(tableName,
        schemaFilePath, segQueryDirPath, invIndexCols, sortedCol);
    TestListenerAdapter tla = new TestListenerAdapter();
    TestNG testng = new TestNG();
    testng.setTestClasses(new Class[]{CustomHybridClusterScanComparisonIntegrationTest.class});
    testng.addListener(tla);
    testng.run();
    System.out.println("Passed tests: " + tla.getPassedTests());
    if (!tla.getSkippedTests().isEmpty()) {
      System.out.println("Skipped tests: " + tla.getSkippedTests());
    }
    System.out.println(tla.toString());
    if (!tla.getFailedTests().isEmpty()) {
      System.err.println("Failed tests:" + tla.getFailedTests());
      System.exit(1);
    }
    System.exit(0);
  }

  public static class CustomHybridClusterScanComparisonIntegrationTest extends HybridClusterScanComparisonIntegrationTest {

    private static String _timeColName;
    private static String _timeColType;
    private static List<File> _avroFiles = new ArrayList<>(4);
    private static List<String> _invIndexCols = new ArrayList<>(4);
    private static String _tableName;
    private static String _queryFilePath;
    private static String _segsQueryDir;
    private static final String _logFileSuffix = "query-comparison.log";
    private static final String QUERY_FILE_NAME = "queries.txt";
    private static final String AVRO_DIR = "avro-files";
    private static final String SCAN_RSP_FILE_NAME = "scan-responses.txt";  // Will have more number of lines than queries file
    private static File _schemaFile;
    private static String _sortedColumn;
    private static String _logFileName;
    private static boolean _inCmdLine = false;
    private static boolean _recordScanResponses = false;  // Must be done in single-threaded mode if true
    private static boolean _useLlc = false;  // Whether to use kafka low-level consumer
    private static boolean _compareWithRspFile = true;
    private static String _scanRspFilePath;

    private boolean _multiThreaded = true;
    private FileWriter _scanRspFileWriter;
    private LineNumberReader _scanRspFileReader;

    public static void setParams(String tableName, String schemaFileName, String segsQueryDir,
        String invIndexCols, String sortedCol) throws Exception {
      // TODO add some basic checks
      // TODO add params for single query
      _tableName = tableName;
      _queryFilePath = segsQueryDir + "/" + QUERY_FILE_NAME;
      _scanRspFilePath = segsQueryDir  + "/" + SCAN_RSP_FILE_NAME;
      _sortedColumn = sortedCol;
      _segsQueryDir = segsQueryDir;
      File avroDir = new File(segsQueryDir + "/" + AVRO_DIR);

      File[] avroFiles = avroDir.listFiles();
      for (File file : avroFiles) {
        if (!file.getName().matches("[0-9].*")) {
          throw new RuntimeException(
              "Avro file names must start with a digit that indicates starting time/day of avro events in the file");
        }
        _avroFiles.add(file);
      }
      Collections.sort(_avroFiles);
      String[] colNames = invIndexCols.split(",");
      for (String colName : colNames) {
        _invIndexCols.add(colName);
      }
      _schemaFile = new File(schemaFileName);
      Schema schema = Schema.fromFile(_schemaFile);
      _timeColName = schema.getTimeColumnName();
      _timeColType = schema.getIncomingTimeUnit().toString();

      _logFileName = _tableName + "-" + System.currentTimeMillis() + "-" + _logFileSuffix;
      _inCmdLine = true;
    }

    @Override
    protected List<File> getAllAvroFiles() {
      return _avroFiles;
    }

    @Nonnull
    @Override
    public File getSchemaFile() {
      return _schemaFile;
    }

    @Override
    protected String getTimeColumnName() {
      return _timeColName;
    }

    @Override
    protected String getTimeColumnType() {
      return _timeColType;
    }

    @Override
    protected String getSortedColumn() {
      return _sortedColumn;
    }

    @Override
    protected int getNumKafkaPartitions() {
      if (_useLlc) {
        return 2;
      } else {
        return 10;
      }
    }

    @Override
    protected int getRealtimeSegmentFlushSize(boolean useLlc) {
      return super.getRealtimeSegmentFlushSize(useLlc) * 10;
    }

    @Override
    @BeforeClass
    public void setUp() throws Exception {
      if (!_inCmdLine) {
        return;
      }
      _nQueriesRead = 0;
      _createSegmentsInParallel = true;
      File scanRspFile = new File(_scanRspFilePath);
      if (_recordScanResponses) {
        _compareWithRspFile = false;
        if (scanRspFile.exists()) {
          throw new RuntimeException(_scanRspFilePath + " already exists");
        }
        _scanRspFileWriter = new FileWriter(scanRspFile);
        _multiThreaded = false;
      } else {
        // Attempt to compare with a response file if it exists
        if (scanRspFile.exists()) {
          _scanRspFileReader = new LineNumberReader(new FileReader(scanRspFile));
        } else {
          // Run live queries with the scan-based query runner.
          _compareWithRspFile = false;
        }
      }

      for (String col : _invIndexCols) {
        invertedIndexColumns.add(col);
      }
      super.setUp();
    }

    @Override
    @AfterClass
    public void tearDown() throws Exception{
      if (!_inCmdLine) {
        return;
      }
      if (_recordScanResponses) {
        _scanRspFileWriter.flush();
        _scanRspFileWriter.close();
      }
      super.tearDown();
    }

    @Override
    protected boolean useLlc() {
      return _useLlc;
    }

    @Override
    protected long getStabilizationTimeMs() {
      return 5 * 60 * 1000L;
    }

    @Override
    protected FileWriter getScanRspRecordFileWriter() {
      return _scanRspFileWriter;
    }

    @Override
    protected FileWriter getLogWriter()
        throws IOException {
      return new FileWriter(new File(_logFileName));
    }

    protected FileReader getQueryFile()
        throws FileNotFoundException {
      return new FileReader(_queryFilePath);
    }

    private String replaceTableName(String query) {
      return query.replace(_tableName, "mytable");
    }

    @Test
    public void testQueriesFromLog() throws Exception {
      try {
        if (!_inCmdLine) {
          return;
        }
        runTestLoop(new Callable<Object>() {
          @Override
          public Object call()
              throws Exception {
            LineNumberReader queryReader = new LineNumberReader(getQueryFile());
            for (; ; ) {
              String line = queryReader.readLine();
              if (line == null) {
                break;
              }
              _nQueriesRead++;
              final String query = replaceTableName(line);
              String compareStr = null;
              if (_compareWithRspFile) {
                compareStr = _scanRspFileReader.readLine();
                if (compareStr == null) {
                  Assert.fail("Not enough lines in " + _scanRspFilePath);
                }
              }
              final String scanRsp = compareStr;
              if (_multiThreaded) {
                runQueryAsync(query, scanRsp);
              } else {
                runQuery(query, _scanBasedQueryProcessor, false, scanRsp);
              }
            }
            queryReader.close();
            return null;
          }
        }, _multiThreaded);
        System.out.println(
            getNumSuccesfulQueries() + " Passed, " + getNumFailedQueries() + " Failed, " + getNumEmptyResults() + " empty results");
        Assert.assertEquals(0, getNumFailedQueries(), "There were query failures. See " + _logFileName);
      } catch (Exception e) {
        System.out.println("Caught exception while running queries from log");
        e.printStackTrace();
        throw e;
      }
    }
  }
}
