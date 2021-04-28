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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.tools.query.comparison.QueryComparison;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;
import org.testng.TestNG;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * A command line runner to invoke the customized hybrid cluster integration test via command-line.
 *
 * <p>The arguments expected are as follows:
 * <ul>
 *   <li>
 *     --llc: Whether to use LLC or HLC for the realtime table
 *   </li>
 *   <li>
 *     tableName: Table name
 *   </li>
 *   <li>
 *     schemaFile: Path of the Pinot schema file
 *   </li>
 *   <li>
 *     dataDir: Path of the data directory which should includes a directory called "avro-files" under which all the
 *     Avro files reside, a file called "queries.txt" with all the queries and a file called "scan-responses.txt" with
 *     all the expected responses
 *   </li>
 *   <li>
 *     invertedIndexColumns: A list of comma-separated columns which we want to generate inverted index on
 *   </li>
 *   <li>
 *     sortedColumn: The sorted column to be used for building realtime segments
 *   </li>
 * </ul>
 *
 * The command can be invoked as follows:
 * <p>CLASSPATH_PREFIX=pinot-integration-tests/target/pinot-integration-tests-*-tests.jar
 * <p>pinot-integration-tests/target/pinot-integration-tests-pkg/bin/pinot-hybrid-cluster-test.sh args...
 */
public class HybridClusterIntegrationTestCommandLineRunner {
  private HybridClusterIntegrationTestCommandLineRunner() {
  }

  public static void printUsage() {
    System.err.println(
        "Usage: pinot-hybrid-cluster.sh [--llc] tableName schemaFile timeColumnName dataDir invertedIndexColumns sortedColumn");
    System.exit(1);
  }

  public static void main(String[] args)
      throws Exception {
    int numArgs = args.length;
    if (!((numArgs == 6) || (numArgs == 7 && args[0].equals("--llc")))) {
      printUsage();
    }

    CustomHybridClusterIntegrationTest._enabled = true;

    int argIdx = 0;
    if (args[0].equals("--llc")) {
      CustomHybridClusterIntegrationTest._useLlc = true;
      argIdx++;
    }

    CustomHybridClusterIntegrationTest._tableName = args[argIdx++];
    File schemaFile = new File(args[argIdx++]);
    CustomHybridClusterIntegrationTest._schema = Schema.fromFile(schemaFile);
    String timeColumnName = args[argIdx++];
    CustomHybridClusterIntegrationTest._timeColumnName =
        (CustomHybridClusterIntegrationTest._schema.getFieldSpecFor(timeColumnName) != null) ? timeColumnName : null;
    File dataDir = new File(args[argIdx++]);
    Preconditions.checkState(dataDir.isDirectory());
    CustomHybridClusterIntegrationTest._dataDir = dataDir;
    CustomHybridClusterIntegrationTest._invertedIndexColumns = Arrays.asList(args[argIdx++].split(","));
    CustomHybridClusterIntegrationTest._sortedColumn = args[argIdx];

    TestListenerAdapter testListenerAdapter = new TestListenerAdapter();
    TestNG testNG = new TestNG();
    testNG.setTestClasses(new Class[]{CustomHybridClusterIntegrationTest.class});
    testNG.addListener(testListenerAdapter);
    testNG.run();

    System.out.println(testListenerAdapter.toString());
    boolean success = true;
    List<ITestResult> skippedTests = testListenerAdapter.getSkippedTests();
    if (!skippedTests.isEmpty()) {
      System.out.println("Skipped tests: " + skippedTests);
      for (ITestResult skippedTest : skippedTests) {
        System.out.println(skippedTest.getName() + ": " + skippedTest.getThrowable());
      }
      success = false;
    }
    List<ITestResult> failedTests = testListenerAdapter.getFailedTests();
    if (!failedTests.isEmpty()) {
      System.err.println("Failed tests: " + failedTests);
      for (ITestResult failedTest : failedTests) {
        System.out.println(failedTest.getName() + ": " + failedTest.getThrowable());
      }
      success = false;
    }
    if (success) {
      System.exit(0);
    } else {
      System.exit(1);
    }
  }

  /**
   * Custom hybrid cluster integration test that runs queries from a query file and compares with the pre-generated
   * responses. It takes at least three Avro files and creates offline segments for the first two, while it pushes all
   * Avro files except the first one into Kafka.
   * <p>
   * Each Avro file is pushed in order into Kafka, and after each Avro file is pushed, we run all queries from the query
   * file given when the record count is stabilized. This makes it so that we cover as many test conditions as possible
   * (eg. partially in memory, partially on disk with data overlap between offline and realtime) and should give us good
   * confidence that the cluster behaves as it should.
   */
  public static final class CustomHybridClusterIntegrationTest extends BaseClusterIntegrationTest {
    private static final String TENANT_NAME = "TestTenant";
    private static final int ZK_PORT = 3191;
    private static final String ZK_STR = "localhost:" + ZK_PORT;
    private static final int NUM_KAFKA_BROKERS = 1;
    private static final int KAFKA_PORT = 20092;
    private static final String KAFKA_ZK_STR = ZK_STR + "/kafka";
    private static final int CONTROLLER_PORT = 9998;
    private static final int BROKER_PORT = 19099;
    private static final int SERVER_BASE_ADMIN_API_PORT = 9097;
    private static final int SERVER_BASE_NETTY_PORT = 9098;

    private static final String AVRO_DIR = "avro-files";
    private static final String QUERY_FILE_NAME = "queries.txt";
    private static final String RESPONSE_FILE_NAME = "scan-responses.txt";
    private static final int NUM_OFFLINE_SEGMENTS = 2;

    private static boolean _enabled = false;
    private static boolean _useLlc = false;
    private static String _tableName;
    private static Schema _schema;
    private static String _timeColumnName;
    private static File _dataDir;
    private static List<String> _invertedIndexColumns;
    private static String _sortedColumn;

    private List<File> _offlineAvroFiles;
    private List<File> _realtimeAvroFiles;
    private File _queryFile;
    private File _responseFile;
    private long _countStarResult;

    public CustomHybridClusterIntegrationTest() {
      if (!_enabled) {
        return;
      }

      File[] avroFiles = new File(_dataDir, AVRO_DIR).listFiles();
      Assert.assertNotNull(avroFiles);
      int numAvroFiles = avroFiles.length;
      Assert.assertTrue(numAvroFiles > 2, "Need at least 3 Avro files to run the test");
      Arrays.sort(avroFiles);

      _offlineAvroFiles = new ArrayList<>(NUM_OFFLINE_SEGMENTS);
      _realtimeAvroFiles = new ArrayList<>(numAvroFiles - 1);
      for (int i = 0; i < numAvroFiles; i++) {
        if (i < NUM_OFFLINE_SEGMENTS) {
          _offlineAvroFiles.add(avroFiles[i]);
        }
        if (i > 0) {
          _realtimeAvroFiles.add(avroFiles[i]);
        }
      }

      _queryFile = new File(_dataDir, QUERY_FILE_NAME);
      Assert.assertTrue(_queryFile.isFile());

      _responseFile = new File(_dataDir, RESPONSE_FILE_NAME);
      Assert.assertTrue(_responseFile.isFile());
    }

    @Override
    protected String getTableName() {
      return _tableName;
    }

    @Override
    protected String getSchemaName() {
      return _schema.getSchemaName();
    }

    @Nullable
    @Override
    protected String getTimeColumnName() {
      return _timeColumnName;
    }

    @Override
    protected long getCountStarResult() {
      return _countStarResult;
    }

    @Override
    protected boolean useLlc() {
      return _useLlc;
    }

    @Override
    protected int getRealtimeSegmentFlushSize() {
      return super.getRealtimeSegmentFlushSize() * 100;
    }

    @Override
    protected int getNumKafkaBrokers() {
      return NUM_KAFKA_BROKERS;
    }

    @Override
    protected int getBaseKafkaPort() {
      return KAFKA_PORT;
    }

    @Override
    protected String getKafkaZKAddress() {
      return KAFKA_ZK_STR;
    }

    @Override
    protected String getSortedColumn() {
      return _sortedColumn;
    }

    @Override
    protected List<String> getInvertedIndexColumns() {
      return _invertedIndexColumns;
    }

    @Nullable
    @Override
    protected List<String> getNoDictionaryColumns() {
      return null;
    }

    @Nullable
    @Override
    protected List<String> getRangeIndexColumns() {
      return null;
    }

    @Nullable
    @Override
    protected List<String> getBloomFilterColumns() {
      return null;
    }

    @Override
    protected String getLoadMode() {
      return ReadMode.mmap.name();
    }

    @Override
    protected String getBrokerTenant() {
      return TENANT_NAME;
    }

    @Override
    protected String getServerTenant() {
      return TENANT_NAME;
    }

    @Override
    protected long getCurrentCountStarResult()
        throws Exception {
      return postQuery("SELECT COUNT(*) FROM " + getTableName()).get("aggregationResults").get(0).get("value").asLong();
    }

    @BeforeClass
    public void setUp()
        throws Exception {
      if (!_enabled) {
        return;
      }

      TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

      // Start Zk and Kafka
      startZk(ZK_PORT);
      startKafka();

      // Start the Pinot cluster
      Map<String, Object> properties = getDefaultControllerConfiguration();

      properties.put(ControllerConf.CONTROLLER_PORT, CONTROLLER_PORT);
      properties.put(ControllerConf.ZK_STR, ZK_STR);
      properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);

      startController(properties);

      startBroker(BROKER_PORT, ZK_STR);
      startServers(2, SERVER_BASE_ADMIN_API_PORT, SERVER_BASE_NETTY_PORT, ZK_STR);

      // Create tenants
      createBrokerTenant(TENANT_NAME, 1);
      createServerTenant(TENANT_NAME, 1, 1);

      // Create and upload the schema and table config
      addSchema(_schema);
      TableConfig offlineTableConfig = createOfflineTableConfig();
      addTableConfig(offlineTableConfig);
      addTableConfig(createRealtimeTableConfig(_realtimeAvroFiles.get(0)));

      // Create and upload segments
      ClusterIntegrationTestUtils
          .buildSegmentsFromAvro(_offlineAvroFiles, offlineTableConfig, _schema, 0, _segmentDir, _tarDir);
      uploadSegments(getTableName(), _tarDir);
    }

    @Test
    public void testQueriesFromQueryFile()
        throws Exception {
      if (!_enabled) {
        return;
      }

      // Do not compare numDocsScanned
      QueryComparison.setCompareNumDocs(false);

      final AtomicInteger numFailedQueries = new AtomicInteger();
      try (BufferedReader responseFileReader = new BufferedReader(new FileReader(_responseFile))) {
        for (File realtimeAvroFile : _realtimeAvroFiles) {
          // Push one avro file into the Kafka topic
          pushAvroIntoKafka(Collections.singletonList(realtimeAvroFile));

          try (BufferedReader queryFileReader = new BufferedReader(new FileReader(_queryFile))) {
            // Set the expected COUNT(*) result and wait for all documents loaded
            responseFileReader.mark(4096);
            _countStarResult = JsonUtils.stringToJsonNode(responseFileReader.readLine()).get("totalDocs").asLong();
            responseFileReader.reset();
            waitForAllDocsLoaded(600_000L);

            // Run queries in multiple threads
            ExecutorService executorService =
                new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(10),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            String query;
            while ((query = queryFileReader.readLine()) != null) {
              String currentQuery = query;
              JsonNode expectedResponse = JsonUtils.stringToJsonNode(responseFileReader.readLine());
              executorService.execute(new Runnable() {
                @Override
                public void run() {
                  try {
                    JsonNode actualResponse = postQuery(currentQuery, "http://localhost:" + BROKER_PORT);
                    if (QueryComparison.compareWithEmpty(actualResponse, expectedResponse)
                        == QueryComparison.ComparisonStatus.FAILED) {
                      numFailedQueries.getAndIncrement();
                      System.out.println(
                          "Query comparison failed for query: " + currentQuery + "\nActual: " + actualResponse
                              .toString() + "\nExpected: " + expectedResponse.toString());
                    }
                  } catch (Exception e) {
                    numFailedQueries.getAndIncrement();
                    System.out.println("Caught exception while comparing query: " + currentQuery);
                    e.printStackTrace();
                  }
                }
              });
            }
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.MINUTES);
          }
        }
      }

      Assert.assertEquals(numFailedQueries.get(), 0, "Caught " + numFailedQueries + " failed queries");
    }

    @AfterClass
    public void tearDown()
        throws Exception {
      if (!_enabled) {
        return;
      }

      String tableName = getTableName();
      dropOfflineTable(tableName);
      dropRealtimeTable(tableName);

      stopServer();
      stopBroker();
      stopController();
      stopKafka();
      stopZk();

      FileUtils.deleteDirectory(_tempDir);
    }
  }
}
