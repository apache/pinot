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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.tools.query.comparison.QueryComparison;
import com.linkedin.pinot.util.TestUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
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
        "Usage: pinot-hybrid-cluster.sh [--llc] tableName schemaFile dataDir invertedIndexColumns sortedColumn");
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    int numArgs = args.length;
    if (!((numArgs == 5) || (numArgs == 6 && args[0].equals("--llc")))) {
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
    Preconditions.checkState(schemaFile.isFile());
    CustomHybridClusterIntegrationTest._schemaFile = schemaFile;
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
    private static final String KAFKA_ZK_STR = ZK_STR + "/kafka";
    private static final int KAFKA_PORT = 20092;
    private static final String KAFKA_BROKER = "localhost:" + KAFKA_PORT;
    private static final int CONTROLLER_PORT = 9998;
    private static final int BROKER_BASE_PORT = 19099;
    private static final int SERVER_BASE_ADMIN_API_PORT = 9097;
    private static final int SERVER_BASE_NETTY_PORT = 9098;

    private static final String AVRO_DIR = "avro-files";
    private static final String QUERY_FILE_NAME = "queries.txt";
    private static final String RESPONSE_FILE_NAME = "scan-responses.txt";
    private static final int NUM_OFFLINE_SEGMENTS = 2;

    private static boolean _enabled = false;
    private static boolean _useLlc = false;
    private static String _tableName;
    private static File _schemaFile;
    private static File _dataDir;
    private static List<String> _invertedIndexColumns;
    private static String _sortedColumn;

    private List<File> _offlineAvroFiles;
    private List<File> _realtimeAvroFiles;
    private File _queryFile;
    private File _responseFile;
    private KafkaServerStartable _kafkaStarter;
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

    @Nonnull
    @Override
    protected String getTableName() {
      return _tableName;
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
    protected long getCurrentCountStarResult() throws Exception {
      return postQuery("SELECT COUNT(*) FROM " + getTableName()).getJSONArray("aggregationResults")
          .getJSONObject(0)
          .getLong("value");
    }

    @BeforeClass
    public void setUp() throws Exception {
      if (!_enabled) {
        return;
      }

      TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

      // Start Zk and Kafka
      startZk(ZK_PORT);
      _kafkaStarter = KafkaStarterUtils.startServer(KAFKA_PORT, KafkaStarterUtils.DEFAULT_BROKER_ID, KAFKA_ZK_STR,
          KafkaStarterUtils.getDefaultKafkaConfiguration());

      // Create Kafka topic
      KafkaStarterUtils.createTopic(getKafkaTopic(), KAFKA_ZK_STR, getNumKafkaPartitions());

      // Start the Pinot cluster
      ControllerConf config = getDefaultControllerConfiguration();
      config.setControllerPort(Integer.toString(CONTROLLER_PORT));
      config.setZkStr(ZK_STR);
      config.setTenantIsolationEnabled(false);
      startController(config);
      startBroker(BROKER_BASE_PORT, ZK_STR);
      startServers(2, SERVER_BASE_ADMIN_API_PORT, SERVER_BASE_NETTY_PORT, ZK_STR);

      // Create tenants
      createBrokerTenant(TENANT_NAME, 1);
      createServerTenant(TENANT_NAME, 1, 1);

      // Create segments from Avro data
      ExecutorService executor = Executors.newCachedThreadPool();
      Schema schema = Schema.fromFile(_schemaFile);
      ClusterIntegrationTestUtils.buildSegmentsFromAvro(_offlineAvroFiles, 0, _segmentDir, _tarDir, _tableName, false,
          getRawIndexColumns(), schema, executor);
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.MINUTES);

      // Create Pinot table
      String schemaName = schema.getSchemaName();
      addSchema(_schemaFile, schemaName);
      String timeColumnName = schema.getTimeColumnName();
      Assert.assertNotNull(timeColumnName);
      TimeUnit outgoingTimeUnit = schema.getOutgoingTimeUnit();
      Assert.assertNotNull(outgoingTimeUnit);
      String timeType = outgoingTimeUnit.toString();
      addHybridTable(_tableName, _useLlc, KAFKA_BROKER, KAFKA_ZK_STR, getKafkaTopic(), getRealtimeSegmentFlushSize(),
          _realtimeAvroFiles.get(0), timeColumnName, timeType, schemaName, TENANT_NAME, TENANT_NAME, "MMAP",
          _sortedColumn, _invertedIndexColumns, null, null);

      // Upload all segments
      uploadSegments(_tarDir);
    }

    @Test
    public void testQueriesFromQueryFile() throws Exception {
      if (!_enabled) {
        return;
      }

      // Do not compare numDocsScanned
      QueryComparison.setCompareNumDocs(false);

      final AtomicInteger numFailedQueries = new AtomicInteger();
      try (BufferedReader responseFileReader = new BufferedReader(new FileReader(_responseFile))) {
        for (File realtimeAvroFile : _realtimeAvroFiles) {
          // Push one avro file into the Kafka topic
          ClusterIntegrationTestUtils.pushAvroIntoKafka(Collections.singletonList(realtimeAvroFile), KAFKA_BROKER,
              getKafkaTopic(), getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn());

          try (BufferedReader queryFileReader = new BufferedReader(new FileReader(_queryFile))) {
            // Set the expected COUNT(*) result and wait for all documents loaded
            responseFileReader.mark(4096);
            _countStarResult = new JSONObject(responseFileReader.readLine()).getLong("totalDocs");
            responseFileReader.reset();
            waitForAllDocsLoaded(600_000L);

            // Run queries in multiple threads
            ExecutorService executorService =
                new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(10),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            String query;
            while ((query = queryFileReader.readLine()) != null) {
              final String currentQuery = query;
              final JSONObject expectedResponse = new JSONObject(responseFileReader.readLine());
              executorService.execute(new Runnable() {
                @Override
                public void run() {
                  try {
                    JSONObject actualResponse = postQuery(currentQuery, "http://localhost:" + BROKER_BASE_PORT);
                    if (QueryComparison.compareWithEmpty(actualResponse, expectedResponse)
                        == QueryComparison.ComparisonStatus.FAILED) {
                      numFailedQueries.getAndIncrement();
                      System.out.println(
                          "Query comparison failed for query: " + currentQuery
                              + "\nActual: " + actualResponse.toString(2)
                              + "\nExpected: " + expectedResponse.toString(2));
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
    public void tearDown() throws Exception {
      if (!_enabled) {
        return;
      }

      String tableName = getTableName();
      dropOfflineTable(tableName);
      dropRealtimeTable(tableName);

      stopServer();
      stopBroker();
      stopController();
      KafkaStarterUtils.stopServer(_kafkaStarter);
      stopZk();

      FileUtils.deleteDirectory(_tempDir);
    }
  }
}
