/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.tools.query.comparison.QueryComparison;
import com.linkedin.pinot.tools.scan.query.QueryResponse;
import com.linkedin.pinot.tools.scan.query.ScanBasedQueryProcessor;


/**
 * Cluster integration test that compares with the scan based comparison tool. It takes at least three Avro files and
 * creates offline segments for the first two, while it pushes all Avro files except the first one into Kafka.
 *
 * Each Avro file is pushed in order into Kafka, and after each Avro file is pushed, we run all queries from the query
 * source given when the record count is stabilized. This makes it so that we cover as many test conditions as possible
 * (eg. partially in memory, partially on disk with data overlap between offline and realtime) and should give us good
 * confidence that the cluster behaves as it should.
 *
 * Because this test is configurable, it can also be used with arbitrary Avro files (within reason) and query logs from
 * production to reproduce issues seen in the offline-realtime interaction in a controlled environment.
 *
 * To reuse this test, you need to implement getAvroFileCount, getAllAvroFiles, getOfflineAvroFiles and
 * getRealtimeAvroFiles, as well as the optional extractAvroIfNeeded. You'll also need to use one of the test methods.
 */
public abstract class HybridClusterScanComparisonIntegrationTest extends HybridClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(HybridClusterScanComparisonIntegrationTest.class);
  protected final File _offlineTarDir = new File("/tmp/HybridClusterIntegrationTest/offlineTarDir");
  protected final File _realtimeTarDir = new File("/tmp/HybridClusterIntegrationTest/realtimeTarDir");
  protected final File _unpackedSegments = new File("/tmp/HybridClusterIntegrationTest/unpackedSegments");
  protected final File _offlineSegmentDir = new File(_segmentDir, "offline");
  protected final File _realtimeSegmentDir = new File(_segmentDir, "realtime");
  private Map<File, File> _offlineAvroToSegmentMap;
  private Map<File, File> _realtimeAvroToSegmentMap;
  private File _schemaFile;
  private Schema _schema;
  protected ScanBasedQueryProcessor _scanBasedQueryProcessor;
  private FileWriter _fileWriter;
  private final AtomicInteger _successfulQueries = new AtomicInteger(0);
  private final AtomicInteger _failedQueries = new AtomicInteger(0);
  private final AtomicInteger _emptyResults = new AtomicInteger(0);
  private long startTimeMs;
  private boolean _logMatchingResults = false;
  private ThreadPoolExecutor _queryExecutor;
  protected List<String> invertedIndexColumns = new ArrayList<String>();

  @AfterClass
  @Override
  public void tearDown() throws Exception {
    super.tearDown();

    _fileWriter.write("\nSuccessful queries: " + _successfulQueries.get() + "\n" +
        "Failed queries: " + _failedQueries.get() + "\n" +
            "Empty results: " + _emptyResults.get() + "\n");
    _fileWriter.write("Finish time:" + System.currentTimeMillis() + "\n");
    _fileWriter.close();

    if (_failedQueries.get() != 0) {
      Assert.fail("More than one query failed to compare properly, see the log file.");
    }
  }

  @Override
  protected void cleanup() throws Exception {
    // Uncomment this to preserve segments for examination later.
    super.cleanup();
  }

  protected abstract String getTimeColumnName();

  protected abstract String getTimeColumnType();

  protected abstract String getSortedColumn();

  protected void setUpTable(String tableName, String timeColumnName, String timeColumnType, String kafkaZkUrl,
      String kafkaTopic, File schemaFile, File avroFile, String sortedColumn, List<String> invertedIndexColumns)
      throws Exception {
    Schema schema = Schema.fromFile(schemaFile);
    addSchema(schemaFile, schema.getSchemaName());
    addHybridTable(tableName, timeColumnName, timeColumnType, kafkaZkUrl, kafkaTopic, schema.getSchemaName(),
        "TestTenant", "TestTenant", avroFile, sortedColumn, invertedIndexColumns);
  }

  @Override
  @BeforeClass
  public void setUp() throws Exception {
    //Clean up
    ensureDirectoryExistsAndIsEmpty(_tmpDir);
    ensureDirectoryExistsAndIsEmpty(_segmentDir);
    ensureDirectoryExistsAndIsEmpty(_offlineSegmentDir);
    ensureDirectoryExistsAndIsEmpty(_realtimeSegmentDir);
    ensureDirectoryExistsAndIsEmpty(_offlineTarDir);
    ensureDirectoryExistsAndIsEmpty(_realtimeTarDir);
    ensureDirectoryExistsAndIsEmpty(_unpackedSegments);

    // Start Zk, Kafka and Pinot
    startHybridCluster();

    extractAvroIfNeeded();

    int avroFileCount = getAvroFileCount();
    Preconditions.checkArgument(3 <= avroFileCount, "Need at least three Avro files for this test");

    setSegmentCount(avroFileCount);
    setOfflineSegmentCount(2);
    setRealtimeSegmentCount(avroFileCount - 1);

    final List<File> avroFiles = getAllAvroFiles();

    _schemaFile = getSchemaFile();
    _schema = Schema.fromFile(_schemaFile);

    // Create Pinot table
    setUpTable("mytable", getTimeColumnName(), getTimeColumnType(), KafkaStarterUtils.DEFAULT_ZK_STR, KAFKA_TOPIC,
        _schemaFile, avroFiles.get(0), getSortedColumn(), invertedIndexColumns);

    final List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles);
    final List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles);

    // Create segments from Avro data
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Map<File, File>> offlineAvroToSegmentMapFuture =
        buildSegmentsFromAvro(offlineAvroFiles, executor, 0, _offlineSegmentDir, _offlineTarDir, "mytable", false);
    Future<Map<File, File>> realtimeAvroToSegmentMapFuture =
        buildSegmentsFromAvro(realtimeAvroFiles, executor, 0, _realtimeSegmentDir, _realtimeTarDir, "mytable", false);

    // Initialize query generator
    setupQueryGenerator(avroFiles, executor);

    // Redeem futures
    _offlineAvroToSegmentMap = offlineAvroToSegmentMapFuture.get();
    _realtimeAvroToSegmentMap = realtimeAvroToSegmentMapFuture.get();

    LOGGER.info("Offline avro to segment map: {}", _offlineAvroToSegmentMap);
    LOGGER.info("Realtime avro to segment map: {}", _realtimeAvroToSegmentMap);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Set up a Helix spectator to count the number of segments that are uploaded and unlock the latch once 12 segments are online
    final CountDownLatch latch = setupSegmentCountCountDownLatch("mytable", getOfflineSegmentCount());

    // Upload the offline segments
    int i = 0;
    for (String segmentName : _offlineTarDir.list()) {
      i++;
      LOGGER.info("Uploading segment {} : {}", i, segmentName);
      File file = new File(_offlineTarDir, segmentName);
      FileUploadUtils.sendSegmentFile("localhost", "8998", segmentName, new FileInputStream(file), file.length());
    }

    // Wait for all offline segments to be online
    latch.await();

    _fileWriter = getLogWriter();
    _fileWriter.write("Start time:" + System.currentTimeMillis() + "\n");
    _fileWriter.flush();
    startTimeMs = System.currentTimeMillis();
    LOGGER.info("Setup completed");
  }

  protected void extractAvroIfNeeded() throws IOException {
    // Do nothing.
  }

  /**
   * Returns the number of avro files to use in this test.
   */
  protected abstract int getAvroFileCount();

  protected void setLogMatchingResults(boolean logMatchingResults) {
    _logMatchingResults = logMatchingResults;
  }

  protected FileWriter getLogWriter() throws IOException {
    return new FileWriter("failed-queries-hybrid.log");
  }

  protected void runQueryAsync(final String pqlQuery) throws Exception {
    _queryExecutor.execute(new Runnable() {
      @Override
      public void run() {
        final ScanBasedQueryProcessor scanBasedQueryProcessor =
            (new ThreadLocal<ScanBasedQueryProcessor>() {
              @Override
              public ScanBasedQueryProcessor get() {
                return _scanBasedQueryProcessor.clone();
              }
            }).get();

        try {
          runQuery(pqlQuery, scanBasedQueryProcessor, false);
        } catch (Exception e) {
          Assert.fail("Caught exception", e);
        }
      }
    });
  }

  protected void runQuery(String pqlQuery, ScanBasedQueryProcessor scanBasedQueryProcessor, boolean displayStatus)
      throws Exception {
    QueryResponse scanResponse = scanBasedQueryProcessor.processQuery(pqlQuery);
    JSONObject scanJson = new JSONObject(new ObjectMapper().writeValueAsString(scanResponse));
    JSONObject pinotJson = postQuery(pqlQuery);
    QueryComparison.setCompareNumDocs(false);

    try {
      QueryComparison.ComparisonStatus comparisonStatus = QueryComparison.compareWithEmpty(pinotJson, scanJson);

      if (comparisonStatus.equals(QueryComparison.ComparisonStatus.FAILED)) {
        _fileWriter.write("\nQuery comparison failed for query:" + pqlQuery + "\n" +
            "Scan json: " + scanJson + "\n" +
            "Pinot json: " + pinotJson + "\n");
        _failedQueries.getAndIncrement();
      } else {
        _successfulQueries.getAndIncrement();
        if (comparisonStatus.equals(QueryComparison.ComparisonStatus.EMPTY) ) {
          _emptyResults.getAndIncrement();
        } else if (_logMatchingResults) {
          _fileWriter.write("\nMatched for query:" + pqlQuery + "\n" + scanJson + "\n");
        }
      }
      _fileWriter.flush();
    } catch (Exception e) {
      _fileWriter.write("Caught exception while running query comparison, failed for query " + pqlQuery + "\n" +
          "Scan json: " + scanJson + "\n" +
          "Pinot json: " + pinotJson + "\n");
      _failedQueries.getAndIncrement();
      _fileWriter.flush();
    }

    int totalQueries = _successfulQueries.get() + _failedQueries.get();

    if (displayStatus || totalQueries % 1000 == 0) {
      doDisplayStatus(totalQueries);
    }
  }

  private void doDisplayStatus(int nQueries) {
    long nowMs = System.currentTimeMillis();
    LOGGER.info("Failures {}/{} ({} empty query results)", _failedQueries.get(),
        (_failedQueries.get() + _successfulQueries.get()), _emptyResults);
    LOGGER.info("Query failure percentage {}% (empty result {}%, rate {} queries/s)",
        _failedQueries.get() * 100.0 / (_failedQueries.get() + _successfulQueries.get()),
        _emptyResults.get() * 100.00 / (_failedQueries.get() + _successfulQueries.get()),
        nQueries * 1000.0 / (nowMs-startTimeMs)
    );
  }

  @Override
  protected void runQuery(String pqlQuery, List<String> sqlQueries) throws Exception {
    runQuery(pqlQuery, _scanBasedQueryProcessor, false);
  }

  protected void runTestLoop(Callable<Object> testMethod) throws Exception {
    runTestLoop(testMethod, false);
  }

  protected void runTestLoop(Callable<Object> testMethod, boolean useMultipleThreads) throws Exception {
    // Clean up the Kafka topic
    // TODO jfim: Re-enable this once PINOT-2598 is fixed
    // purgeKafkaTopicAndResetRealtimeTable();

    List<Pair<File, File>> enabledRealtimeSegments = new ArrayList<>();

    // Sort the realtime segments based on their segment name so they get added from earliest to latest
    TreeMap<File, File> sortedRealtimeSegments = new TreeMap<File, File>(new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        return _realtimeAvroToSegmentMap.get(o1).getName().compareTo(_realtimeAvroToSegmentMap.get(o2).getName());
      }
    });
    sortedRealtimeSegments.putAll(_realtimeAvroToSegmentMap);

    for (File avroFile: sortedRealtimeSegments.keySet()) {
      enabledRealtimeSegments.add(Pair.of(avroFile, sortedRealtimeSegments.get(avroFile)));

      if (useMultipleThreads) {
        _queryExecutor = new ThreadPoolExecutor(4, 4, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(50),
            new ThreadPoolExecutor.CallerRunsPolicy());
      }

      // Push avro for the new segment
      LOGGER.info("Pushing Avro file {}", avroFile);
      pushAvroIntoKafka(Collections.singletonList(avroFile), KafkaStarterUtils.DEFAULT_KAFKA_BROKER, KAFKA_TOPIC);

      // Configure the scan based comparator to use the distinct union of the offline and realtime segments
      configureScanBasedComparator(enabledRealtimeSegments);

      QueryResponse queryResponse = _scanBasedQueryProcessor.processQuery("select count(*) from mytable");

      int expectedRecordCount = queryResponse.getNumDocsScanned();
      waitForRecordCountToStabilizeToExpectedCount(expectedRecordCount, System.currentTimeMillis() + 120000L);

      // Run the actual tests
      testMethod.call();

      if (useMultipleThreads) {
        _queryExecutor.shutdown();
        _queryExecutor.awaitTermination(5, TimeUnit.MINUTES);
        int totalQueries = _failedQueries.get() + _successfulQueries.get();
        doDisplayStatus(totalQueries);
      }

      // Release resources
      _scanBasedQueryProcessor.close();
    }
  }

  private void configureScanBasedComparator(List<Pair<File, File>> enabledRealtimeSegments) throws Exception {
    // Deduplicate overlapping realtime and offline segments
    Set<String> enabledAvroFiles = new HashSet<String>();
    List<File> segmentsToUnpack = new ArrayList<File>();

    LOGGER.info("Offline avro to segment map {}", _offlineAvroToSegmentMap);
    LOGGER.info("Enabled realtime segments {}", enabledRealtimeSegments);

    for (Map.Entry<File, File> avroAndSegment: _offlineAvroToSegmentMap.entrySet()) {
      if (!enabledAvroFiles.contains(avroAndSegment.getKey().getName())) {
        enabledAvroFiles.add(avroAndSegment.getKey().getName());
        segmentsToUnpack.add(avroAndSegment.getValue());
      }
    }

    for (Pair<File, File> avroAndSegment : enabledRealtimeSegments) {
      if (!enabledAvroFiles.contains(avroAndSegment.getLeft().getName())) {
        enabledAvroFiles.add(avroAndSegment.getLeft().getName());
        segmentsToUnpack.add(avroAndSegment.getRight());
      }
    }

    LOGGER.info("Enabled Avro files {}", enabledAvroFiles);

    // Unpack enabled segments
    ensureDirectoryExistsAndIsEmpty(_unpackedSegments);
    for (File file : segmentsToUnpack) {
      LOGGER.info("Unpacking file {}", file);
      TarGzCompressionUtils.unTar(file, _unpackedSegments);
    }

    _scanBasedQueryProcessor = new ScanBasedQueryProcessor(_unpackedSegments.getAbsolutePath());
  }

  @Override
  @Test(enabled = false)
  public void testMultipleQueries() throws Exception {
    runTestLoop(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        HybridClusterScanComparisonIntegrationTest.super.testMultipleQueries();
        return null;
      }
    });
  }

  private void purgeKafkaTopicAndResetRealtimeTable() throws Exception {
    // Drop the realtime table
    dropRealtimeTable("mytable");
    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

    // Drop and recreate the Kafka topic
    KafkaStarterUtils.deleteTopic(KAFKA_TOPIC, KafkaStarterUtils.DEFAULT_ZK_STR);
    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
    KafkaStarterUtils.createTopic(KAFKA_TOPIC, KafkaStarterUtils.DEFAULT_ZK_STR);

    // Recreate the realtime table
    addRealtimeTable("mytable", "DaysSinceEpoch", "daysSinceEpoch", 900, "Days", KafkaStarterUtils.DEFAULT_ZK_STR,
        KAFKA_TOPIC, _schema.getSchemaName(), "TestTenant", "TestTenant",
        _realtimeAvroToSegmentMap.keySet().iterator().next(), 1000000, getSortedColumn(), new ArrayList<String>());
  }

  @Override
  @Test(enabled = false)
  public void testSingleQuery() throws Exception {
    runTestLoop(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        HybridClusterScanComparisonIntegrationTest.super.testSingleQuery();
        return null;
      }
    });
  }

  @Override
  @Test(enabled = false)
  public void testHardcodedQuerySet() throws Exception {
    runTestLoop(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        HybridClusterScanComparisonIntegrationTest.super.testHardcodedQuerySet();
        return null;
      }
    });
  }

  @Override
  @Test(enabled = false)
  public void testGeneratedQueries() throws Exception {
    runTestLoop(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        HybridClusterScanComparisonIntegrationTest.super.testGeneratedQueries();
        return null;
      }
    });
  }

  @Override
  @Test(enabled = false)
  public void testGeneratedQueriesWithMultivalues() throws Exception {
    runTestLoop(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        HybridClusterScanComparisonIntegrationTest.super.testGeneratedQueriesWithMultivalues();
        return null;
      }
    });
  }

  @Override
  @Test(enabled = false)
  public void testMetricAndDimColumns() throws Exception {
    // This test will not work by calling super() since we depend on h2 queries.
  }
}
