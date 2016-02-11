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

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.tools.query.comparison.QueryComparison;
import com.linkedin.pinot.tools.scan.query.QueryResponse;
import com.linkedin.pinot.tools.scan.query.ScanBasedQueryProcessor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.util.TestUtils;
import org.testng.annotations.Test;


/**
 * Cluster integration test that compares with the scan based comparison tool.
 */
public class HybridClusterScanComparisonIntegrationTest extends HybridClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(HybridClusterScanComparisonIntegrationTest.class);
  protected final File _offlineTarDir = new File("/tmp/HybridClusterIntegrationTest/offlineTarDir");
  protected final File _realtimeTarDir = new File("/tmp/HybridClusterIntegrationTest/realtimeTarDir");
  protected final File _unpackedSegments = new File("/tmp/HybridClusterIntegrationTest/unpackedSegments");
  private Map<File, File> _offlineAvroToSegmentMap;
  private Map<File, File> _realtimeAvroToSegmentMap;
  private File _schemaFile;
  private Schema _schema;
  private ScanBasedQueryProcessor _scanBasedQueryProcessor;

  @AfterClass
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    FileUtils.deleteQuietly(_offlineTarDir);
    FileUtils.deleteQuietly(_realtimeTarDir);
    FileUtils.deleteQuietly(_unpackedSegments);
  }

  @Override
  @BeforeClass
  public void setUp() throws Exception {
    //Clean up
    ensureDirectoryExistsAndIsEmpty(_tmpDir);
    ensureDirectoryExistsAndIsEmpty(_segmentDir);
    ensureDirectoryExistsAndIsEmpty(_offlineTarDir);
    ensureDirectoryExistsAndIsEmpty(_realtimeTarDir);
    ensureDirectoryExistsAndIsEmpty(_unpackedSegments);

    // Start Zk, Kafka and Pinot
    startHybridCluster();

    // Unpack the Avro files
    TarGzCompressionUtils.unTar(
        new File(TestUtils.getFileFromResourceUrl(OfflineClusterIntegrationTest.class.getClassLoader().getResource(
            "On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz"))), _tmpDir);

    _tmpDir.mkdirs();

    final List<File> avroFiles = new ArrayList<File>(SEGMENT_COUNT);
    for (int segmentNumber = 1; segmentNumber <= SEGMENT_COUNT; ++segmentNumber) {
      avroFiles.add(new File(_tmpDir.getPath() + "/On_Time_On_Time_Performance_2014_" + segmentNumber + ".avro"));
    }

    _schemaFile = getSchemaFile();
    _schema = Schema.fromFile(_schemaFile);

    // Create Pinot table
    setUpTable("mytable", "DaysSinceEpoch", "daysSinceEpoch", KafkaStarterUtils.DEFAULT_ZK_STR, KAFKA_TOPIC,
        _schemaFile,
        avroFiles.get(0));

    final List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles);
    final List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles);

    // Create segments from Avro data
    ExecutorService executor = Executors.newCachedThreadPool();
    Future<Map<File, File>> offlineAvroToSegmentMapFuture =
        buildSegmentsFromAvro(offlineAvroFiles, executor, 0, _segmentDir, _offlineTarDir, "mytable");
    Future<Map<File, File>> realtimeAvroToSegmentMapFuture =
        buildSegmentsFromAvro(realtimeAvroFiles, executor, 0, _segmentDir, _realtimeTarDir, "mytable");

    // Initialize query generator
    setupQueryGenerator(avroFiles, executor);

    // Redeem futures
    _offlineAvroToSegmentMap = offlineAvroToSegmentMapFuture.get();
    _realtimeAvroToSegmentMap = realtimeAvroToSegmentMapFuture.get();

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Set up a Helix spectator to count the number of segments that are uploaded and unlock the latch once 12 segments are online
    final CountDownLatch latch = setupSegmentCountCountDownLatch("mytable", OFFLINE_SEGMENT_COUNT);

    // Upload the offline segments
    int i = 0;
    for (String segmentName : _offlineTarDir.list()) {
      System.out.println("Uploading segment " + (i++) + " : " + segmentName);
      File file = new File(_offlineTarDir, segmentName);
      FileUploadUtils.sendSegmentFile("localhost", "8998", segmentName, new FileInputStream(file), file.length());
    }

    // Wait for all offline segments to be online
    latch.await();
  }

  @Override
  protected void runQuery(String pqlQuery, List<String> sqlQueries) throws Exception {
    QueryResponse scanResponse = _scanBasedQueryProcessor.processQuery(pqlQuery);
    JSONObject scanJson = new JSONObject(new ObjectMapper().writeValueAsString(scanResponse));
    System.out.println("scanJson = " + scanJson);
    JSONObject pinotJson = postQuery(pqlQuery);
    Assert.assertTrue(QueryComparison.compare(pinotJson, scanJson));
  }

  private void runTestLoop(Callable<Object> testMethod) throws Exception {
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

      // Push avro for the new segment
      System.out.println("pushing avroFile = " + avroFile);
      pushAvroIntoKafka(Collections.singletonList(avroFile), KafkaStarterUtils.DEFAULT_KAFKA_BROKER,
          KAFKA_TOPIC);

      // Configure the scan based comparator to use the distinct union of the offline and realtime segments
      configureScanBasedComparator(enabledRealtimeSegments);

      QueryResponse queryResponse = _scanBasedQueryProcessor.processQuery("select count(*) from mytable");

      int expectedRecordCount = queryResponse.getNumDocsScanned();
      waitForRecordCountToStabilizeToExpectedCount(expectedRecordCount, System.currentTimeMillis() + 120000L);

      // Run the actual tests
      testMethod.call();
    }
  }

  private void configureScanBasedComparator(List<Pair<File, File>> enabledRealtimeSegments) throws Exception {
    // Deduplicate overlapping realtime and offline segments
    Set<String> enabledAvroFiles = new HashSet<String>();
    List<File> segmentsToUnpack = new ArrayList<File>();

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

    // Unpack enabled segments
    ensureDirectoryExistsAndIsEmpty(_unpackedSegments);
    for (File file : segmentsToUnpack) {
      System.out.println("unpacked file = " + file);
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
        _realtimeAvroToSegmentMap.keySet().iterator().next(), 20000, new ArrayList<String>());
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
}
