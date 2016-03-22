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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.util.TestUtils;
import kafka.server.KafkaServerStartable;


/**
 * Hybrid cluster integration test that uploads 8 months of data as offline and 6 months of data as realtime (with a
 * two month overlap).
 *
 */
public class HybridClusterIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeClusterIntegrationTest.class);
  protected final File _tmpDir = new File("/tmp/HybridClusterIntegrationTest");
  protected final File _segmentDir = new File("/tmp/HybridClusterIntegrationTest/segmentDir");
  protected final File _tarDir = new File("/tmp/HybridClusterIntegrationTest/tarDir");
  protected static final String KAFKA_TOPIC = "hybrid-integration-test";
  protected static final int QUERY_COUNT = 1000;

  private int segmentCount = 12;
  private int offlineSegmentCount = 8;
  private int realtimeSegmentCount = 6;

  private KafkaServerStartable kafkaStarter;

  protected void setUpTable(String tableName, String timeColumnName, String timeColumnType, String kafkaZkUrl,
      String kafkaTopic, File schemaFile, File avroFile, String sortedColumn, List<String> invertedIndexColumns)
      throws Exception {
    Schema schema = Schema.fromFile(schemaFile);
    addSchema(schemaFile, schema.getSchemaName());
    addHybridTable(tableName, timeColumnName, timeColumnType, kafkaZkUrl, kafkaTopic, schema.getSchemaName(),
        "TestTenant", "TestTenant", avroFile, sortedColumn, invertedIndexColumns);
  }

  protected void setSegmentCount(int segmentCount) {
    this.segmentCount = segmentCount;
  }

  protected void setOfflineSegmentCount(int offlineSegmentCount) {
    this.offlineSegmentCount = offlineSegmentCount;
  }

  protected void setRealtimeSegmentCount(int realtimeSegmentCount) {
    this.realtimeSegmentCount = realtimeSegmentCount;
  }

  protected int getOfflineSegmentCount() {
    return offlineSegmentCount;
  }

  @BeforeClass
  public void setUp() throws Exception {
    //Clean up
    ensureDirectoryExistsAndIsEmpty(_tmpDir);
    ensureDirectoryExistsAndIsEmpty(_segmentDir);
    ensureDirectoryExistsAndIsEmpty(_tarDir);

    // Start Zk, Kafka and Pinot
    startHybridCluster();

    // Unpack the Avro files
    TarGzCompressionUtils.unTar(
        new File(TestUtils.getFileFromResourceUrl(OfflineClusterIntegrationTest.class.getClassLoader().getResource(
            "On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz"))), _tmpDir);

    _tmpDir.mkdirs();

    final List<File> avroFiles = getAllAvroFiles();

    File schemaFile = getSchemaFile();

    // Create Pinot table
    setUpTable("mytable", "DaysSinceEpoch", "daysSinceEpoch", KafkaStarterUtils.DEFAULT_ZK_STR, KAFKA_TOPIC, schemaFile,
        avroFiles.get(0), "Carrier", null);

    // Create a subset of the first 8 segments (for offline) and the last 6 segments (for realtime)
    final List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles);
    final List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles);

    // Load data into H2
    ExecutorService executor = Executors.newCachedThreadPool();
    setupH2AndInsertAvro(avroFiles, executor);

    // Create segments from Avro data
    buildSegmentsFromAvro(offlineAvroFiles, executor, 0, _segmentDir, _tarDir, "mytable", false);

    // Initialize query generator
    setupQueryGenerator(avroFiles, executor);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Set up a Helix spectator to count the number of segments that are uploaded and unlock the latch once 12 segments are online
    final CountDownLatch latch = new CountDownLatch(1);
    HelixManager manager =
        HelixManagerFactory.getZKHelixManager(getHelixClusterName(), "test_instance", InstanceType.SPECTATOR,
            ZkStarter.DEFAULT_ZK_STR);
    manager.connect();
    manager.addExternalViewChangeListener(new ExternalViewChangeListener() {
      @Override
      public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
        for (ExternalView externalView : externalViewList) {
          if (externalView.getId().contains("mytable")) {

            Set<String> partitionSet = externalView.getPartitionSet();
            if (partitionSet.size() == offlineSegmentCount) {
              int onlinePartitionCount = 0;

              for (String partitionId : partitionSet) {
                Map<String, String> partitionStateMap = externalView.getStateMap(partitionId);
                if (partitionStateMap.containsValue("ONLINE")) {
                  onlinePartitionCount++;
                }
              }

              if (onlinePartitionCount == offlineSegmentCount) {
                System.out.println("Got " + offlineSegmentCount + " online tables, unlatching the main thread");
                latch.countDown();
              }
            }
          }
        }
      }
    });

    // Upload the segments
    int i = 0;
    for (String segmentName : _tarDir.list()) {
      System.out.println("Uploading segment " + (i++) + " : " + segmentName);
      File file = new File(_tarDir, segmentName);
      FileUploadUtils.sendSegmentFile("localhost", "8998", segmentName, new FileInputStream(file), file.length());
    }

    // Wait for all offline segments to be online
    latch.await();

    // Load realtime data into Kafka
    pushAvroIntoKafka(realtimeAvroFiles, KafkaStarterUtils.DEFAULT_KAFKA_BROKER, KAFKA_TOPIC);

    // Wait until the Pinot event count matches with the number of events in the Avro files
    int pinotRecordCount, h2RecordCount;
    long timeInTwoMinutes = System.currentTimeMillis() + 2 * 60 * 1000L;

    Statement statement = _connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    statement.execute("select count(*) from mytable");
    ResultSet rs = statement.getResultSet();
    rs.first();
    h2RecordCount = rs.getInt(1);
    rs.close();

    waitForRecordCountToStabilizeToExpectedCount(h2RecordCount, timeInTwoMinutes);
  }

  protected List<File> getAllAvroFiles() {
    final List<File> avroFiles = new ArrayList<File>(segmentCount);
    for (int segmentNumber = 1; segmentNumber <= segmentCount; ++segmentNumber) {
      avroFiles.add(new File(_tmpDir.getPath() + "/On_Time_On_Time_Performance_2014_" + segmentNumber + ".avro"));
    }
    return avroFiles;
  }

  protected List<File> getRealtimeAvroFiles(List<File> avroFiles) {
    final List<File> realtimeAvroFiles = new ArrayList<File>(realtimeSegmentCount);
    for (int i = segmentCount - realtimeSegmentCount; i < segmentCount; i++) {
      realtimeAvroFiles.add(avroFiles.get(i));
    }
    return realtimeAvroFiles;
  }

  protected List<File> getOfflineAvroFiles(List<File> avroFiles) {
    final List<File> offlineAvroFiles = new ArrayList<File>(offlineSegmentCount);
    for (int i = 0; i < offlineSegmentCount; i++) {
      offlineAvroFiles.add(avroFiles.get(i));
    }
    return offlineAvroFiles;
  }

  protected void startHybridCluster() throws Exception {
    // Start Zk and Kafka
    startZk();
    kafkaStarter =
        KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT, KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    // Create Kafka topic
    KafkaStarterUtils.createTopic(KAFKA_TOPIC, KafkaStarterUtils.DEFAULT_ZK_STR);

    // Start the Pinot cluster
    startController(true);
    startBroker();
    startServers(2);

    // Create tenants
    createBrokerTenant("TestTenant", 1);
    createServerTenant("TestTenant", 1, 1);
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopBroker();
    stopController();
    stopServer();
    KafkaStarterUtils.stopServer(kafkaStarter);
    try {
      stopZk();
    } catch (Exception e) {
      // Swallow ZK Exceptions.
    }
    cleanup();
  }

  protected void cleanup() throws  Exception {
    FileUtils.deleteDirectory(_tmpDir);
  }

  @Override
  protected int getGeneratedQueryCount() {
    return QUERY_COUNT;
  }

  @Override
  @Test
  public void testMultipleQueries() throws Exception {
    super.testMultipleQueries();
  }

  @Test
  public void testSingleQuery() throws Exception {
    String query;
    query = "select count(*) from 'mytable' where DaysSinceEpoch >= 16312 and Carrier = 'DL'";
    super.runQuery(query, Collections.singletonList(query.replace("'mytable'", "mytable")));
    query = "select count(*) from 'mytable' where DaysSinceEpoch < 16312 and Carrier = 'DL'";
    super.runQuery(query, Collections.singletonList(query.replace("'mytable'", "mytable")));
    query = "select count(*) from 'mytable' where DaysSinceEpoch <= 16312 and Carrier = 'DL'";
    super.runQuery(query, Collections.singletonList(query.replace("'mytable'", "mytable")));
    query = "select count(*) from 'mytable' where DaysSinceEpoch > 16312 and Carrier = 'DL'";
    super.runQuery(query, Collections.singletonList(query.replace("'mytable'", "mytable")));
  }

  @Override
  @Test
  public void testHardcodedQuerySet() throws Exception {
    super.testHardcodedQuerySet();
  }

  @Override
  @Test
  public void testGeneratedQueries() throws Exception {
    super.testGeneratedQueries();
  }
}
