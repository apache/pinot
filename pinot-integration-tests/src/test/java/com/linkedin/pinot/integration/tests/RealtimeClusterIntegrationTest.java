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
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


/**
 * Integration test that creates a Kafka broker, creates a Pinot cluster that consumes from Kafka and queries Pinot.
 *
 */
public class RealtimeClusterIntegrationTest extends BaseClusterIntegrationTestWithQueryGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeClusterIntegrationTest.class);
  private final File _tmpDir = new File("/tmp/RealtimeClusterIntegrationTest");
  protected static final String KAFKA_TOPIC = "realtime-integration-test";

  private static final int SEGMENT_COUNT = 12;
  protected static final int ROW_COUNT_FOR_REALTIME_SEGMENT_FLUSH = 20000;
  protected List<KafkaServerStartable> kafkaStarters;

  protected void setUpTable(String tableName, String timeColumnName, String timeColumnType, String kafkaZkUrl,
      String kafkaTopic, File schemaFile, File avroFile) throws Exception {
    Schema schema = Schema.fromFile(schemaFile);
    addSchema(schemaFile, schema.getSchemaName());
    List<String> noDictionaryColumns = Arrays.asList("NASDelay", "ArrDelayMinutes", "DepDelayMinutes");
    addRealtimeTable(tableName, timeColumnName, timeColumnType, -1, "", kafkaZkUrl, kafkaTopic, schema.getSchemaName(),
        null, null, avroFile, ROW_COUNT_FOR_REALTIME_SEGMENT_FLUSH, "Carrier", new ArrayList<String>(), null,
        noDictionaryColumns, null);
  }

  protected void startKafka() {
    kafkaStarters =
        KafkaStarterUtils.startServers(getKafkaBrokerCount(), KafkaStarterUtils.DEFAULT_KAFKA_PORT,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    // Create Kafka topic
    createKafkaTopic(KAFKA_TOPIC, KafkaStarterUtils.DEFAULT_ZK_STR);
  }

  @BeforeClass
  public void setUp() throws Exception {
    // Start ZK and Kafka
    startZk();
    startKafka();

    // Start the Pinot cluster
    startController();
    startBroker();
    startServer();

    // Unpack data
    final List<File> avroFiles = unpackAvroData(_tmpDir, SEGMENT_COUNT);

    File schemaFile = getSchemaFile();

    // Load data into H2
    ExecutorService executor = Executors.newCachedThreadPool();
    setupH2AndInsertAvro(avroFiles, executor);

    // Initialize query generator
    setupQueryGenerator(avroFiles, executor);

    // Push data into the Kafka topic
    pushAvroIntoKafka(avroFiles, executor, KAFKA_TOPIC);

    // Wait for data push, query generator initialization and H2 load to complete
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Create Pinot table
    setUpTable("mytable", "DaysSinceEpoch", "daysSinceEpoch", KafkaStarterUtils.DEFAULT_ZK_STR, KAFKA_TOPIC,
        schemaFile, avroFiles.get(0));

    // Wait until the Pinot event count matches with the number of events in the Avro files
    long timeInFiveMinutes = System.currentTimeMillis() + 5 * 60 * 1000L;
    Statement statement = _connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    statement.execute("select count(*) from mytable");
    ResultSet rs = statement.getResultSet();
    rs.first();
    int h2RecordCount = rs.getInt(1);
    rs.close();

    waitForRecordCountToStabilizeToExpectedCount(h2RecordCount, timeInFiveMinutes);
  }

  protected int getKafkaBrokerCount() {
    return 1;
  }

  protected void createKafkaTopic(String kafkaTopic, String zkStr) {
    KafkaStarterUtils.createTopic(kafkaTopic, zkStr, 10);
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopBroker();
    stopController();
    stopServer();
    for (KafkaServerStartable kafkaStarter : kafkaStarters) {
      KafkaStarterUtils.stopServer(kafkaStarter);
    }
    try {
      stopZk();
    } catch (Exception e) {
      // Swallow ZK Exceptions.
    }
    FileUtils.deleteDirectory(_tmpDir);
  }

  @Override
  protected String getTableName() {
    return "mytable";
  }
}
