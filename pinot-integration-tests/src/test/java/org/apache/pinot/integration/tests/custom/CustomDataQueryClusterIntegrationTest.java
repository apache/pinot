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
package org.apache.pinot.integration.tests.custom;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;


public abstract class CustomDataQueryClusterIntegrationTest extends BaseClusterIntegrationTest {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CustomDataQueryClusterIntegrationTest.class);
  private static final int REALTIME_TABLE_CONFIG_RETRY_COUNT = 5;
  private static final long REALTIME_TABLE_CONFIG_RETRY_WAIT_MS = 1_000L;
  private static final long KAFKA_TOPIC_METADATA_READY_TIMEOUT_MS = 30_000L;
  protected static CustomDataQueryClusterIntegrationTest _sharedClusterTestSuite = null;
  protected static final String TIMESTAMP_FIELD_NAME = "ts";

  @BeforeSuite
  public void setUpSuite()
      throws Exception {
    LOGGER.warn("Setting up integration test suite");
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    _sharedClusterTestSuite = this;

    // Start the Pinot cluster
    startZk();
    LOGGER.warn("Start Kafka in the integration test suite");
    startKafkaWithoutTopic();
    startController();
    startBroker();
    startServer();
    startMinion();
    LOGGER.warn("Finished setting up integration test suite");
  }

  @AfterSuite
  public void tearDownSuite()
      throws Exception {
    LOGGER.warn("Tearing down integration test suite");
    // Shutdown the Pinot cluster
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    // Stop Kafka
    LOGGER.warn("Stop Kafka in the integration test suite");
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
    LOGGER.warn("Finished tearing down integration test suite");
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    LOGGER.warn("Setting up integration test class: {}", getClass().getSimpleName());
    initControllerRequestURLBuilder();
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    setUpTable();

    waitForAllDocsLoaded(60_000);
    LOGGER.warn("Finished setting up integration test class: {}", getClass().getSimpleName());
  }

  /**
   * Initializes the controller request URL builder for the shared suite.
   * Called at the start of setUp; safe to call from overridden setUp methods.
   */
  protected void initControllerRequestURLBuilder() {
    if (_controllerRequestURLBuilder == null) {
      _controllerRequestURLBuilder =
          ControllerRequestURLBuilder.baseUrl("http://localhost:" + _sharedClusterTestSuite.getControllerPort());
    }
  }

  /**
   * Creates schema, table config, builds segments, and uploads them.
   * Subclasses can override this to customize the table creation flow
   * (e.g., for dimension tables, upsert tables, or tables built from GenericRow).
   */
  protected void setUpTable()
      throws Exception {
    // create & upload schema AND table config
    Schema schema = createSchema();
    addSchema(schema);

    List<File> avroFiles = createAvroFiles();
    if (isRealtimeTable()) {
      // In suite mode multiple realtime tests use different topics, so make sure
      // this class-specific topic exists before the controller validates stream metadata.
      _sharedClusterTestSuite.createKafkaTopic(getKafkaTopic());
      waitForKafkaTopicMetadataReadyForConsumer(getKafkaTopic(), getNumKafkaPartitions());

      // create realtime table
      TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
      addRealtimeTableConfigWithRetry(tableConfig);

      // Push data into Kafka
      pushAvroIntoKafka(avroFiles);
    } else {
      // create offline table
      TableConfig tableConfig = createOfflineTableConfig();
      addTableConfig(tableConfig);

      // create & upload segments
      int segmentIndex = 0;
      for (File avroFile : avroFiles) {
        ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, segmentIndex++, _segmentDir,
            _tarDir);
        uploadSegments(getTableName(), _tarDir);
      }
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    LOGGER.warn("Tearing down integration test class: {}", getClass().getSimpleName());
    if (isRealtimeTable()) {
      dropRealtimeTable(getTableName());
    } else {
      dropOfflineTable(getTableName());
    }
    FileUtils.deleteDirectory(_tempDir);
    LOGGER.warn("Finished tearing down integration test class: {}", getClass().getSimpleName());
  }

  private void addRealtimeTableConfigWithRetry(TableConfig tableConfig)
      throws Exception {
    for (int attempt = 1; attempt <= REALTIME_TABLE_CONFIG_RETRY_COUNT; attempt++) {
      try {
        addTableConfig(tableConfig);
        return;
      } catch (IOException e) {
        if (!isRetryableRealtimePartitionMetadataError(e) || attempt == REALTIME_TABLE_CONFIG_RETRY_COUNT) {
          throw e;
        }
        LOGGER.warn("Retrying realtime table creation for topic {} after metadata propagation failure (attempt {}/{})",
            getKafkaTopic(), attempt, REALTIME_TABLE_CONFIG_RETRY_COUNT, e);
        waitForKafkaTopicMetadataReadyForConsumer(getKafkaTopic(), getNumKafkaPartitions());
        Thread.sleep(REALTIME_TABLE_CONFIG_RETRY_WAIT_MS);
      }
    }
    throw new IllegalStateException("Failed to create realtime table after retries for topic: " + getKafkaTopic());
  }

  private boolean isRetryableRealtimePartitionMetadataError(Throwable throwable) {
    String errorToken = "Failed to fetch partition information for topic: " + getKafkaTopic();
    Throwable current = throwable;
    while (current != null) {
      String message = current.getMessage();
      if (message != null && message.contains(errorToken)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private void waitForKafkaTopicMetadataReadyForConsumer(String topic, int expectedPartitions) {
    TestUtils.waitForCondition(aVoid -> isKafkaTopicMetadataReadyForConsumer(topic, expectedPartitions), 200L,
        KAFKA_TOPIC_METADATA_READY_TIMEOUT_MS,
        "Kafka topic '" + topic + "' metadata is not visible to consumers in custom cluster suite");
  }

  private boolean isKafkaTopicMetadataReadyForConsumer(String topic, int expectedPartitions) {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, _sharedClusterTestSuite.getKafkaBrokerList());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "pinot-custom-topic-ready-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    consumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
      List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic, Duration.ofSeconds(5));
      return partitionInfos != null && partitionInfos.size() >= expectedPartitions;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  protected void startServer()
      throws Exception {
    startServers(2);
  }

  @Override
  protected void pushAvroIntoKafka(List<File> avroFiles)
      throws Exception {
    ClusterIntegrationTestUtils.pushAvroIntoKafka(avroFiles,
        _sharedClusterTestSuite.getKafkaBrokerList(), getKafkaTopic(),
        getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn(), injectTombstones());
  }

  @Override
  public String getZkUrl() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getZkUrl();
    }
    return super.getZkUrl();
  }

  @Override
  protected String getBrokerBaseApiUrl() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getBrokerBaseApiUrl();
    }
    return super.getBrokerBaseApiUrl();
  }

  @Override
  protected String getBrokerGrpcEndpoint() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getBrokerGrpcEndpoint();
    }
    return super.getBrokerGrpcEndpoint();
  }

  @Override
  public int getControllerPort() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getControllerPort();
    }
    return super.getControllerPort();
  }

  @Override
  public int getRandomBrokerPort() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getRandomBrokerPort();
    }
    return super.getRandomBrokerPort();
  }

  /**
   * Returns the controller starter from the shared suite instance.
   */
  protected BaseControllerStarter getSharedControllerStarter() {
    return _sharedClusterTestSuite._controllerStarter;
  }

  /**
   * Returns the property store from the shared suite instance.
   */
  protected ZkHelixPropertyStore<ZNRecord> getSharedPropertyStore() {
    return _sharedClusterTestSuite._propertyStore;
  }

  /**
   * Returns the task manager from the shared suite's controller.
   */
  protected PinotTaskManager getTaskManager() {
    return getSharedControllerStarter().getTaskManager();
  }

  /**
   * Returns the Helix task resource manager from the shared suite's controller.
   */
  protected PinotHelixTaskResourceManager getHelixTaskResourceManager() {
    return getSharedControllerStarter().getHelixTaskResourceManager();
  }

  /**
   * Returns the Helix resource manager from the shared suite's controller.
   */
  protected PinotHelixResourceManager getSharedHelixResourceManager() {
    return getSharedControllerStarter().getHelixResourceManager();
  }

  /**
   * Returns the Kafka broker list from the shared suite instance.
   */
  protected String getSharedKafkaBrokerList() {
    return _sharedClusterTestSuite.getKafkaBrokerList();
  }

  /**
   * Creates a Kafka topic on the shared suite's Kafka cluster.
   */
  protected void createSharedKafkaTopic(String topic, int numPartitions) {
    _sharedClusterTestSuite.createKafkaTopic(topic, numPartitions);
  }

  @Override
  public String getHelixClusterName() {
    return "CustomDataQueryClusterIntegrationTest";
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
  }

  @Nullable
  protected String getSortedColumn() {
    return TIMESTAMP_FIELD_NAME;
  }

  @Nullable
  protected List<String> getInvertedIndexColumns() {
    return List.of();
  }

  @Nullable
  protected List<String> getNoDictionaryColumns() {
    return List.of();
  }

  @Nullable
  protected List<String> getRangeIndexColumns() {
    return List.of();
  }

  @Nullable
  protected List<String> getBloomFilterColumns() {
    return List.of();
  }

  @Override
  protected Map<String, String> getStreamConfigMap() {
    Map<String, String> streamConfigMap = new HashMap<>();
    String streamType = "kafka";
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(KafkaStreamConfigProperties.constructStreamProperty(
            KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST),
        _sharedClusterTestSuite.getKafkaBrokerList());
    if (useKafkaTransaction()) {
      streamConfigMap.put(KafkaStreamConfigProperties.constructStreamProperty(
              KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL),
          KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL_READ_COMMITTED);
    }
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), getStreamConsumerFactoryClassName());
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
        getKafkaTopic());
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        AvroFileSchemaKafkaAvroMessageDecoder.class.getName());
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS,
        Integer.toString(getRealtimeSegmentFlushSize()));
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");
    return streamConfigMap;
  }

  public String getTimeColumnName() {
    return TIMESTAMP_FIELD_NAME;
  }

  @Override
  public String getKafkaTopic() {
    return getTableName() + "-kafka";
  }

  @Override
  public abstract String getTableName();

  @Override
  public abstract Schema createSchema();

  public abstract List<File> createAvroFiles()
      throws Exception;

  public int getNumAvroFiles() {
    return 2;
  }

  public boolean isRealtimeTable() {
    return false;
  }

  protected AvroFilesAndWriters createAvroFilesAndWriters(org.apache.avro.Schema avroSchema)
      throws IOException {
    List<File> avroFiles = new ArrayList<>();
    List<DataFileWriter<GenericData.Record>> writers = new ArrayList<>();
    for (int i = 0; i < getNumAvroFiles(); i++) {
      File avroFile = new File(_tempDir, "data-" + i + ".avro");
      avroFiles.add(avroFile);
      DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema));
      writers.add(fileWriter);
      fileWriter.create(avroSchema, avroFile);
    }
    return new AvroFilesAndWriters(avroFiles, writers);
  }

  protected static class AvroFilesAndWriters implements Closeable {
    private final List<File> _avroFiles;
    private final List<DataFileWriter<GenericData.Record>> _writers;

    AvroFilesAndWriters(List<File> avroFiles, List<DataFileWriter<GenericData.Record>> writers) {
      _avroFiles = avroFiles;
      _writers = writers;
    }

    public List<File> getAvroFiles() {
      return _avroFiles;
    }

    public List<DataFileWriter<GenericData.Record>> getWriters() {
      return _writers;
    }

    @Override
    public void close()
        throws IOException {
      for (DataFileWriter<GenericData.Record> writer : _writers) {
        writer.close();
      }
    }
  }
}
