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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
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
    startKafka();
    startController();
    startBroker();
    startServer();
    LOGGER.warn("Finished setting up integration test suite");
  }

  @AfterSuite
  public void tearDownSuite()
      throws Exception {
    LOGGER.warn("Tearing down integration test suite");
    // Stop Kafka
    LOGGER.warn("Stop Kafka in the integration test suite");
    stopKafka();
    // Shutdown the Pinot cluster
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
    LOGGER.warn("Finished tearing down integration test suite");
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    LOGGER.warn("Setting up integration test class: {}", getClass().getSimpleName());
    if (_controllerRequestURLBuilder == null) {
      _controllerRequestURLBuilder =
          ControllerRequestURLBuilder.baseUrl("http://localhost:" + _sharedClusterTestSuite.getControllerPort());
    }
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    // create & upload schema AND table config
    Schema schema = createSchema();
    addSchema(schema);

    List<File> avroFiles = createAvroFiles();
    if (isRealtimeTable()) {
      // create realtime table
      TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
      addTableConfig(tableConfig);

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

    waitForAllDocsLoaded(60_000);
    LOGGER.warn("Finished setting up integration test class: {}", getClass().getSimpleName());
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

  @Override
  protected void pushAvroIntoKafka(List<File> avroFiles)
      throws Exception {
    ClusterIntegrationTestUtils.pushAvroIntoKafka(avroFiles,
        "localhost:" + _sharedClusterTestSuite._kafkaStarters.get(0).getPort(), getKafkaTopic(),
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
        "localhost:" + _sharedClusterTestSuite._kafkaStarters.get(0).getPort());
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

  public boolean isRealtimeTable() {
    return false;
  }
}
