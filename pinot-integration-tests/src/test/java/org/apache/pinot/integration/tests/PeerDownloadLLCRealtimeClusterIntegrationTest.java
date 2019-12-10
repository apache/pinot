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
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.avro.reflect.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.config.CompletionConfig;
import org.apache.pinot.common.config.IndexingConfig;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TableTaskConfig;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.core.realtime.impl.kafka.KafkaStarterUtils;
import org.apache.pinot.core.realtime.impl.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that extends RealtimeClusterIntegrationTest but uses low-level Kafka consumer. The servers will NOT
 * upload segments to controller. During Segment Completion Protocol, a server will instead download segments from
 * peer servers.
 */
public class PeerDownloadLLCRealtimeClusterIntegrationTest extends RealtimeClusterIntegrationTest {
  private static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  private static final String TEST_UPDATED_INVERTED_INDEX_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE DivActualElapsedTime = 305";
  private static final List<String> UPDATED_INVERTED_INDEX_COLUMNS = Collections.singletonList("DivActualElapsedTime");
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);

  private final boolean _isDirectAlloc = true; //Set as true; otherwise trigger indexing exception.
  private final boolean _isConsumerDirConfigured = true;
  private final boolean _enableSplitCommit = true;
  private final boolean _enableLeadControllerResource = RANDOM.nextBoolean();
  private final long _startTime = System.currentTimeMillis();
  private TableConfig _tableConfig;

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    System.out.println(String.format(
        "Using random seed: %s, isDirectAlloc: %s, isConsumerDirConfigured: %s, enableSplitCommit: %s, enableLeadControllerResource: %s",
        RANDOM_SEED, _isDirectAlloc, _isConsumerDirConfigured, _enableSplitCommit, _enableLeadControllerResource));

    // Remove the consumer directory
    File consumerDirectory = new File(CONSUMER_DIRECTORY);
    if (consumerDirectory.exists()) {
      FileUtils.deleteDirectory(consumerDirectory);
    }
    super.setUp();
  }


  @Override
  public void startServer() {
    startServers(2);
  }

  @Override
  public void addRealtimeTable(String tableName, boolean useLlc, String kafkaBrokerList, String kafkaZkUrl,
      String kafkaTopic, int realtimeSegmentFlushRows, File avroFile, String timeColumnName, String timeType,
      String schemaName, String brokerTenant, String serverTenant, String loadMode, String sortedColumn,
      List<String> invertedIndexColumns, List<String> bloomFilterColumns, List<String> noDictionaryColumns,
      TableTaskConfig taskConfig, String streamConsumerFactoryName, int numReplicas)
      throws Exception {
    System.out.println("Setup realtime table with download completion mode");
    Map<String, String> streamConfigs = new HashMap<>();
    String streamType = "kafka";
    streamConfigs.put(StreamConfigProperties.STREAM_TYPE, streamType);
    if (useLlc) {
      // LLC
      streamConfigs
          .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
              StreamConfig.ConsumerType.LOWLEVEL.toString());
      streamConfigs.put(KafkaStreamConfigProperties
          .constructStreamProperty(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST), kafkaBrokerList);
    } else {
      // HLC
      streamConfigs
          .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
              StreamConfig.ConsumerType.HIGHLEVEL.toString());
      streamConfigs.put(KafkaStreamConfigProperties
              .constructStreamProperty(KafkaStreamConfigProperties.HighLevelConsumer.KAFKA_HLC_ZK_CONNECTION_STRING),
          kafkaZkUrl);
      streamConfigs.put(KafkaStreamConfigProperties
              .constructStreamProperty(KafkaStreamConfigProperties.HighLevelConsumer.KAFKA_HLC_BOOTSTRAP_SERVER),
          kafkaBrokerList);
    }
    streamConfigs.put(StreamConfigProperties
            .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        streamConsumerFactoryName);
    streamConfigs
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
            kafkaTopic);
    AvroFileSchemaKafkaAvroMessageDecoder.avroFile = avroFile;
    streamConfigs
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
            AvroFileSchemaKafkaAvroMessageDecoder.class.getName());
    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, Integer.toString(realtimeSegmentFlushRows));
    streamConfigs.put(StreamConfigProperties
        .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");

    _tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName(tableName).setLLC(useLlc)
        .setTimeColumnName(timeColumnName).setTimeType(timeType).setSchemaName(schemaName).setBrokerTenant(brokerTenant)
        .setServerTenant(serverTenant).setLoadMode(loadMode).setSortedColumn(sortedColumn)
        .setInvertedIndexColumns(invertedIndexColumns).setBloomFilterColumns(bloomFilterColumns)
        .setNoDictionaryColumns(noDictionaryColumns).setStreamConfigs(streamConfigs).setTaskConfig(taskConfig)
        .setNumReplicas(numReplicas).build();


    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig = new SegmentsValidationAndRetentionConfig();
    CompletionConfig completionConfig = new CompletionConfig("DOWNLOAD");
    segmentsValidationAndRetentionConfig.setCompletionConfig(completionConfig);
    segmentsValidationAndRetentionConfig.setReplicasPerPartition(String.valueOf(numReplicas));
    _tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);

    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), _tableConfig.toJsonConfigString());
  }

  @Override
  protected void setUpRealtimeTable(File avroFile)
      throws Exception {
    System.out.println("Setup realtime table with 2 replicas");
    setUpRealtimeTable(avroFile, 2, useLlc(), getTableName());
  }


  @Override
  public void startController() {
    ControllerConf controllerConfig = getDefaultControllerConfiguration();
    controllerConfig.setHLCTablesAllowed(false);
    controllerConfig.setSplitCommit(_enableSplitCommit);
    startController(controllerConfig);
    enableResourceConfigForLeadControllerResource(_enableLeadControllerResource);
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Nullable
  @Override
  protected String getLoadMode() {
    return "MMAP";
  }

  @Override
  protected void overrideServerConf(Configuration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION, _isDirectAlloc);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_ENABLE_UPLOAD_TO_CONTROLLER, false);
    if (_isConsumerDirConfigured) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_CONSUMER_DIR, CONSUMER_DIRECTORY);
    }
    if (_enableSplitCommit) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_SPLIT_COMMIT, true);
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_COMMIT_END_WITH_METADATA, true);
    }
  }

  @Test
  public void testConsumerDirectoryExists() {
    File consumerDirectory = new File(CONSUMER_DIRECTORY, "mytable_REALTIME");
    assertEquals(consumerDirectory.exists(), _isConsumerDirConfigured,
        "The off heap consumer directory does not exist");
  }

  @Test
  public void testSegmentFlushSize() {
    String zkSegmentsPath = "/SEGMENTS/" + TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    List<String> segmentNames = _propertyStore.getChildNames(zkSegmentsPath, 0);
    for (String segmentName : segmentNames) {
      ZNRecord znRecord = _propertyStore.get(zkSegmentsPath + "/" + segmentName, null, 0);
      assertEquals(znRecord.getSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE),
          Integer.toString(getRealtimeSegmentFlushSize() / getNumKafkaPartitions()),
          "Segment: " + segmentName + " does not have the expected flush size");
    }
  }

  @Test
  public void testInvertedIndexTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    JsonNode queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertTrue(queryResponse.get("numEntriesScannedInFilter").asLong() > 0L);

    {
      IndexingConfig config = _tableConfig.getIndexingConfig();
      config.setInvertedIndexColumns(UPDATED_INVERTED_INDEX_COLUMNS);
      config.setBloomFilterColumns(null);

      sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(getTableName()),
          _tableConfig.toJsonConfigString());
    }
    sendPostRequest(_controllerRequestURLBuilder.forTableReload(getTableName(), "realtime"), null);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse1 = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse1.get("totalDocs").asLong(), numTotalDocs);
        assertEquals(queryResponse1.get("numConsumingSegmentsQueried").asLong(), 2);
        assertTrue(queryResponse1.get("minConsumingFreshnessTimeMs").asLong() > _startTime);
        assertTrue(queryResponse1.get("minConsumingFreshnessTimeMs").asLong() < System.currentTimeMillis());
        return queryResponse1.get("numEntriesScannedInFilter").asLong() == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate inverted index");
  }

  @Test(expectedExceptions = IOException.class)
  public void testAddHLCTableShouldFail()
      throws IOException {
    TableConfig tableConfig = new TableConfig.Builder(TableType.REALTIME).setTableName("testTable")
        .setStreamConfigs(Collections.singletonMap("stream.kafka.consumer.type", "HIGHLEVEL")).build();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonConfigString());
  }
}
