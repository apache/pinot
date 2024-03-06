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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.debug.TableDebugInfo;
import org.apache.pinot.controller.helix.ControllerRequestClient;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Reporter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RealtimeOffsetValidationIntegrationTest extends BaseClusterIntegrationTest {
  private static final String PARTITION_COLUMN = "DestState";
  private static final long NUM_DOCS_IN_FIRST_AVRO_FILE = 9292;
  private List<File> _avroFiles;
  int _controllerPort = 0;
  private String _partitionColumn;
  private long _countStarResult;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    Map<String, Object> controllerProperties = getDefaultControllerConfiguration();
    _controllerPort = (Integer) controllerProperties.get(ControllerConf.CONTROLLER_PORT);
    startController(controllerProperties);
    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    _avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config with reduced number of columns and partition config
    Schema schema = new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(PARTITION_COLUMN, FieldSpec.DataType.STRING)
        .addDateTime("DaysSinceEpoch", FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS").build();
    addSchema(schema);

    TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));

    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setSegmentPartitionConfig(new SegmentPartitionConfig(
        Collections.singletonMap(PARTITION_COLUMN, new ColumnPartitionConfig("murmur", 2))));
    addTableConfig(tableConfig);

    // Push data into Kafka (only ingest the first Avro file)
    _partitionColumn = PARTITION_COLUMN;
    pushAvroIntoKafka(Collections.singletonList(_avroFiles.get(0)));

    // Wait for all documents loaded
    _countStarResult = NUM_DOCS_IN_FIRST_AVRO_FILE;
    waitForAllDocsLoaded(600_000L);
  }

  @Override
  public void startKafka(int port) {
    Properties kafkaConfig = KafkaStarterUtils.getDefaultKafkaConfiguration();
    _kafkaStarters = KafkaStarterUtils.startServers(getNumKafkaBrokers(), port, getKafkaZKAddress(), kafkaConfig);

    Properties topicProperties = KafkaStarterUtils.getTopicCreationProps(getNumKafkaPartitions());
    topicProperties.put("retention.ms", "1000"); // Retain for 1 second only
    topicProperties.put("cleanup.policy", "delete");
    topicProperties.put("segment.bytes", "10240");
    topicProperties.put("segment.ms", "1000"); // Retain for 1 second only

    _kafkaStarters.get(0)
        .createTopic(getKafkaTopic(), topicProperties);
  }

  @Override
  protected long getCountStarResult() {
    return _countStarResult;
  }

  @Nullable
  @Override
  protected String getPartitionColumn() {
    return _partitionColumn;
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
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

  private Boolean checkTopicOffsetFastForwarded(AdminClient admin, String topic) throws Exception {
    DescribeTopicsResult topicsResult = admin.describeTopics(Collections.singletonList(getKafkaTopic()),
        new DescribeTopicsOptions());

    TopicDescription description = topicsResult.all().get().get(topic);

    Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpec = new HashMap<>();
    for (TopicPartitionInfo info : description.partitions()) {
      topicPartitionOffsetSpec.put(new TopicPartition(topic, info.partition()), OffsetSpec.earliest());
    }

    ListOffsetsResult listOffsetsResult = admin.listOffsets(topicPartitionOffsetSpec, new ListOffsetsOptions());

    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsetsResultInfoMap = listOffsetsResult.all().get();
    boolean fastForwarded = true;
    for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry: offsetsResultInfoMap.entrySet()) {
      // Assume the first avro file was split between 2 partitions.
      // So the offset has to move beyond the number of messages in the first avro file at least.
      fastForwarded &= (entry.getValue().offset() > NUM_DOCS_IN_FIRST_AVRO_FILE / 2);
    }

    return fastForwarded;
  }

  private Boolean checkForValidationError(ControllerRequestClient client) throws IOException {
    List<TableDebugInfo> debugInfos = client.getTableDebugInfo(getTableName(), "REALTIME" /*how to get this?*/);
    assert debugInfos.size() == 1;

    for (TableDebugInfo.SegmentDebugInfo segmentDebugInfo : debugInfos.get(0).getSegmentDebugInfos()) {
      if (segmentDebugInfo.getValidationError() != null) {
        // Do more validation
        return true;
      }
    }
    return false;
  }

  @Test
  public void testProduceSecondBatch() throws Exception {

    HttpClient httpClient = HttpClient.getInstance();
    ControllerRequestClient client = new ControllerRequestClient(
        ControllerRequestURLBuilder.baseUrl("http://" + LOCAL_HOST + ":" + _controllerPort), httpClient);
    client.pauseConsumption(getTableName());
    Reporter.log("Consumption has been stopped");
    // Push data into Kafka
    pushAvroIntoKafka(Collections.singletonList(_avroFiles.get(1)));
    Thread.sleep(60000);
    Reporter.log("Second file has been pushed");

    TestUtils.waitForCondition(() -> this.checkTopicOffsetFastForwarded(getKafkaAdminClient(), getKafkaTopic()),
        1000L, // 1 second
        10 * 1000L, // 10 Seconds,
        "Kafka did not fast forward offsets within 10 seconds.", true, null);

    client.resumeConsumption(getTableName());
    Reporter.log("Consumption has been resumed.");

    TestUtils.waitForCondition(() -> this.checkForValidationError(client),
        1000L, // 1 second
        10 * 1000L, // 10 Seconds,
        "Validation Error was not reported in TableDebugInfo within 10 seconds.", true, null);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    waitForTableDataManagerRemoved(TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
