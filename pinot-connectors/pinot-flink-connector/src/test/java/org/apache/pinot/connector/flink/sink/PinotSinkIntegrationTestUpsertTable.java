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
package org.apache.pinot.connector.flink.sink;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.pinot.connector.flink.common.FlinkRowGenericRowConverter;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class PinotSinkIntegrationTestUpsertTable extends BaseClusterIntegrationTest {

  private List<Row> _data;
  public RowTypeInfo _typeInfo;
  public TableConfig _tableConfig;
  public Schema _schema;
  private String _rawTableName;

  protected PinotTaskManager _taskManager;
  protected PinotHelixTaskResourceManager _helixTaskResourceManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBroker();
    startServers(2);
    startMinion();

    // Start Kafka and push data into Kafka
    startKafka();

    // Push data to Kafka and set up table
    setupTable(getTableName(), getKafkaTopic(), "gameScores_csv.tar.gz", null);

    // Wait for all documents loaded
    waitForAllDocsLoaded(60_000L);
    assertEquals(getCurrentCountStarResult(), getCountStarResult());

    // Create partial upsert table schema
    Schema partialUpsertSchema = createSchema("upsert_table_test.schema");
    addSchema(partialUpsertSchema);
    _taskManager = _controllerStarter.getTaskManager();
    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();

    _schema = createSchema();
    _rawTableName = getTableName();
    _tableConfig = createCSVUpsertTableConfig(_rawTableName, getKafkaTopic(), getNumKafkaPartitions(),
        getCSVDecoderProperties(",", "playerId,name,game,score,timestampInEpoch,deleted"), null, "playerId");
    _typeInfo = new RowTypeInfo(
        new TypeInformation[]{Types.INT, Types.STRING, Types.STRING, Types.FLOAT, Types.LONG, Types.BOOLEAN},
        new String[]{"playerId", "name", "game", "score", "timestampInEpoch", "deleted"});
    _data = Arrays.asList(Row.of(1, "Alice", "Football", 55F, 1571037400000L, false),
        Row.of(2, "BOB", "Tennis", 100F, 1571038400000L, false),
        Row.of(3, "Carl", "Cricket", 100F, 1571039400000L, false),
        Row.of(2, "BOB", "Badminton", 100F, 1571040400000L, false));

    Map<String, String> batchConfigs = new HashMap<>();
    batchConfigs.put(BatchConfigProperties.OUTPUT_DIR_URI, _tarDir.getAbsolutePath());
    batchConfigs.put(BatchConfigProperties.OVERWRITE_OUTPUT, "false");
    batchConfigs.put(BatchConfigProperties.PUSH_CONTROLLER_URI, getControllerBaseApiUrl());
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Collections.singletonList(batchConfigs), "APPEND", "HOURLY"));

    _tableConfig.setIngestionConfig(ingestionConfig);
  }

  @Test
  public void testPinotSinkWrite()
      throws Exception {
    Configuration config = new Configuration();
    config.set(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT,
        Duration.ofMillis(120000)); // Set heartbeat timeout to 60 seconds
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment(config);

    // Single-thread write
    execEnv.setParallelism(1);
    DataStream<Row> srcDs = execEnv.fromCollection(_data).returns(_typeInfo);
    srcDs.addSink(new PinotSinkFunction<>(new FlinkRowGenericRowConverter(_typeInfo), _tableConfig, _schema,
        PinotSinkFunction.DEFAULT_SEGMENT_FLUSH_MAX_NUM_RECORDS, PinotSinkFunction.DEFAULT_EXECUTOR_POOL_SIZE, "batch",
        1724045185L));
    execEnv.execute();
    // 1 uploaded, 2 realtime in progress segment
    verifySegments(3, 4);

    // Parallel write with partitioned segments
    execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.setParallelism(2);

    srcDs = execEnv.fromCollection(_data).returns(_typeInfo)
        .partitionCustom((Partitioner<Integer>) (key, partitions) -> key % partitions, r -> (Integer) r.getField(0));
    srcDs.addSink(new PinotSinkFunction<>(new FlinkRowGenericRowConverter(_typeInfo), _tableConfig, _schema,
        PinotSinkFunction.DEFAULT_SEGMENT_FLUSH_MAX_NUM_RECORDS, PinotSinkFunction.DEFAULT_EXECUTOR_POOL_SIZE, "batch",
        1724045186L));
    execEnv.execute();
    verifySegments(5, 8);

    // Generate next sequence segments in partitioned segments
    srcDs = execEnv.fromCollection(_data).returns(_typeInfo)
        .partitionCustom((Partitioner<Integer>) (key, partitions) -> key % partitions, r -> (Integer) r.getField(0));
    srcDs.addSink(new PinotSinkFunction<>(new FlinkRowGenericRowConverter(_typeInfo), _tableConfig, _schema, 1,
        PinotSinkFunction.DEFAULT_EXECUTOR_POOL_SIZE, "batch", 1724045187L));
    execEnv.execute();
    verifySegments(9, 12);
    verifyContainsSegments(Arrays.asList("batch__mytable__0__1724045187__0", "batch__mytable__0__1724045187__1",
        "batch__mytable__1__1724045187__0", "batch__mytable__1__1724045187__1"));
  }

  private void verifyContainsSegments(List<String> segmentsToCheck)
      throws IOException {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(_rawTableName);
    JsonNode segments = JsonUtils.stringToJsonNode(sendGetRequest(
            _controllerRequestURLBuilder.forSegmentListAPI(tableNameWithType, TableType.REALTIME.toString()))).get(0)
        .get("REALTIME");
    Set<String> segmentNames = new HashSet<>();
    for (int i = 0; i < segments.size(); i++) {
      String segmentName = segments.get(i).asText();
      segmentNames.add(segmentName);
    }

    for (String segmentName : segmentsToCheck) {
      assertTrue(segmentNames.contains(segmentName));
    }
  }

  private void verifySegments(int numSegments, int numTotalDocs)
      throws IOException {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(_rawTableName);
    JsonNode segments = JsonUtils.stringToJsonNode(sendGetRequest(
            _controllerRequestURLBuilder.forSegmentListAPI(tableNameWithType, TableType.REALTIME.toString()))).get(0)
        .get("REALTIME");
    assertEquals(segments.size(), numSegments);
    int actualNumTotalDocs = 0;
    // count docs in completed segments only
    for (int i = 0; i < numSegments; i++) {
      String segmentName = segments.get(i).asText();
      JsonNode segmentMetadata = JsonUtils.stringToJsonNode(
          sendGetRequest(_controllerRequestURLBuilder.forSegmentMetadata(tableNameWithType, segmentName)));
      if (segmentMetadata.get("segment.realtime.status").asText().equals("IN_PROGRESS")) {
        continue;
      }
      actualNumTotalDocs += segmentMetadata.get("segment.total.docs").asInt();
    }
    assertEquals(actualNumTotalDocs, numTotalDocs);
  }

  private TableConfig setupTable(String tableName, String kafkaTopicName, String inputDataFile,
      UpsertConfig upsertConfig)
      throws Exception {
    // SETUP
    // Create table with delete Record column
    Map<String, String> csvDecoderProperties =
        getCSVDecoderProperties(",", "playerId,name,game,score,timestampInEpoch,deleted");
    Schema upsertSchema = createSchema();
    upsertSchema.setSchemaName(tableName);
    addSchema(upsertSchema);
    TableConfig tableConfig =
        createCSVUpsertTableConfig(tableName, kafkaTopicName, getNumKafkaPartitions(), csvDecoderProperties,
            upsertConfig, "playerId");
    addTableConfig(tableConfig);

    // Push initial 10 upsert records - 3 pks 100, 101 and 102
    List<File> dataFiles = unpackTarData(inputDataFile, _tempDir);
    pushCsvIntoKafka(dataFiles.get(0), kafkaTopicName, 0);

    return tableConfig;
  }

  @Override
  protected String getSchemaFileName() {
    return "upsert_table_test.schema";
  }

  @Override
  protected String getTimeColumnName() {
    return "timestampInEpoch";
  }

  @Override
  protected long getCountStarResult() {
    return 3;
  }
}
