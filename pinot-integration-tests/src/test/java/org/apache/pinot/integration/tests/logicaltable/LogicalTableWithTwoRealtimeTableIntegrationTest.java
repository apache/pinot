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
package org.apache.pinot.integration.tests.logicaltable;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.primitives.Longs;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class LogicalTableWithTwoRealtimeTableIntegrationTest extends BaseLogicalTableIntegrationTest {
  private static final String KAFKA_TOPIC = "logicalTableWithTwoRealtimeTopic";
  private static final String LOGICAL_TABLE_NAME = "mytable";
  private static final String TABLE_NAME_0 = "rt_1";
  private static final String TABLE_NAME_1 = "rt_2";
  private static final List<String> REALTIME_TABLE_NAMES = List.of(TABLE_NAME_0, TABLE_NAME_1);
  private static final Map<String, Integer> REALTIME_TABLE_PARTITIONS =
      Map.of(TABLE_NAME_0, 0, TABLE_NAME_1, 1);
  private static final int NUM_PARTITIONS = 2;
  private static final int DOCS_LOADED_TIMEOUT_MS = 600_000;

  private long _table0RecordCount;
  private long _table1RecordCount;
  private int _realtimeTableConfigIndex;
  private int _kafkaPushIndex;

  @Override
  protected String getKafkaTopic() {
    return KAFKA_TOPIC;
  }

  @Override
  protected String getLogicalTableName() {
    return LOGICAL_TABLE_NAME;
  }

  @Override
  protected String getTableName() {
    return LOGICAL_TABLE_NAME;
  }

  @Override
  protected int getNumKafkaPartitions() {
    return NUM_PARTITIONS;
  }

  @Override
  protected long getCountStarResult() {
    return super.getCountStarResult();
  }

  @Override
  protected List<String> getRealtimeTableNames() {
    return REALTIME_TABLE_NAMES;
  }

  @Override
  protected Map<String, List<File>> getRealtimeTableDataFiles() {
    Map<String, List<File>> tableNameToFilesMap = new LinkedHashMap<>();
    for (String tableName : REALTIME_TABLE_NAMES) {
      tableNameToFilesMap.put(tableName, new ArrayList<>());
    }

    for (int i = 0; i < _avroFiles.size(); i++) {
      tableNameToFilesMap.get(REALTIME_TABLE_NAMES.get(i % REALTIME_TABLE_NAMES.size())).add(_avroFiles.get(i));
    }
    return tableNameToFilesMap;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    super.setUp();
    waitForRecordCounts();
  }

  @Test
  public void testFederatedCountStar()
      throws Exception {
    assertEquals(_table0RecordCount,
        getCurrentCountStarResult(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME_0)));
    assertEquals(_table1RecordCount,
        getCurrentCountStarResult(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME_1)));
    assertEquals(_table0RecordCount + _table1RecordCount, getCurrentCountStarResult(LOGICAL_TABLE_NAME));
  }

  @Override
  @Test
  public void testQueryTimeOut()
      throws Exception {
    String starQuery = "SELECT * from " + getLogicalTableName();
    QueryConfig queryConfig = new QueryConfig(1L, null, null, null, null, null);
    var logicalTableConfig = getLogicalTableConfig(getLogicalTableName());
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);
    JsonNode response = postQuery(starQuery);
    JsonNode exceptions = response.get("exceptions");
    if (!exceptions.isEmpty()) {
      int errorCode = exceptions.get(0).get("errorCode").asInt();
      assertTrue(errorCode == QueryErrorCode.BROKER_TIMEOUT.getId()
          || errorCode == QueryErrorCode.SERVER_NOT_RESPONDING.getId()
          || errorCode == QueryErrorCode.QUERY_SCHEDULING_TIMEOUT.getId(),
          "Unexpected error code: " + errorCode);
    }

    // Query succeeds with a high limit.
    queryConfig = new QueryConfig(1000000L, null, null, null, null, null);
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);
    response = postQuery(starQuery);
    exceptions = response.get("exceptions");
    assertTrue(exceptions.isEmpty(), "Query should not throw exception");

    // Reset to null.
    queryConfig = new QueryConfig(null, null, null, null, null, null);
    logicalTableConfig.setQueryConfig(queryConfig);
    updateLogicalTableConfig(logicalTableConfig);
    response = postQuery(starQuery);
    exceptions = response.get("exceptions");
    assertTrue(exceptions.isEmpty(), "Query should not throw exception");
  }

  @Override
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    TableConfig tableConfig = super.createRealtimeTableConfig(sampleAvroFile);
    int tableIndex = _realtimeTableConfigIndex++;
    String tableName = REALTIME_TABLE_NAMES.get(tableIndex);
    Integer partitionId = REALTIME_TABLE_PARTITIONS.get(tableName);
    tableConfig.setTableName(tableName);
    Map<String, String> streamConfigs = new HashMap<>(tableConfig.getIndexingConfig().getStreamConfigs());
    streamConfigs.put("stream.kafka.partition.ids", String.valueOf(partitionId));

    tableConfig.getIndexingConfig().setStreamConfigs(null);

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(streamConfigs)));
    tableConfig.setIngestionConfig(ingestionConfig);
    return tableConfig;
  }

  @Override
  protected void pushAvroIntoKafka(List<File> avroFiles)
      throws Exception {
    Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaPort());
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");

    String tableName = REALTIME_TABLE_NAMES.get(_kafkaPushIndex);
    int partition = REALTIME_TABLE_PARTITIONS.get(tableName);
    _kafkaPushIndex++;

    long recordCount = 0;
    long keySequence = 0;
    byte[] kafkaMessageHeader = getKafkaMessageHeader();

    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProperties);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536)) {

      for (File avroFile : avroFiles) {
        try (DataFileStream<GenericRecord> dataFileStream = AvroUtils.getAvroReader(avroFile)) {
          GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(dataFileStream.getSchema());
          for (GenericRecord genericRecord : dataFileStream) {
            outputStream.reset();
            if (kafkaMessageHeader != null && kafkaMessageHeader.length > 0) {
              outputStream.write(kafkaMessageHeader);
            }

            BinaryEncoder binaryEncoder = new EncoderFactory().directBinaryEncoder(outputStream, null);
            datumWriter.write(genericRecord, binaryEncoder);
            binaryEncoder.flush();

            producer.send(new ProducerRecord<>(
                getKafkaTopic(), partition, Longs.toByteArray(keySequence++), outputStream.toByteArray()))
                .get();
            recordCount++;
          }
        }
      }
    }

    if (partition == 0) {
      _table0RecordCount = recordCount;
    } else if (partition == 1) {
      _table1RecordCount = recordCount;
    } else {
      throw new IllegalStateException("Unexpected partition: " + partition);
    }
  }

  private void waitForRecordCounts() {
    String realtimeTableName0 = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME_0);
    String realtimeTableName1 = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME_1);
    TestUtils.waitForCondition(ignored -> {
      _table0RecordCount = getCurrentCountStarResult(realtimeTableName0);
      _table1RecordCount = getCurrentCountStarResult(realtimeTableName1);
      return getCurrentCountStarResult(LOGICAL_TABLE_NAME) == _table0RecordCount + _table1RecordCount;
    }, 100L, DOCS_LOADED_TIMEOUT_MS,
        "Failed to load the expected record counts for realtime logical tables");

    Assert.assertEquals(getCurrentCountStarResult(realtimeTableName0), _table0RecordCount);
    Assert.assertEquals(getCurrentCountStarResult(realtimeTableName1), _table1RecordCount);
    Assert.assertEquals(getCurrentCountStarResult(LOGICAL_TABLE_NAME), _table0RecordCount + _table1RecordCount);
  }
}
