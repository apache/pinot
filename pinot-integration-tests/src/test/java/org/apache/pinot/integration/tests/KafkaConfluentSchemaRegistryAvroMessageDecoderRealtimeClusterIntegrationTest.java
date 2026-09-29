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

import com.google.common.primitives.Longs;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.integration.tests.SharedKafkaRealtimeIntegrationTestSuite.ScenarioLease;
import org.apache.pinot.integration.tests.kafka.schemaregistry.SchemaRegistryStarter;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Focused Confluent Schema Registry decoder scenario for the shared Kafka realtime suite.
public class KafkaConfluentSchemaRegistryAvroMessageDecoderRealtimeClusterIntegrationTest {
  private static final String TABLE_NAME = "mytableConfluentSchemaRegistry";
  private static final String TOPIC_NAME =
      "KafkaConfluentSchemaRegistryAvroMessageDecoderRealtimeClusterIntegrationTest";
  private static final int NUM_INVALID_RECORDS_PER_FILE = 5;
  private static final int NUM_TOMBSTONES = 1_000;
  private static final long EXPECTED_ARR_TIME_ONE_COUNT = 104L;

  private SchemaRegistryStarter.KafkaSchemaRegistryInstance _schemaRegistry;

  @Test
  public void testSchemaRegistryDecoderSkipsInvalidRecordsAndSupportsSegmentRefresh()
      throws Throwable {
    SharedKafkaRealtimeIntegrationTestSuite owner =
        SharedKafkaRealtimeIntegrationTestSuite.getSharedSuiteOwner();
    ScenarioLease lease = owner.newScenario(TABLE_NAME, TOPIC_NAME);
    Throwable primaryFailure = null;
    try {
      owner.createScenarioTopic(lease);
      List<File> avroFiles = owner.unpackScenarioData(lease);
      Schema schema = owner.addScenarioSchema(lease);
      startSchemaRegistry();

      Map<String, String> streamConfigs = owner.getScenarioStreamConfigs(lease._topicName, false);
      String streamType = streamConfigs.get(StreamConfigProperties.STREAM_TYPE);
      streamConfigs.put(
          StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
          KafkaConfluentSchemaRegistryAvroMessageDecoder.class.getName());
      streamConfigs.put("stream.kafka.decoder.prop.schema.registry.rest.url", _schemaRegistry.getUrl());
      TableConfig tableConfig = owner.createScenarioTableConfig(lease, avroFiles.get(0), streamConfigs);
      owner.addScenarioTable(lease, tableConfig);

      pushSchemaRegistryRecords(owner, lease, avroFiles);
      createAndRefreshSegments(owner, lease, avroFiles, schema, tableConfig);

      // Uploaded segments contribute one copy and valid Kafka records contribute the other. Reaching exactly 2x
      // proves tombstones/malformed records were skipped while every schema-registry record was decoded.
      owner.waitForScenarioCount(lease, owner.getDefaultScenarioCount() * 2L, 600_000L);
      // The fixture contains 52 records with ArrTime=1. Seeing two copies also proves the schema-registry decoder
      // populated field values, rather than merely accepting the expected number of Kafka messages.
      owner.waitForScenarioQueryResult(
          "SELECT COUNT(*) FROM " + lease._tableName + " WHERE ArrTime = 1", EXPECTED_ARR_TIME_ONE_COUNT, 60_000L);
    } catch (Throwable t) {
      primaryFailure = t;
      throw t;
    } finally {
      owner.closeScenario(lease, primaryFailure, this::stopSchemaRegistry);
    }
  }

  private void startSchemaRegistry() {
    if (_schemaRegistry == null) {
      _schemaRegistry = SchemaRegistryStarter.createLocalInstance(SchemaRegistryStarter.DEFAULT_PORT);
      _schemaRegistry.start();
    }
  }

  private void stopSchemaRegistry() {
    if (_schemaRegistry != null) {
      _schemaRegistry.stop();
      _schemaRegistry = null;
    }
  }

  private void pushSchemaRegistryRecords(SharedKafkaRealtimeIntegrationTestSuite owner, ScenarioLease lease,
      List<File> avroFiles)
      throws Exception {
    Properties avroProducerProps = new Properties();
    avroProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, owner.getKafkaBrokerList());
    avroProducerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, _schemaRegistry.getUrl());
    avroProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    avroProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.KafkaAvroSerializer");

    Properties invalidProducerProps = new Properties();
    invalidProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, owner.getKafkaBrokerList());
    invalidProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    invalidProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");

    try (Producer<byte[], GenericRecord> avroProducer = new KafkaProducer<>(avroProducerProps);
        Producer<byte[], byte[]> invalidProducer = new KafkaProducer<>(invalidProducerProps)) {
      for (int i = 0; i < NUM_TOMBSTONES; i++) {
        avroProducer.send(new ProducerRecord<>(lease._topicName,
            Longs.toByteArray(System.currentTimeMillis()), null));
      }

      for (File avroFile : avroFiles) {
        int numInvalidRecords = 0;
        try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
          for (GenericRecord genericRecord : reader) {
            byte[] keyBytes = owner.getPartitionColumn() == null
                ? Longs.toByteArray(System.currentTimeMillis())
                : genericRecord.get(owner.getPartitionColumn()).toString().getBytes(UTF_8);
            if (numInvalidRecords < NUM_INVALID_RECORDS_PER_FILE) {
              invalidProducer.send(new ProducerRecord<>(lease._topicName, keyBytes, "Rubbish".getBytes(UTF_8)));
              numInvalidRecords++;
            }
            avroProducer.send(new ProducerRecord<>(lease._topicName, keyBytes, genericRecord));
          }
        }
      }
    }
  }

  private void createAndRefreshSegments(SharedKafkaRealtimeIntegrationTestSuite owner, ScenarioLease lease,
      List<File> avroFiles, Schema schema, TableConfig tableConfig)
      throws Exception {
    List<File> copyOfAvroFiles = new ArrayList<>(avroFiles);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(copyOfAvroFiles, tableConfig, schema, 0, lease._segmentDir,
        lease._tarDir);

    uploadSegmentsToController(owner, lease, false, false);
    uploadSegmentsToController(owner, lease, true, false);
    uploadSegmentsToController(owner, lease, true, true);
  }

  private void uploadSegmentsToController(SharedKafkaRealtimeIntegrationTestSuite owner, ScenarioLease lease,
      boolean onlyFirstSegment, boolean changeCrc)
      throws Exception {
    File[] segmentTarFiles = lease._tarDir.listFiles();
    assertNotNull(segmentTarFiles);
    int numSegments = segmentTarFiles.length;
    assertTrue(numSegments > 0);
    if (onlyFirstSegment) {
      numSegments = 1;
    }

    URI uploadSegmentUri = URI.create(owner.getOrCreateAdminClient().getSegmentUploadUrl());
    try (FileUploadDownloadClient client = new FileUploadDownloadClient()) {
      if (numSegments == 1) {
        File segmentTarFile = segmentTarFiles[0];
        if (changeCrc) {
          changeCrcInSegmentZKMetadata(owner, lease._tableName, segmentTarFile);
        }
        assertEquals(client.uploadSegment(uploadSegmentUri, segmentTarFile.getName(), segmentTarFile,
            lease._tableName, TableType.REALTIME).getStatusCode(), HttpStatus.SC_OK);
      } else {
        ExecutorService executorService = Executors.newFixedThreadPool(numSegments);
        try {
          List<Future<Integer>> futures = new ArrayList<>(numSegments);
          for (File segmentTarFile : segmentTarFiles) {
            futures.add(executorService.submit(
                () -> client.uploadSegment(uploadSegmentUri, segmentTarFile.getName(), segmentTarFile,
                    lease._tableName, TableType.REALTIME).getStatusCode()));
          }
          for (Future<Integer> future : futures) {
            assertEquals((int) future.get(), HttpStatus.SC_OK);
          }
        } finally {
          executorService.shutdownNow();
        }
      }
    }
  }

  private static void changeCrcInSegmentZKMetadata(SharedKafkaRealtimeIntegrationTestSuite owner, String tableName,
      File segmentTarFile) {
    String segmentFilePath = segmentTarFile.toString();
    int startIndex = segmentFilePath.indexOf(tableName + "_");
    int endIndex = segmentFilePath.indexOf(".tar.gz");
    assertTrue(startIndex >= 0 && endIndex > startIndex,
        "Cannot derive segment name from path: " + segmentFilePath);
    String segmentName = segmentFilePath.substring(startIndex, endIndex);
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    SegmentZKMetadata metadata = owner.getSegmentZKMetadata(tableNameWithType, segmentName);
    assertNotNull(metadata);
    metadata.setCrc(111L);
    owner.updateSegmentZKMetadata(tableNameWithType, metadata);
  }
}
