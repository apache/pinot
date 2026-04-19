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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pinot.plugin.stream.kafka30.utils.MiniKafkaCluster;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.create;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class KafkaPartitionLevelMicroBatchConsumerTest {
  private static final long STABILIZE_SLEEP_DELAYS = 3000;
  private static final String TEST_TOPIC = "microbatch_avro_topic";
  private static final int NUM_MESSAGES = 5;
  private static final int NUM_AVRO_RECORDS_PER_MESSAGE = 10;
  private static final long TIMESTAMP = Instant.now().toEpochMilli();

  private MiniKafkaCluster _kafkaCluster;
  private String _kafkaBrokerAddress;
  private Schema _avroSchema;
  private File _tempDir;

  @BeforeClass
  public void setUp() throws Exception {
    // Initialize PinotFS for local file system
    PinotConfiguration pinotFSConfig = new PinotConfiguration();
    PinotFSFactory.register("file", LocalPinotFS.class.getName(), pinotFSConfig);

    // Create temp directory for test files
    _tempDir = new File(FileUtils.getTempDirectory(), "microbatch-test-" + System.currentTimeMillis());
    _tempDir.mkdirs();

    _kafkaCluster = new MiniKafkaCluster("0");
    _kafkaCluster.start();
    _kafkaBrokerAddress = _kafkaCluster.getKafkaServerAddress();
    _kafkaCluster.createTopic(TEST_TOPIC, 1, 1);
    Thread.sleep(STABILIZE_SLEEP_DELAYS);

    // Create Avro schema
    _avroSchema = Schema.createRecord("TestRecord", null, null, false);
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("id", create(Schema.Type.INT), null, null));
    fields.add(new Schema.Field("name", create(Schema.Type.STRING), null, null));
    fields.add(new Schema.Field("value", create(Schema.Type.DOUBLE), null, null));
    _avroSchema.setFields(fields);

    produceMicroBatchMessagesToKafka();
    Thread.sleep(STABILIZE_SLEEP_DELAYS);
  }

  private void produceMicroBatchMessagesToKafka() throws IOException {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaBrokerAddress);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "microbatch-test-producer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
      for (int msgIdx = 0; msgIdx < NUM_MESSAGES; msgIdx++) {
        // Write Avro records to a file
        File avroFile = writeAvroRecordBatchToFile(msgIdx);

        // Create JSON protocol message with numRecords
        byte[] protocolMessage = MicroBatchProtocol.createUriMessage(
            avroFile.toURI().toString(), MicroBatchPayloadV1.Format.AVRO, NUM_AVRO_RECORDS_PER_MESSAGE);

        producer.send(
            new ProducerRecord<>(TEST_TOPIC, 0, TIMESTAMP + msgIdx, "key_" + msgIdx, protocolMessage));
      }
      producer.flush();
    }
  }

  private File writeAvroRecordBatchToFile(int batchIndex) throws IOException {
    File avroFile = new File(_tempDir, "batch-" + batchIndex + ".avro");

    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(_avroSchema);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(_avroSchema, avroFile);

      // Write 10 Avro records to the file
      for (int i = 0; i < NUM_AVRO_RECORDS_PER_MESSAGE; i++) {
        GenericRecord record = new GenericData.Record(_avroSchema);
        int recordId = batchIndex * NUM_AVRO_RECORDS_PER_MESSAGE + i;
        record.put("id", recordId);
        record.put("name", "record_" + recordId);
        record.put("value", recordId * 1.5);
        dataFileWriter.append(record);
      }
    }

    return avroFile;
  }

  @AfterClass
  public void tearDown() throws Exception {
    try {
      _kafkaCluster.deleteTopic(TEST_TOPIC);
    } finally {
      _kafkaCluster.close();
    }

    // Clean up temp directory
    if (_tempDir != null && _tempDir.exists()) {
      FileUtils.deleteDirectory(_tempDir);
    }
  }

  @Test
  public void testConsumeMessagesWithSerializedAvroRecords() throws Exception {
    String streamType = "kafka";
    String clientId = "microbatch-test-client";
    String tableNameWithType = "testTable_REALTIME";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", TEST_TOPIC);
    streamConfigMap.put("stream.kafka.broker.list", _kafkaBrokerAddress);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name",
        "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory");
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    KafkaPartitionLevelMicroBatchConsumer consumer =
        new KafkaPartitionLevelMicroBatchConsumer(clientId, streamConfig, 0, 2);

    // Fetch messages starting from offset 0
    MicroBatchStreamPartitionMsgOffset startOffset = MicroBatchStreamPartitionMsgOffset.of(0);
    MessageBatch<GenericRow> messageBatch = consumer.fetchMessages(startOffset, 10000);

    assertNotNull(messageBatch);
    // Each fetchMessages call returns one batch from one Kafka message (one Avro file with 10 records)
    assertEquals(messageBatch.getMessageCount(), NUM_AVRO_RECORDS_PER_MESSAGE);

    // Verify the records from the first batch
    for (int i = 0; i < messageBatch.getMessageCount(); i++) {
      GenericRow row = messageBatch.getStreamMessage(i).getValue();
      assertNotNull(row);
      assertNotNull(row.getValue("id"));
      assertNotNull(row.getValue("name"));
      assertNotNull(row.getValue("value"));
      assert row.getValue("name").toString().startsWith("record_")
          : "Record " + i + " should have name starting with 'record_'";
    }

    // Fetch and verify all messages
    int totalRecords = messageBatch.getMessageCount();
    while (totalRecords < NUM_MESSAGES * NUM_AVRO_RECORDS_PER_MESSAGE) {
      StreamPartitionMsgOffset nextOffset = messageBatch.getOffsetOfNextBatch();
      messageBatch = consumer.fetchMessages(nextOffset, 10000);
      if (messageBatch.getMessageCount() == 0) {
        break;
      }
      totalRecords += messageBatch.getMessageCount();
    }

    // Verify we got all records
    assertEquals(totalRecords, NUM_MESSAGES * NUM_AVRO_RECORDS_PER_MESSAGE);

    consumer.close();
  }

  /**
   * Test that mid-batch resume works correctly.
   * This simulates what happens after a segment commit when we need to resume
   * from a specific record offset within a Kafka message.
   */
  @Test
  public void testMidBatchResume() throws Exception {
    String streamType = "kafka";
    String clientId = "microbatch-test-client-midbatch";
    String tableNameWithType = "testTable_REALTIME";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", TEST_TOPIC);
    streamConfigMap.put("stream.kafka.broker.list", _kafkaBrokerAddress);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name",
        "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory");
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    KafkaPartitionLevelMicroBatchConsumer consumer =
        new KafkaPartitionLevelMicroBatchConsumer(clientId, streamConfig, 0, 2);

    // First, consume from the beginning to establish context
    MicroBatchStreamPartitionMsgOffset startOffset = MicroBatchStreamPartitionMsgOffset.of(0);
    MessageBatch<GenericRow> messageBatch = consumer.fetchMessages(startOffset, 10000);
    assertNotNull(messageBatch);
    assertEquals(messageBatch.getMessageCount(), NUM_AVRO_RECORDS_PER_MESSAGE);

    // Verify first record starts with id=0
    GenericRow firstRow = messageBatch.getStreamMessage(0).getValue();
    assertEquals(firstRow.getValue("id"), 0);

    consumer.close();

    // Now simulate a new consumer (like after segment commit) starting mid-batch
    // Start from kafka offset 0, but record offset 5 (skip first 5 records)
    KafkaPartitionLevelMicroBatchConsumer consumer2 =
        new KafkaPartitionLevelMicroBatchConsumer(clientId + "-2", streamConfig, 0, 2);

    int skipRecords = 5;
    MicroBatchStreamPartitionMsgOffset midBatchOffset =
        new MicroBatchStreamPartitionMsgOffset(0, skipRecords);
    MessageBatch<GenericRow> resumedBatch = consumer2.fetchMessages(midBatchOffset, 10000);

    assertNotNull(resumedBatch);
    // Should get remaining records: 10 - 5 = 5
    assertEquals(resumedBatch.getMessageCount(), NUM_AVRO_RECORDS_PER_MESSAGE - skipRecords);

    // Verify first record after resume has id=5 (the 6th record, 0-indexed as 5)
    GenericRow firstResumedRow = resumedBatch.getStreamMessage(0).getValue();
    assertEquals(firstResumedRow.getValue("id"), skipRecords,
        "First record after mid-batch resume should have id=" + skipRecords);

    // Verify the offset of the first resumed record
    StreamPartitionMsgOffset firstResumedOffset =
        resumedBatch.getStreamMessage(0).getMetadata().getOffset();
    assertNotNull(firstResumedOffset);
    MicroBatchStreamPartitionMsgOffset mbOffset = (MicroBatchStreamPartitionMsgOffset) firstResumedOffset;
    assertEquals(mbOffset.getKafkaMessageOffset(), 0);
    assertEquals(mbOffset.getRecordOffsetInMicroBatch(), skipRecords);

    // Continue consuming to verify we get all remaining records
    int totalRecords = resumedBatch.getMessageCount();
    StreamPartitionMsgOffset nextOffset = resumedBatch.getOffsetOfNextBatch();

    while (totalRecords < (NUM_MESSAGES * NUM_AVRO_RECORDS_PER_MESSAGE) - skipRecords) {
      messageBatch = consumer2.fetchMessages(nextOffset, 10000);
      if (messageBatch.getMessageCount() == 0) {
        break;
      }
      totalRecords += messageBatch.getMessageCount();
      nextOffset = messageBatch.getOffsetOfNextBatch();
    }

    // Should have all records minus the skipped ones from the first batch
    assertEquals(totalRecords, (NUM_MESSAGES * NUM_AVRO_RECORDS_PER_MESSAGE) - skipRecords);

    consumer2.close();
  }
}
