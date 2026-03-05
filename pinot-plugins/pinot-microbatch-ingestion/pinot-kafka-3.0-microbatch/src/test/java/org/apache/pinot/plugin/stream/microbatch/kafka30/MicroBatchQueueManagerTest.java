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
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class MicroBatchQueueManagerTest {

  private static final int TEST_PARTITION = 0;
  private static final int NUM_FILE_FETCH_THREADS = 2;

  private MicroBatchQueueManager _queueManager;
  private File _tempDir;
  private Schema _avroSchema;

  @BeforeClass
  public void setUpClass() {
    // Initialize PinotFS for local file system
    PinotConfiguration pinotFSConfig = new PinotConfiguration();
    PinotFSFactory.register("file", LocalPinotFS.class.getName(), pinotFSConfig);

    // Create Avro schema for test files
    _avroSchema = Schema.createRecord("TestRecord", null, null, false);
    _avroSchema.setFields(java.util.Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)
    ));
  }

  @BeforeMethod
  public void setUp() throws IOException {
    _queueManager = new MicroBatchQueueManager(TEST_PARTITION, NUM_FILE_FETCH_THREADS);
    _tempDir = new File(FileUtils.getTempDirectory(), "microbatch-test-" + System.currentTimeMillis());
    _tempDir.mkdirs();
  }

  @AfterMethod
  public void tearDown() throws IOException {
    if (_queueManager != null) {
      _queueManager.close();
    }
    if (_tempDir != null && _tempDir.exists()) {
      FileUtils.deleteDirectory(_tempDir);
    }
  }

  @Test
  public void testSubmitAndRetrieveBatch() throws Exception {
    // Create a valid Avro file
    File avroFile = createAvroFile("test-batch.avro", 5);
    MicroBatch microBatch = createMicroBatch(avroFile.toURI().toString(), 5);

    _queueManager.submitBatch(microBatch);
    assertTrue(_queueManager.hasData());

    // Wait for processing and retrieve
    MessageBatch<GenericRow> messageBatch = null;
    long startTime = System.currentTimeMillis();
    while (messageBatch == null && System.currentTimeMillis() - startTime < 10000) {
      messageBatch = _queueManager.getNextMessageBatch();
      if (messageBatch == null) {
        Thread.sleep(100);
      }
    }

    assertNotNull(messageBatch, "Should retrieve a message batch");
    assertEquals(messageBatch.getMessageCount(), 5);
  }

  @Test
  public void testFileNotFound() throws Exception {
    // Create a MicroBatch pointing to a non-existent file
    String nonExistentUri = "file:///non/existent/path/batch.avro";
    MicroBatch microBatch = createMicroBatch(nonExistentUri, 10);

    _queueManager.submitBatch(microBatch);
    assertTrue(_queueManager.hasData(), "Queue should have the submitted batch");

    // Wait for the error to be processed - the batch should be marked as read
    // after the download failure (current error handling behavior)
    long startTime = System.currentTimeMillis();
    while (_queueManager.hasData() && System.currentTimeMillis() - startTime < 5000) {
      _queueManager.getNextMessageBatch();
      Thread.sleep(100);
    }

    // After error handling, queue should be empty (batch marked as processed)
    assertFalse(_queueManager.hasData(),
        "Queue should be empty after failed batch is processed");

    // No message batch should be returned since download failed
    MessageBatch<GenericRow> batch = _queueManager.getNextMessageBatch();
    assertNull(batch, "Should return null when queue is empty after error");
  }

  @Test
  public void testInvalidFileFormat() throws Exception {
    // Create a file with invalid content (not valid Avro)
    File invalidFile = new File(_tempDir, "invalid.avro");
    FileUtils.writeStringToFile(invalidFile, "This is not valid Avro content", StandardCharsets.UTF_8);

    MicroBatch microBatch = createMicroBatch(invalidFile.toURI().toString(), 10);
    _queueManager.submitBatch(microBatch);
    assertTrue(_queueManager.hasData(), "Queue should have the submitted batch");

    // Wait for the error to be processed
    long startTime = System.currentTimeMillis();
    while (_queueManager.hasData() && System.currentTimeMillis() - startTime < 5000) {
      _queueManager.getNextMessageBatch();
      Thread.sleep(100);
    }

    // After error handling, queue should be empty (batch marked as processed)
    assertFalse(_queueManager.hasData(),
        "Queue should be empty after invalid batch is processed");
  }

  @Test
  public void testClearQueue() throws Exception {
    File avroFile = createAvroFile("test-clear.avro", 5);
    MicroBatch microBatch = createMicroBatch(avroFile.toURI().toString(), 5);

    _queueManager.submitBatch(microBatch);
    assertTrue(_queueManager.hasData());

    _queueManager.clear();
    assertFalse(_queueManager.hasData());
  }

  @Test
  public void testCloseWhileProcessing() throws Exception {
    File avroFile = createAvroFile("test-close.avro", 100);
    MicroBatch microBatch = createMicroBatch(avroFile.toURI().toString(), 100);

    _queueManager.submitBatch(microBatch);

    // Close immediately while processing might still be happening
    _queueManager.close();

    // Should not throw exception
    assertFalse(_queueManager.hasData());
  }

  @Test
  public void testMultipleBatches() throws Exception {
    // Submit multiple batches
    for (int i = 0; i < 3; i++) {
      File avroFile = createAvroFile("batch-" + i + ".avro", 5);
      MicroBatch microBatch = createMicroBatch(avroFile.toURI().toString(), 5);
      _queueManager.submitBatch(microBatch);
    }

    assertTrue(_queueManager.hasData());

    // Retrieve all batches
    int totalRecords = 0;
    int batchCount = 0;
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTime < 30000) {
      MessageBatch<GenericRow> batch = _queueManager.getNextMessageBatch();
      if (batch != null && batch.getMessageCount() > 0) {
        totalRecords += batch.getMessageCount();
        batchCount++;
      }
      if (totalRecords >= 15) {
        break;
      }
      if (!_queueManager.hasData() && batch == null) {
        break;
      }
      Thread.sleep(100);
    }

    assertEquals(totalRecords, 15, "Should process all 15 records from 3 batches");
  }

  @Test
  public void testInlineDataBatch() throws Exception {
    // Create a small Avro file and encode it as inline data
    File avroFile = createAvroFile("inline-test.avro", 3);
    byte[] fileContent = FileUtils.readFileToByteArray(avroFile);

    MicroBatch microBatch = createInlineDataMicroBatch(fileContent, 3);
    _queueManager.submitBatch(microBatch);

    // Wait for processing
    MessageBatch<GenericRow> batch = null;
    long startTime = System.currentTimeMillis();
    while (batch == null && System.currentTimeMillis() - startTime < 10000) {
      batch = _queueManager.getNextMessageBatch();
      if (batch == null) {
        Thread.sleep(100);
      }
    }

    assertNotNull(batch, "Should retrieve inline data batch");
    assertEquals(batch.getMessageCount(), 3);
  }

  @Test
  public void testEmptyQueueReturnsNull() {
    assertFalse(_queueManager.hasData());
    MessageBatch<GenericRow> batch = _queueManager.getNextMessageBatch();
    assertNull(batch);
  }

  @Test
  public void testThreadInterruptionHandling() throws Exception {
    File avroFile = createAvroFile("interrupt-test.avro", 5);
    MicroBatch microBatch = createMicroBatch(avroFile.toURI().toString(), 5);

    _queueManager.submitBatch(microBatch);

    // Interrupt the current thread
    Thread.currentThread().interrupt();

    // Should handle interruption gracefully
    MessageBatch<GenericRow> batch = _queueManager.getNextMessageBatch();

    // Clear interrupt flag
    Thread.interrupted();
  }

  // Helper methods

  private File createAvroFile(String filename, int numRecords) throws IOException {
    File avroFile = new File(_tempDir, filename);
    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(_avroSchema);

    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(_avroSchema, avroFile);

      for (int i = 0; i < numRecords; i++) {
        GenericRecord record = new GenericData.Record(_avroSchema);
        record.put("id", i);
        record.put("name", "record_" + i);
        dataFileWriter.append(record);
      }
    }

    return avroFile;
  }

  private MicroBatch createMicroBatch(String uri, int numRecords) throws IOException {
    byte[] protocolMessage = MicroBatchProtocol.createUriMessage(
        uri, MicroBatchPayloadV1.Format.AVRO, numRecords);
    MicroBatchProtocol protocol = MicroBatchProtocol.parse(protocolMessage);

    MicroBatchStreamPartitionMsgOffset offset = new MicroBatchStreamPartitionMsgOffset(0, 0);
    MicroBatchStreamPartitionMsgOffset nextOffset = new MicroBatchStreamPartitionMsgOffset(1, 0);

    StreamMessageMetadata metadata = new StreamMessageMetadata.Builder()
        .setRecordIngestionTimeMs(System.currentTimeMillis())
        .setOffset(offset, nextOffset)
        .build();

    return new MicroBatch(null, 1, protocol, metadata, false, 0);
  }

  private MicroBatch createInlineDataMicroBatch(byte[] data, int numRecords) throws IOException {
    byte[] protocolMessage = MicroBatchProtocol.createDataMessage(
        data, MicroBatchPayloadV1.Format.AVRO, numRecords);
    MicroBatchProtocol protocol = MicroBatchProtocol.parse(protocolMessage);

    MicroBatchStreamPartitionMsgOffset offset = new MicroBatchStreamPartitionMsgOffset(0, 0);
    MicroBatchStreamPartitionMsgOffset nextOffset = new MicroBatchStreamPartitionMsgOffset(1, 0);

    StreamMessageMetadata metadata = new StreamMessageMetadata.Builder()
        .setRecordIngestionTimeMs(System.currentTimeMillis())
        .setOffset(offset, nextOffset)
        .build();

    return new MicroBatch(null, 1, protocol, metadata, false, 0);
  }
}
