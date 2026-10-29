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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.pinot.integration.tests.BatchedKafkaDataProducer;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.mockito.InOrder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies fixture record equivalence, acknowledgement failures and producer cleanup without starting a cluster.
/// Each test owns its producer and temporary data; no state is shared between invocations.
@Test(suiteName = "CustomClusterIntegrationTest")
public class BatchedKafkaDataProducerTest {
  @Test
  public void testAvroRecordsPreservedAcrossBatches()
      throws Exception {
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Fixture\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"value\",\"type\":\"string\"}]}");
    File directory = Files.createTempDirectory("batched-kafka-fixture-").toFile();
    try {
      List<File> files = new ArrayList<>();
      for (int fileIndex = 0; fileIndex < 2; fileIndex++) {
        File file = new File(directory, fileIndex + ".avro");
        files.add(file);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
          writer.create(schema, file);
          for (int row = fileIndex * 2; row < Math.min(fileIndex * 2 + 2, 3); row++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("id", row + 7);
            record.put("value", "value-" + row);
            writer.append(record);
          }
        }
      }
      for (boolean usePartitionColumn : List.of(false, true)) {
        Producer<byte[], byte[]> kafkaProducer = mockProducer();
        List<ProducerRecord<byte[], byte[]>> sent = new ArrayList<>();
        when(kafkaProducer.send(any())).thenAnswer(invocation -> {
          sent.add(invocation.getArgument(0));
          return CompletableFuture.completedFuture(null);
        });
        byte[] header = {13, 37};
        try (BatchedKafkaDataProducer producer = new BatchedKafkaDataProducer(kafkaProducer, 64)) {
          ClusterIntegrationTestUtils.pushAvroIntoKafka(files, "fixture", 64, header,
              usePartitionColumn ? "id" : null, true, producer);
          // Fifteen full batches have drained; the remaining 43 records must drain on close.
          verify(kafkaProducer, times(15)).flush();
        }
        verify(kafkaProducer, times(16)).flush();
        verify(kafkaProducer).close();
        assertEquals(sent.size(), 1003);
        for (int i = 0; i < sent.size(); i++) {
          ProducerRecord<byte[], byte[]> record = sent.get(i);
          assertEquals(record.topic(), "fixture");
          assertNull(record.partition());
          if (i < 1000) {
            assertEquals(ByteBuffer.wrap(record.key()).getLong(), i);
            assertNull(record.value());
          } else {
            int row = i - 1000;
            if (usePartitionColumn) {
              assertEquals(new String(record.key(), StandardCharsets.UTF_8), Integer.toString(row + 7));
            } else {
              assertEquals(ByteBuffer.wrap(record.key()).getLong(), i);
            }
            byte[] payload = record.value();
            assertEquals(payload[0], header[0]);
            assertEquals(payload[1], header[1]);
            GenericRecord decoded = new GenericDatumReader<GenericRecord>(schema).read(null,
                DecoderFactory.get().binaryDecoder(payload, header.length, payload.length - header.length, null));
            assertEquals(decoded.get("id"), row + 7);
            assertEquals(decoded.get("value").toString(), "value-" + row);
          }
        }
      }
    } finally {
      FileUtils.deleteDirectory(directory);
    }
  }

  @Test
  public void testPartialBatchAcknowledgedBeforeClose()
      throws Exception {
    Producer<byte[], byte[]> kafkaProducer = mockProducer();
    Future<RecordMetadata> acknowledgement = mockAcknowledgement();
    when(kafkaProducer.send(any())).thenReturn(acknowledgement);
    try (BatchedKafkaDataProducer producer = new BatchedKafkaDataProducer(kafkaProducer, 2)) {
      producer.produce("fixture", new byte[]{1});
      verify(kafkaProducer, times(0)).flush();
      verify(acknowledgement, times(0)).get();
    }
    InOrder order = inOrder(kafkaProducer, acknowledgement);
    order.verify(kafkaProducer).send(any());
    order.verify(kafkaProducer).flush();
    order.verify(acknowledgement).get();
    order.verify(kafkaProducer).close();
  }

  @Test
  public void testAllAsyncFailuresReportedAndCloseFailureSuppressed()
      throws Exception {
    Producer<byte[], byte[]> kafkaProducer = mockProducer();
    Future<RecordMetadata> first = mockAcknowledgement();
    Future<RecordMetadata> second = mockAcknowledgement();
    IllegalArgumentException firstCause = new IllegalArgumentException("first record rejected");
    IllegalArgumentException secondCause = new IllegalArgumentException("second record rejected");
    IllegalStateException closeFailure = new IllegalStateException("close failed");
    when(first.get()).thenThrow(new ExecutionException(firstCause));
    when(second.get()).thenThrow(new ExecutionException(secondCause));
    when(kafkaProducer.send(any())).thenReturn(first).thenReturn(second);
    doThrow(closeFailure).when(kafkaProducer).close();
    IllegalStateException failure = expectThrows(IllegalStateException.class, () -> {
      try (BatchedKafkaDataProducer producer = new BatchedKafkaDataProducer(kafkaProducer, 2)) {
        producer.produce("fixture", new byte[]{1});
        producer.produce("fixture", new byte[]{2});
      }
    });
    assertSame(failure.getCause(), firstCause);
    assertEquals(failure.getSuppressed().length, 2);
    assertSame(failure.getSuppressed()[0].getCause(), secondCause);
    assertSame(failure.getSuppressed()[1], closeFailure);
    verify(first).get();
    verify(second).get();
    verify(kafkaProducer).close();
  }

  @Test
  public void testSynchronousSendFailureDrainsEarlierRecordsAndClosesProducer()
      throws Exception {
    Producer<byte[], byte[]> kafkaProducer = mockProducer();
    Future<RecordMetadata> acknowledgement = mockAcknowledgement();
    IllegalArgumentException sendFailure = new IllegalArgumentException("send rejected");
    when(kafkaProducer.send(any())).thenReturn(acknowledgement).thenThrow(sendFailure);
    IllegalArgumentException failure = expectThrows(IllegalArgumentException.class, () -> {
      try (BatchedKafkaDataProducer producer = new BatchedKafkaDataProducer(kafkaProducer, 4)) {
        producer.produce("fixture", new byte[]{1});
        producer.produce("fixture", new byte[]{2});
      }
    });
    assertSame(failure, sendFailure);
    verify(acknowledgement).get();
    verify(kafkaProducer).flush();
    verify(kafkaProducer).close();
  }

  @Test
  public void testInterruptedAcknowledgementRestoresInterruptAndClosesProducer()
      throws Exception {
    assertFalse(Thread.currentThread().isInterrupted());
    Producer<byte[], byte[]> kafkaProducer = mockProducer();
    Future<RecordMetadata> acknowledgement = mockAcknowledgement();
    when(kafkaProducer.send(any())).thenReturn(acknowledgement);
    InterruptedException interrupted = new InterruptedException("test interruption");
    when(acknowledgement.get()).thenThrow(interrupted);
    try {
      IllegalStateException failure = expectThrows(IllegalStateException.class, () -> {
        try (BatchedKafkaDataProducer producer = new BatchedKafkaDataProducer(kafkaProducer, 2)) {
          producer.produce("fixture", new byte[]{1});
        }
      });
      assertSame(failure.getCause(), interrupted);
      assertTrue(Thread.currentThread().isInterrupted());
      verify(kafkaProducer).close();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testFlushFailureStillClosesProducer() {
    Producer<byte[], byte[]> kafkaProducer = mockProducer();
    when(kafkaProducer.send(any())).thenReturn(CompletableFuture.completedFuture(null));
    IllegalStateException flushFailure = new IllegalStateException("flush failed");
    doThrow(flushFailure).when(kafkaProducer).flush();
    IllegalStateException failure = expectThrows(IllegalStateException.class, () -> {
      try (BatchedKafkaDataProducer producer = new BatchedKafkaDataProducer(kafkaProducer, 2)) {
        producer.produce("fixture", new byte[]{1});
      }
    });
    assertSame(failure, flushFailure);
    verify(kafkaProducer).close();
  }

  @Test
  public void testRejectsNonPositiveBatchBeforeOpeningProducer() {
    for (int batchSize : List.of(0, -1)) {
      IllegalArgumentException failure = expectThrows(IllegalArgumentException.class,
          () -> new BatchedKafkaDataProducer("unused-broker", batchSize));
      assertEquals(failure.getMessage(), "Batch size must be positive");
    }
  }

  @SuppressWarnings("unchecked")
  private static Producer<byte[], byte[]> mockProducer() {
    return mock(Producer.class);
  }

  @SuppressWarnings("unchecked")
  private static Future<RecordMetadata> mockAcknowledgement() {
    return mock(Future.class);
  }
}
