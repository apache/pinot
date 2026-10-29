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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.pinot.spi.stream.StreamDataProducer;


/// Batches fixture writes while retaining every send acknowledgement. Not thread-safe; owned by one setup thread.
/// Closing drains the final partial batch and closes the Kafka producer, including when a send fails.
public final class BatchedKafkaDataProducer implements StreamDataProducer {
  private final Producer<byte[], byte[]> _producer;
  private final int _batchSize;
  private final List<Future<RecordMetadata>> _pending = new ArrayList<>();
  private boolean _closed;

  public BatchedKafkaDataProducer(String brokerList, int batchSize) {
    this(createProducer(brokerList, batchSize), batchSize);
  }

  public BatchedKafkaDataProducer(Producer<byte[], byte[]> producer, int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("Batch size must be positive");
    }
    _producer = producer;
    _batchSize = batchSize;
  }

  private static Producer<byte[], byte[]> createProducer(String brokerList, int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("Batch size must be positive");
    }
    // These are the effective properties used by KafkaDataProducer after it removes the legacy producer properties.
    // Keep the Kafka defaults for acknowledgements, retries, idempotence and partitioning unchanged.
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return new KafkaProducer<>(properties);
  }

  @Override
  public void init(Properties properties) {
    throw new UnsupportedOperationException("Producer is initialized by the constructor");
  }

  @Override
  public void produce(String topic, byte[] payload) {
    produce(topic, null, payload);
  }

  @Override
  public void produce(String topic, byte[] key, byte[] payload) {
    if (_closed) {
      throw new IllegalStateException("Producer is closed");
    }
    _pending.add(_producer.send(new ProducerRecord<>(topic, key, payload)));
    if (_pending.size() == _batchSize) {
      drain();
    }
  }

  private void drain() {
    if (_pending.isEmpty()) {
      return;
    }
    IllegalStateException failure = null;
    try {
      _producer.flush();
      for (Future<RecordMetadata> future : _pending) {
        try {
          future.get();
        } catch (ExecutionException e) {
          IllegalStateException sendFailure =
              new IllegalStateException("Failed to publish Kafka fixture record", e.getCause());
          if (failure == null) {
            failure = sendFailure;
          } else {
            failure.addSuppressed(sendFailure);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          IllegalStateException interrupted =
              new IllegalStateException("Interrupted publishing Kafka fixture records", e);
          if (failure != null) {
            interrupted.addSuppressed(failure);
          }
          throw interrupted;
        }
      }
    } catch (InterruptException e) {
      Thread.currentThread().interrupt();
      throw e;
    } finally {
      _pending.clear();
    }
    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    try {
      drain();
    } catch (RuntimeException | Error failure) {
      try {
        closeProducer();
      } catch (RuntimeException | Error closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
    closeProducer();
  }

  private void closeProducer() {
    boolean interrupted = Thread.currentThread().isInterrupted();
    try {
      _producer.close();
    } catch (InterruptException e) {
      interrupted = true;
      throw e;
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
