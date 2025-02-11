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
package org.apache.pinot.spi.stream;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * StreamDataProducer is the interface for stream data sources. E.g. KafkaDataProducer.
 */
public interface StreamDataProducer extends AutoCloseable {
  void init(Properties props);

  void produce(String topic, byte[] payload);

  void produce(String topic, byte[] key, byte[] payload);

  default void produce(String topic, byte[] key, byte[] payload, GenericRow headers) {
    produce(topic, key, payload);
  }

  void close();

  /**
   * Allows the producer to optimize for a batched write.
   * This will help increase throughput in some cases
   * @param topic the topic of the output
   * @param rows the rows
   */
  default void produceBatch(String topic, List<byte[]> rows) {
    for (byte[] row : rows) {
      produce(topic, row);
    }
  }

  /**
   * Allows the producer to optimize for a batched write.
   * This will help increase throughput in some cases
   * @param topic the topic of the output
   * @param payloadWithKey the payload rows with key
   */
  default void produceKeyedBatch(String topic, List<RowWithKey> payloadWithKey, boolean includeHeaders) {
    for (RowWithKey rowWithKey : payloadWithKey) {
      if (rowWithKey.getKey() == null) {
        produce(topic, rowWithKey.getPayload());
      } else {
        if (includeHeaders) {
          GenericRow header = new GenericRow();
          header.putValue("header1", System.currentTimeMillis());
          produce(topic, rowWithKey.getKey(), rowWithKey.getPayload(), header);
        } else {
          produce(topic, rowWithKey.getKey(), rowWithKey.getPayload());
        }
      }
    }
  }

  default void produceKeyedBatch(String topic, List<RowWithKey> payloadWithKey) {
    produceKeyedBatch(topic, payloadWithKey, false);
  }

    /**
     * Helper class so the key and payload can be easily tied together instead of using a pair
     * The class is intended for StreamDataProducer only
     */
  class RowWithKey {
    private final byte[] _key;
    private final byte[] _payload;

    public RowWithKey(@Nullable byte[] key, byte[] payload) {
      _key = key;
      _payload = payload;
    }

    public byte[] getKey() {
      return _key;
    }

    public byte[] getPayload() {
      return _payload;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RowWithKey that = (RowWithKey) o;
      return Arrays.equals(_key, that._key) && Arrays.equals(_payload, that._payload);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(_key);
      result = 31 * result + Arrays.hashCode(_payload);
      return result;
    }
  }
}
