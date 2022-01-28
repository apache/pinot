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
package org.apache.pinot.plugin.stream.kafka20;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.TransientConsumerException;


public class KafkaStreamMetadataProvider extends KafkaPartitionLevelConnectionHandler
    implements StreamMetadataProvider {

  public KafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    this(clientId, streamConfig, Integer.MIN_VALUE);
  }

  public KafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition);
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    try {
      return _consumer.partitionsFor(_topic, Duration.ofMillis(timeoutMillis)).size();
    } catch (TimeoutException e) {
      throw new TransientConsumerException(e);
    }
  }

  @Override
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis) {
    Preconditions.checkNotNull(offsetCriteria);
    long offset;
    try {
      if (offsetCriteria.isLargest()) {
        offset = _consumer.endOffsets(Collections.singletonList(_topicPartition), Duration.ofMillis(timeoutMillis))
            .get(_topicPartition);
      } else if (offsetCriteria.isSmallest()) {
        offset =
            _consumer.beginningOffsets(Collections.singletonList(_topicPartition), Duration.ofMillis(timeoutMillis))
                .get(_topicPartition);
      } else {
        throw new IllegalArgumentException("Unknown initial offset value " + offsetCriteria);
      }
      return new LongMsgOffset(offset);
    } catch (TimeoutException e) {
      throw new TransientConsumerException(e);
    }
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
