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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.utils.retry.RetryPolicy;


/**
 * Factory for a stream which provides a consumer and a metadata provider for the stream
 */
public abstract class StreamConsumerFactory {
  private static final AtomicInteger CLIENT_ID_SEQ = new AtomicInteger(0);

  protected StreamConfig _streamConfig;

  /**
   * Initializes the stream consumer factory with the stream metadata for the table
   * @param streamConfig the stream config object from the table config
   */
  protected void init(StreamConfig streamConfig) {
    _streamConfig = streamConfig;
  }

  /**
   * Creates a metadata provider which provides partition specific metadata
   * @param clientId a client id to identify the creator of this consumer
   * @param partition the partition id of the partition for which this metadata provider is being created
   * @return
   */
  public abstract StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition);

  /**
   * Creates a metadata provider which provides stream specific metadata
   * @param clientId a client id to identify the creator of this consumer
   * @return
   */
  public abstract StreamMetadataProvider createStreamMetadataProvider(String clientId);

  public StreamMetadataProvider createStreamMetadataProvider(String clientId, boolean concurrentAccessExpected) {
    return createStreamMetadataProvider(clientId);
  }

  public StreamPartitionMsgOffsetFactory createStreamMsgOffsetFactory() {
    return new LongMsgOffsetFactory();
  }

  /// Creates a [PartitionGroupConsumer] that fetches messages from the partition group described by
  /// `partitionGroupConsumptionStatus`.
  ///
  /// Every [StreamConsumerFactory] implementation must override this method. The returned consumer is owned by
  /// the caller, which is responsible for closing it via [PartitionGroupConsumer#close()]. Each invocation returns
  /// a new consumer instance; implementations are not required to make the returned consumer thread-safe.
  ///
  /// @param clientId identifies the creator of this consumer
  /// @param partitionGroupConsumptionStatus the partition group to consume and the offsets to start from
  /// @return a new, non-null partition group consumer
  public abstract PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus);

  public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus, RetryPolicy retryPolicy) {
    return createPartitionGroupConsumer(clientId, partitionGroupConsumptionStatus);
  }

  public static String getUniqueClientId(String prefix) {
    if (prefix == null) {
      return String.valueOf(CLIENT_ID_SEQ.getAndIncrement());
    }
    return prefix + "-" + CLIENT_ID_SEQ.getAndIncrement();
  }
}
