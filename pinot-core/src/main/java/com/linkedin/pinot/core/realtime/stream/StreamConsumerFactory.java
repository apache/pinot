/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.stream;

import com.linkedin.pinot.core.realtime.impl.kafka.KafkaLowLevelStreamProviderConfig;


/**
 * Factory for a stream which provides a consumer and a metadata provider for the stream
 */
public abstract class StreamConsumerFactory {
  protected StreamConfig _streamConfig;

  /**
   * Initializes the stream consumer factory with the stream metadata for the table
   * @param streamConfig
   */
  void init(StreamConfig streamConfig) {
    _streamConfig = streamConfig;
  }

  /**
   * Creates a partition level consumer which can fetch messages from a partitioned stream
   * @param clientId
   * @param partition
   * @return
   */
  public abstract PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition);

  /**
   * Creates a stream level consumer (high level) which can fetch messages from the stream
   * @param clientId
   * @return
   */
  public abstract StreamLevelConsumer createStreamLevelConsumer(String clientId);

  /**
   * Creates a metadata provider which provides partition specific metadata
   * @param partition
   * @return
   */
  public abstract StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition);

  /**
   * Creates a metadata provider which provides stream specific metadata
   * @return
   */
  public abstract StreamMetadataProvider createStreamMetadataProvider(String clientId);

  // TODO First split KafkaLowLevelStreamProviderConfig to be kafka agnostic and kafka-specific and then rename.
  public StreamMessageDecoder getDecoder(KafkaLowLevelStreamProviderConfig kafkaStreamProviderConfig) throws Exception {
    return kafkaStreamProviderConfig.getDecoder();
  }
}
