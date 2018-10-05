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
package com.linkedin.pinot.core.realtime.impl.kafka;

import com.linkedin.pinot.core.realtime.stream.PartitionLevelConsumer;
import com.linkedin.pinot.core.realtime.stream.StreamConsumerFactory;
import com.linkedin.pinot.core.realtime.stream.StreamLevelConsumer;
import com.linkedin.pinot.core.realtime.stream.StreamMetadataProvider;


/**
 * A {@link StreamConsumerFactory} implementation for consuming a kafka stream using Kafka's Simple Consumer
 */
// TODO: this should ideally be called KafkaConsumerFactory. It is not a factory for simple consumer.
// We cannot change this because open source usages of this factory will need to change the class name defined in their stream configs inside table configs
public class SimpleConsumerFactory extends StreamConsumerFactory {

  /**
   * Creates a partition level consumer for fetching from a partition of a kafka stream
   * @param clientId
   * @param partition
   * @return
   */
  @Override
  public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
    return new KafkaPartitionLevelConsumer(clientId, _streamMetadata, partition);
  }

  /**
   * Creates a stream level consumer for a kafka stream
   * @param clientId
   * @return
   */
  @Override
  public StreamLevelConsumer createStreamLevelConsumer(String clientId) {
    return new KafkaStreamLevelConsumer();
  }

  /**
   * Creates a partition metadata provider for a kafka stream
   * @param clientId
   * @param partition
   * @return
   */
  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    return new KafkaStreamMetadataProvider(clientId, _streamMetadata, partition);
  }

  /**
   * Creates a stream metadata provider for a kafka stream
   * @param clientId
   * @return
   */
  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new KafkaStreamMetadataProvider(clientId, _streamMetadata);
  }
}
