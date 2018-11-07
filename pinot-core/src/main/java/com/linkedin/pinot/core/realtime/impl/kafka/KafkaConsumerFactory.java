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

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.realtime.stream.PartitionLevelConsumer;
import com.linkedin.pinot.core.realtime.stream.StreamConsumerFactory;
import com.linkedin.pinot.core.realtime.stream.StreamLevelConsumer;
import com.linkedin.pinot.core.realtime.stream.StreamMetadataProvider;


/**
 * A {@link StreamConsumerFactory} implementation for consuming a kafka stream using kafka consumers
 */
public class KafkaConsumerFactory extends StreamConsumerFactory {

  /**
   * Creates a partition level consumer for fetching from a partition of a kafka stream
   * @param clientId
   * @param partition
   * @return
   */
  @Override
  public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
    return new KafkaPartitionLevelConsumer(clientId, _streamConfig, partition);
  }

  /**
   * Creates a stream level consumer for a kafka stream
   * @param clientId
   * @param tableName
   * @param schema
   * @param instanceZKMetadata
   * @param serverMetrics
   * @return
   */
  @Override
  public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Schema schema,
      InstanceZKMetadata instanceZKMetadata, ServerMetrics serverMetrics) {
    return new KafkaStreamLevelConsumer(clientId, tableName, _streamConfig, schema, instanceZKMetadata, serverMetrics);
  }

  /**
   * Creates a partition metadata provider for a kafka stream
   * @param clientId
   * @param partition
   * @return
   */
  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    return new KafkaStreamMetadataProvider(clientId, _streamConfig, partition);
  }

  /**
   * Creates a stream metadata provider for a kafka stream
   * @param clientId
   * @return
   */
  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new KafkaStreamMetadataProvider(clientId, _streamConfig);
  }
}
