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
package org.apache.pinot.core.realtime.impl.kafka2;

import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.realtime.stream.PartitionLevelConsumer;
import org.apache.pinot.core.realtime.stream.StreamConsumerFactory;
import org.apache.pinot.core.realtime.stream.StreamLevelConsumer;
import org.apache.pinot.core.realtime.stream.StreamMetadataProvider;


public class Kafka2ConsumerFactory extends StreamConsumerFactory {
  @Override
  public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
    return new Kafka2PartitionLevelPartitionLevelConsumer(clientId, _streamConfig, partition);
  }

  @Override
  public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Schema schema,
      InstanceZKMetadata instanceZKMetadata, ServerMetrics serverMetrics) {
    return new Kafka2StreamLevelConsumer(clientId, tableName, _streamConfig, schema, instanceZKMetadata, serverMetrics);
  }

  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    return new Kafka2PartitionLevelStreamMetadataProvider(clientId, _streamConfig, partition);
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new Kafka2PartitionLevelStreamMetadataProvider(clientId, _streamConfig);
  }
}
