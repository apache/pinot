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

import java.util.Set;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMetadataProvider;


public class KafkaConsumerFactory extends StreamConsumerFactory {

  @Override
  public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
    return new KafkaPartitionLevelConsumer(clientId, _streamConfig, partition);
  }

  @Override
  public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Set<String> fieldsToRead,
      String groupId) {
    throw new UnsupportedOperationException("Apache pinot no longer support stream level consumer");
  }

  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    return new KafkaStreamMetadataProvider(clientId, _streamConfig, partition);
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new KafkaStreamMetadataProvider(clientId, _streamConfig);
  }
}
