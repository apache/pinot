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
package org.apache.pinot.plugin.stream.rocketmq;

import java.util.Set;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMetadataProvider;


/**
 * A {@link StreamConsumerFactory} implementation for the RocketMQ stream
 */
public class RocketMQConsumerFactory extends StreamConsumerFactory {
  @Override
  public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Set<String> fieldsToRead,
      String groupId) {
    return new RocketMQStreamLevelConsumer(clientId, tableName, _streamConfig, fieldsToRead, groupId);
  }

  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new RocketMQStreamMetadataProvider(clientId, _streamConfig);
  }
}
