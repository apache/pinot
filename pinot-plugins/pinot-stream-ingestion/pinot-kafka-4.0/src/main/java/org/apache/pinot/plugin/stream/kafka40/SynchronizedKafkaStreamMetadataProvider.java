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
package org.apache.pinot.plugin.stream.kafka40;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

/**
 * A thread-safe variant of {@link KafkaStreamMetadataProvider} that synchronizes
 * access to metadata fetch operations.
 * This is particularly useful when a shared instance of {@link KafkaStreamMetadataProvider}
 * is accessed concurrently by multiple threads.
 */
public class SynchronizedKafkaStreamMetadataProvider extends KafkaStreamMetadataProvider {

  public SynchronizedKafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    super(clientId, streamConfig);
  }

  @Override
  public Map<Integer, StreamPartitionMsgOffset> fetchLatestStreamOffset(Set<Integer> partitionIds, long timeoutMillis) {
    synchronized (this) {
      return super.fetchLatestStreamOffset(partitionIds, timeoutMillis);
    }
  }
}
