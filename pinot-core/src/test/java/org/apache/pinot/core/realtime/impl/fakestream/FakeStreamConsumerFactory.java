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
package org.apache.pinot.core.realtime.impl.fakestream;

import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.realtime.stream.MessageBatch;
import org.apache.pinot.core.realtime.stream.OffsetCriteria;
import org.apache.pinot.core.realtime.stream.PartitionLevelConsumer;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.apache.pinot.core.realtime.stream.StreamConsumerFactory;
import org.apache.pinot.core.realtime.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.core.realtime.stream.StreamDecoderProvider;
import org.apache.pinot.core.realtime.stream.StreamLevelConsumer;
import org.apache.pinot.core.realtime.stream.StreamMessageDecoder;
import org.apache.pinot.core.realtime.stream.StreamMetadataProvider;


/**
 * Implementation of {@link StreamConsumerFactory} for a fake stream
 * 2 partitions
 * Data source is /resources/data/On_Time_Performance_2014_partition.tar.gz
 */
public class FakeStreamConsumerFactory extends StreamConsumerFactory {

  @Override
  public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
    return new FakePartitionLevelConsumer(partition);
  }

  @Override
  public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Schema schema,
      InstanceZKMetadata instanceZKMetadata, ServerMetrics serverMetrics) {
    return new FakeStreamLevelConsumer();
  }

  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    return new FakeStreamMetadataProvider();
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new FakeStreamMetadataProvider();
  }

  public static void main(String[] args) throws Exception {
    String clientId = "client_id_localhost_tester";

    // stream config
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();

    // stream consumer factory
    StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);

    // stream metadata provider
    StreamMetadataProvider streamMetadataProvider = streamConsumerFactory.createStreamMetadataProvider(clientId);
    int partitionCount = streamMetadataProvider.fetchPartitionCount(10_000);
    System.out.println(partitionCount);

    // Partition metadata provider
    StreamMetadataProvider partitionMetadataProvider =
        streamConsumerFactory.createPartitionMetadataProvider(clientId, 1);
    long partitionOffset =
        partitionMetadataProvider.fetchPartitionOffset(OffsetCriteria.SMALLEST_OFFSET_CRITERIA, 10_000);
    System.out.println(partitionOffset);

    // Partition level consumer
    PartitionLevelConsumer partitionLevelConsumer = streamConsumerFactory.createPartitionLevelConsumer(clientId, 0);
    MessageBatch messageBatch = partitionLevelConsumer.fetchMessages(10, 40, 10_000);

    // Message decoder
    Schema pinotSchema = FakeStreamConfigUtils.getPinotSchema();
    StreamMessageDecoder streamMessageDecoder = StreamDecoderProvider.create(streamConfig, pinotSchema);
    GenericRow decodedRow = new GenericRow();
    streamMessageDecoder.decode(messageBatch.getMessageAtIndex(0), decodedRow);
    System.out.println(decodedRow);
  }
}
