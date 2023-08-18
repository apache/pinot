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

import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamDecoderProvider;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


/**
 * Implementation of {@link StreamConsumerFactory} for a fake stream
 * Data source is /resources/data/fakestream_avro_data.tar.gz
 * Avro schema is /resources/data/fakestream/fake_stream_avro_schema.avsc
 * Pinot schema is /resources/data/fakestream/fake_stream_pinot_schema.avsc
 */
public class FakeStreamConsumerFactory extends StreamConsumerFactory {

  @Override
  public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
    return new FakePartitionLevelConsumer(partition, _streamConfig, FakeStreamConfigUtils.MESSAGE_BATCH_SIZE);
  }

  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    return new FakeStreamMetadataProvider(_streamConfig);
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new FakeStreamMetadataProvider(_streamConfig);
  }

  public static void main(String[] args)
      throws Exception {
    String clientId = "client_id_localhost_tester";

    // stream config
    int numPartitions = 5;
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs(numPartitions);

    // stream consumer factory
    StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);

    // stream metadata provider
    StreamMetadataProvider streamMetadataProvider = streamConsumerFactory.createStreamMetadataProvider(clientId);
    int partitionCount = streamMetadataProvider.fetchPartitionCount(10_000);
    System.out.println(partitionCount);

    // Partition metadata provider
    int partition = 3;
    StreamMetadataProvider partitionMetadataProvider =
        streamConsumerFactory.createPartitionMetadataProvider(clientId, partition);
    StreamPartitionMsgOffset partitionOffset =
        partitionMetadataProvider.fetchStreamPartitionOffset(OffsetCriteria.SMALLEST_OFFSET_CRITERIA, 10_000);
    System.out.println(partitionOffset);

    // Partition level consumer
    PartitionLevelConsumer partitionLevelConsumer =
        streamConsumerFactory.createPartitionLevelConsumer(clientId, partition);
    MessageBatch messageBatch =
        partitionLevelConsumer.fetchMessages(new LongMsgOffset(10), new LongMsgOffset(40), 10_000);

    // Message decoder
    Schema pinotSchema = FakeStreamConfigUtils.getPinotSchema();
    TableConfig tableConfig = FakeStreamConfigUtils.getTableConfig();
    StreamMessageDecoder streamMessageDecoder = StreamDecoderProvider.create(streamConfig,
        IngestionUtils.getFieldsForRecordExtractor(tableConfig.getIngestionConfig(), pinotSchema));
    GenericRow decodedRow = new GenericRow();
    streamMessageDecoder.decode(messageBatch.getMessageAtIndex(0), decodedRow);
    System.out.println(decodedRow);
  }
}
