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
package org.apache.pinot.plugin.stream.push;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StreamMetadataProvider} implementation for the Kinesis stream
 */
public class PushBasedIngestionMetadataProvider implements StreamMetadataProvider {
  private final StreamConsumerFactory _pushBasedIngestionConsumerFactory;
  private final String _clientId;
  private static final Logger LOGGER = LoggerFactory.getLogger(PushBasedIngestionMetadataProvider.class);

  public PushBasedIngestionMetadataProvider(String clientId, StreamConfig streamConfig) {
    _pushBasedIngestionConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    _clientId = clientId;
  }

  public PushBasedIngestionMetadataProvider(String clientId, StreamConfig streamConfig,
      StreamConsumerFactory streamConsumerFactory) {
    _pushBasedIngestionConsumerFactory = streamConsumerFactory;
    _clientId = clientId;
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    return 1;
  }

  @Override
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis) {
    return new LongMsgOffset(0L);
  }

  @Override
  public Map<String, PartitionLagState> getCurrentPartitionLagState(
      Map<String, ConsumerPartitionState> currentPartitionStateMap) {
    Map<String, PartitionLagState> perPartitionLag = new HashMap<>();
    for (Map.Entry<String, ConsumerPartitionState> entry : currentPartitionStateMap.entrySet()) {
      ConsumerPartitionState partitionState = entry.getValue();
      // Compute records-lag
      StreamPartitionMsgOffset currentOffset = partitionState.getCurrentOffset();
      StreamPartitionMsgOffset upstreamLatest = partitionState.getUpstreamLatestOffset();
      String offsetLagString = "UNKNOWN";

      if (currentOffset instanceof LongMsgOffset && upstreamLatest instanceof LongMsgOffset) {
        long offsetLag = ((LongMsgOffset) upstreamLatest).getOffset() - ((LongMsgOffset) currentOffset).getOffset();
        offsetLagString = String.valueOf(offsetLag);
      }

      // Compute record-availability
      String availabilityLagMs = "UNKNOWN";
      RowMetadata lastProcessedMessageMetadata = partitionState.getLastProcessedRowMetadata();
      if (lastProcessedMessageMetadata != null && partitionState.getLastProcessedTimeMs() > 0) {
        long availabilityLag =
            partitionState.getLastProcessedTimeMs() - lastProcessedMessageMetadata.getRecordIngestionTimeMs();
        availabilityLagMs = String.valueOf(availabilityLag);
      }

      perPartitionLag.put(entry.getKey(),
          new PushBasedIngestionConsumerPartitionLag(offsetLagString, availabilityLagMs));
    }
    return perPartitionLag;
  }

  @Override
  public void close() {
  }
}
