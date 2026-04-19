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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.plugin.stream.kafka30.KafkaStreamMetadataProvider;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


/**
 * Metadata provider for MicroBatch streams that wraps Kafka offsets in composite format.
 *
 * <p>This provider delegates to {@link KafkaStreamMetadataProvider} for partition and offset
 * discovery, then wraps the returned Kafka offsets into {@link MicroBatchStreamPartitionMsgOffset}
 * format. This ensures that all offsets used by the MicroBatch consumer are in the composite
 * format required for mid-batch resume.
 *
 * <p>Key behaviors:
 * <ul>
 *   <li>Partition operations (count, IDs, topics) delegate directly to Kafka provider</li>
 *   <li>Offset operations wrap Kafka's {@link LongMsgOffset} into composite format with mbro=0</li>
 *   <li>{@link #supportsOffsetLag()} returns false since lag estimation requires knowing
 *       record counts in unconsumed batches</li>
 * </ul>
 *
 * @see MicroBatchStreamPartitionMsgOffset for the composite offset format
 * @see KafkaMicroBatchConsumerFactory which creates instances of this provider
 */
public class MicroBatchStreamMetadataProvider implements StreamMetadataProvider {

  private final KafkaStreamMetadataProvider _kafkaStreamMetadataProvider;

  public MicroBatchStreamMetadataProvider(KafkaStreamMetadataProvider kafkaStreamMetadataProvider) {
    _kafkaStreamMetadataProvider = kafkaStreamMetadataProvider;
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    return _kafkaStreamMetadataProvider.fetchPartitionCount(timeoutMillis);
  }

  @Override
  public Set<Integer> fetchPartitionIds(long timeoutMillis) {
    return _kafkaStreamMetadataProvider.fetchPartitionIds(timeoutMillis);
  }

  @Override
  public Map<Integer, StreamPartitionMsgOffset> fetchLatestStreamOffset(
      Set<Integer> partitionIds, long timeoutMillis) {
    Map<Integer, StreamPartitionMsgOffset> kafkaStreamPartitionMsgOffsets =
        _kafkaStreamMetadataProvider.fetchLatestStreamOffset(partitionIds, timeoutMillis);
    Map<Integer, StreamPartitionMsgOffset> microBatchStreamPartitionMsgOffset = new HashMap<>();
    kafkaStreamPartitionMsgOffsets.forEach(
        (partitionId, offset) -> microBatchStreamPartitionMsgOffset.put(partitionId, wrap(offset)));
    return microBatchStreamPartitionMsgOffset;
  }

  @Override
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(
      OffsetCriteria offsetCriteria, long timeoutMillis) {
    return wrap(
        _kafkaStreamMetadataProvider.fetchStreamPartitionOffset(offsetCriteria, timeoutMillis));
  }

  @Override
  public List<TopicMetadata> getTopics() {
    return _kafkaStreamMetadataProvider.getTopics();
  }

  @Override
  public boolean supportsOffsetLag() {
    // We wouldn't be able to estimate offset lag accurately because we don't know how many records
    // are in each microbatch.
    // That's why we return false here and don't override getCurrentPartitionLagState(Map<String,
    // ConsumerPartitionState> currentPartitionStateMap) even when KafkaStreamMetadataProvider has a
    // method override.
    // TODO: should we return batches count instead of records count?
    return false;
  }

  @Override
  public StreamPartitionMsgOffset getOffsetAtTimestamp(
      int partitionId, long timestampMillis, long timeoutMillis) {
    return wrap(
        _kafkaStreamMetadataProvider.getOffsetAtTimestamp(
            partitionId, timestampMillis, timeoutMillis));
  }

  @Override
  public Map<String, StreamPartitionMsgOffset> getStreamStartOffsets() {
    Map<String, StreamPartitionMsgOffset> kafkaStreamPartitionMsgOffsets =
        _kafkaStreamMetadataProvider.getStreamStartOffsets();
    Map<String, StreamPartitionMsgOffset> microBatchStreamPartitionMsgOffset = new HashMap<>();
    kafkaStreamPartitionMsgOffsets.forEach(
        (partitionId, offset) -> microBatchStreamPartitionMsgOffset.put(partitionId, wrap(offset)));
    return microBatchStreamPartitionMsgOffset;
  }

  @Override
  public Map<String, StreamPartitionMsgOffset> getStreamEndOffsets() {
    Map<String, StreamPartitionMsgOffset> kafkaStreamPartitionMsgOffsets =
        _kafkaStreamMetadataProvider.getStreamEndOffsets();
    Map<String, StreamPartitionMsgOffset> microBatchStreamPartitionMsgOffset = new HashMap<>();
    kafkaStreamPartitionMsgOffsets.forEach(
        (partitionId, offset) -> microBatchStreamPartitionMsgOffset.put(partitionId, wrap(offset)));
    return microBatchStreamPartitionMsgOffset;
  }

  @Override
  public void close() throws IOException {
    _kafkaStreamMetadataProvider.close();
  }

  private StreamPartitionMsgOffset wrap(StreamPartitionMsgOffset offset) {
    if (!(offset instanceof LongMsgOffset)) {
      throw new IllegalStateException("Expected LongMsgOffset but got: " + offset.getClass());
    }
    return MicroBatchStreamPartitionMsgOffset.of(((LongMsgOffset) offset).getOffset());
  }
}
