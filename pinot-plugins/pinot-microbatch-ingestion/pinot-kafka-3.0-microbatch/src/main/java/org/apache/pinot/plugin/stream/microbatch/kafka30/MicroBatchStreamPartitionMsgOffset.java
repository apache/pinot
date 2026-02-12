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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Comparator;
import java.util.Objects;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Composite offset for MicroBatch streams containing both Kafka message offset and record position.
 *
 * <h2>Serialization Format</h2>
 * <p>Offsets are serialized as JSON with abbreviated field names to minimize storage overhead:
 * <pre>
 * {"kmo":&lt;kafka_offset&gt;,"mbro":&lt;record_offset&gt;}
 * </pre>
 *
 * <p>Field definitions:
 * <ul>
 *   <li>{@code kmo} (Kafka Message Offset): The Kafka partition offset of the microbatch message</li>
 *   <li>{@code mbro} (MicroBatch Record Offset): Zero-based index of the record within the batch file</li>
 * </ul>
 *
 * <p>Examples:
 * <ul>
 *   <li>{@code {"kmo":0,"mbro":0}} - First record of first microbatch</li>
 *   <li>{@code {"kmo":5,"mbro":100}} - 101st record of microbatch at Kafka offset 5</li>
 *   <li>{@code {"kmo":10,"mbro":0}} - First record of microbatch at Kafka offset 10</li>
 * </ul>
 *
 * <h2>Deserialization</h2>
 * <p>The {@link MicroBatchStreamPartitionMsgOffsetFactory} handles two input formats:
 * <ul>
 *   <li>JSON format: {@code {"kmo":X,"mbro":Y}} - Full composite offset</li>
 *   <li>Numeric string: {@code "123"} - Interpreted as Kafka offset with mbro=0</li>
 * </ul>
 *
 * <h2>Ordering</h2>
 * <p>Offsets are ordered first by {@code kmo}, then by {@code mbro}. This ensures correct
 * resume behavior when restarting mid-batch after a segment commit.
 *
 * <h2>Compatibility</h2>
 * <p>This offset format is specific to MicroBatch consumers and is NOT compatible with
 * standard Kafka consumer offsets. Mixing consumers on the same table will cause issues.
 */
public class MicroBatchStreamPartitionMsgOffset implements StreamPartitionMsgOffset {

  private final long _kafkaMessageOffset;
  private final long _recordOffsetInMicroBatch;

  /**
   * Create an offset with just the Kafka message offset (record offset defaults to 0).
   */
  public static MicroBatchStreamPartitionMsgOffset of(long kafkaMessageOffset) {
    return new MicroBatchStreamPartitionMsgOffset(kafkaMessageOffset, 0);
  }

  @JsonCreator
  public MicroBatchStreamPartitionMsgOffset(
      @JsonProperty("kmo") long kafkaMessageOffset,
      @JsonProperty("mbro") long microBatchRecordOffset) {
    _kafkaMessageOffset = kafkaMessageOffset;
    _recordOffsetInMicroBatch = microBatchRecordOffset;
  }

  @JsonProperty("kmo")
  public long getKafkaMessageOffset() {
    return _kafkaMessageOffset;
  }

  @JsonProperty("mbro")
  public long getRecordOffsetInMicroBatch() {
    return _recordOffsetInMicroBatch;
  }

  @Override
  public String toString() {
    try {
      return JsonUtils.objectToString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int compareTo(StreamPartitionMsgOffset o) {
    MicroBatchStreamPartitionMsgOffset other = (MicroBatchStreamPartitionMsgOffset) o;
    return Comparator.comparing(MicroBatchStreamPartitionMsgOffset::getKafkaMessageOffset)
        .thenComparing(MicroBatchStreamPartitionMsgOffset::getRecordOffsetInMicroBatch)
        .compare(this, other);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MicroBatchStreamPartitionMsgOffset that = (MicroBatchStreamPartitionMsgOffset) o;
    return _kafkaMessageOffset == that._kafkaMessageOffset
        && _recordOffsetInMicroBatch == that._recordOffsetInMicroBatch;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_kafkaMessageOffset, _recordOffsetInMicroBatch);
  }
}
