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
package org.apache.pinot.plugin.stream.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

/**
 * A {@link StreamPartitionMsgOffset} implementation for the Kinesis partition group consumption
 * A partition group consists of 1 or more shards. The KinesisCheckpoint maintains a Map of shards to the
 * sequenceNumber.
 * The sequenceNumber is the id (equivalent to offset in kafka) for the messages in the shard.
 * From the Kinesis documentation:
 * Each data record has a sequence number that is unique per partition-key within its shard.
 * Kinesis Data Streams assigns the sequence number after you write to the stream with client.putRecords or client
 * .putRecord.
 * Sequence numbers for the same partition key generally increase over time.
 * The longer the time period between write requests, the larger the sequence numbers become.
 */
public class KinesisPartitionGroupOffset implements StreamPartitionMsgOffset {
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();
  public static final ObjectReader DEFAULT_READER = DEFAULT_MAPPER.reader();
  public static final ObjectWriter DEFAULT_WRITER = DEFAULT_MAPPER.writer();

  private final Map<String, String> _shardToStartSequenceMap;

  public KinesisPartitionGroupOffset(Map<String, String> shardToStartSequenceMap) {
    _shardToStartSequenceMap = shardToStartSequenceMap;
  }

  public KinesisPartitionGroupOffset(String offsetStr)
      throws IOException {
    _shardToStartSequenceMap = stringToObject(offsetStr, new TypeReference<Map<String, String>>() {
    });
  }

  public Map<String, String> getShardToStartSequenceMap() {
    return _shardToStartSequenceMap;
  }

  @Override
  public String toString() {
    try {
      return objectToString(_shardToStartSequenceMap);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          "Caught exception when converting KinesisCheckpoint to string: " + _shardToStartSequenceMap);
    }
  }

  @Override
  public KinesisPartitionGroupOffset fromString(String kinesisCheckpointStr) {
    try {
      return new KinesisPartitionGroupOffset(kinesisCheckpointStr);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Caught exception when converting string to KinesisCheckpoint: " + kinesisCheckpointStr);
    }
  }

  @Override
  public int compareTo(StreamPartitionMsgOffset o) {
    Preconditions.checkNotNull(o);
    KinesisPartitionGroupOffset other = (KinesisPartitionGroupOffset) o;
    Preconditions.checkNotNull(other._shardToStartSequenceMap);
    Preconditions.checkNotNull(_shardToStartSequenceMap);
    Preconditions
        .checkState(other._shardToStartSequenceMap.size() == 1, "Only 1 shard per consumer supported. Found: %s",
            other._shardToStartSequenceMap);
    Preconditions
        .checkState(_shardToStartSequenceMap.size() == 1, "Only 1 shard per consumer supported. Found: %s",
            _shardToStartSequenceMap);
    return _shardToStartSequenceMap.values().iterator().next()
        .compareTo(other._shardToStartSequenceMap.values().iterator().next());
  }

  public static <T> T stringToObject(String jsonString, TypeReference<T> valueTypeRef)
      throws IOException {
    return DEFAULT_READER.forType(valueTypeRef).readValue(jsonString);
  }

  public static String objectToString(Object object)
      throws JsonProcessingException {
    return DEFAULT_WRITER.writeValueAsString(object);
  }
}
