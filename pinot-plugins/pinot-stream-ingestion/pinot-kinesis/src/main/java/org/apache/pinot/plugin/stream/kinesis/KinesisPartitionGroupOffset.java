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
import java.io.IOException;
import java.util.Map;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * A {@link StreamPartitionMsgOffset} implementation for the Kinesis partition group consumption
 * A partition group consists of 1 or more shards. The KinesisCheckpoint maintains a Map of shards to the sequenceNumber
 */
public class KinesisPartitionGroupOffset implements StreamPartitionMsgOffset {
  private final Map<String, String> _shardToStartSequenceMap;

  public KinesisPartitionGroupOffset(Map<String, String> shardToStartSequenceMap) {
    _shardToStartSequenceMap = shardToStartSequenceMap;
  }

  public KinesisPartitionGroupOffset(String offsetStr)
      throws IOException {
    _shardToStartSequenceMap = JsonUtils.stringToObject(offsetStr, new TypeReference<Map<String, String>>() {
    });
  }

  public Map<String, String> getShardToStartSequenceMap() {
    return _shardToStartSequenceMap;
  }

  @Override
  public String toString() {
    try {
      return JsonUtils.objectToString(_shardToStartSequenceMap);
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
  public int compareTo(Object o) {

    return this._shardToStartSequenceMap.values().iterator().next()
        .compareTo(((KinesisPartitionGroupOffset) o)._shardToStartSequenceMap.values().iterator().next());
  }
}
