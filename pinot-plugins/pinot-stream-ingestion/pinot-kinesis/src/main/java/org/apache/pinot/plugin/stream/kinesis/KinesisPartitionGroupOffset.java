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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * A {@link StreamPartitionMsgOffset} implementation for the Kinesis partition group consumption
 * A partition group consists of 1 shard. The KinesisCheckpoint maintains the shardId and sequenceNumber.
 * The sequenceNumber is the id (equivalent to offset in kafka) for the messages in the shard.
 * From the Kinesis documentation:
 * Each data record has a sequence number that is unique per partition-key within its shard.
 * Kinesis Data Streams assigns the sequence number after you write to the stream with client.putRecords or client
 * .putRecord.
 * Sequence numbers for the same partition key generally increase over time.
 * The longer the time period between write requests, the larger the sequence numbers become.
 */
public class KinesisPartitionGroupOffset implements StreamPartitionMsgOffset {
  private final String _shardId;
  @Nullable
  private final String _sequenceNumber;

  public KinesisPartitionGroupOffset(String shardId, @Nullable String sequenceNumber) {
    _shardId = shardId;
    _sequenceNumber = sequenceNumber;
  }

  public KinesisPartitionGroupOffset(String offsetStr) {
    try {
      ObjectNode objectNode = (ObjectNode) JsonUtils.stringToJsonNode(offsetStr);
      Preconditions.checkArgument(objectNode.size() == 1);
      Map.Entry<String, JsonNode> entry = objectNode.fields().next();
      _shardId = entry.getKey();
      _sequenceNumber = entry.getValue().asText();
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid Kinesis offset: " + offsetStr);
    }
  }

  public String getShardId() {
    return _shardId;
  }

  @Nullable
  public String getSequenceNumber() {
    return _sequenceNumber;
  }

  @Override
  public String toString() {
    return JsonUtils.newObjectNode().put(_shardId, _sequenceNumber).toString();
  }

  @Override
  public int compareTo(StreamPartitionMsgOffset other) {
    KinesisPartitionGroupOffset otherKinesisPartitionGroupOffset = ((KinesisPartitionGroupOffset) other);
    assert otherKinesisPartitionGroupOffset.getSequenceNumber() != null;
    assert _sequenceNumber != null;
    return _sequenceNumber.compareTo(otherKinesisPartitionGroupOffset.getSequenceNumber());
  }
}
