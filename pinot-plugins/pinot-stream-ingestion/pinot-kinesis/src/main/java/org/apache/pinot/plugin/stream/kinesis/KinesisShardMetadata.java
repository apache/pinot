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

import org.apache.pinot.spi.stream.v2.Checkpoint;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadata;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

//TODO: Implement shardId as Array and have unique id
public class KinesisShardMetadata extends KinesisConnectionHandler implements PartitionGroupMetadata {
  String _shardId;
  KinesisCheckpoint _startCheckpoint;
  KinesisCheckpoint _endCheckpoint;

  public KinesisShardMetadata(String shardId, String streamName, String awsRegion) {
    super(streamName, awsRegion);
    _startCheckpoint = null;
    _endCheckpoint = null;
    _shardId = shardId;
  }

  public String getShardId() {
    return _shardId;
  }

  @Override
  public KinesisCheckpoint getStartCheckpoint() {
    return _startCheckpoint;
  }

  @Override
  public KinesisCheckpoint getEndCheckpoint() {
    return _endCheckpoint;
  }

  @Override
  public void setStartCheckpoint(Checkpoint startCheckpoint) {
    _startCheckpoint = (KinesisCheckpoint) startCheckpoint;
  }

  @Override
  public void setEndCheckpoint(Checkpoint endCheckpoint) {
    _endCheckpoint = (KinesisCheckpoint) endCheckpoint;
  }

  @Override
  public byte[] serialize() {
    return new byte[0];
  }

  @Override
  public KinesisShardMetadata deserialize(byte[] blob) {
    return null;
  }
}
