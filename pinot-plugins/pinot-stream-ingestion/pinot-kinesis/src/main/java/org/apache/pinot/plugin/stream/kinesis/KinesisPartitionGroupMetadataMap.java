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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadataMap;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;


public class KinesisPartitionGroupMetadataMap extends KinesisConnectionHandler implements PartitionGroupMetadataMap {
  private final List<PartitionGroupMetadata> _stringPartitionGroupMetadataIndex = new ArrayList<>();

  public KinesisPartitionGroupMetadataMap(String stream, String awsRegion) {
    super(stream, awsRegion);
    List<Shard> shardList = getShards();
    for (Shard shard : shardList) {
      String startSequenceNumber = shard.sequenceNumberRange().startingSequenceNumber();
      String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
      KinesisShardMetadata shardMetadata = new KinesisShardMetadata(shard.shardId(), stream, awsRegion);
      shardMetadata.setStartCheckpoint(new KinesisCheckpoint(startSequenceNumber));
      shardMetadata.setEndCheckpoint(new KinesisCheckpoint(endingSequenceNumber));
      _stringPartitionGroupMetadataIndex.add(shardMetadata);
    }
  }

  @Override
  public List<PartitionGroupMetadata> getMetadataList() {
    return _stringPartitionGroupMetadataIndex;
  }

  @Override
  public PartitionGroupMetadata getPartitionGroupMetadata(int index) {
    return _stringPartitionGroupMetadataIndex.get(index);
  }
}
