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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadataMap;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;


public class KinesisPartitionGroupMetadataMap extends KinesisConnectionHandler implements PartitionGroupMetadataMap {
  private final List<PartitionGroupMetadata> _stringPartitionGroupMetadataIndex = new ArrayList<>();

  public KinesisPartitionGroupMetadataMap(String stream, String awsRegion,
      PartitionGroupMetadataMap currentPartitionGroupMetadataMap) {
    //TODO: Handle child shards. Do not consume data from child shard unless parent is finished.
    //Return metadata only for shards in current metadata
    super(stream, awsRegion);
    KinesisPartitionGroupMetadataMap currentPartitionMeta =
        (KinesisPartitionGroupMetadataMap) currentPartitionGroupMetadataMap;
    List<PartitionGroupMetadata> currentMetaList = currentPartitionMeta.getMetadataList();

    List<Shard> shardList = getShards();

    Map<String, PartitionGroupMetadata> currentMetadataMap = new HashMap<>();
    for (PartitionGroupMetadata partitionGroupMetadata : currentMetaList) {
      KinesisShardMetadata kinesisShardMetadata = (KinesisShardMetadata) partitionGroupMetadata;
      currentMetadataMap.put(kinesisShardMetadata.getShardId(), kinesisShardMetadata);
    }

    for (Shard shard : shardList) {
      if (currentMetadataMap.containsKey(shard.shardId())) {
        //Return existing shard metadata
        _stringPartitionGroupMetadataIndex.add(currentMetadataMap.get(shard.shardId()));
      } else if (currentMetadataMap.containsKey(shard.parentShardId())) {
        KinesisShardMetadata kinesisShardMetadata = (KinesisShardMetadata) currentMetadataMap.get(shard.parentShardId());
        if (isProcessingFinished(kinesisShardMetadata)) {
          //Add child shards for processing since parent has finished
          appendShardMetadata(stream, awsRegion, shard);
        } else {
          //Do not process this shard unless the parent shard is finished or expired
        }
      } else {
        //This is a new shard with no parents. We can start processing this shard.
        appendShardMetadata(stream, awsRegion, shard);
      }
    }
  }

  private boolean isProcessingFinished(KinesisShardMetadata kinesisShardMetadata) {
    return kinesisShardMetadata.getEndCheckpoint().getSequenceNumber() != null && kinesisShardMetadata
        .getStartCheckpoint().getSequenceNumber().equals(kinesisShardMetadata.getEndCheckpoint().getSequenceNumber());
  }

  private void appendShardMetadata(String stream, String awsRegion, Shard shard) {
    String startSequenceNumber = shard.sequenceNumberRange().startingSequenceNumber();
    String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
    KinesisShardMetadata shardMetadata = new KinesisShardMetadata(shard.shardId(), stream, awsRegion);
    shardMetadata.setStartCheckpoint(new KinesisCheckpoint(startSequenceNumber));
    shardMetadata.setEndCheckpoint(new KinesisCheckpoint(endingSequenceNumber));
    _stringPartitionGroupMetadataIndex.add(shardMetadata);
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
