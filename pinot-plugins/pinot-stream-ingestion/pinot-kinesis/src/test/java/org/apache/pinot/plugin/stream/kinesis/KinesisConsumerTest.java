package org.apache.pinot.plugin.stream.kinesis; /**
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;


public class KinesisConsumerTest {
  public static void main(String[] args) {
    Map<String, String> props = new HashMap<>();
    props.put("stream", "kinesis-test");
    props.put("aws-region", "us-west-2");
    props.put("maxRecords", "10");

    KinesisConfig kinesisConfig = new KinesisConfig(props);

    KinesisConnectionHandler kinesisConnectionHandler = new KinesisConnectionHandler("kinesis-test", "us-west-2");

    List<Shard> shardList = kinesisConnectionHandler.getShards();

    for(Shard shard : shardList) {
      KinesisConsumer kinesisConsumer = new KinesisConsumer(kinesisConfig, new KinesisShardMetadata(shard.shardId(), "kinesis-test", "us-west-2"));

      KinesisCheckpoint kinesisCheckpoint = new KinesisCheckpoint(shard.sequenceNumberRange().startingSequenceNumber());
      KinesisFetchResult fetchResult = kinesisConsumer.fetch(kinesisCheckpoint, null, 6 * 10 * 1000L);

      List<Record> list = fetchResult.getMessages();

      System.out.println("SHARD: " + shard.shardId());
      for (Record record : list) {
        System.out.println("SEQ-NO: " + record.sequenceNumber() + ", DATA: " + record.data().asUtf8String());
      }
    }
  }
}
