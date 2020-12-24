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

import java.util.List;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.v2.ConsumerV2;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;


public class KinesisConnectionHandler {
  private String _stream;
  private String _awsRegion;
  KinesisClient _kinesisClient;

  public KinesisConnectionHandler() {

  }

  public KinesisConnectionHandler(String stream, String awsRegion) {
    _stream = stream;
    _awsRegion = awsRegion;
    createConnection();
  }

  public List<Shard> getShards() {
    ListShardsResponse listShardsResponse =
        _kinesisClient.listShards(ListShardsRequest.builder().streamName(_stream).build());
    return listShardsResponse.shards();
  }

  public void createConnection(){
    if(_kinesisClient == null) {
      _kinesisClient = KinesisClient.builder().region(Region.of(_awsRegion)).credentialsProvider(DefaultCredentialsProvider.create())
          .build();
    }
  }

  public void close(){
    if(_kinesisClient != null) {
      _kinesisClient.close();
      _kinesisClient = null;
    }
  }

}
