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
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;


/**
 * Manages the Kinesis stream connection, given the stream name and aws region
 */
public class KinesisConnectionHandler {
  KinesisClient _kinesisClient;
  private final String _stream;
  private final String _awsRegion;

  public KinesisConnectionHandler(String stream, String awsRegion) {
    _stream = stream;
    _awsRegion = awsRegion;
    createConnection();
  }

  public KinesisConnectionHandler(String stream, String awsRegion, KinesisClient kinesisClient) {
    _stream = stream;
    _awsRegion = awsRegion;
    _kinesisClient = kinesisClient;
  }

  /**
   * Lists all shards of the stream
   */
  public List<Shard> getShards() {
    ListShardsResponse listShardsResponse =
        _kinesisClient.listShards(ListShardsRequest.builder().streamName(_stream).build());
    return listShardsResponse.shards();
  }

  /**
   * Creates a Kinesis client for the stream
   */
  public void createConnection() {
    if (_kinesisClient == null) {
      _kinesisClient =
          KinesisClient.builder().region(Region.of(_awsRegion)).credentialsProvider(DefaultCredentialsProvider.create())
              .build();
    }
  }

  public void close() {
    if (_kinesisClient != null) {
      _kinesisClient.close();
      _kinesisClient = null;
    }
  }
}
