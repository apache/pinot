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

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;


/**
 * Manages the Kinesis stream connection, given the stream name and aws region
 */
public class KinesisConnectionHandler {
  protected KinesisClient _kinesisClient;
  private final String _stream;
  private final String _region;
  private final String _accessKey;
  private final String _secretKey;

  public KinesisConnectionHandler(KinesisConfig kinesisConfig) {
    _stream = kinesisConfig.getStreamTopicName();
    _region = kinesisConfig.getAwsRegion();
    _accessKey = kinesisConfig.getAccessKey();
    _secretKey = kinesisConfig.getSecretKey();
    createConnection();
  }

  @VisibleForTesting
  public KinesisConnectionHandler(KinesisConfig kinesisConfig, KinesisClient kinesisClient) {
    _stream = kinesisConfig.getStreamTopicName();
    _region = kinesisConfig.getAwsRegion();
    _accessKey = kinesisConfig.getAccessKey();
    _secretKey = kinesisConfig.getSecretKey();
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
      if (StringUtils.isNotBlank(_accessKey) && StringUtils.isNotBlank(_secretKey)) {
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(_accessKey, _secretKey);
        _kinesisClient = KinesisClient.builder().region(Region.of(_region))
            .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials)).build();
      } else {
        _kinesisClient =
            KinesisClient.builder().region(Region.of(_region)).credentialsProvider(DefaultCredentialsProvider.create())
                .build();
      }
    }
  }

  public void close() {
    if (_kinesisClient != null) {
      _kinesisClient.close();
      _kinesisClient = null;
    }
  }
}
