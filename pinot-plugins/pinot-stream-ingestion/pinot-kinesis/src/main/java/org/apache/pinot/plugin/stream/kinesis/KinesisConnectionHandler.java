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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheSdkHttpService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;


/**
 * Manages the Kinesis stream connection, given the stream name and aws region
 */
public class KinesisConnectionHandler {
  protected KinesisClient _kinesisClient;
  private final String _stream;
  private final String _region;
  private final String _accessKey;
  private final String _secretKey;
  private final String _endpoint;
  private final KinesisConfig _kinesisConfig;
  protected final KinesisMetadataExtractor _kinesisMetadataExtractor;

  public KinesisConnectionHandler(KinesisConfig kinesisConfig) {
    _stream = kinesisConfig.getStreamTopicName();
    _region = kinesisConfig.getAwsRegion();
    _accessKey = kinesisConfig.getAccessKey();
    _secretKey = kinesisConfig.getSecretKey();
    _endpoint = kinesisConfig.getEndpoint();
    _kinesisConfig = kinesisConfig;
    _kinesisMetadataExtractor = KinesisMetadataExtractor.build(kinesisConfig.isPopulateMetadata());
    createConnection();
  }

  @VisibleForTesting
  public KinesisConnectionHandler(KinesisConfig kinesisConfig, KinesisClient kinesisClient) {
    _stream = kinesisConfig.getStreamTopicName();
    _region = kinesisConfig.getAwsRegion();
    _accessKey = kinesisConfig.getAccessKey();
    _secretKey = kinesisConfig.getSecretKey();
    _endpoint = kinesisConfig.getEndpoint();
    _kinesisConfig = kinesisConfig;
    _kinesisMetadataExtractor = KinesisMetadataExtractor.build(kinesisConfig.isPopulateMetadata());
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
      KinesisClientBuilder kinesisClientBuilder;

      AwsCredentialsProvider awsCredentialsProvider;
      if (StringUtils.isNotBlank(_accessKey) && StringUtils.isNotBlank(_secretKey)) {
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(_accessKey, _secretKey);
        awsCredentialsProvider = StaticCredentialsProvider.create(awsBasicCredentials);
      } else {
        awsCredentialsProvider = DefaultCredentialsProvider.builder().build();
      }

      if (_kinesisConfig.isIamRoleBasedAccess()) {
        AssumeRoleRequest.Builder assumeRoleRequestBuilder =
            AssumeRoleRequest.builder()
                .roleArn(_kinesisConfig.getRoleArn())
                .roleSessionName(_kinesisConfig.getRoleSessionName())
                .durationSeconds(_kinesisConfig.getSessionDurationSeconds());

        AssumeRoleRequest assumeRoleRequest;
        if (StringUtils.isNotEmpty(_kinesisConfig.getExternalId())) {
          assumeRoleRequest = assumeRoleRequestBuilder
              .externalId(_kinesisConfig.getExternalId())
              .build();
        } else {
          assumeRoleRequest = assumeRoleRequestBuilder.build();
        }

        StsClient stsClient =
            StsClient.builder()
                .region(Region.of(_region))
                .credentialsProvider(awsCredentialsProvider)
                .build();

        awsCredentialsProvider =
            StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(assumeRoleRequest)
                .asyncCredentialUpdateEnabled(_kinesisConfig.isAsyncSessionUpdateEnabled())
                .build();
      }

      kinesisClientBuilder =
          KinesisClient.builder()
              .region(Region.of(_region))
              .credentialsProvider(awsCredentialsProvider)
              .httpClientBuilder(new ApacheSdkHttpService().createHttpClientBuilder());

      if (StringUtils.isNotBlank(_endpoint)) {
        try {
          kinesisClientBuilder = kinesisClientBuilder.endpointOverride(new URI(_endpoint));
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException("URI syntax is not correctly specified for endpoint: " + _endpoint, e);
        }
      }

      _kinesisClient = kinesisClientBuilder.build();
    }
  }

  public void close() {
    if (_kinesisClient != null) {
      _kinesisClient.close();
      _kinesisClient = null;
    }
  }
}
