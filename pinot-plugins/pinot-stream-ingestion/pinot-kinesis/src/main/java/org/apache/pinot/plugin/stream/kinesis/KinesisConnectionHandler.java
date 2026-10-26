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
import java.io.Closeable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.ingestion.error.IngestionSourceCredentialMode;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorEvidence;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.apache.ApacheSdkHttpService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;


/// Manages the Kinesis stream connection, given the stream name and aws region
public class KinesisConnectionHandler implements Closeable {
  protected final KinesisConfig _config;
  protected final KinesisClient _kinesisClient;
  // Whose credential the client signs with: the same AWS credential code is the table's bad key under STATIC and
  // Pinot's own identity under AMBIENT. It records the mode and whether a role is assumed, never a key or the ARN.
  private final IngestionSourceErrorEvidence _credentialEvidence;

  public KinesisConnectionHandler(KinesisConfig config) {
    _config = config;
    _credentialEvidence = credentialEvidence(config);
    _kinesisClient = createClient();
  }

  @VisibleForTesting
  public KinesisConnectionHandler(KinesisConfig config, KinesisClient kinesisClient) {
    _config = config;
    _credentialEvidence = credentialEvidence(config);
    _kinesisClient = kinesisClient;
  }

  private static boolean hasStaticKeys(KinesisConfig config) {
    return StringUtils.isNotBlank(config.getAccessKey()) && StringUtils.isNotBlank(config.getSecretKey());
  }

  private static IngestionSourceErrorEvidence credentialEvidence(KinesisConfig config) {
    return new IngestionSourceErrorEvidence().setCredentialMode(
            hasStaticKeys(config) ? IngestionSourceCredentialMode.STATIC : IngestionSourceCredentialMode.AMBIENT)
        .setRoleArnConfigured(config.isIamRoleBasedAccess());
  }

  /// Makes a call on the Kinesis client. An SDK failure leaves with the credential evidence attached, including the
  /// raw `StsException` the assume-role provider raises from inside the call.
  protected <T> T callKinesis(Supplier<T> call) {
    try {
      return call.get();
    } catch (SdkException e) {
      // The SDK builds a new exception for every failed call (checked against SDK 2.57): services unmarshal one per
      // response, the credential cache rethrows each refresh's own failure, and the default chain summarizes its
      // providers' failures in a new exception. So the evidence goes on the SDK's exception and its type is kept.
      throw IngestionSourceErrorEvidence.attach(e, _credentialEvidence);
    }
  }

  private KinesisClient createClient() {
    KinesisClientBuilder kinesisClientBuilder;

    AwsCredentialsProvider awsCredentialsProvider;
    String accessKey = _config.getAccessKey();
    String secretKey = _config.getSecretKey();
    if (hasStaticKeys(_config)) {
      AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(accessKey, secretKey);
      awsCredentialsProvider = StaticCredentialsProvider.create(awsBasicCredentials);
    } else {
      awsCredentialsProvider = DefaultCredentialsProvider.builder().build();
    }

    if (_config.isIamRoleBasedAccess()) {
      AssumeRoleRequest.Builder assumeRoleRequestBuilder =
          AssumeRoleRequest.builder().roleArn(_config.getRoleArn()).roleSessionName(_config.getRoleSessionName())
              .durationSeconds(_config.getSessionDurationSeconds());
      AssumeRoleRequest assumeRoleRequest;
      String externalId = _config.getExternalId();
      if (StringUtils.isNotBlank(externalId)) {
        assumeRoleRequest = assumeRoleRequestBuilder.externalId(externalId).build();
      } else {
        assumeRoleRequest = assumeRoleRequestBuilder.build();
      }
      StsClient stsClient =
          StsClient.builder().region(Region.of(_config.getAwsRegion())).credentialsProvider(awsCredentialsProvider)
              .build();
      awsCredentialsProvider =
          StsAssumeRoleCredentialsProvider.builder().stsClient(stsClient).refreshRequest(assumeRoleRequest)
              .asyncCredentialUpdateEnabled(_config.isAsyncSessionUpdateEnabled()).build();
    }

    kinesisClientBuilder =
        KinesisClient.builder().region(Region.of(_config.getAwsRegion())).credentialsProvider(awsCredentialsProvider)
            .httpClientBuilder(new ApacheSdkHttpService().createHttpClientBuilder());

    String endpoint = _config.getEndpoint();
    if (StringUtils.isNotBlank(endpoint)) {
      try {
        kinesisClientBuilder = kinesisClientBuilder.endpointOverride(new URI(endpoint));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("URI syntax is not correctly specified for endpoint: " + endpoint, e);
      }
    }

    return kinesisClientBuilder.build();
  }

  /// Lists all shards of the stream
  public List<Shard> getShards() {
    ListShardsResponse listShardsResponse = callKinesis(
        () -> _kinesisClient.listShards(ListShardsRequest.builder().streamName(_config.getStreamTopicName()).build()));
    return listShardsResponse.shards();
  }

  public List<String> getStreamNames() {
    final int limit = 1000; // default value for the API is 100
    try {
      List<String> allStreamNames = new ArrayList<>();
      ListStreamsResponse response =
          callKinesis(() -> _kinesisClient.listStreams(ListStreamsRequest.builder().limit(limit).build()));

      while (response != null && response.streamNames() != null) {
        allStreamNames.addAll(response.streamNames());

        if (!response.hasMoreStreams()) {
          break;
        }

        // Fetch the next page
        ListStreamsRequest nextPageRequest = ListStreamsRequest.builder()
            .exclusiveStartStreamName(allStreamNames.get(allStreamNames.size() - 1))
            .limit(limit)
            .build();
        response = callKinesis(() -> _kinesisClient.listStreams(nextPageRequest));
      }

      return allStreamNames;
    } catch (Exception e) {
      // Log the exception (if a logger is available) for better observability
      throw new RuntimeException("Failed to list Kinesis streams: " + e.getMessage(), e);
    }
  }


  @VisibleForTesting
  public KinesisClient getClient() {
    return _kinesisClient;
  }

  @Override
  public void close() {
    _kinesisClient.close();
  }
}
