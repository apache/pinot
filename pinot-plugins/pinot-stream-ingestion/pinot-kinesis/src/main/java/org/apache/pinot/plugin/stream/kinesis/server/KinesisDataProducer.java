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
package org.apache.pinot.plugin.stream.kinesis.server;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.UUID;
import org.apache.pinot.spi.stream.StreamDataProducer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheSdkHttpService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

public class KinesisDataProducer implements StreamDataProducer {
  public static final String ENDPOINT = "endpoint";
  public static final String REGION = "region";
  public static final String ACCESS = "access";
  public static final String SECRET = "secret";
  public static final String DEFAULT_PORT = "4566";
  public static final String DEFAULT_ENDPOINT = "http://localhost:4566";

  private KinesisClient _kinesisClient;

  @Override
  public void init(Properties props) {
    try {
      KinesisClientBuilder kinesisClientBuilder;
      if (props.containsKey(ACCESS) && props.containsKey(SECRET)) {
        kinesisClientBuilder = KinesisClient.builder().region(Region.of(props.getProperty(REGION)))
            .credentialsProvider(getLocalAWSCredentials(props))
            .httpClientBuilder(new ApacheSdkHttpService().createHttpClientBuilder());
      } else {
        kinesisClientBuilder =
            KinesisClient.builder().region(Region.of(props.getProperty(REGION)))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .httpClientBuilder(new ApacheSdkHttpService().createHttpClientBuilder());
      }

      if (props.containsKey(ENDPOINT)) {
        String kinesisEndpoint = props.getProperty(ENDPOINT, DEFAULT_ENDPOINT);
        try {
          kinesisClientBuilder = kinesisClientBuilder.endpointOverride(new URI(kinesisEndpoint));
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException("URI syntax is not correctly specified for endpoint: "
              + kinesisEndpoint, e);
        }
      }

      _kinesisClient = kinesisClientBuilder.build();
    } catch (Exception e) {
      _kinesisClient = null;
    }
  }

  @Override
  public void produce(String topic, byte[] payload) {
    PutRecordRequest putRecordRequest =
        PutRecordRequest.builder().streamName(topic).data(SdkBytes.fromByteArray(payload))
            .partitionKey(UUID.randomUUID().toString()).build();
    PutRecordResponse putRecordResponse = _kinesisClient.putRecord(putRecordRequest);
  }

  @Override
  public void produce(String topic, byte[] key, byte[] payload) {
    PutRecordRequest putRecordRequest =
        PutRecordRequest.builder().streamName(topic).data(SdkBytes.fromByteArray(payload)).partitionKey(new String(key))
            .build();
    PutRecordResponse putRecordResponse = _kinesisClient.putRecord(putRecordRequest);
  }

  @Override
  public void close() {
    _kinesisClient.close();
  }

  private AwsCredentialsProvider getLocalAWSCredentials(Properties props) {
    return StaticCredentialsProvider.create(
        AwsBasicCredentials.create(props.getProperty(ACCESS), props.getProperty(SECRET)));
  }
}
