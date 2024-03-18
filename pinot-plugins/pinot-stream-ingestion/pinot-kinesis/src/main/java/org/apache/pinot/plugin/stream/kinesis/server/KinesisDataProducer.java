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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.pinot.plugin.stream.kinesis.AwsSdkUtil;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.FixedDelayRetryPolicy;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;


public class KinesisDataProducer implements StreamDataProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisDataProducer.class);

  public static final String ENDPOINT = "endpoint";
  public static final String REGION = "region";
  public static final String ACCESS = "access";
  public static final String SECRET = "secret";
  public static final String NUM_RETRIES = "num_retries";
  public static final String RETRY_DELAY_MILLIS = "retry_delay_millis";

  public static final String DEFAULT_ENDPOINT = "http://localhost:4566";
  public static final String DEFAULT_RETRY_DELAY_MILLIS = "10000";
  public static final String DEFAULT_NUM_RETRIES = "0";

  private KinesisClient _kinesisClient;
  private RetryPolicy _retryPolicy;

  public KinesisDataProducer() { }

  public KinesisDataProducer(KinesisClient kinesisClient) {
    this(kinesisClient, new FixedDelayRetryPolicy(
        Integer.parseInt(DEFAULT_NUM_RETRIES + 1), Integer.parseInt(DEFAULT_RETRY_DELAY_MILLIS)));
  }

  public KinesisDataProducer(KinesisClient kinesisClient, RetryPolicy retryPolicy) {
    _kinesisClient = kinesisClient;
    _retryPolicy = retryPolicy;
  }

  @Override
  public void init(Properties props) {
    if (_kinesisClient == null) {
      try {
        KinesisClientBuilder kinesisClientBuilder;
        if (props.containsKey(ACCESS) && props.containsKey(SECRET)) {
          kinesisClientBuilder = KinesisClient.builder().region(Region.of(props.getProperty(REGION)))
              .credentialsProvider(getLocalAWSCredentials(props))
              .httpClientBuilder(AwsSdkUtil.createSdkHttpService().createHttpClientBuilder());
        } else {
          kinesisClientBuilder = KinesisClient.builder().region(Region.of(props.getProperty(REGION)))
              .credentialsProvider(DefaultCredentialsProvider.builder().build())
              .httpClientBuilder(AwsSdkUtil.createSdkHttpService().createHttpClientBuilder());
        }

        if (props.containsKey(ENDPOINT)) {
          String kinesisEndpoint = props.getProperty(ENDPOINT, DEFAULT_ENDPOINT);
          try {
            kinesisClientBuilder = kinesisClientBuilder.endpointOverride(new URI(kinesisEndpoint));
          } catch (URISyntaxException e) {
            throw new IllegalArgumentException("URI syntax is not correctly specified for endpoint: " + kinesisEndpoint,
                e);
          }
        }

        _kinesisClient = kinesisClientBuilder.build();

        int numRetries = Integer.parseInt(props.getProperty(NUM_RETRIES, DEFAULT_NUM_RETRIES));
        long retryDelayMs = Long.parseLong(props.getProperty(RETRY_DELAY_MILLIS, DEFAULT_RETRY_DELAY_MILLIS));
        _retryPolicy = new FixedDelayRetryPolicy(numRetries + 1, retryDelayMs);
      } catch (Exception e) {
        LOGGER.warn("Failed to create a kinesis client due to ", e);
        _kinesisClient = null;
      }
    }
  }

  @Override
  public void produce(String topic, byte[] payload) {
    try {
      _retryPolicy.attempt(() -> putRecord(topic, null, payload));
    } catch (AttemptsExceededException ae) {
      LOGGER.error("Retries exhausted while pushing record in stream {}", topic);
    } catch (RetriableOperationException roe) {
      LOGGER.error("Error occurred while pushing records in stream {}", topic, roe);
    }
  }

  @Override
  public void produce(String topic, byte[] key, byte[] payload) {
    try {
      _retryPolicy.attempt(() -> putRecord(topic, key, payload));
    } catch (AttemptsExceededException ae) {
      LOGGER.error("Retries exhausted while pushing record in stream {}", topic);
    } catch (RetriableOperationException roe) {
      LOGGER.error("Error occurred while pushing records in stream {}", topic, roe);
    }
  }

  @Override
  public void produceBatch(String topic, List<byte[]> rows) {
    try {
      _retryPolicy.attempt(() -> putRecordBatch(topic, rows));
    } catch (AttemptsExceededException ae) {
      LOGGER.error("Retries exhausted while pushing record in stream {}", topic);
    } catch (RetriableOperationException roe) {
      LOGGER.error("Error occurred while pushing records in stream {}", topic, roe);
    }
  }

  @Override
  public void close() {
    _kinesisClient.close();
  }

  private AwsCredentialsProvider getLocalAWSCredentials(Properties props) {
    return StaticCredentialsProvider.create(
        AwsBasicCredentials.create(props.getProperty(ACCESS), props.getProperty(SECRET)));
  }

  private boolean putRecordBatch(String topic, List<byte[]> rows) {
    try {
      List<PutRecordsRequestEntry> putRecordsRequestEntries = new ArrayList<>();
      for (byte[] row : rows) {
        putRecordsRequestEntries.add(PutRecordsRequestEntry.builder().data(SdkBytes.fromByteArray(row))
            .partitionKey(UUID.randomUUID().toString()).build());
      }
      PutRecordsRequest putRecordsRequest =
          PutRecordsRequest.builder().streamName(topic).records(putRecordsRequestEntries).build();

      PutRecordsResponse putRecordsResponse = _kinesisClient.putRecords(putRecordsRequest);
      return putRecordsResponse.sdkHttpResponse().isSuccessful();
    } catch (Exception e) {
      LOGGER.warn("Exception occurred while pushing record to Kinesis {}", e.getMessage());
      return false;
    }
  }

  private boolean putRecord(String topic, byte[] key, byte[] payload) {
    try {
      PutRecordRequest.Builder putRecordRequestBuilder =
          PutRecordRequest.builder().streamName(topic).data(SdkBytes.fromByteArray(payload));

      if (key != null) {
        putRecordRequestBuilder = putRecordRequestBuilder.partitionKey(new String(key));
      } else {
        putRecordRequestBuilder = putRecordRequestBuilder.partitionKey(UUID.randomUUID().toString());
      }
      PutRecordResponse putRecordResponse = _kinesisClient.putRecord(putRecordRequestBuilder.build());
      return putRecordResponse.sdkHttpResponse().isSuccessful();
    } catch (Exception e) {
      LOGGER.warn("Exception occurred while pushing record to Kinesis {}", e.getMessage());
      return false;
    }
  }
}
