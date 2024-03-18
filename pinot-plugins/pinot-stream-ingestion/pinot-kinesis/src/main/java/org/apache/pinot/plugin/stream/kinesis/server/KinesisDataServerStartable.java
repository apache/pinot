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

import cloud.localstack.Localstack;
import cloud.localstack.ServiceName;
import cloud.localstack.docker.annotation.LocalstackDockerConfiguration;
import com.google.common.base.Function;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.pinot.plugin.stream.kinesis.AwsSdkUtil;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.spi.utils.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.utils.AttributeMap;


public class KinesisDataServerStartable implements StreamDataServerStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisDataServerStartable.class);

  public static final String NUM_SHARDS_PROPERTY = "numShards";
  public static final String DEFAULT_REGION = "us-east-1";
  public static final String DEFAULT_ACCESS_KEY = "access";
  public static final String DEFAULT_SECRET_KEY = "secret";
  public static final String DEFAULT_PORT = "4566";

  private final Localstack _localstackDocker = Localstack.INSTANCE;
  LocalstackDockerConfiguration _dockerConfig;
  Properties _serverProperties;
  private String _localStackKinesisEndpoint = "http://localhost:%s";

  @Override
  public void init(Properties props) {
    _serverProperties = props;
    final Map<String, String> environmentVariables = new HashMap<>();
    environmentVariables.put("SERVICES", ServiceName.KINESIS);
    _dockerConfig =
        LocalstackDockerConfiguration.builder().portEdge(_serverProperties.getProperty("port", DEFAULT_PORT))
            .portElasticSearch(String.valueOf(NetUtils.findOpenPort(4571))).imageTag("0.12.15")
            .environmentVariables(environmentVariables).build();

    _localStackKinesisEndpoint =
        String.format(_localStackKinesisEndpoint, _serverProperties.getProperty("port", DEFAULT_PORT));
  }

  @Override
  public void start() {
    _localstackDocker.startup(_dockerConfig);
  }

  @Override
  public void stop() {
    _localstackDocker.stop();
  }

  @Override
  public void createTopic(String topic, Properties topicProps) {
    try {
      KinesisClient kinesisClient = KinesisClient.builder().httpClient(
              AwsSdkUtil.createSdkHttpService().createHttpClientBuilder().buildWithDefaults(
                  AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
          .credentialsProvider(getLocalAWSCredentials()).region(Region.of(DEFAULT_REGION))
          .endpointOverride(new URI(_localStackKinesisEndpoint)).build();

      kinesisClient.createStream(
          CreateStreamRequest.builder().streamName(topic).shardCount((Integer) topicProps.get(NUM_SHARDS_PROPERTY))
              .build());

      waitForCondition(new Function<Void, Boolean>() {
        @Nullable
        @Override
        public Boolean apply(@Nullable Void aVoid) {
          try {
            String kinesisStreamStatus =
                kinesisClient.describeStream(DescribeStreamRequest.builder().streamName(topic).build())
                    .streamDescription().streamStatusAsString();

            return kinesisStreamStatus.contentEquals("ACTIVE");
          } catch (Exception e) {
            LOGGER.warn("Could not fetch kinesis stream status", e);
            return null;
          }
        }
      }, 1000L, 30000, "Kinesis stream " + topic + " is not created or is not in active state");

      LOGGER.info("Kinesis stream created successfully: " + topic);
    } catch (Exception e) {
      LOGGER.warn("Error occurred while creating topic: " + topic, e);
    }
  }

  @Override
  public int getPort() {
    return _localstackDocker.getEdgePort();
  }

  private AwsCredentialsProvider getLocalAWSCredentials() {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY));
  }

  private static void waitForCondition(Function<Void, Boolean> condition, long checkIntervalMs, long timeoutMs,
      @Nullable String errorMessage) {
    long endTime = System.currentTimeMillis() + timeoutMs;
    String errorMessageSuffix = errorMessage != null ? ", error message: " + errorMessage : "";
    while (System.currentTimeMillis() < endTime) {
      try {
        if (Boolean.TRUE.equals(condition.apply(null))) {
          return;
        }
        Thread.sleep(checkIntervalMs);
      } catch (Exception e) {
        LOGGER.error("Caught exception while checking the condition" + errorMessageSuffix, e);
      }
    }
    LOGGER.error("Failed to meet condition in " + timeoutMs + "ms" + errorMessageSuffix);
  }
}
