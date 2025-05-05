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
package org.apache.pinot.integration.tests.realtime.ingestion;

import cloud.localstack.Localstack;
import cloud.localstack.ServiceName;
import cloud.localstack.docker.annotation.LocalstackDockerAnnotationProcessor;
import cloud.localstack.docker.annotation.LocalstackDockerConfiguration;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import cloud.localstack.docker.command.Command;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.realtime.ingestion.utils.KinesisUtils;
import org.apache.pinot.plugin.stream.kinesis.KinesisConfig;
import org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheSdkHttpService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.utils.AttributeMap;


/**
 * Creates all dependencies (docker image, kinesis server, kinesis client, configs) for all tests requiring kinesis
 */
@LocalstackDockerProperties(services = {ServiceName.KINESIS}, imageTag = BaseKinesisIntegrationTest.LOCALSTACK_IMAGE)
abstract class BaseKinesisIntegrationTest extends BaseClusterIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseKinesisIntegrationTest.class);

  static final String LOCALSTACK_IMAGE = "2.3.2";
  private static final LocalstackDockerAnnotationProcessor PROCESSOR = new LocalstackDockerAnnotationProcessor();
  private final Localstack _localstackDocker = Localstack.INSTANCE;
  protected KinesisClient _kinesisClient;

  private static final String REGION = "us-east-1";
  private static final String LOCALSTACK_KINESIS_ENDPOINT = "http://localhost:4566";
  protected static final String STREAM_TYPE = "kinesis";
  protected static final String STREAM_NAME = "kinesis-test";

  @BeforeClass
  public void setUp()
      throws Exception {
    try {
      DockerInfoCommand dockerInfoCommand = new DockerInfoCommand();
      dockerInfoCommand.execute();
    } catch (IllegalStateException e) {
      LOGGER.warn("Skipping kinesis tests! Docker is not found running", e);
      throw new SkipException(e.getMessage());
    }

    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    startKinesis();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    stopKinesis();
    FileUtils.deleteDirectory(_tempDir);
  }

  protected void createStream(int numShards) {
    LOGGER.warn("Stream " + STREAM_NAME + " being created");
    _kinesisClient.createStream(CreateStreamRequest.builder().streamName(STREAM_NAME).shardCount(numShards).build());

    TestUtils.waitForCondition(aVoid ->
            KinesisUtils.isKinesisStreamActive(_kinesisClient, STREAM_NAME), 2000L, 60000,
        "Kinesis stream " + STREAM_NAME + " is not created or is not in active state", true);
  }

  protected void deleteStream() {
    try {
      _kinesisClient.deleteStream(DeleteStreamRequest.builder().streamName(STREAM_NAME).build());
    } catch (ResourceNotFoundException ignored) {
      return;
    }
    TestUtils.waitForCondition(aVoid -> {
          try {
            KinesisUtils.getKinesisStreamStatus(_kinesisClient, STREAM_NAME);
          } catch (ResourceNotFoundException e) {
            return true;
          }
          return false;
        }, 2000L, 60000,
        "Kinesis stream " + STREAM_NAME + " is not deleted", true);

    LOGGER.warn("Stream " + STREAM_NAME + " deleted");
  }

  protected PutRecordResponse putRecord(String data, String partitionKey) {
    PutRecordRequest putRecordRequest =
        PutRecordRequest.builder().streamName(STREAM_NAME).data(SdkBytes.fromUtf8String(data))
            .partitionKey(partitionKey).build();
    return _kinesisClient.putRecord(putRecordRequest);
  }

  @Override
  public Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigMap = new HashMap<>();
    String streamType = STREAM_TYPE;
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);

    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_TOPIC_NAME), STREAM_NAME);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS), "30000");
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), KinesisConsumerFactory.class.getName());
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_DECODER_CLASS),
        "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder");
    streamConfigMap.put(KinesisConfig.REGION, REGION);
    streamConfigMap.put(KinesisConfig.SHARD_ITERATOR_TYPE, ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString());
    streamConfigMap.put(KinesisConfig.ENDPOINT, LOCALSTACK_KINESIS_ENDPOINT);
    streamConfigMap.put(KinesisConfig.ACCESS_KEY, getLocalAWSCredentials().resolveCredentials().accessKeyId());
    streamConfigMap.put(KinesisConfig.SECRET_KEY, getLocalAWSCredentials().resolveCredentials().secretAccessKey());
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, Integer.toString(2000));
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");
    return streamConfigMap;
  }

  @Override
  public TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    // Calls the super class to create the table config.
    // Properties like stream configs are overriden in the getStreamConfigs() method.
    return super.createRealtimeTableConfig(sampleAvroFile);
  }

  private void stopKinesis() {
    if (_kinesisClient != null) {
      _kinesisClient.close();
    }
    if (_localstackDocker.isRunning()) {
      _localstackDocker.stop();
    }
  }

  private void startKinesis()
      throws Exception {
    LocalstackDockerConfiguration dockerConfig = PROCESSOR.process(this.getClass());
    StopAllLocalstackDockerCommand stopAllLocalstackDockerCommand = new StopAllLocalstackDockerCommand();
    stopAllLocalstackDockerCommand.execute();
    _localstackDocker.startup(dockerConfig);

    _kinesisClient = KinesisClient.builder().httpClient(new ApacheSdkHttpService().createHttpClientBuilder()
            .buildWithDefaults(
                AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
        .credentialsProvider(getLocalAWSCredentials()).region(Region.of(REGION))
        .endpointOverride(new URI(LOCALSTACK_KINESIS_ENDPOINT)).build();
  }

  private static class StopAllLocalstackDockerCommand extends Command {

    public void execute() {
      String runningDockerContainers =
          dockerExe.execute(
              Arrays.asList("ps", "-a", "-q", "-f", "ancestor=localstack/localstack:" + LOCALSTACK_IMAGE));
      if (StringUtils.isNotBlank(runningDockerContainers) && !runningDockerContainers.toLowerCase().contains("error")) {
        String[] containerList = runningDockerContainers.split("\n");

        for (String containerId : containerList) {
          dockerExe.execute(Arrays.asList("stop", containerId));
        }
      }
    }
  }

  private static class DockerInfoCommand extends Command {

    public void execute() {
      String dockerInfo = dockerExe.execute(Collections.singletonList("info"));

      if (dockerInfo.toLowerCase().contains("error")) {
        throw new IllegalStateException("Docker daemon is not running!");
      }
    }
  }

  private static AwsCredentialsProvider getLocalAWSCredentials() {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"));
  }
}
