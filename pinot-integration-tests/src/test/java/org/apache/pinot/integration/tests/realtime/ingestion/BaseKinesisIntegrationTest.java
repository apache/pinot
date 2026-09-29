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
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.realtime.ingestion.utils.KinesisUtils;
import org.apache.pinot.plugin.stream.kinesis.KinesisConfig;
import org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
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


/// Creates all dependencies (docker image, kinesis server, kinesis client, configs) for all tests requiring kinesis
@LocalstackDockerProperties(services = {ServiceName.KINESIS}, imageTag = BaseKinesisIntegrationTest.LOCALSTACK_IMAGE)
abstract class BaseKinesisIntegrationTest extends BaseClusterIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseKinesisIntegrationTest.class);

  static final String LOCALSTACK_IMAGE = "2.3.2";
  private static final LocalstackDockerAnnotationProcessor PROCESSOR = new LocalstackDockerAnnotationProcessor();
  private static BaseKinesisIntegrationTest _sharedClusterTestSuite;

  private final Localstack _localstackDocker = Localstack.INSTANCE;
  private final Set<String> _trackedRealtimeTables = new LinkedHashSet<>();
  private final Set<String> _trackedSchemas = new LinkedHashSet<>();

  protected KinesisClient _kinesisClient;
  private boolean _localstackStarted;
  private boolean _streamCreated;
  private boolean _zkStarted;

  private static final String REGION = "us-east-1";
  private static final String LOCALSTACK_KINESIS_ENDPOINT = "http://localhost:4566";
  protected static final String STREAM_TYPE = "kinesis";

  @BeforeSuite(alwaysRun = true)
  public void setUpSuite()
      throws Exception {
    if (_sharedClusterTestSuite != null) {
      return;
    }

    try {
      DockerInfoCommand dockerInfoCommand = new DockerInfoCommand();
      dockerInfoCommand.execute();
    } catch (IllegalStateException e) {
      LOGGER.warn("Skipping kinesis tests! Docker is not found running", e);
      throw new SkipException(e.getMessage());
    }

    _sharedClusterTestSuite = this;

    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    _zkStarted = true;
    startController();
    startBroker();
    startServer();

    startKinesis();
  }

  @AfterSuite(alwaysRun = true)
  public void tearDownSuite()
      throws Exception {
    BaseKinesisIntegrationTest sharedClusterTestSuite = _sharedClusterTestSuite;
    if (sharedClusterTestSuite == null) {
      return;
    }
    _sharedClusterTestSuite = null;
    sharedClusterTestSuite.tearDownSuiteResources();
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    BaseKinesisIntegrationTest sharedClusterTestSuite = _sharedClusterTestSuite;
    if (sharedClusterTestSuite == null) {
      throw new IllegalStateException("Kinesis integration test suite has not been initialized");
    }
    if (sharedClusterTestSuite != this) {
      bindToSharedCluster(sharedClusterTestSuite);
    }
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Throwable cleanupFailure = null;
    try {
      cleanupKinesisResources();
    } catch (Throwable t) {
      cleanupFailure = t;
    }
    try {
      FileUtils.deleteDirectory(_tempDir);
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    if (cleanupFailure != null) {
      rethrowCleanupFailure(cleanupFailure);
    }
  }

  protected void createStream(int numShards) {
    String streamName = getKinesisStreamName();
    LOGGER.warn("Stream " + streamName + " being created");
    _streamCreated = true;
    _kinesisClient.createStream(CreateStreamRequest.builder().streamName(streamName).shardCount(numShards).build());

    TestUtils.waitForCondition(aVoid -> KinesisUtils.isKinesisStreamActive(_kinesisClient, streamName), 2000L, 60_000L,
        "Kinesis stream " + streamName + " is not created or is not in active state");
  }

  protected void deleteStream() {
    if (!_streamCreated) {
      return;
    }
    String streamName = getKinesisStreamName();
    try {
      _kinesisClient.deleteStream(DeleteStreamRequest.builder().streamName(streamName).build());
    } catch (ResourceNotFoundException ignored) {
      _streamCreated = false;
      return;
    }
    TestUtils.waitForCondition(aVoid -> {
      try {
        KinesisUtils.getKinesisStreamStatus(_kinesisClient, streamName);
        return false;
      } catch (ResourceNotFoundException e) {
        return true;
      }
    }, 2000L, 60_000L, "Kinesis stream " + streamName + " is not deleted");

    _streamCreated = false;
    LOGGER.warn("Stream " + streamName + " deleted");
  }

  protected PutRecordResponse putRecord(String data, String partitionKey) {
    PutRecordRequest putRecordRequest =
        PutRecordRequest.builder().streamName(getKinesisStreamName()).data(SdkBytes.fromUtf8String(data))
            .partitionKey(partitionKey).build();
    return _kinesisClient.putRecord(putRecordRequest);
  }

  protected void addTrackedSchema(Schema schema)
      throws IOException {
    _trackedSchemas.add(schema.getSchemaName());
    addSchema(schema);
  }

  protected void addTrackedTable(TableConfig tableConfig)
      throws IOException {
    _trackedRealtimeTables.add(TableNameBuilder.extractRawTableName(tableConfig.getTableName()));
    addTableConfig(tableConfig);
  }

  protected void cleanupKinesisResources()
      throws Exception {
    Throwable cleanupFailure = null;

    List<String> tableNames = new ArrayList<>(_trackedRealtimeTables);
    for (int i = tableNames.size() - 1; i >= 0; i--) {
      String tableName = tableNames.get(i);
      try {
        cleanupRealtimeTable(tableName);
        _trackedRealtimeTables.remove(tableName);
      } catch (Throwable t) {
        cleanupFailure = addCleanupFailure(cleanupFailure, t);
      }
    }

    List<String> schemaNames = new ArrayList<>(_trackedSchemas);
    for (int i = schemaNames.size() - 1; i >= 0; i--) {
      String schemaName = schemaNames.get(i);
      if (_trackedRealtimeTables.contains(schemaName)) {
        continue;
      }
      try {
        if (_helixResourceManager != null && _helixResourceManager.getSchema(schemaName) != null) {
          deleteSchema(schemaName);
        }
        _trackedSchemas.remove(schemaName);
      } catch (Throwable t) {
        cleanupFailure = addCleanupFailure(cleanupFailure, t);
      }
    }

    if (_trackedRealtimeTables.isEmpty()) {
      try {
        deleteStream();
      } catch (Throwable t) {
        cleanupFailure = addCleanupFailure(cleanupFailure, t);
      }
    }

    if (cleanupFailure != null) {
      rethrowCleanupFailure(cleanupFailure);
    }
  }

  @Override
  public Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigMap = new HashMap<>();
    String streamType = STREAM_TYPE;
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);

    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_TOPIC_NAME), getKinesisStreamName());
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
  protected void overrideControllerConf(Map<String, Object> properties) {
    // Shard-change tests invoke validation explicitly and assert the state before and after each invocation.
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_PERIOD, "2h");
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        3600);
  }

  @Override
  public TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    // Calls the super class to create the table config.
    // Properties like stream configs are overridden in the getStreamConfigs() method.
    return super.createRealtimeTableConfig(sampleAvroFile);
  }

  @Override
  public String getHelixClusterName() {
    return "KinesisIngestionIntegrationTest";
  }

  @Override
  public String getZkUrl() {
    if (_sharedClusterTestSuite != null && _sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getZkUrl();
    }
    return super.getZkUrl();
  }

  @Override
  protected String getBrokerBaseApiUrl() {
    if (_sharedClusterTestSuite != null && _sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getBrokerBaseApiUrl();
    }
    return super.getBrokerBaseApiUrl();
  }

  @Override
  protected String getBrokerGrpcEndpoint() {
    if (_sharedClusterTestSuite != null && _sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getBrokerGrpcEndpoint();
    }
    return super.getBrokerGrpcEndpoint();
  }

  @Override
  public int getControllerPort() {
    if (_sharedClusterTestSuite != null && _sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getControllerPort();
    }
    return super.getControllerPort();
  }

  @Override
  public PinotAdminClient getOrCreateAdminClient()
      throws IOException {
    if (_sharedClusterTestSuite != null && _sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getOrCreateAdminClient();
    }
    return super.getOrCreateAdminClient();
  }

  @Override
  protected int getRandomBrokerPort() {
    if (_sharedClusterTestSuite != null && _sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getRandomBrokerPort();
    }
    return super.getRandomBrokerPort();
  }

  protected abstract String getKinesisStreamName();

  private void stopKinesis()
      throws Exception {
    Throwable cleanupFailure = null;
    try {
      if (_kinesisClient != null) {
        _kinesisClient.close();
      }
    } catch (Throwable t) {
      cleanupFailure = t;
    } finally {
      _kinesisClient = null;
    }
    try {
      if (_localstackStarted && _localstackDocker.isRunning()) {
        _localstackDocker.stop();
      }
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    } finally {
      _localstackStarted = false;
    }
    if (cleanupFailure != null) {
      rethrowCleanupFailure(cleanupFailure);
    }
  }

  private void startKinesis()
      throws Exception {
    LocalstackDockerConfiguration dockerConfig = PROCESSOR.process(this.getClass());
    try {
      _localstackDocker.startup(dockerConfig);
      _localstackStarted = true;
    } catch (Throwable startupFailure) {
      try {
        if (_localstackDocker.isRunning()) {
          _localstackDocker.stop();
        }
      } catch (Throwable cleanupFailure) {
        startupFailure.addSuppressed(cleanupFailure);
      }
      rethrowCleanupFailure(startupFailure);
    }

    _kinesisClient = KinesisClient.builder().httpClient(new ApacheSdkHttpService().createHttpClientBuilder()
            .buildWithDefaults(
                AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
        .credentialsProvider(getLocalAWSCredentials()).region(Region.of(REGION))
        .endpointOverride(new URI(LOCALSTACK_KINESIS_ENDPOINT)).build();
  }

  private void bindToSharedCluster(BaseKinesisIntegrationTest sharedClusterTestSuite) {
    _kinesisClient = sharedClusterTestSuite._kinesisClient;
    _controllerStarter = sharedClusterTestSuite._controllerStarter;
    _controllerPort = sharedClusterTestSuite._controllerPort;
    _controllerConfig = sharedClusterTestSuite._controllerConfig;
    _controllerBaseApiUrl = sharedClusterTestSuite._controllerBaseApiUrl;
    _controllerRequestURLBuilder = sharedClusterTestSuite._controllerRequestURLBuilder;
    _controllerDataDir = sharedClusterTestSuite._controllerDataDir;
    _helixResourceManager = sharedClusterTestSuite._helixResourceManager;
    _helixManager = sharedClusterTestSuite._helixManager;
    _helixDataAccessor = sharedClusterTestSuite._helixDataAccessor;
    _helixAdmin = sharedClusterTestSuite._helixAdmin;
    _propertyStore = sharedClusterTestSuite._propertyStore;
    _serverStarters.addAll(sharedClusterTestSuite._serverStarters);
  }

  private void cleanupRealtimeTable(String tableName)
      throws Exception {
    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
    if (_helixResourceManager != null && _helixResourceManager.getTableConfig(tableNameWithType) != null) {
      dropRealtimeTable(tableName);
    }
    Throwable cleanupFailure = null;
    if (_helixResourceManager != null) {
      try {
        waitForEVToDisappear(tableNameWithType);
      } catch (Throwable t) {
        cleanupFailure = t;
      }
      try {
        waitForTableDataManagerRemoved(tableNameWithType);
      } catch (Throwable t) {
        cleanupFailure = addCleanupFailure(cleanupFailure, t);
      }
    }
    if (cleanupFailure != null) {
      rethrowCleanupFailure(cleanupFailure);
    }
  }

  private void tearDownSuiteResources()
      throws Exception {
    Throwable cleanupFailure = null;
    try {
      if (!_serverStarters.isEmpty()) {
        stopServer();
      }
    } catch (Throwable t) {
      cleanupFailure = t;
    }
    try {
      if (!_brokerStarters.isEmpty()) {
        stopBroker();
      }
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    try {
      if (_controllerStarter != null) {
        if (_controllerDataDir != null) {
          stopController();
        } else {
          _controllerStarter.stop();
          _controllerStarter = null;
          _controllerPort = 0;
          _controllerRequestURLBuilder = null;
        }
      }
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    try {
      if (_zkStarted) {
        stopZk();
        _zkStarted = false;
      }
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    try {
      stopKinesis();
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    try {
      FileUtils.deleteDirectory(_tempDir);
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    if (cleanupFailure != null) {
      rethrowCleanupFailure(cleanupFailure);
    }
  }

  private static Throwable addCleanupFailure(Throwable currentFailure, Throwable newFailure) {
    if (currentFailure == null) {
      return newFailure;
    }
    currentFailure.addSuppressed(newFailure);
    return currentFailure;
  }

  private static void rethrowCleanupFailure(Throwable cleanupFailure)
      throws Exception {
    if (cleanupFailure instanceof Error) {
      throw (Error) cleanupFailure;
    }
    if (cleanupFailure instanceof Exception) {
      throw (Exception) cleanupFailure;
    }
    throw new RuntimeException(cleanupFailure);
  }

  private static class DockerInfoCommand extends Command {

    public void execute() {
      String dockerInfo = dockerExe.execute(List.of("info"));

      if (dockerInfo.toLowerCase().contains("error")) {
        throw new IllegalStateException("Docker daemon is not running!");
      }
    }
  }

  private static AwsCredentialsProvider getLocalAWSCredentials() {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"));
  }
}
